// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafthttp

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"github.com/xiang90/probing"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Raft interface {
	// 将指定的消息实例传递到底层的 etcd-raft 模块进行处理
	Process(ctx context.Context, m raftpb.Message) error
	// 检测指定节点是否从当前集群中移出
	IsIDRemoved(id uint64) bool
	// 通知底层 etcd-raft 模块，当前节点与指定节点无法联通
	ReportUnreachable(id uint64)
	// 通知底层 etcd-raft 模块，快照数据是否发送成功
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	// 初始化操作
	Start() error
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	// 创建 Handler 实例，并关联到指定的URL上
	Handler() http.Handler
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	// 发送消息
	Send(m []raftpb.Message)
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	// 发送快照
	SendSnapshot(m snap.Message)
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// 在集群中添加一个节点时，其它节点会通过该方法添加新加入节点的信息
	AddRemote(id types.ID, urls []string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	// Peer 接口是当前节点对集群中其他节点的抽象表示，而结构体 peer,则是 Peer 接口的一个具体实现
	AddPeer(id types.ID, urls []string)
	// RemovePeer removes the peer with given id.
	RemovePeer(id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	UpdatePeer(id types.ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id types.ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	// 关闭操作，会关闭全部的网络连接
	Stop()
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
//
// 注意区分 net.http.Transport 接口，两者是完全不同的东西。
type Transport struct {
	Logger *zap.Logger

	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	// 当前节点自己的ID
	ID types.ID // local member ID
	// 当前节点与集群中其他节点交互时使用的URL地址
	URLs types.URLs // local peer URLs
	// 当前节点所在的集群的ID
	ClusterID types.ID // raft cluster ID for request validation

	// Raft是一个接口，其实现的底层封装了前面介绍的etcd-raft模块，当rafthttp.Transport收到消息之后，会将其交给Raft实例进行处理。
	Raft Raft // raft state machine, to which the Transport forwards received messages and reports status

	// Snapshotter负责管理快照文件
	Snapshotter *snap.Snapshotter
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// used to record transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	// Stream消息通道
	// 维护HTTP长链接，主要负责数据传输数据量较小，发送比较频繁的消息
	// 如：MsgApp消息、MsgHeartbeat消息、MsgVote消息等
	// Stream消息通道是节点启动后，主动与集群中的其他节点建立的。
	// 每个Stream消息通道有2个关联的 goroutine，其中一个用于建立关联的 HTTP 连接，
	// 并从连接上读取数据，然后将这些读取到的数据反序列化成 Message实例，传递到 etcd-raft 模块中进行处理。
	// 另一个 goroutine 会读取 etcd-raft 模块返回的消息数据并将其序列化，最后写入 Stream消息通道。
	streamRt http.RoundTripper // roundTripper used by streams

	// Pipeline消息通道
	// 传输数据完成后会立即关闭连接，主要负责数据传输量较大、发送频率较低的消息
	// 如：MsgSnap消息
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu sync.RWMutex // protect the remote and peer map

	// remote 中只封装了 pipeline 实例，remote 主要负责发送快照数据，帮助新加入的节点快速追赶上其他节点的数据。
	remotes map[types.ID]*remote // remotes map that helps newly joined member to catch up

	// Peer接口是当前节点对集群中其他节点的抽象表示。
	// 对于当前节点来说，集群中其他节点在本地都会有一个 Peer 实例与之对应，peers 字段维护了节点ID到对应Peer实例之间的映射关系。
	peers map[types.ID]Peer // peers map

	// 用于探测Pipeline消息通道是否可用
	pipelineProber probing.Prober
	streamProber   probing.Prober
}

// Start : Transport 实现了 Transporter 接口，它提供了将 raft 消息发送到 peer 并从 peer 接收 raft 消息的功能。
// 我们需要调用 Handler 方法来获取处理程序，以处理从 peerURLs 接收到的请求。用户需要先调用 Start 才能调用 其他功能，
// 并在停止使用 Transport 时调用 Stop。 rafthttp 的启动过程中首先要构建 Transport，并将 m.PeerURLs 分别赋值到
// Transport 中的 Remote 和 Peer 中，之后将 srv.r.transport 指向构建好的 Transport 即可。
func (t *Transport) Start() error {
	var err error

	// 创建 Stream消息通道 使用的 http.RoundTripper 实例，底层实际上是创建 http.Transport 实例
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}

	// 创建 Pipeline消息通道
	// 与streamRt不同的是，读写请求的超时时间设置成了永不过期
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	t.pipelineProber = probing.NewProber(t.pipelineRt)
	t.streamProber = probing.NewProber(t.streamRt)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}

// Handler 方法主要负责创建 Steam 消息通道和 Pipeline 消息通道用到的 Handler实例，并注册到相应的请求路径上
func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.ID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID)

	// ServeMux 是一个多路复用器，
	// 主要通过 map[string]muxEntry 存储 URL 和 Handler 实例的映射关系
	mux := http.NewServeMux()

	// "/raft"
	mux.Handle(RaftPrefix, pipelineHandler)
	// "/raft/stream/"
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	// "/raft/snapshot"
	mux.Handle(RaftSnapshotPrefix, snapHandler)
	// "/raft/probing"
	mux.Handle(ProbingPrefix, probing.NewHandler())
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

// Send() 方法负责发送指定的 raftpb.Message 消息，
// 其中首先尝试使用目标节点对应的 Peer 实例发送消息，
// 如果没有找到对应的 Peer 实例，则尝试使用对应的 remote 实例发送消息。
func (t *Transport) Send(msgs []raftpb.Message) {
	// 遍历全部消息
	for _, m := range msgs {
		// 根据 raftpb.Message.To 字段，获取目标节点对应的Peer实例
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()

		// 如果存在对应的 Peer 实例，则使用 Peer 发送消息
		if pok {
			// 统计信息
			if m.Type == raftpb.MsgApp {
				t.ServerStats.SendAppendReq(m.Size())
			}
			p.send(m)
			continue
		}

		// 如果指定节点ID不存在对应的 Peer 实例，则尝试使用查找对应 remote 实例
		if rok {
			g.send(m)
			continue
		}

		if t.Logger != nil {
			t.Logger.Debug(
				"ignored message send request; unknown remote peer target",
				zap.String("type", m.Type.String()),
				zap.String("unknown-target-peer-id", to.String()),
			)
		}
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}
	t.pipelineProber.RemoveAll()
	t.streamProber.RemoveAll()
	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

func (t *Transport) AddRemote(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	t.remotes[id] = startRemote(t, urls, id)

	if t.Logger != nil {
		t.Logger.Info(
			"added new remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

// AddPeer 的主要工作是创建并启动对应节点的 Peer 实例
func (t *Transport) AddPeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		panic("transport stopped")
	}

	// 是否已经与指定ID的节点建立了连接
	// 为啥会已经建立了？？
	if _, ok := t.peers[id]; ok {
		return
	}

	// 解析 us 切片中指定的 URL 连接
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	fs := t.LeaderStats.Follower(id.String())

	// 创建指定节点对应的 Peer 实例，其中会相关的 Stream消息通道和 Pipeline消息通道
	t.peers[id] = startPeer(t, urls, id, fs)
	// 每隔一段时间，prober 会向该节点发送探测消息，检测对端的健康状况
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"added remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
//
// 该方法会调用 peer.stop() 方法关闭底层的连接，同时会停止定时发送的探测消息
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		if t.Logger != nil {
			t.Logger.Panic("unexpected removal of unknown remote peer", zap.String("remote-peer-id", id.String()))
		}
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())
	t.pipelineProber.Remove(id.String())
	t.streamProber.Remove(id.String())

	if t.Logger != nil {
		t.Logger.Info(
			"removed remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("removed-remote-peer-id", id.String()),
		)
	}
}

// 更新对端暴露的URL地址，同时更新探测消息发送的目标地址
func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	t.peers[id].update(urls)

	t.pipelineProber.Remove(id.String())
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	t.streamProber.Remove(id.String())
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"updated remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("updated-remote-peer-id", id.String()),
			zap.Strings("updated-remote-peer-urls", us),
		)
	}
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

// SendSnapshot 方法负责发送指定的 snap.Message 消息（其中封装了对应的 MsgSnap 消息实例及其他相关信息）
func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}
	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}

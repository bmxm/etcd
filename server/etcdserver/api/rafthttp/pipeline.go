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
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"runtime"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

type pipeline struct {
	// 该pipeline对应节点的ID。
	peerID types.ID

	// 关联的 rafthttp.Transport 实例。
	tr     *Transport
	picker *urlPicker
	status *peerStatus

	//底层的Raft实例。
	raft   Raft
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	// pipeline实例从该通道中获取待发送的消息。
	msgc chan raftpb.Message
	// wait for the handling routines
	// 负责同步多个goroutine结束。
	// 每个 pipeline实例会启动多个后台 goroutine（默认值是4个）来处理 msgc 通道中的消息，
	// 在pipeline.stop()方法中必须等待这些goroutine都结束（通过wg.Wait()方法实现），才能真正关闭该pipeline实例。
	wg    sync.WaitGroup
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})

	// 注意缓存，默认是64，主要是为了防止瞬时网络延迟造成消息丢失
	p.msgc = make(chan raftpb.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	// 启动用于发送消息的goroutine（默认是4个）
	for i := 0; i < connPerPipeline; i++ {
		go p.handle()
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) handle() {
	// handle()方法执行完成，也就是当前这个goroutine结束
	defer p.wg.Done()

	for {
		select {
		// 获取待发送的 MsgSnap 类型的 Message
		case m := <-p.msgc:
			start := time.Now()
			// 将消息序列化，然后创建HTTP请求并发送出去
			err := p.post(pbutil.MustMarshal(&m))
			end := time.Now()

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}

				// 向底层的Raft状态机报告失败信息
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}

			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			// 向底层的Raft状态机报告发送成功的信息
			if isMsgSnap(m) {
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
// post 方法是真正完成消息发送的地方，其中会启动一个后台goroutine监听控制发送过程及获取发送结果
func (p *pipeline) post(data []byte) (err error) {
	// 获取对端暴露的URL地址
	u := p.picker.pick()

	// 创建HTTP POST请求
	req := createPostRequest(p.tr.Logger, u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	// 主要用于通知下面的 goroutine 请求是否已经发送完成
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	// 该 goroutine 主要用于监听请求是否需要取消
	go func() {
		select {
		case <-done:
			cancel()
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()

	// 发送上述HTTP POST请求，并获取到对应的响应
	resp, err := p.tr.pipelineRt.RoundTrip(req)
	// 通知上述 goroutine，请求已经发送完毕
	done <- struct{}{}
	// 出现异常时，则将该URL标识为不可用，再尝试其他URL地址
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	// 读取HttpResponse.Body内容
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}

	err = checkPostResponse(p.tr.Logger, resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { runtime.Gosched() }

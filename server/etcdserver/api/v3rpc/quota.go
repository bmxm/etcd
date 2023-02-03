// Copyright 2016 The etcd Authors
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

package v3rpc

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type quotaKVServer struct {
	pb.KVServer
	qa quotaAlarmer
}

type quotaAlarmer struct {
	q  etcdserver.Quota
	a  Alarmer
	id types.ID
}

// check whether request satisfies the quota. If there is not enough space,
// ignore request and raise the free space alarm.
func (qa *quotaAlarmer) check(ctx context.Context, r interface{}) error {
	if qa.q.Available(r) {
		return nil
	}
	req := &pb.AlarmRequest{
		MemberID: uint64(qa.id),
		Action:   pb.AlarmRequest_ACTIVATE,
		Alarm:    pb.AlarmType_NOSPACE,
	}
	qa.a.Alarm(ctx, req)
	return rpctypes.ErrGRPCNoSpace
}

func NewQuotaKVServer(s *etcdserver.EtcdServer) pb.KVServer {
	return &quotaKVServer{
		NewKVServer(s),
		quotaAlarmer{etcdserver.NewBackendQuota(s, "kv"), s, s.ID()},
	}
}

// Put 方法的调用过程，可以列出如下的主要方法：
// Quota 配额模块
// quotaKVServer.Put() api/v3rpc/quota.go 首先检查是否满足需求
//
//	|-quotoAlarm.check() 检查
//	|-KVServer.Put() api/v3rpc/key.go 真正的处理请求
//	|-checkPutRequest() 校验请求参数是否合法
//	|-RaftKV.Put() etcdserver/v3_server.go 处理请求
//	|=EtcdServer.Put() 实际调用的是该函数
//	  |-raftRequest()
//	  |-raftRequestOnce()
//	  |-processInternalRaftRequestOnce() 真正开始处理请求
//	  |-context.WithTimeout() 创建超时的上下文信息
//	  |-raftNode.Propose() raft/node.go
//	  |-raftNode.step() 对于类型为 MsgProp 类型消息，向 propc 通道中传入数据
//	|-header.fill() etcdserver/api/v3rpc/header.go 填充响应的头部信息
func (s *quotaKVServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	// check方法将检查请求是否满足配额。如果空间不足，将会忽略请求并发出可用空间不足的警报。
	// etcdserver: mvcc: database space exceeded
	// 另：db 默认配额是 2G (社区建议不超过 8G)
	// 另：etcd 3.2.10 之前的旧版本，备份可能会触发 boltdb 的一个 Bug, 会导致 db 大小不断上涨，最终达到配额限制。
	//
	// 如果超过了配额，它会产生一个告警（Alarm）请求，告警类型是 NO SPACE，
	// 并通过 Raft 日志同步给其它节点，告知 db 无空间了，并将告警持久化存储到 db 中。
	// 最终，无论是 API 层 gRPC 模块还是负责将 Raft 侧已提交的日志条目应用到状态机的 Apply 模块，都拒绝写入，集群只读。
	if err := s.qa.check(ctx, r); err != nil {
		return nil, err
	}
	return s.KVServer.Put(ctx, r)
}

func (s *quotaKVServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if err := s.qa.check(ctx, r); err != nil {
		return nil, err
	}
	return s.KVServer.Txn(ctx, r)
}

type quotaLeaseServer struct {
	pb.LeaseServer
	qa quotaAlarmer
}

func (s *quotaLeaseServer) LeaseGrant(ctx context.Context, cr *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	if err := s.qa.check(ctx, cr); err != nil {
		return nil, err
	}
	return s.LeaseServer.LeaseGrant(ctx, cr)
}

func NewQuotaLeaseServer(s *etcdserver.EtcdServer) pb.LeaseServer {
	return &quotaLeaseServer{
		NewLeaseServer(s),
		quotaAlarmer{etcdserver.NewBackendQuota(s, "lease"), s, s.ID()},
	}
}

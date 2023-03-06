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

// Package v3rpc implements etcd v3 RPC system based on gRPC.
package v3rpc

import (
	"context"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/pkg/v3/adt"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type kvServer struct {
	hdr header
	kv  etcdserver.RaftKV
	// maxTxnOps is the max operations per txn.
	// e.g suppose maxTxnOps = 128.
	// Txn.Success can have at most 128 operations,
	// and Txn.Failure can have at most 128 operations.
	maxTxnOps uint
}

func NewKVServer(s *etcdserver.EtcdServer) pb.KVServer {
	return &kvServer{hdr: newHeader(s), kv: s, maxTxnOps: s.Cfg.MaxTxnOps}
}

// Range etcd 中读取单个key和批量key所使用的都是此方法。
//
// 客户端发起请求之后，clientv3 首先会根据负载均衡算法选择一个合适的 etcd 节点，
// 接着调用 KVServer 模块对应的 RPC 接口，发起 Range 请求的 gRPC 远程调用；
// gRPC Server 上注册的拦截器拦截到 Range 请求，实现 Metrics 统计、日志记录等功能；
// 然后进入读的主要过程，etcd 模式实现了线性读，使得任何客户端通过线性读都能及时访问到键值对的更新；
// 线性读获取到 Leader 已提交日志索引构造的最新 ReadState 对象，实现本节点状态机的同步；
// 接着就是调用 MVCC 模块，根据 treeIndex模块 B-tree 快速查找 key对应的版本号；
// 通过获取的版本号作为 key，查询存储在 boltdb 中的键值对，我们在之前的存储部分讲解过此过程。
func (s *kvServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	// 如果执行的是 ./bin/etcdctl get hello
	// 则 这里的 string(r.Key) = hello

	// 检验 Range 请求的参数
	if err := checkRangeRequest(r); err != nil {
		return nil, err
	}

	// Range 请求的主要部分在于调用 RaftKV.Range()方法。这将会调用到 etcdserver 包中对 RaftKV 的实现：
	// 这里执行的是 v3_server.go 中的 EtcdServer 对象的 Range 方法
	// -> EtcdServer.Range()
	// -> applierV3backend.Range()
	// -> metricsTxnWrite.Range()
	// -> storeTxnRead.Range()
	resp, err := s.kv.Range(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}

	// 使用 etcd Server 的信息填充 pb.ResponseHeader
	s.hdr.fill(resp.Header)
	return resp, nil
}

// Put 操作用于插入或者更新指定的键值对。
//
// 再前面的配额检查后，请求就从 API 层转发到了 KVServer 模块的 put 方法
func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := checkPutRequest(r); err != nil {
		return nil, err
	}

	resp, err := s.kv.Put(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}

	s.hdr.fill(resp.Header)
	return resp, nil
}

// DeleteRange grpc 请求的第一层函数
//
// etcd 实现的是延期删除模式，原理与 key 更新类似。与更新 key 不一样之处在于，
//	一方面，生成的 boltdb key 版本号{4,0,t}追加了删除标识（tombstone, 简写 t），boltdb value 变成只含用户 key 的 KeyValue 结构体。
//	另一方面 treeIndex 模块也会给此 key hello 对应的 keyIndex 对象，追加一个空的 generation 对象，表示此索引对应的 key 被删除了。
//
// 那么 key 打上删除标记后有哪些用途呢？什么时候会真正删除它呢？
//	一方面删除 key 时会生成 events，Watch 模块根据 key 的删除标识，会生成对应的 Delete 事件。
//	另一方面，当你重启 etcd，遍历 boltdb 中的 key 构建 treeIndex 内存树时，你需要知道哪些 key 是已经被删除的，并为对应的 key 索引生成 tombstone 标识。
//	而真正删除 treeIndex 中的索引对象、boltdb 中的 key 是通过压缩 (compactor) 组件异步完成。
//
//	正因为 etcd 的删除 key 操作是基于以上延期删除原理实现的，因此只要压缩组件未回收历史版本，我们就能从 etcd 中找回误删的数据。
//
// etcd采用延迟删除:，
// 	1是为了保证 key 对应的 watcher 能够获取到 key 的所有状态信息，留给 watcher 时间做相应的处理。
//	2是实时从 boltdb 删除 key，会可能触发树的不平衡，影响其他读写请求的性能。
func (s *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if err := checkDeleteRequest(r); err != nil {
		return nil, err
	}

	// v3_server.go -> EtcdServer.DeleteRange()
	resp, err := s.kv.DeleteRange(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}

	s.hdr.fill(resp.Header)
	return resp, nil
}

func (s *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if err := checkTxnRequest(r, int(s.maxTxnOps)); err != nil {
		return nil, err
	}
	// check for forbidden put/del overlaps after checking request to avoid quadratic blowup
	if _, _, err := checkIntervals(r.Success); err != nil {
		return nil, err
	}
	if _, _, err := checkIntervals(r.Failure); err != nil {
		return nil, err
	}

	resp, err := s.kv.Txn(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}

	s.hdr.fill(resp.Header)
	return resp, nil
}

func (s *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	resp, err := s.kv.Compact(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}

	s.hdr.fill(resp.Header)
	return resp, nil
}

func checkRangeRequest(r *pb.RangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
}

func checkPutRequest(r *pb.PutRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	if r.IgnoreValue && len(r.Value) != 0 {
		return rpctypes.ErrGRPCValueProvided
	}
	if r.IgnoreLease && r.Lease != 0 {
		return rpctypes.ErrGRPCLeaseProvided
	}
	return nil
}

func checkDeleteRequest(r *pb.DeleteRangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
}

func checkTxnRequest(r *pb.TxnRequest, maxTxnOps int) error {
	opc := len(r.Compare)
	if opc < len(r.Success) {
		opc = len(r.Success)
	}
	if opc < len(r.Failure) {
		opc = len(r.Failure)
	}
	if opc > maxTxnOps {
		return rpctypes.ErrGRPCTooManyOps
	}

	for _, c := range r.Compare {
		if len(c.Key) == 0 {
			return rpctypes.ErrGRPCEmptyKey
		}
	}
	for _, u := range r.Success {
		if err := checkRequestOp(u, maxTxnOps-opc); err != nil {
			return err
		}
	}
	for _, u := range r.Failure {
		if err := checkRequestOp(u, maxTxnOps-opc); err != nil {
			return err
		}
	}

	return nil
}

// checkIntervals tests whether puts and deletes overlap for a list of ops. If
// there is an overlap, returns an error. If no overlap, return put and delete
// sets for recursive evaluation.
func checkIntervals(reqs []*pb.RequestOp) (map[string]struct{}, adt.IntervalTree, error) {
	dels := adt.NewIntervalTree()

	// collect deletes from this level; build first to check lower level overlapped puts
	for _, req := range reqs {
		tv, ok := req.Request.(*pb.RequestOp_RequestDeleteRange)
		if !ok {
			continue
		}
		dreq := tv.RequestDeleteRange
		if dreq == nil {
			continue
		}
		var iv adt.Interval
		if len(dreq.RangeEnd) != 0 {
			iv = adt.NewStringAffineInterval(string(dreq.Key), string(dreq.RangeEnd))
		} else {
			iv = adt.NewStringAffinePoint(string(dreq.Key))
		}
		dels.Insert(iv, struct{}{})
	}

	// collect children puts/deletes
	puts := make(map[string]struct{})
	for _, req := range reqs {
		tv, ok := req.Request.(*pb.RequestOp_RequestTxn)
		if !ok {
			continue
		}
		putsThen, delsThen, err := checkIntervals(tv.RequestTxn.Success)
		if err != nil {
			return nil, dels, err
		}
		putsElse, delsElse, err := checkIntervals(tv.RequestTxn.Failure)
		if err != nil {
			return nil, dels, err
		}
		for k := range putsThen {
			if _, ok := puts[k]; ok {
				return nil, dels, rpctypes.ErrGRPCDuplicateKey
			}
			if dels.Intersects(adt.NewStringAffinePoint(k)) {
				return nil, dels, rpctypes.ErrGRPCDuplicateKey
			}
			puts[k] = struct{}{}
		}
		for k := range putsElse {
			if _, ok := puts[k]; ok {
				// if key is from putsThen, overlap is OK since
				// either then/else are mutually exclusive
				if _, isSafe := putsThen[k]; !isSafe {
					return nil, dels, rpctypes.ErrGRPCDuplicateKey
				}
			}
			if dels.Intersects(adt.NewStringAffinePoint(k)) {
				return nil, dels, rpctypes.ErrGRPCDuplicateKey
			}
			puts[k] = struct{}{}
		}
		dels.Union(delsThen, adt.NewStringAffineInterval("\x00", ""))
		dels.Union(delsElse, adt.NewStringAffineInterval("\x00", ""))
	}

	// collect and check this level's puts
	for _, req := range reqs {
		tv, ok := req.Request.(*pb.RequestOp_RequestPut)
		if !ok || tv.RequestPut == nil {
			continue
		}
		k := string(tv.RequestPut.Key)
		if _, ok := puts[k]; ok {
			return nil, dels, rpctypes.ErrGRPCDuplicateKey
		}
		if dels.Intersects(adt.NewStringAffinePoint(k)) {
			return nil, dels, rpctypes.ErrGRPCDuplicateKey
		}
		puts[k] = struct{}{}
	}
	return puts, dels, nil
}

func checkRequestOp(u *pb.RequestOp, maxTxnOps int) error {
	// TODO: ensure only one of the field is set.
	switch uv := u.Request.(type) {
	case *pb.RequestOp_RequestRange:
		return checkRangeRequest(uv.RequestRange)
	case *pb.RequestOp_RequestPut:
		return checkPutRequest(uv.RequestPut)
	case *pb.RequestOp_RequestDeleteRange:
		return checkDeleteRequest(uv.RequestDeleteRange)
	case *pb.RequestOp_RequestTxn:
		return checkTxnRequest(uv.RequestTxn, maxTxnOps)
	default:
		// empty op / nil entry
		return rpctypes.ErrGRPCKeyNotFound
	}
}

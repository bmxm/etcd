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

// Package main is a simple wrapper of the real etcd entrypoint package
// (located at go.etcd.io/etcd/etcdmain) to ensure that etcd is still
// "go getable"; e.g. `go get go.etcd.io/etcd` works as expected and
// builds a binary in $GOBIN/etcd
//
// This package should NOT be extended or modified in any way; to modify the
// etcd binary, work in the `go.etcd.io/etcd/etcdmain` package.
//
package main

import (
	"os"

	"go.etcd.io/etcd/server/v3/etcdmain"
)

// 客户端层如:
//     clientv3 库和 etcdctl等工具，用户通过 RESTful方式进行调用，降低了 etcd 的使用复杂度；
// API 接口层:
//     提供了客户端访问服务 端的通信协议和接口定义，以及服务端节点之间相互通信的协议。etcd v3 使用 gRPC 作为消息传输协议；
// etcd Raft 层:
//     负责 Leader 选举和 日志复制等功能，除了与本节点的 etcd server 通信之外，还与集群中的其他 etcd 节点进行交互，实现分布式一致性数据同步的关键工作；
// etcd 的业务逻辑层:
//     包括鉴权、租约、KVServer、MVCC 和 Compactor 压缩等核心功能特性；
// etcd 存储:
//     实现了快照、预写式日志 WAL（Write Ahead Log）。etcd V3 版本中，使用 boltdb 来持久化存储集群元数据和用户写入的数据。

// 默认监听两个端口
// 2379 用于与客户端交互
// 2380 用于与etcd节点内部交互
func main() {
	etcdmain.Main(os.Args)
}

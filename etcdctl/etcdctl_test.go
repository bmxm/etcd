package main

import "testing"

// etcdctl get hello --endpoints http://127.0.0.1:2379

// 参数get是请求的方法，它是KVServer模块的API
// hello 是我们查询的Key名

// 在 etcd v3.4.9 版本中，etcdctl 是通过 clientv3 库来访问 etcd server 的，
// clientv3 库基于 gRPC client API 封装了操作 etcd KVServer、Cluster、Auth、Lease、Watch 等模块的 API，同时还包含了负载均衡、健康探测和故障切换等特性

func TestGetHello(t *testing.T) {

}

// go run main.go put hello world
// go run main.go get hello

// 在解析完请求中的参数后，etcdctl 会创建一个 clientv3 库对象，使用 KVServer 模块的 API 来访问 etcd server
// 接下来，就需要为这个 get hello 请求选择一个合适的 etcd server 节点了，这里得用到负载均衡算法。
// 在 etcd 3.4 中，clientv3 库采用的负载均衡算法为 Round-robin。
// 针对每一个请求，Round-robin 算法通过轮询的方式依次从 endpoint 列表中选择一个 endpoint 访问 (长连接)，使 etcd server 负载尽量均衡。
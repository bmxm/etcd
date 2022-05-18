API 网络层

主要包括 client 访问 server 和 server 节点之间的通信协议。
v2 HTTP/1.x
v3 gRPC

v3 通过 etcd grpc-gateway 组件也支持 HTTP/1.x 协议，便于各语言的服务调用。

server 之间的通信协议，是指节点间通过 Raft 算法实现数据复制和 Leader 选举等功能时使用的 HTTP 协议。
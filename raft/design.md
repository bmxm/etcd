- Raft算法是一种用于管理复制日志的一致性算法。

- Paxos协议是Leslie Lamport于1990年提出的一种基于消息传递的、具有高度容错特性的一致性算法，Paxos 算法解决的主要问题是分布式系统内如何就某个值达成一致。

- Raft算法中有两个重要的计时器：选举计时器 & 心跳计时器

选举超时时间（election timeout）：每个 Follower 节点在接收不到 Leader 节点的心跳信息之后，并不会立即发起一轮选举，而是等待一段时间后才会切换成 Canditate 状态发起一轮选举。
这段等待时长，就是这里说的election timeout，主要是 Leader 节点发送的心跳消息可能因为瞬间的网络延迟或程序的卡顿而迟到或者丢失，这时间触发新一轮选举是没有必要的。
election timeout 一般设置为 150ms~300ms 之间的随机数。

心跳超时时间（heartbeat timeout）: Leader 节点向集群中其它 Follower 节点发送心跳消息的时间间隔。

Raft 算法将时间划分为任期，任期是一个递增的整数，一个任期时从开始选举 Leader 到 Leader 失效的这段时间。

- Leader选举是Raft算法中对时间要求较为严格的一个点，一般要求整个集群中的时间满足如下不等式：
  广播时间 << 选举超时时间 << 平均故障间隔时间


如当前任期为 term = n：
1. 一个节点 A 长时间未收到 Leader 的心跳消息，就会切换成 Candidate 状态并发起选举（并给自己投票），并重置自己的选举计时器， term = n+1。
2. 此时如果别的 Follower 节点还在 term = n 的任期中，并未投出 term = n+1 任期的选票，则会在收到节点A的选举请求后把选票投给节点A，并重置自己的选举计时器。
   1. 集群中的节点 除了记录当前任期号，还会记录在该任期中当前节点的投票结果。
   2. 由此可见心跳超时时间 需要远远小于 选举超时时间。
3. 如果集群中有四个节点，其中两个过期了，都触发了选举，得票数都是而 2，这个任期就会选举结束，当任意节点的选举计时器超时后，会再次发起新一轮的选举。
4. 如果一个 Candidate 节点收到任期比自身记录的 Term 值大的请求时，节点会切换成 Follower 状态并更新自身记录的 Term 值，同时会把选票投给对方。
5. 集群中其它节点每次收到 Leader 节点的心跳消息都会重置字段的选举计时器。
6. 假设 Leader 节点A 宕机了，最先选举超时的节点会发起选举，如果后面节点A 有上线了，当收到 term 大于当前记录的任期号时，会将自身切换成 Follower 状态。
   1. 在Raft协议中，当某个节点接收到的消息所携带的任期号大于当前节点本身记录的任期号，那么该节点会更新自身记录的任期号，同时会切换为Follower状态并重置选举计时器，这是Raft算法中所有节点都要遵守的一条准则。



## Progress

Progress represents a follower’s progress in the view of the leader. Leader maintains progresses of all followers, and sends `replication message` to the follower based on its progress. 

`replication message` is a `msgApp` with log entries.

A progress has two attribute: `match` and `next`. `match` is the index of the highest known matched entry. If leader knows nothing about follower’s replication status, `match` is set to zero. `next` is the index of the first entry that will be replicated to the follower. Leader puts entries from `next` to its latest one in next `replication message`.

A progress is in one of the three state: `probe`, `replicate`, `snapshot`. 

```
                            +--------------------------------------------------------+          
                            |                  send snapshot                         |          
                            |                                                        |          
                  +---------+----------+                                  +----------v---------+
              +--->       probe        |                                  |      snapshot      |
              |   |  max inflight = 1  <----------------------------------+  max inflight = 0  |
              |   +---------+----------+                                  +--------------------+
              |             |            1. snapshot success                                    
              |             |               (next=snapshot.index + 1)                           
              |             |            2. snapshot failure                                    
              |             |               (no change)                                         
              |             |            3. receives msgAppResp(rej=false&&index>lastsnap.index)
              |             |               (match=m.index,next=match+1)                        
receives msgAppResp(rej=true)                                                                   
(next=match+1)|             |                                                                   
              |             |                                                                   
              |             |                                                                   
              |             |   receives msgAppResp(rej=false&&index>match)                     
              |             |   (match=m.index,next=match+1)                                    
              |             |                                                                   
              |             |                                                                   
              |             |                                                                   
              |   +---------v----------+                                                        
              |   |     replicate      |                                                        
              +---+  max inflight = n  |                                                        
                  +--------------------+                                                        
```

When the progress of a follower is in `probe` state, leader sends at most one `replication message` per heartbeat interval. The leader sends `replication message` slowly and probing the actual progress of the follower. A `msgHeartbeatResp` or a `msgAppResp` with reject might trigger the sending of the next `replication message`.

When the progress of a follower is in `replicate` state, leader sends `replication message`, then optimistically increases `next` to the latest entry sent. This is an optimized state for fast replicating log entries to the follower.

When the progress of a follower is in `snapshot` state, leader stops sending any `replication message`.

A newly elected leader sets the progresses of all the followers to `probe` state with `match` = 0 and `next` = last index. The leader slowly (at most once per heartbeat) sends `replication message` to the follower and probes its progress.

A progress changes to `replicate` when the follower replies with a non-rejection `msgAppResp`, which implies that it has matched the index sent. At this point, leader starts to stream log entries to the follower fast. The progress will fall back to `probe` when the follower replies a rejection `msgAppResp` or the link layer reports the follower is unreachable. We aggressively reset `next` to `match`+1 since if we receive any `msgAppResp` soon, both `match` and `next` will increase directly to the `index` in `msgAppResp`. (We might end up with sending some duplicate entries when aggressively reset `next` too low.  see open question)

A progress changes from `probe` to `snapshot` when the follower falls very far behind and requires a snapshot. After sending `msgSnap`, the leader waits until the success, failure or abortion of the previous snapshot sent. The progress will go back to `probe` after the sending result is applied.

### Flow Control

1. limit the max size of message sent per message. Max should be configurable.
Lower the cost at probing state as we limit the size per message; lower the penalty when aggressively decreased to a too low `next`

2. limit the # of in flight messages < N when in `replicate` state. N should be configurable. Most implementation will have a sending buffer on top of its actual network transport layer (not blocking raft node). We want to make sure raft does not overflow that buffer, which can cause message dropping and triggering a bunch of unnecessary resending repeatedly. 

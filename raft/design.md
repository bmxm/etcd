- Raft算法是一种用于管理复制日志的一致性算法。
- 是一种分布式一致性算法
- 是一种共识算法

- Paxos协议是Leslie Lamport于1990年提出的一种基于消息传递的、具有高度容错特性的一致性算法，Paxos 算法解决的主要问题是分布式系统内如何就某个值达成一致。
- Raft 算法属于 Multi-Paxos 算法，它是在兰伯特 Multi-Paxos 思想的基础上，做了一些简化和限制，比如增加了日志必须是连续的，只支持领导者、跟随者和候选人三种状态，在理解和算法实现上都相对容易许多。
- 除此之外，Raft 算法是现在分布式系统开发首选的共识算法。绝大多数选用 Paxos 算法的系统（比如 Cubby、Spanner）都是在 Raft 算法发布前开发的，当时没得选；而全新的系统大多选择了 Raft 算法（比如 Etcd、Consul、CockroachDB）。


- 从本质上说，Raft 算法是通过一切以领导者为准的方式，实现一系列值的共识和各节点日志的一致。

- Raft算法中有两个重要的计时器：选举计时器 & 心跳计时器

选举超时时间（election timeout）：每个 Follower 节点在接收不到 Leader 节点的心跳信息之后，并不会立即发起一轮选举，而是等待一段时间后才会切换成 Canditate 状态发起一轮选举。
这段等待时长，就是这里说的election timeout，主要是 Leader 节点发送的心跳消息可能因为瞬间的网络延迟或程序的卡顿而迟到或者丢失，这时间触发新一轮选举是没有必要的。
election timeout 一般设置为 150ms~300ms 之间的随机数。

心跳超时时间（heartbeat timeout）: Leader 节点向集群中其它 Follower 节点发送心跳消息的时间间隔。

Raft 算法将时间划分为任期，任期是一个递增的整数，一个任期时从开始选举 Leader 到 Leader 失效的这段时间。

- Leader 选举是Raft算法中对时间要求较为严格的一个点，一般要求整个集群中的时间满足如下不等式：
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


Leader 是大多数的节点选举产生的，并且节点的状态可以随着时间发生变化。
某个 Leader 节点在领导的这段时期被称为任期（Term）。
新的 Term 是从选举 Leader 时开始增加的，每次 Candidate 节点开始新的选举，Term 都会加 1。
如果 Candidate 选举成为了 Leader，意味着它成为了这个 Term 后续时间的 Leader。
每一个节点会存储当前的 Term，如果某一个节点当前的 Term 小于其他节点，那么节点会更新自己的 Term 为已知的最大 Term。
如果一个 Candidate 发现自己当前的 Term 过时了，它会立即变为 Follower。

一般情况下（网络分区除外）在一个时刻只会存在一个 Leader，其余的节点都是 Follower。
Leader 会处理所有的客户端写请求（如果是客户端写请求到 Follower，也会被转发到 Leader 处理），将操作作为一个 Entry 追加到复制日志中，并把日志复制到所有节点上。
而 Candidate 则是节点选举时的过渡状态，用于自身拉票选举 Leader。Raft 节点之间通过 RPC（Remote Prcedure Cal，远程过程调用）来进行通信。
Raft 论文中指定了两种方法用于节点的通信，其中，RequestVote 方法由 Candidate 在选举时使用，AppendEntries 则是 Leader 复制 log 到其他节点时使用，同时也可以用于心跳检测。
RPC 方法可以是并发的，且支持失败重试。



## 选举与任期

在 Raft 中有一套心跳检测，只要 Follower 收到来自 Leader 或者 Candidate 的信息，它就会保持 Follower 的状态。
但是如果 Follower 一段时间内没有收到 RPC 请求（例如可能是 Leader 挂了），新一轮选举的机会就来了。（election timeout）
这时 Follower 会将当前 Term 加 1 并过渡到 Candidate 状态。它会给自己投票，并发送 RequestVote RPC 请求给其他的节点进行拉票。
Candidate 的状态会持续，直到下面的三种情况发生。
- 如果这个 Candidate 节点获得了大部分节点的支持，赢得选举变为了 Leader。一旦它变为 Leader，这个新的 Leader 节点就会向其他节点发送 AppendEntries RPC， 确认自己 Leader 的地位，终止选举。
- 如果其他节点成为了 Leader。它会收到其他节点的 AppendEntries RPC。如果发现其他节点的当前 Term 比自己的大，则会变为 Follower 状态。
- 如果有许多节点同时变为了 Candidate，则可能会出现一段时间内没有节点能够选举成功的情况，这会导致选举超时。

为了快速解决并修复这第三种情况，Raft 规定了每一个 Candidate 在选举前会重置一个随机的选举超时（Election Timeout）时间，这个随机时间会在一个区间内（例如 150-300ms）。

随机时间保证了在大部分情况下，有一个唯一的节点首先选举超时，它会在大部分节点选举超时前发送心跳检测，赢得选举。如果一个 Leader 在心跳检测中发现另一个节点有更高的 Term，它会转变为 Follower，否则将一直保持 Leader 状态。


收到申请的服务，在发送申请服务的任期和同步进度都比它超前或相同，那么它就会投申请服务一票，并把当前的任期更新成最新的任期。同时，这个收到申请的服务不在发起投票，会等待其他服务的邀请。
每个服务在任期内只投票一次。如果所有服务都没有获取到多少票（2/3以上，包含吗？），就会等待当前选举超时后，对任期+1，再次进行选举。最终，获取多数票且最先结束选举倒计时的服务会被选为 Leader.

被选为 Leader 的服务会发布广播通知其他服务，并向其他服务同步最新的任期和其进度情况。同时，新任Leader会在任职期间周期性发送心跳，保证各个子服务（Follower）不会因为超时而切换选举模式。
在选举期间，若有服务收到上一任Leader的心跳则会拒绝。



## 日志复制（Log Replication）

一个节点成为 Leader 之后，会开始接受来自客户端的请求。每一个客户端请求都包含一个节点的状态机将要执行的操作（Command）。
Leader 会将这个操作包装为一个 Entry 放入到 log 中，并通过 AppendEntries RPC 发送给其他节点，要求其他节点把这个 Entry 添加到 log 中。

当 Entry 被复制到大多数节点之后，也就是被大部分的节点认可之后，这个 Entry 的状态就变为 Committed。Raft 算法会保证 Committed Entry 一定能够被所有节点的状态机执行。

一旦 Follower 通过 RPC 协议知道某一个 Entry 被 commit 了，Follower 就可以按顺序执行 log 中的 Committed Entry 了。

我们可以把 log 理解为 Entry 的集合。Entry 中包含了 Command 命令（例如 x←3），Entry 所在的 Term（方框里面的数字），以及每一个 Entry 的顺序编号（最上面标明的 log index，顺序递增）。

但这里还有一个重要的问题，就是 Raft 节点在日志复制的过程中需要保证日志数据的一致性。 要实现这一点，需要确认下面几个关键的属性：
- 如果不同节点的 log 中的 Entry 有相同的 index 和 Term, 那么它们存储的一定是相同的 Command；
- 如果不同节点的 log 中的 Entry 有相同的 index 和 Term，那么这个 Entry 之前所有的 Entry 都是相同的。

在正常的情况下，Raft 可以满足上面的两个属性，但是异常情况下，这种情况就可能被打破，出现数据不一致的情况。为了让数据保持最终一致，Raft 算法会强制要求 Follower 的复制日志和 Leader 的复制日志一致，
这样一来，Leader 就必须要维护一个 Entry index 了。在这个 Entry index 之后的都是和 Follower 不相同的 Entry，在这个 Entry 之前的都是和 Follower 一致的 Entry。

Leader 会为每一个 Follower 维护一份 next index 数组，里面标志了将要发送给 Follower 的下一个 Entry 的序号。最后，Follower 会删除掉所有不同的 Entry，并保留和 Leader 一致的复制日志，这一过程都会通过 AppendEntries RPC 执行完毕。

另外，Raft 为 Leader 添加了下面几个限制：
- 要成为 Leader 必须要包含过去所有的 Committed Entry；
- Candidate 要想成为 Leader，必须要经过大部分 Follower 节点的同意。而当 Entry 成为 Committed Entry 时，表明该 Entry 其实已经存在于大部分节点中了，所以这个 Committed Entry 会出现在至少一个 Follower 节点中。因此我们可以证明，当前 Follower 节点中，至少有一个节点是包含了上一个 Leader 节点的所有 Committed Entry 的。Raft 算法规定，只有当一个 Follower 节点的复制日志是最新的（如果复制日志的 Term 最大，则其日志最新，如果 Term 相同，那么越长的复制日志越新），它才可能成为 Leader。


可以看出，Raft 算法采用的也是主从方式同步，只不过 Leader 不是固定的服务，而是被选举出来的。这样个别节点出现故障时，是不会影响整体服务的。不过，这种机制也有缺点；如果 Leader 失联，那么整体服务会有一段时间忙于选举，而无法提供数据服务。

通常来说，客户端的数据修改请求都会发送到 Leader 节点进行统一决策，如果客户端请求发送到了 Follower，Follower 就会将请求重定向到 Leader。

具体来讲，Leader 成功修改数据后，会产生对应的日志，然后 Leader 会给所有 Follower 发送单条日志同步信息。只要大多数 Follower 返回同步成功，Leader 就会对预提交的日志进行 commit，并向客户端返回修改成功。

接着，Leader 在下一次心跳时（消息中 Leader commit 字段），会把当前最新 commit 的 Log index (日志进度) 告知给各个 Follower 节点，然后各 Follower 按照这个 index 进度对外提供数据，未被 Leader 最终 commit 的数据则不会落地对外展示。

如果在数据同步期间，客户端还有其他的数据请求发到 Leader，那么这些请求会排队，因为这个时候的 Leader 在阻塞等待其他节点回应。

不过，这种阻塞等待的设计也让 Raft 算法对网络性能的依赖很大，因为每次修改都要并发请求多个节点，等待大部分节点成功同步的结果。
最惨的情况是，返回的 RTT 会按照最慢的网络服务响应耗时（“两地三中心”的一次同步时间为 100ms 左右），再加上主节点只有一个，一组 Raft 的服务性能是有上限的。
对此，我们可以减少数据量并对数据做切片，提高整体集群的数据修改性能。
请你注意，当大多数 Follower 与 Leader 同步的日志进度差异过大时，数据变更请求会处于等待状态，直到一半以上的 Follower 与 Leader 的进度一致，才会返回变更成功。当然，这种情况比较少见



## 总结

在 Raft 算法中，写请求具有线性一致性，但是读请求由于 Follower 节点数据暂时的不一致，可能会读取到过时的数据。
因此，Raft 保证的是读数据的最终一致性，这是为了性能做的一种妥协。但我们可以在此基础上很容易地实现强一致性的读取，例如将读操作转发到 Leader 再读取数据。


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

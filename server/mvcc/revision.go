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

package mvcc

import "encoding/binary"

// revBytesLen is the byte length of a normal revision.
// First 8 bytes is the revision.main in big-endian format. The 9th byte
// is a '_'. The last 8 bytes is the revision.sub in big-endian format.
const revBytesLen = 8 + 1 + 8

// A revision indicates modification of the key-value space.
// The set of changes that share same main revision changes the key-value space atomically.
type revision struct {
	// 一个全局递增的主版本号，随put/txn/delete事务递增，一个事务内的key main版本号是一致的
	// main is the main revision of a set of changes that happen atomically.
	main int64

	// 一个事务内的子版本号，从0开始随事务内put/delete操作递增
	// sub is the sub revision of a change in a set of changes that happen
	// atomically. Each change has different increasing sub revision in that
	// set.
	sub int64
}

/**
比如启动一个空集群，全局版本号默认为 1，执行下面的 txn 事务，它包含两次 put、一次 get 操作，
那么按照上面的原理，全局版本号随读写事务自增，因此是 main 为 2，sub 随事务内的 put/delete 操作递增，
因此 key hello 的 revison 为{2,0}，key world 的 revision 为{2,1}。

# 以交互模式进入 etcd 事务
$ etcdctl txn -i
/usr/local/bin # etcdctl txn -i
compares:
# 判定条件 这里不填，直接按回车键跳过
success requests (get, put, del):
# 判定条件成功时需要执行的命令 手动输入
put hello 1
get hello
put world 2

failure requests (get, put, del):
# 判定条件失败时时需要执行的命令 这里也为空 直接跳过
SUCCESS # SUCCESS 为前面 compares 的结果 由于没有填任何条件 所以当然是成功的

OK # 第一条 put 命令的结果

hello # 第二条 get 命令的结果
1

OK # 第三条 put 命令的结果
 */

func (a revision) GreaterThan(b revision) bool {
	if a.main > b.main {
		return true
	}
	if a.main < b.main {
		return false
	}
	return a.sub > b.sub
}

func newRevBytes() []byte {
	return make([]byte, revBytesLen, markedRevBytesLen)
}

func revToBytes(rev revision, bytes []byte) {
	binary.BigEndian.PutUint64(bytes, uint64(rev.main))
	bytes[8] = '_'
	binary.BigEndian.PutUint64(bytes[9:], uint64(rev.sub))
}

func bytesToRev(bytes []byte) revision {
	return revision{
		main: int64(binary.BigEndian.Uint64(bytes[0:8])),
		sub:  int64(binary.BigEndian.Uint64(bytes[9:])),
	}
}

type revisions []revision

func (a revisions) Len() int           { return len(a) }
func (a revisions) Less(i, j int) bool { return a[j].GreaterThan(a[i]) }
func (a revisions) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

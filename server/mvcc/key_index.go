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

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// 对etcd v2 来说，当你通过 etcdctl put值的时候，etcd v2 会直接刷新内存树，这就导致历史版本直接被覆盖，无法支持key的历史版本。
// 在 etcd v3 中引入 treeIndex 模块正是为了解决这个问题，支持保存 key 的历史版本，提供稳定的 Watch 机制和事务隔离等能力
//
// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
type keyIndex struct {
	key      []byte   // 客户端提供的原始Key值
	modified revision // the main rev of the last modification 记录该Key值最后一次修改对应的revision信息

	// 当第一次创建客户端给定的 Key 值时，对应的第0代版本信息（即generations[0]项）也会被创建，
	// 所以每个 Key 值至少对应一个generation实例（如果没有，则表示当前Key值应该被删除），
	// 每代中包含多个revision信息。当客户端后续不断修改该Key时，generation[0]中会不断追加revision信息
	generations []generation // generation保存了一个key若干代版本号信息，每代中包含对key的多次修改的版本号列表

	// generations 表示一个 key 从创建到删除的过程，每代对应 key 的一个生命周期的开始与结束。
	// 当你第一次创建一个 key 时，会生成第 0 代，后续的修改操作都是在往第 0 代中追加修改版本号。
	// 当你把 key 删除后，它就会生成新的第 1 代，一个 key 不断经历创建、删除的过程，它就会生成多个代。
}

// backend store 实现了多版本的机制，也就时键值对的每一次更新操作都被单独记录下来了。
// 在BoltDB中存储的键值对中，key实际上是revision（由main revision和sub revision两部分组成）。
// 这样，要从BoltDB中检索数据就必须通过revision完成，
// 为了将客户端提供的原始键值对信息与revision关联起来，backend在内存索引实现中，为每个客户端提供的原始Key关联了一个keyIndex实例，其中维护了多版本信息。

// 从整体上来看，客户端在查找指定键值对时，会先通过内存中维护的B树索引（该B树索引中维护了原始Key值到keyIndex的映射关系）查找到对应的keyIndex实例，
// 然后通过keyIndex查找到对应的 revision 信息（keyIndex 内维护了多个版本的 revision 信息），最后通过 revision映射到磁盘中的BoltDB查找并返回真正的键值对数据。

// 每个Key会对应多个generation，当Key首次创建的时候，会同时创建一个与之关联的generation实例
// 当该Key被修改时，会将对应的版本记录到generation中
// 当Key被删除时，会向 generation 中添加 bombstone，并创建新的generation，会向新generation中写入后续的版本信息

// 在每次修改 key 时会生成一个全局递增的版本号（revision）
// 然后通过数据结构 B-tree 保存用户 key 与版本号之间的关系
// 再以版本号作为 boltdb key，以用户的 key-value 等信息作为 boltdb value，保存到 boltdb

// !!!
// boltdb 中只能通过reversion来查询数据,但是客户端都是通过 key 来查询的。所以 etcd 在内存中使用 treeIndex 模块 维护了一个kvindex,保存的就是 key-reversion 之间的映射关系，用来加速查询

// put puts a revision to the keyIndex.
// 该方法负责向keyIndex中追加新的revision信息
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	// 根据传入的main revision和sub revision创建revision实例
	rev := revision{main: main, sub: sub}

	// 检测该revision实例的合法性
	if !rev.GreaterThan(ki.modified) {
		lg.Panic(
			"'put' with an unexpected smaller revision",
			zap.Int64("given-revision-main", rev.main),
			zap.Int64("given-revision-sub", rev.sub),
			zap.Int64("modified-revision-main", ki.modified.main),
			zap.Int64("modified-revision-sub", ki.modified.sub),
		)
	}

	// 创建generations[0]实例
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]

	// 新建Key，则初始化对应 generation 实例的 created 字段
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev
	}
	// 向 generation.revs 中追加revision信
	g.revs = append(g.revs, rev)
	// 递增generation.ver
	g.ver++
	// 更新keyIndex.modified字段
	ki.modified = rev
}

// 负责恢复当前的keyIndex 中的信息
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		lg.Panic(
			"'restore' got an unexpected non-empty generations",
			zap.Int("generations-size", len(ki.generations)),
		)
	}

	ki.modified = modified
	// 创建generation实例，并添加到generations中
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
// 该方法会在当前 generation 中追加一个 revision 实例，然后新建一个 generation 实例
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	// 检测当前的 keyIndex.generations 字段是否为空，以及当前使用的 generation 实例是否为空
	if ki.isEmpty() {
		lg.Panic(
			"'tombstone' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}

	// 调用put()方法，在当前generation中追加一个revision
	ki.put(lg, main, sub)

	// 在generations中创建新generation实例
	ki.generations = append(ki.generations, generation{})
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
// 在当前 keyIndex 实例中查找小于指定的 main revision 的最大 revision
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	// 检测当前 keyIndex 中是否记录了 generation 实例
	if ki.isEmpty() {
		lg.Panic(
			"'get' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}

	// 根据给定的main revision，查找对应的generation实例，如果没有对应的generation，则报错
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
// 用于批量查找revision，该方法负责返回当前keyIndex实例中 main 部分大于指定值的 revision 实例，
// 如果查询结果中包含了多个 main 部分相同的revision实例，则只返回其中 sub 部分最大的实例
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		lg.Panic(
			"'since' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	// 倒序遍历所有 generation 实例，查询从哪个generation开始查找(即gi对应的generation实例)
	// gi 不能等于 0 ？？
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision
	var last int64
	// 从gi处开始遍历generation实例
	for ; gi < len(ki.generations); gi++ {
		for _, r := range ki.generations[gi].revs {
			if since.GreaterThan(r) {
				continue
			}

			// 如果查找到mainrevision部分相同且sub revision更大的revision实例，
			// 则用其替换之前记录的返回结果，从而实现main部分相同时只返回sub部分较大的revision实例
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)
			last = r.main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
//
// 随着客户端不断修改键值对，keyIndex 中记录的 revision 实例和 generation 实例会不断增加，
// 我们可以通过调用 compact() 方法对 keyIndex 进行压缩。在压缩时会将 main 部分小于指定值的全部 revision 实例全部删除。
// 在压缩过程中，如果出现了空的 generation 实例，则会将其删除。
// 如果 keyIndex 中全部的 generation 实例都被清除了，则该 keyIndex 实例也会被删除。
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		lg.Panic(
			"'compact' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	// 遍历目标 generation 中的全部 revision 实例，清空目标 generation 实例中 main 部分小于指定值的 revision
	if !g.isEmpty() {
		// remove the previous contents.
		// 清理目标 generation
		if revIndex != -1 {
			g.revs = g.revs[revIndex:]
		}
		// remove any tombstone
		// 如果目标generation实例中只有tombstone，则将其删除
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++
		}
	}

	// remove the previous generations.
	// 清理目标 generation 实例之前的全部 generation 实例
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	// 后面遍历generation时使用的回调函数
	f := func(rev revision) bool {
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	// 遍历所有的 generation 实例，目标 generation 实例中的 tombstone 大于指定的 revision
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg // 指向当前keyIndex实例中最后一个generation实例，并逐个向前查找

	for cg >= 0 {
		// 过滤调用空的generation实例
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		g := ki.generations[cg]

		// 如果不是最后一个generation实例，则先与tombone revision进行比较
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}

		// 与generation中的第一个revision比较
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
// 当向 generation 实例中追加一个 Tombstone 时，表示删除当前Key值，此时就会结束当前的 generation，
// 后续不再向该 generation 实例中追加 revision 信息，同时会创建新的 generation 实例
// 可以认为 generation 对应了当前 Key 一次从创建到删除的生命周期。
type generation struct {
	ver     int64      // 记录当前generation所包含的修改次数，即 revs 数组的长度
	created revision   // when the generation is created (put in first revision). 表示generation结构创建时的版本号
	revs    []revision // 每次修改key时的revision追加到此数组

	// 版本号（revision）并不是一个简单的整数，而是一个结构体
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}

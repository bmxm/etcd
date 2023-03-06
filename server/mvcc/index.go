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
	"sort"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"
)

// 在etcd中，对 Google 开源的 BTree 实现进行了一层封装，对外提供了 index 接口
type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64, limit int) ([]revision, int)
	CountRevisions(key, end []byte, atRev int64) int
	Put(key []byte, rev revision)
	Tombstone(key []byte, rev revision) error
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{}
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

// treeIndex 中，每个节点的 key 是一个 keyIndex 结构，etcd 通过它保存了key 与版本号的映射关系。
// treeIndex 是v3版本存储提供的index接口实现，其中内嵌了sync.RWMutex，在进行更新操作时，
// 例如，Insert()、Compact()方法中，都需要获取该锁。
//
// 在 etcd treeIndex 模块中，创建的是最大度 32 的 B-tree，也就是一个叶子节点最多可以保存 63 个 key。
// (在一个度为 d 的 B-tree 中，节点保存的最大 key 数为 2d - 1，否则需要进行平衡、分裂操作。)
type treeIndex struct {
	sync.RWMutex
	tree *btree.BTree
	lg   *zap.Logger
}

func newTreeIndex(lg *zap.Logger) index {
	return &treeIndex{
		// 创建 BTree 实例，这里将 BTree 的度初始化为32，
		// 即除了根节点的每个节点至少有32个元素，每个节点最多有64个元素
		tree: btree.New(32),
		lg:   lg,
	}
}

// Put 方法，其中主要完成两项操作，一是向 BTree 中添加 keyIndex 实例，二是向 keyIndex 中追加 revision 信息
func (ti *treeIndex) Put(key []byte, rev revision) {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()

	// 通过BTree.Get()方法在 BTree 上查找指定的元素
	item := ti.tree.Get(keyi)
	if item == nil {
		// 向 keyIndex 中追加一个revision
		keyi.put(ti.lg, rev.main, rev.sub)

		// 通过 BTree.ReplaceOrInsert() 方法向 BTree 中添加 keyIndex 实例
		ti.tree.ReplaceOrInsert(keyi)
		return
	}
	// 如果在BTree中查找到该Key对应的元素，则向其中追加一个revision实例
	okeyi := item.(*keyIndex)
	okeyi.put(ti.lg, rev.main, rev.sub)
}

func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	keyi := &keyIndex{key: key}
	ti.RLock()
	defer ti.RUnlock()

	// 查询指定 Key 对应的keyIndex实例
	if keyi = ti.keyIndex(keyi); keyi == nil {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	// 在查询到的 keyIndex 实例中，查找对应的 revision 信息
	return keyi.get(ti.lg, atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex) bool) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.RLock()
	defer ti.RUnlock()

	// 调用 BTree.AscendGreaterOrEqual() 方法遍历比 key 大的元素
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		if !f(item.(*keyIndex)) {
			return false
		}
		return true
	})
}

func (ti *treeIndex) Revisions(key, end []byte, atRev int64, limit int) (revs []revision, total int) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, 0
		}
		return []revision{rev}, 1
	}
	ti.visit(key, end, func(ki *keyIndex) bool {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			if limit <= 0 || len(revs) < limit {
				revs = append(revs, rev)
			}
			total++
		}
		return true
	})
	return revs, total
}

func (ti *treeIndex) CountRevisions(key, end []byte, atRev int64) int {
	if end == nil {
		_, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return 0
		}
		return 1
	}
	total := 0
	ti.visit(key, end, func(ki *keyIndex) bool {
		if _, _, _, err := ki.get(ti.lg, atRev); err == nil {
			total++
		}
		return true
	})
	return total
}

func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	// 如果未指定end参数，则只查询key对应的revision信息
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}

	ti.visit(key, end, func(ki *keyIndex) bool {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key)
		}
		return true
	})
	return keys, revs
}

func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)
	return ki.tombstone(ti.lg, rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}

	ti.RLock()
	defer ti.RUnlock()

	if end == nil {
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(ti.lg, rev)
	}

	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(ti.lg, rev)...)
		return true
	})
	sort.Sort(revisions(revs))

	return revs
}

func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	ti.Lock()
	clone := ti.tree.Clone()
	ti.Unlock()

	clone.Ascend(func(item btree.Item) bool {
		keyi := item.(*keyIndex)
		//Lock is needed here to prevent modification to the keyIndex while
		//compaction is going on or revision added to empty before deletion
		ti.Lock()
		keyi.compact(ti.lg, rev, available)
		if keyi.isEmpty() {
			item := ti.tree.Delete(keyi)
			if item == nil {
				ti.lg.Panic("failed to delete during compaction")
			}
		}
		ti.Unlock()
		return true
	})
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}

func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	// 在 BTree 中提供了 Ascend* 方法用于正序遍历 BTree 中的元素，
	// 其中 Ascend(iterator ItemIterator) 方法会正序遍历并处理当前BTree中的所有元素
	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}

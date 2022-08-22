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
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"

	"go.uber.org/zap"
)

var (
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing
var defaultCompactBatchLimit = 1000

type StoreConfig struct {
	CompactionBatchLimit int
}

// 如果要从 BoltDB 中查询键值对，必须通过 revision 进行查找。
// 但客户端只知道具体的键值对中的 Key 值，并不清楚每个键值对对应的 revision 信息，
// 所以在v3版本存储的内存索引（kvIndex）中保存的就是 Key 与 revision 之前的映射关系。
type store struct {
	ReadView
	WriteView

	cfg StoreConfig

	// mu read locks for txns and write locks for non-txn store changes.
	// 在开启只读/读写事务时，需要获取该读锁进行同步，即在 Read()方法和 Write()方法中获取该读锁，
	// 在 End()方法中释放。在进行压缩等非事务性的操作时，需要加写锁进行同步。
	mu sync.RWMutex

	// 当前store实例关联的后端存储
	b backend.Backend

	// 当前 store 实例关联的内存索引
	kvindex index

	// 租约相关的内容
	le lease.Lessor

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	// 在修改 currentRev 字段和 compactMainRev 字段时，需要获取该锁进行同步。
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	// 该字段记录当前的 revision 信息（main revision部分的值）
	currentRev int64
	// compactMainRev is the main revision of the last compaction.
	// 该字段记录最近一次压缩后最小的 revision 信息（main revision部分的值）。
	compactMainRev int64

	// FIFO 调度器
	fifoSched schedule.Scheduler

	stopc chan struct{}

	lg *zap.Logger
}

// bytesBuf8（[]byte 类型） 索引缓冲区，主要用于记录 ConsistentIndex (旧结构体 ？？)

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) *store {
	if lg == nil {
		lg = zap.NewNop()
	}
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = defaultCompactBatchLimit
	}
	s := &store{
		cfg:     cfg,
		b:       b,
		kvindex: newTreeIndex(lg),

		le: le,

		currentRev:     1,
		compactMainRev: -1,

		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}

	// 创建readView实例和writeView实例
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}

	// 获取 backend 的读写事务，创建名为“key”和“meta”的两个Bucket，然后提交事务
	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(buckets.Key)
	tx.UnsafeCreateBucket(buckets.Meta)
	tx.Unlock()
	s.b.ForceCommit()

	s.mu.Lock()
	defer s.mu.Unlock()

	// 从backend中恢复当前store的所有状态，其中包括内存中的BTree索引等
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		select {
		case <-s.stopc:
		default:
			// fix deadlock in mvcc,for more information, please refer to pr 11817.
			// s.stopc is only updated in restore operation, which is called by apply
			// snapshot call, compaction and apply snapshot requests are serialized by
			// raft, and do not happen at the same time.
			s.mu.Lock()
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
			s.mu.Unlock()
		}
		return
	}
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	// TODO: hash and revision could be inconsistent, one possible fix is to add s.revMu.RLock() at the beginning of function, which is costly
	start := time.Now()

	s.b.ForceCommit()
	h, err := s.b.Hash(buckets.DefaultIgnores)

	hashSec.Observe(time.Since(start).Seconds())
	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(buckets.Key.Name())
	err = tx.UnsafeForEach(buckets.Key, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevSec.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}

func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock()
	if rev <= s.compactMainRev {
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}
	if rev > s.currentRev {
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

	s.compactMainRev = rev

	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes)

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafePut(buckets.Meta, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit()

	s.revMu.Unlock()

	return nil, nil
}

func (s *store) compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {
			s.compactBarrier(ctx, ch)
			return
		}
		start := time.Now()
		keep := s.kvindex.Compact(rev)
		indexCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))
		if !s.scheduleCompaction(rev, keep) {
			s.compactBarrier(context.TODO(), ch)
			return
		}
		close(ch)
	}

	s.fifoSched.Schedule(j)
	trace.Step("schedule compaction")
	return ch, nil
}

func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if err != nil {
		return ch, err
	}

	return s.compact(traceutil.TODO(), rev)
}

func (s *store) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	s.mu.Lock()

	ch, err := s.updateCompactRev(rev)
	trace.Step("check and update compact revision")
	if err != nil {
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	return s.compact(trace, rev)
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.ForceCommit()
}

// 对于 Raft 协议，Follower节点在收到快照数据时，会使用快照数据恢复当前节点的状态。
// 在这个恢复过程中就会调用 store.Restore() 方法完成内存索引和 store 中其他状态的恢复
func (s *store) Restore(b backend.Backend) error {
	// 获取mu上的写锁
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopc)
	// 关闭当前的FIFO Scheduler
	s.fifoSched.Stop()

	// 更新 store.b 字段，指向新的 backend 实例
	s.b = b
	// 更新 store.kvindex 字段，指向新的内存索引
	s.kvindex = newTreeIndex(s.lg)

	{
		// During restore the metrics might report 'special' values
		// 重置 currentRev 字段和 compactMainRev 字段
		s.revMu.Lock()
		s.currentRev = 1
		s.compactMainRev = -1
		s.revMu.Unlock()
	}

	// 更新 fifoSched 字段，指向新的FIFO Scheduler
	s.fifoSched = schedule.NewFIFOScheduler()
	s.stopc = make(chan struct{})

	// 调用restore()开始的恢复内存索引
	return s.restore()
}

// store.restore() 方法恢复内存索引的大致逻辑是：
// 首先批量读取 BoltDB 中所有的键值对数据，
// 然后将每个键值对封装成 revKeyValue 实例，并写入一个通道中，
// 最后由另一个单独的goroutine读取该通道，并完成内存索引的恢复。
func (s *store) restore() error {
	s.setupMetricsReporter()

	// 创建 min 和 max，后续在 BoltDB 中进行范围查询时的起始 key 和结束 key 就是 min 和 max
	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, min)
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	// 获取读写事务，并加锁
	tx := s.b.BatchTx()
	tx.Lock()

	// 调用 UnsafeRange() 方法，在meta Bucket中查询上次的压缩完成时的相关记录(Key为finishedCompactRev)
	_, finishedCompactBytes := tx.UnsafeRange(buckets.Meta, finishedCompactKeyName, nil, 0)
	// 根据查询结果，恢复 store.compactMainRev 字段
	if len(finishedCompactBytes) != 0 {
		s.revMu.Lock()
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main

		s.lg.Info(
			"restored last compact revision",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-key", string(finishedCompactKeyName)),
			zap.Int64("restored-compact-revision", s.compactMainRev),
		)
		s.revMu.Unlock()
	}
	// 调用UnsafeRange()方法，在meta Bucket中查询上次的压缩启动时的相关记录(Key为scheduledCompactRev)
	_, scheduledCompactBytes := tx.UnsafeRange(buckets.Meta, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	// 根据查询结果，更新 scheduledCompact 变量
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	// 在restoreIntoIndex() 方法中会启动一个单独的 goroutine，用于接收从 backend 中读取的键值对数据，
	// 并恢复到新建的内存索引中(store.kvindex)，该方法的返回值 rkvc 是一个通道，也是当前 goroutine 与上述 goroutine 通信的桥梁
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
		// 调用 UnsafeRange() 方法查询 BoltDB 中的key Bucket，返回键值对数量的上限是restoreChunkKeys，默认是10000
		keys, vals := tx.UnsafeRange(buckets.Key, min, max, int64(restoreChunkKeys))
		// 查询结果为空，则直接结束当前for循环
		if len(keys) == 0 {
			break
		}

		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		// 将查询到的键值对数据写入 rkvc 这个通道中，并由 restoreIntoIndex() 方法中创建的 goroutine 进行处理
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease)
		// 范围查询得到的结果数小于restoreChunkKeys，即表示最后一次查询
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			break
		}
		// next set begins after where this one ended
		// 更新 min 作为下一次范围查询的起始key
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	// 关闭rkvc通道
	close(rkvc)

	{
		s.revMu.Lock()
		// 从 revc 通道中获取恢复之后的 currentRev
		s.currentRev = <-revc

		// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
		// the correct revision should be set to compaction revision in the case, not the largest revision
		// we have seen.
		if s.currentRev < s.compactMainRev {
			s.currentRev = s.compactMainRev
		}
		s.revMu.Unlock()
	}

	// 校正 currentRev 和 scheduledCompact
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	// 租期相关的处理
	for key, lid := range keyToLease {
		if s.le == nil {
			tx.Unlock()
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			s.lg.Error(
				"failed to attach a lease",
				zap.String("lease-id", fmt.Sprintf("%016x", lid)),
				zap.Error(err),
			)
		}
	}

	tx.Unlock()

	s.lg.Info("kvstore restored", zap.Int64("current-rev", s.currentRev))

	// 如果在开始恢复之前，存在未执行完的压缩操作，则重启该压缩操作
	if scheduledCompact != 0 {
		if _, err := s.compactLockfree(scheduledCompact); err != nil {
			s.lg.Warn("compaction encountered error", zap.Error(err))
		}

		s.lg.Info(
			"resume scheduled compaction",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-key", string(scheduledCompactKeyName)),
			zap.Int64("scheduled-compact-revision", scheduledCompact),
		)
	}

	return nil
}

type revKeyValue struct {
	// BoltDB 中的 Key 值，可以转换得到的 revision 实例
	key []byte
	// BoltDB 中保存的 Value
	kv mvccpb.KeyValue
	// 原始的Key值
	kstr string
}

// 启动一个后台的 goroutine，读取 rkvc 通道中的 revKeyValue 实例，并将其中的键值对数据恢复到内存索引中等一系列操作
func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
	//  创建两个通道，其中 rkvc 通道就是前面一直提到的，用来传递 revKeyValue 实例的通道
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {
		// 记录当前遇到的最大的main revision值
		currentRev := int64(1)
		// 在该goroutine结束时（即 rkvc 通道被关闭时），将当前已知的最大main revision值写入 revc通道中，待其他goroutine读取
		defer func() { revc <- currentRev }()

		// restore the tree index from streaming the unordered index.
		// 虽然BTree的查找效率很高，但是随着 BTree 层次的加深，效率也随之下降，这里使用 kiCache 这个map做了一层缓存，
		// 更加高效地查找对应的 keyIndex 实例前面从 BoltDB 中读取到的键值对数据并不是按照原始Key值进行排序的，
		// 如果直接向 BTree 中写入，则可能会引起节点的分裂等变换操作，效率比较低。所以这里使用 kiCache 这个 map 做了一层缓存
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		// 从 rkvc 通道中读取 revKeyValue 实例
		for rkv := range rkvc {
			// 先查询缓存
			ki, ok := kiCache[rkv.kstr]
			// purge kiCache if many keys but still missing in the cache
			// 如果 kiCache 中缓存了大量的 Key，但是依然没有命中，则清理缓存
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10 // 只清理10个Key
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
			// cache miss, fetch from tree index if there
			if !ok {
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			// 将 revKeyValue.key 转换成 revision 实例
			rev := bytesToRev(rkv.key)
			// 更新 currentRev 值
			currentRev = rev.main
			if ok {
				// 当前 revKeyValue 实例对应一个 tombstone 键值对
				if isTombstone(rkv.key) {
					// 在对应的 keyIndex 实例中插入 tombstone
					if err := ki.tombstone(lg, rev.main, rev.sub); err != nil {
						lg.Warn("tombstone encountered error", zap.Error(err))
					}
					continue
				}
				// 在对应 keyIndex 实例中添加正常的 revision信息
				ki.put(lg, rev.main, rev.sub)
			} else if !isTombstone(rkv.key) {
				// 如果从内存索引中依然未查询到对应的 keyIndex 实例，则需要填充 keyIndex 实例中其他字段，并添加到内存索引中
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				// 同时会将该 keyIndex 实例添加到 kiCache 缓存中
				kiCache[rkv.kstr] = ki
			}
		}
	}()
	return rkvc, revc
}

// 在 store.restore() 方法中，通过 UnsafeRange() 方法从 BoltDB 中查询到的键值对，
// 然后通过 restoreChunk() 函数转换成 revKeyValue 实例，并写入 rkvc 通道中
func restoreChunk(lg *zap.Logger, kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	// 遍历读取到的 revision 信息
	for i, key := range keys {
		// 创建revKeyValue实例
		rkv := revKeyValue{key: key}
		// 反序列化得到KeyValue
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {
			lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}
		// 记录对应的原始Key值
		rkv.kstr = string(rkv.kv.Key)
		if isTombstone(key) {
			// 删除tombstone标识的键值对
			delete(keyToLease, rkv.kstr)
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
			keyToLease[rkv.kstr] = lid
		} else {
			delete(keyToLease, rkv.kstr)
		}

		// 将上述 revKeyValue 实例写入 rkvc 通道中，等待处理
		kvc <- rkv
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

func (s *store) setupMetricsReporter() {
	b := s.b
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInBytesDebugMu.Lock()
	reportDbTotalSizeInBytesDebug = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesDebugMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	reportDbOpenReadTxNMu.Lock()
	reportDbOpenReadTxN = func() float64 { return float64(b.OpenReadTxN()) }
	reportDbOpenReadTxNMu.Unlock()
	reportCurrentRevMu.Lock()
	reportCurrentRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.currentRev)
	}
	reportCurrentRevMu.Unlock()
	reportCompactRevMu.Lock()
	reportCompactRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.compactMainRev)
	}
	reportCompactRevMu.Unlock()
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
		lg.Panic(
			"cannot append tombstone mark to non-normal revision bytes",
			zap.Int("expected-revision-bytes-size", revBytesLen),
			zap.Int("given-revision-bytes-size", len(b)),
		)
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}

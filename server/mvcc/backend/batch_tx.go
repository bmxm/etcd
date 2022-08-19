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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type BucketID int

type Bucket interface {
	// ID returns a unique identifier of a bucket.
	// The id must NOT be persisted and can be used as lightweight identificator
	// in the in-memory maps.
	ID() BucketID
	Name() []byte
	// String implements Stringer (human readable name).
	String() string

	// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
	// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
	// is known to never overwrite any key so range is safe.
	IsSafeRangeBucket() bool
}

// etcd 对批量读写事务的抽象
type BatchTx interface {
	ReadTx // 内嵌了ReadTx接口

	// 创建Bucket
	UnsafeCreateBucket(bucket Bucket)

	UnsafeDeleteBucket(bucket Bucket)

	// 向指定Bucket中添加键值对
	UnsafePut(bucket Bucket, key []byte, value []byte)

	// 向指定Bucket中添加键值对，与UnsafePut()方法的区别是，其中会将对应Bucket实例的
	// 填充比例设置为90%，这样可以在顺序写入时，提高Bucket的利用率
	UnsafeSeqPut(bucket Bucket, key []byte, value []byte)

	// 在指定Bucket中删除指定的键值对
	UnsafeDelete(bucket Bucket, key []byte)

	// Commit commits a previous tx and begins a new writable one.
	// 提交当前的读写事务，之后立即打开一个新的读写事务
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTx struct {
	sync.Mutex

	// 该batchTx实例底层封装的bolt.Tx实例，即BoltDB层面的读写事务
	tx *bolt.Tx

	// 该batchTx实例关联的backend实例
	backend *backend

	// 当前事务中执行的修改操作个数，在当前读写事务提交时，该值会被重置为0。
	pending int
}

func (t *batchTx) Lock() {
	t.Mutex.Lock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

// BatchTx interface embeds ReadTx interface. But RLock() and RUnlock() do not
// have appropriate semantics in BatchTx interface. Therefore should not be called.
// TODO: might want to decouple ReadTx and BatchTx

func (t *batchTx) RLock() {
	panic("unexpected RLock")
}

func (t *batchTx) RUnlock() {
	panic("unexpected RUnlock")
}

// 用BoltDB的API创建了相应的Bucket实例
func (t *batchTx) UnsafeCreateBucket(bucket Bucket) {
	_, err := t.tx.CreateBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketExists {
		t.backend.lg.Fatal(
			"failed to create a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

func (t *batchTx) UnsafeDeleteBucket(bucket Bucket) {
	err := t.tx.DeleteBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketNotFound {
		t.backend.lg.Fatal(
			"failed to delete a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
// UnsafePut()方法和 UnsafeSeqPut()方法都是通过调用batchTx.unsafePut()方法实现的，两者是通过seq参数进行区分的
func (t *batchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, true)
}

func (t *batchTx) unsafePut(bucketType Bucket, key []byte, value []byte, seq bool) {
	// 通过BoltDB提供的API获取指定的Bucket实例
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}

	// 如果是顺序写入，则将填充率设置成90%
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}

	// 调用BoltDB提供的API写入键值对
	if err := bucket.Put(key, value); err != nil {
		t.backend.lg.Fatal(
			"failed to write to a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Error(err),
		)
	}
	// 递增pending
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	// 从key位置开始遍历
	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		// 记录符合条件的key值
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketType Bucket, key []byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	err := bucket.Delete(key)
	if err != nil {
		t.backend.lg.Fatal(
			"failed to delete a key",
			zap.Stringer("bucket-name", bucketType),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucket, visitor)
}

// 会遍历指定 Bucket 的缓存和 Bucket 中的全部键值对，并通过visitor回调函数处理这些遍历到的键值对
func unsafeForEach(tx *bolt.Tx, bucket Bucket, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket.Name()); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		// 当前读写事务中未进行任何修改操作，则无须开启新事务
		if t.pending == 0 && !stop {
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		// 通过BoltDB提供的API提交当前读写事务
		err := t.tx.Commit()
		// gofail: var afterCommit struct{}

		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())

		//递增backend.comm
		atomic.AddInt64(&t.backend.commits, 1)

		// 重置pending字
		t.pending = 0
		if err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
	}
	if !stop {
		// 开启新的读写事务
		t.tx = t.backend.begin(true)
	}
}

type batchTxBuffered struct {
	batchTx
	buf txWriteBuffer
}

// batchTxBuffered 和 batchTx 的初始化过程
func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		// 创建内嵌的batchTx实例
		batchTx: batchTx{backend: backend},

		// 创建txWriteBuffer缓冲区
		buf: txWriteBuffer{
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bucket2seq: make(map[BucketID]bool),
		},
	}
	// 开启一个读写事务
	tx.Commit()
	return tx
}

// batchTx 和 batchTxBuffered 中都重写了内嵌的 sync.Mutex 的Unlock()方法（但是它们并没有重写Lock()方法）。
func (t *batchTxBuffered) Unlock() {
	// 检测当前读写事务中是否发生了修改操作
	if t.pending != 0 {
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.

		// 更新readTx的缓存
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()

		// 如果当前事务的修改操作数量是否达到上限，则提交当前事务，开启新事务
		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}
	t.batchTx.Unlock()
}

func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBuffered) commit(stop bool) {
	if t.backend.hooks != nil {
		t.backend.hooks.OnPreCommitUnsafe(t)
	}

	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *batchTxBuffered) unsafeCommit(stop bool) {
	// 如果当前已经开启了只读事务，则将该事务回滚(BoltDB中的只读事务只能回滚，无法提交)
	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)

		// 清空readTx中的缓存
		t.backend.readTx.reset()
	}

	// 如果当前已经开启了读写事务，则将该事务提交，并创建新的读写事务
	t.batchTx.commit(stop)

	// 根据stop参数，决定事务开启新的只读事务
	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBuffered) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucket, key, value)
	t.buf.put(bucket, key, value)
}

func (t *batchTxBuffered) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucket, key, value)
	t.buf.putSeq(bucket, key, value)
}

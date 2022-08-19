// Copyright 2017 The etcd Authors
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
	"sort"
)

const bucketBufferInitialSize = 512

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	// 记录了 Bucket 名称与对应bucketBuffer的映射关系
	// 在bucketBuffer中缓存了对应Bucket中的键值对数据
	buckets map[BucketID]*bucketBuffer
}

// 清空buckets字段中的全部内容
func (txb *txBuffer) reset() {
	// 遍历buckets
	for k, v := range txb.buckets {
		// 删除未使用的bucketBuffer
		if v.used == 0 {
			// demote
			delete(txb.buckets, k)
		}

		// 清空使用过的bucketBuffer
		v.used = 0
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	// Map from bucket ID into information whether this bucket is edited
	// sequentially (i.e. keys are growing monotonically).
	// seq字段（bool类型），用于标记写入当前txWriteBuffer的键值对是否为顺序的。
	bucket2seq map[BucketID]bool
}

// 调用了putSeq()方法，同时将seq字段设置为false，表示非顺序写入
func (txw *txWriteBuffer) put(bucket Bucket, k, v []byte) {
	txw.bucket2seq[bucket.ID()] = false
	txw.putInternal(bucket, k, v)
}

// 该方法完成了向指定bucketBuffer添加键值对的功能
func (txw *txWriteBuffer) putSeq(bucket Bucket, k, v []byte) {
	// TODO: Add (in tests?) verification whether k>b[len(b)]
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putInternal(bucket Bucket, k, v []byte) {
	// 获取指定的bucketBuffer
	b, ok := txw.buckets[bucket.ID()]

	// 如果未查找到，则创建对应的bucketBuffer实例并保存到buckets中
	if !ok {
		b = newBucketBuffer()
		txw.buckets[bucket.ID()] = b
	}

	// 通过bucketBuffer.add()方法添加键值对
	b.add(k, v)
}

// 清空 txWriteBuffer
func (txw *txWriteBuffer) reset() {
	txw.txBuffer.reset()
	for k := range txw.bucket2seq {
		v, ok := txw.buckets[k]
		if !ok {
			delete(txw.bucket2seq, k)
		} else if v.used == 0 {
			txw.bucket2seq[k] = true
		}
	}
}

// 该方法会将当前 txWriteBuffer 中键值对合并到指定的 txReadBuffer 实例中，这样就可以达到更新只读事务缓存的效果
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	// 遍历所有的bucketBuffer
	for k, wb := range txw.buckets {
		// 从传入的 bucketBuffer 中查找指定的 bucketBuffer
		rb, ok := txr.buckets[k]

		// 如果txReadBuffer中不存在对应的bucketBuffer，则直接使用txWriteBuffer中缓存的bucketBuffer实例
		if !ok {
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}

		if seq, ok := txw.bucket2seq[k]; ok && !seq && wb.used > 1 {
			// 如果当前txWriteBuffer中的键值对是非顺序写入的，则需要先进行排序
			// assume no duplicate keys
			sort.Sort(wb)
		}
		// 通过bucketBuffer.merge()方法，合并两个bucketBuffer实例并去重
		rb.merge(wb)
	}
	txw.reset()
	// increase the buffer version
	txr.bufVersion++
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct {
	txBuffer
	// bufVersion is used to check if the buffer is modified recently
	bufVersion uint64
}

func (txr *txReadBuffer) Range(bucket Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// 查询指定的txBuffer实例
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[BucketID]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
		bufVersion: 0,
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	// 每个元素都表示一个键值对，kv.key和kv.value都是[]byte类型。在初始化时，该切片的默认大小是512。
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.
	// 该字段记录buf中目前使用的下标位置
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, bucketBufferInitialSize), used: 0}
}

func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	// 定义Key的比较方式
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }

	// 查询0～used之间是否有指定的key
	idx := sort.Search(bb.used, f)
	if idx < 0 {
		return nil, nil
	}

	// 没有指定endKey，则只返回key对应的键值对
	if len(endKey) == 0 {
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}

	// 如果指定了endKey，则检测endKey的合法性
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}

	// 从前面查找到的idx位置开始遍历，直到遍历到endKey或是遍历的键值对个数达到limit上限为止
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	return keys, vals
}

// 提供了遍历当前 bucketBuffer 实例缓存的所有键值对的功能，其中会调用传入的 visitor() 函数处理每个键值对
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	for i := 0; i < bb.used; i++ {
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

// 添加键值对缓存的功能，当buf的空间被用尽时，会进行扩容
func (bb *bucketBuffer) add(k, v []byte) {
	// 添加键值对
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v

	// 递增use
	bb.used++

	// 当buf空间被用尽时，对其进行扩容
	if bb.used == len(bb.buf) {
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bbsrc into bb.
// 将传入的bbsr（c bucketBuffer实例）与当前的bucketBuffer进行合并，之后会对合并结果进行排序和去重
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	// 将bbsrc中的键值对添加到当前bucketBuffer中
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}

	// 复制之前，如果当前bucketBuffer是空的，则复制完键值对之后直接返回
	if bb.used == bbsrc.used {
		return
	}

	// 复制之前，如果当前bucketBuffer不是空的，则需要判断复制之后，是否需要进行排序
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	// 如果需要排序，则调用sort.Stable()函数对bucketBuffer进行排序
	// 注意，Stable()函数进行的是稳定排序，即相等键值对的相对位置在排序之后不会改变
	sort.Stable(bb)

	// remove duplicates, using only newest update
	widx := 0

	// 清除重复的key，使用key的最新值
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		// 新添加的的键值对覆盖原有的键值对
		bb.buf[widx] = bb.buf[ridx]
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}

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

package mvcc

import (
	"context"

	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
)

// 该结构体实现了 ReadView 接口。结构体 readView 中只有一个 kv 字段（KV 类型），
// 结构体 readView 实现 FirstRev() 方法、Rev() 方法和 Range() 方法的方式基本类似：
// 先调用 kv.Read() 获取只读事务（TxnRead 实例），然后调用 TxnRead 实例的对应方法完成相应操作，最后调用 TxnRead.End() 方法结束事务。
type readView struct{ kv KV }

func (rv *readView) FirstRev() int64 {
	tr := rv.kv.Read(ConcurrentReadTxMode, traceutil.TODO())
	defer tr.End()
	return tr.FirstRev()
}

func (rv *readView) Rev() int64 {
	tr := rv.kv.Read(ConcurrentReadTxMode, traceutil.TODO())
	defer tr.End()
	return tr.Rev()
}

func (rv *readView) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	tr := rv.kv.Read(ConcurrentReadTxMode, traceutil.TODO())
	defer tr.End()
	return tr.Range(ctx, key, end, ro)
}

// 结构体 writeView 实现了 WriteView 接口，其中也是只有一个 kv 字段（KV 类型）。
// writeView 实现 DeleteRange()、Put()的方式与上面介绍的 readView.FirstRev() 方法等类似
type writeView struct{ kv KV }

func (wv *writeView) DeleteRange(key, end []byte) (n, rev int64) {
	tw := wv.kv.Write(traceutil.TODO())
	defer tw.End()
	return tw.DeleteRange(key, end)
}

func (wv *writeView) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	tw := wv.kv.Write(traceutil.TODO())
	defer tw.End()
	return tw.Put(key, value, lease)
}

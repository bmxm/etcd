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

package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

// etcd-raft 模块为了保证Raft核心协议实现的简洁，并没有直接提供WAL日志与快照相关的实现逻辑，
// 而是将其实现独立到 etcd-wal 模块与 etcd-snap 模块，其中提供了操作WAL日志文件与快照文件的相关实现。
// 上层模块自身调用etcd-wal模块与etcd-snap模块即可完成读写WAL日志文件与快照文件的相关操作。

// WAL（Write-ahead logging）是etcd实现一致性的重要手段之一。
// 下面是处理一条 Entry 记录的大致流程：
//（1）当客户端向 etcd 集群发送了一次请求之后，请求中的封装 Entry 记录会先被交给etcd-raft模块进行处理，其中，etcd-raft模块会先将Entry记录保存到raftLog.unstable中。
//（2）etcd-raft模块将该Entry记录封装到前面介绍的Ready实例中，返回给上层模块进行持久化。
//（3）当上层模块收到待持久化的Entry记录之后，会先将其记录到WAL日志文件中，然后进行持久化操作，最后通知etcd-raft模块进行处理。
//（4）此时etcd-raft模块就会将该Entry记录从unstable移动到storage中保存。
//（5）待该Entry记录被复制到集群中的半数以上节点时，该Entry记录会被Leader节点确认为已提交（committed），并封装进Ready实例返回给上层模块。
//（6）此时上层模块即可将该Ready实例中携带的待应用Entry记录应用到状态机中。

const (
	// metadataType: 该类型日志记录的 Data 字段中保存了一些元数据，在每个WAL文件的开头，都会记录一条metadataType类型的日志记录。
	// entryType: 该类型日志记录的Data字段中保存的是Entry记录，也就是客户端发送给服务端处理的数据，例如，raftexample示例中客户端发送的键值对数据。
	// stateType: 该类型日志记录的Data字段中保存了当前集群的状态信息（即HardState），在每次批量写入entryType类型日志记录之前，都会先写入一条stateType类型的日志记录。
	// crcType: 该类型的日志记录主要用于数据校验。
	// snapshotType: 该类型的日志记录中保存了快照数据的相关信息（即walpb.Snapshot，注意，其中不包含完整的快照数据）。

	metadataType int64 = iota + 1
	entryType
	stateType
	crcType
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrMetadataConflict             = errors.New("wal: conflicting metadata found")
	ErrFileNotFound                 = errors.New("wal: file not found")
	ErrCRCMismatch                  = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch             = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound             = errors.New("wal: snapshot not found")
	ErrSliceOutOfRange              = errors.New("wal: slice bounds out of range")
	ErrMaxWALEntrySizeLimitExceeded = errors.New("wal: max entry size limit exceeded")
	ErrDecoderNotFound              = errors.New("wal: decoder not found")
	crcTable                        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL 类似于 MySQL 的 binlog。
// 该日志中记录着每次修改数据的指令和修改任期，并通过 Log index 标注了当前是第几条日志，以此作为同步进度的依据。

// 其中，Leader 的日志永远不会被删除，所有的 Follower 都会保持和 Leader 完成一致，如果存在差异也会被强制覆盖。
// 同时，每个日志都有"写入"和"commit"两个阶段，在选举时，每个服务会根据还未 commit 的 Log index 进度优先选择同步进度最大的节点，以此保证选举出的 Leader 拥有最全的数据。
// Leader 在任期内向各节点发送同步请求，其实就是按顺序向各节点推送一条条日志。如果 Leader 同步的进度比 Follower 超前，Follower 就会拒绝本次同步。
// Leader 收到拒绝后，会从后往前一条条找出日志中还未同步的部分或者有差异的部分，然后开始一个个往后覆盖实现同步。

// Leader 和 Follower 的日志同步进度是通过日志 index 来确认的。Leader 对日志内容和顺序有绝对的决策权，当它发现自己的日志和 Follower 的日志有差异时，为了确保多个副本的数据是完全一致的，它会强制覆盖 Follower 的日志。
//
// 那么 Leader 是怎么识别出 Follower 的日志与自己的日志有没有差异呢？实际上，Leader 给 Follower 同步日志的时候，会同时带上 Leader 上一条日志的任期和索引号，与 Follower 当前的同步进度进行对比。
// 对比分为两个方面：一方面是对比 Leader 和 Follower 当前日志中的 index、多条操作日志和任期；另一方面是对比 Leader 和 Follower 上一条日志的 index 和任期。
// 如果有任意一个不同，那么 Leader 就认为 Follower 的日志与自己的日志不一致，这时候 Leader 会一条条倒序往回对比，直到找到日志内容和任期完全一致的 index，然后从这个 index 开始正序向下覆盖。
// 同时，在日志数据同步期间，Leader 只会 commit 其所在任期内的数据，过往任期的数据完全靠日志同步倒序追回。
//
// 这样一条条推送同步有些缓慢，效率不高，这导致 Raft 对新启动的服务不是很友好。所以 Leader 会定期打快照，通过快照合并之前修改日志的记录，来降低修改日志的大小。
// 而同步进度差距过大的 Follower 会从 Leader 最新的快照中恢复数据，按快照最后的 index 追赶进度。

// 如何保证读取数据的强一致性？
// 那从 Follower 的角度来看，它怎么保证自己对外提供的数据是最新的呢？这里有个小技巧，就是 Follower 在收到查询请求时，会顺便问一下 Leader 当前最新 commit 的 log index 是什么。
// 如果这个 log index 大于当前 Follower 同步的进度，就说明 Follower 的本地数据不是最新的，这时候 Follower 就会从 Leader 获取最新的数据返回给客户端。可见，保证数据强一致性的代价很大。
//

// etcd-raft 模块为了保证 Raft 核心协议实现的简洁，并没有直接提供WAL日志与快照相关的实现逻辑，而是将其实现独立到etcd-wal模块与etcd-snap模块，
// 其中提供了操作WAL日志文件与快照文件的相关实现。上层模块自身调用etcd-wal模块与etcd-snap模块即可完成读写WAL日志文件与快照文件的相关操作。

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
//
// 对外提供了 WAL 日志文件管理的核心 API。
// 在操纵 WAL 日志时，对应的 WAL 实例有 read 和 append 两种模式，
// 新创建的 WAL 实例处于 append 模式，该模式下只能向 WAL 中追加日志。
// 当恢复一个节点时（例如，宕机节点的重启），就需要读取WAL日志的内容，
// 此时刚打开的 WAL 实例处于 read 模式，它只能读取日志记录，
// 当读取完全部的日志之后，WAL 实例转换成 append 模式，可以继续向其追加日志记录。
//
// 在 WAL 日志文件中，日志记录是通过 Record 表示的，该结构体通过 Protocol Buffers 生成，
// 主要用于序列化和反序列化日志记录
type WAL struct {
	lg *zap.Logger

	// 存放WAL日志文件的目录路径。
	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	// 根据dir路径创建的File实例。
	dirFile *os.File

	// 在每个WAL日志文件的头部，都会写入 metadata 元数据。
	metadata []byte // metadata recorded at the head of each WAL

	// WAL 日志记录的追加是批量的，在每次批量写入 entryType 类型的日志之后，都会再追加一条 stateType 类型的日志记录，
	// 在 HardState 中记录了当前的 Term、当前节点的投票结果和已提交日志的位置。
	state raftpb.HardState // hardstate recorded at the head of WAL

	// 每次读取WAL日志时，并不会每次都从头开始读取，而是通过这里的start字段指定具体的起始位置。
	// walpb.Snapshot中的Index字段记录了对应快照数据所涵盖的最后一条 Entry 记录的索引值，Term 字段则记录了对应 Entry记录的Term值。
	// 在读取WAL日志文件时，我们就可以根据这些信息，找到合适的位置并开始读取记录。
	start walpb.Snapshot // snapshot to start reading

	// 负责在读取WAL日志文件时，将二进制数据反序列化成Record实例。
	decoder   *decoder     // decoder to decode records
	readClose func() error // closer for decode reader

	unsafeNoSync bool // if set, do not fsync

	// 读写WAL日志时需要加锁同步
	mu sync.Mutex

	// WAL中最后一条Entry记录的索引值。
	enti uint64 // index of the last entry saved to the wal

	// 负责将写入WAL日志文件的Record实例进行序列化成二进制数据。
	encoder *encoder // encoder to encode records

	// 当前WAL实例管理的所有WAL日志文件对应的句柄。
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)

	// filePipeline 实例负责创建新的临时文件。
	fp *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll
// after the file is Open.
//
// 该方法不仅会创建WAL实例，而是做了很多初始化工作，其大致步骤如下：
// （1）创建临时目录，并在临时目录中创建编号为“0-0”的WAL日志文件，WAL日志文件名由两部分组成，一部分是seq（单调递增），另一部分是该日志文件中的第一条日志记录的索引值。
// （2）尝试为该WAL日志文件预分配磁盘空间。
// （3）向该WAL日志文件中写入一条crcType类型的日志记录、一条metadataType类型的日志记录及一条snapshotType类型的日志记录。
// （4）创建WAL实例关联的filePipeline实例。
// （5）将临时目录重命名为WAL.dir字段指定的名称
//
// 这里之所以先使用临时目录完成初始化操作再将其重命名的方式，主要是为了让整个初始化过程看上去是一个原子操作。
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	// 检测文件夹是否存在
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if lg == nil {
		lg = zap.NewNop()
	}

	// keep temporary wal directory so WAL initialization appears atomic
	// 得到临时目录的路径
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		// 清空临时目录中的文件
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	defer os.RemoveAll(tmpdirpath)

	// 创建临时文件夹
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		lg.Warn(
			"failed to create a temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", dirpath),
			zap.Error(err),
		)
		return nil, err
	}

	// 第一个WAL日志文件的路径(文件名为0-0)
	p := filepath.Join(tmpdirpath, walName(0, 0))
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		lg.Warn(
			"failed to flock an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}

	// 移动临时文件的offset到文件结尾处，
	// 注意Seek()方法的第二个参数(0是相对文件开头，1是相对当前 offset,2是相对文件结尾)
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		lg.Warn(
			"failed to seek an initial WAL file",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}

	// 对新建的临时文件 进行空间预分配，默认值是 64MB(SegmentSizeBytes)
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		lg.Warn(
			"failed to preallocate an initial WAL file",
			zap.String("path", p),
			zap.Int64("segment-bytes", SegmentSizeBytes),
			zap.Error(err),
		)
		return nil, err
	}

	// 创建WAL实例
	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	// 创建写WAL日志文件的encoder
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	// 将WAL日志文件对应的LockedFile实例记录到locks字段中，表示当前WAL实例正在管理该日志文件
	w.locks = append(w.locks, f)
	// 创建一条 crcType 类型的日志写入WAL日志文件
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	// 将元数据封装成一条 metadataType 类型的日志记录写入WAL日志文件
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	// 创建一条空的 snapshotType 类型的日志记录写入临时文件
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}

	logDirPath := w.dir
	// 将临时目录重命名，并创建WAL实例关联的 filePipline 实例
	if w, err = w.renameWAL(tmpdirpath); err != nil {
		lg.Warn(
			"failed to rename the temporary WAL directory",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", logDirPath),
			zap.Error(err),
		)
		return nil, err
	}

	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// directory was renamed; sync parent dir to persist rename
	// 临时目录重命名之后，需要将重命名操作刷新到磁盘上
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		lg.Warn(
			"failed to open the parent data directory",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	dirCloser := func() error {
		if perr = pdir.Close(); perr != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
			return perr
		}
		return nil
	}
	start := time.Now()
	// 同步磁盘的操作
	if perr = fileutil.Fsync(pdir); perr != nil {
		dirCloser()
		lg.Warn(
			"failed to fsync the parent data directory file",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	walFsyncSec.Observe(time.Since(start).Seconds())
	if err = dirCloser(); err != nil {
		return nil, err
	}

	// 返回WAL实例
	return w, nil
}

func (w *WAL) SetUnsafeNoFsync() {
	w.unsafeNoSync = true
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		lg.Panic("failed to close WAL during cleanup", zap.Error(err))
	}
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		lg.Panic(
			"failed to rename WAL during cleanup",
			zap.Error(err),
			zap.String("source-path", w.dir),
			zap.String("rename-path", brokenDirName),
		)
	}
}

// renameWAL 方法在不同系统平台的操作略有不同，但是主要操作都是重命名临时目录并创建关联的 filePipeline 实例
func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	// 清空wal文件夹
	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}

	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	//
	// 重命名临时文件夹
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}

	// 创建 WAL 实例关联的 filePipeline
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)

	// WAL.dirFile字段记录了WAL日志目录对应的文件句柄
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	w.lg.Info(
		"closing WAL to release flock and retry directory renaming",
		zap.String("from", tmpdirpath),
		zap.String("to", w.dir),
	)
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// 说明：
// · 在打开WAL日志文件时，可以指定此次打开日志文件的模式是只读模式还是读写模式。
// · 在打开WAL日志文件时，都会指定随后的读取操作的起始 index，而不是每次都从第一个日志文件开始读取。
// wal 模块根据上面两点，提供了 Open() 和 OpenForRead() 两个函数，两者的区别在于：
// 使用 Open() 函数创建的WAL实例读取完全部日志后，可以继续追加日志；
// 而 OpenForRead() 函数创建的WAL实例只能用于读取日志，不能追加日志。
// 这两个方法底层都是通过调用openAtIndex()方法完成

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

// 需要注意snap和write参数，snap.Index指定了日志读取的起始位置，
// write参数则指定了打开日志文件的模式
func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap)
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	// 创建WAL实例
	w := &WAL{
		lg:    lg,
		dir:   dirpath,
		start: snap,

		// 创建用于读取日志记录的decoder实例，这里并没有初始化encoder，所以还不能写入日志记录
		decoder: newDecoder(rs...),

		// 如果是只读模式，在读取完全部日志文件之后，则会调用该方法关闭所有日志文件
		readClose: closer,

		// 当前WAL实例管理的日志文件
		locks: ls,
	}

	// 如果是读写模式，读取完全部日志文件之后，由于后续有追加操作，所以不需要关闭日志文件；
	// 另外，还要为 WAL 实例创建关联的 filePipeline 实例，用于产生新的日志文件
	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	// 获取全部的WAL日志文件名，并且这些文件名会进行排序?
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, -1, err
	}

	// 根据WAL日志文件名的规则，查找上面得到的所有文件名，找到 index 最大且 index 小于 snap.Index
	// 的WAL日志文件，并返回该文件在 names 数组中的索引(nameIndex)
	nameIndex, ok := searchIndex(lg, names, snap.Index)
	if !ok || !isValidSeq(lg, names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)

	// 从 nameIndex 开始读取剩余的 WAL 日志文件
	for _, name := range names[nameIndex:] {
		// 获取 WAL 日志的绝对路径
		p := filepath.Join(dirpath, name)

		if write {
			// 以读写模式打开WAL日志文件，并且对文件加锁
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			// 以只读模式打开文件
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}

		// 将文件句柄记录到rs切片中
		rs = append(rs, rcs[len(rcs)-1])
	}

	// 后面关闭文件时，会调用该函数
	closer := func() error { return closeAll(lg, rcs...) }

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
//
// ReadAll suppresses WAL entries that got overridden (i.e. a newer entry with the same index
// exists in the log). Such a situation can happen in cases described in figure 7. of the
// RAFT paper (http://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf).
//
// ReadAll may return uncommitted yet entries, that are subject to be overriden.
// Do not apply entries that have index > state.commit, as they are subject to change.
//
// 通过 Open() 函数或 OpenForRead() 函数创建 WAL 实例之后，就可以调用其 ReadAll() 方法读取日志了。
// ReadAll() 方法首先从 WAL.start 字段指定的位置开始读取日志记录，读取完毕之后，会根据读取的情况进行一系列异常处理。
// 然后根据当前 WAL 实例的模式进行不同的处理：
//   - 如果处于读写模式，则需要先对后续的WAL日志文件进行填充并初始化WAL.encoder字段，为后面写入日志做准备；
//   - 如果处于只读模式下，则需要关闭所有的日志文件。另外需要注意的是，WAL.ReadAll()方法的几个返回值都是从日志记录中读取到的。
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 创建Record实例
	rec := &walpb.Record{}

	// 解码器，负责读取日志文件，并将日志数据反序列化成Record实例
	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	decoder := w.decoder

	// 标识是否找到了start字段对应的日志记录
	var match bool
	// 循环读取 WAL 日志文件中的数据，多个 WAL 日志文件的切换是在 decoder 中完成的
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		// 根据日志记录的类型进行分类处理
		switch rec.Type {
		case entryType:
			// 反序列化Record.Data中记录的数据，得到Entry实例
			e := mustUnmarshalEntry(rec.Data)

			// 将start之后的Entry记录添加到ents中保存
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			if e.Index > w.start.Index {
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := e.Index - w.start.Index - 1
				if up > uint64(len(ents)) {
					// return error before append call causes runtime panic
					return nil, state, nil, ErrSliceOutOfRange
				}
				// The line below is potentially overriding some 'uncommitted' entries.
				ents = append(ents[:up], e)
			}
			w.enti = e.Index

		case stateType:
			// 更新待返回的 HardState 状态信息
			state = mustUnmarshalState(rec.Data)

		case metadataType:
			// 检测 metadata 数据是否发生冲突，如果冲突，则抛出异常
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data

		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}

			// 更新decodr.crc字段
			decoder.updateCRC(rec.Crc)

		case snapshotType:
			var snap walpb.Snapshot
			// 解析快照相关的数据
			pbutil.MustUnmarshal(&snap, rec.Data)

			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// 到这里，读取WAL日志文件的操作就完成了
	// 根据WAL.locks字段是否有值判断当前WAL是什么模式
	switch w.tail() {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		//
		// 对于只读模式，并不需要将全部的日志都读出来，因为以只读模式打开WAL日志文件时，并没有加锁，
		// 所以最后一条日志记录可能只写了一半，从而导致io.ErrUnexpectedEOF异
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		//
		// 对于读写模式，则需要将日志记录全部读出来，所以此处不是EOF异常，则报错
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		// 将文件指针移动到读取结束的位置，并将文件后续部分全部填充为0
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	// 如果在读取过程中没有找到与start对应的日志记录，则抛出异常
	if !match {
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	// 如果是只读模式，则关闭所有日志文件
	if w.readClose != nil {
		// WAL.readClose实际指向的是WAL.CloseAll()方法
		w.readClose()
		w.readClose = nil
	}

	// 清空start字
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	// 如果是读写模式，则初始化 WAL.encoder 字段，为后面写入日志做准备
	if w.tail() != nil {
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}

	// 清空 WAL.decoder 字段，后续不能再用该WAL实例进行读取了
	w.decoder = nil

	return metadata, state, ents, err
}

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	names, err := readWALNames(lg, walDir)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// filter out any snaps that are newer than the committed hardstate
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit {
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n]
	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) (*raftpb.HardState, error) {
	var metadata []byte
	var err error
	var match bool
	var state raftpb.HardState

	rec := &walpb.Record{}

	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return nil, ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return nil, ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
			pbutil.MustUnmarshal(&state, rec.Data)
		default:
			return nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	if !match {
		return nil, ErrSnapshotNotFound
	}

	return &state, nil
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
//
// 随着 WAL 日志文件的不断写入，单个日志文件会不断变大。在前面提到过，每个日志文件的大小是有上限的，
// 该阈值由 SegmentSizeBytes 指定（默认值是64MB），该值也是日志文件预分配磁盘空间的大小。
// 当单个日志文件的大小超过该值时，就会触发日志文件的切换，该切换过程是在 WAL.cut()方法中实现
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	// 获取当前日志文件的文件指针位置
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}

	// 根据当前的文件指针位置，将后续填充内容 Truncate 掉，这主要是处理提早切换和预分配空间未使用的情况，
	// truncate 后可以释放该日志文件后续未使用的空间
	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	// 紧接着执行一次 WAL.sync() 方法，将修改同步刷新到磁盘上
	if err := w.sync(); err != nil {
		return err
	}

	// 根据当前最后一个日志文件的名称，确定下一个新日志文件的名称，
	// seq()方法返回当前最后一个日志文件的编号，w.enti 记录了当前最后一条日志记录的索引值
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	// 从 filePipeline 获取新建的临时文件
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	// 将临时文件的句柄保存到 WAL.locks 中
	w.locks = append(w.locks, newTail)
	prevCrc := w.encoder.crc.Sum32()

	// 创建临时文件对应的 encoder 实例，并更新到 WAL.encoder 字段中
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	// 向临时文件中追加一条 crcType 类型的日志记录
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	// 向临时文件中追加一条 metadataType 类型的日志记录
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	// 向临时文件中追加一条 stateType 类型的日志记录
	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	// 通过 WAL.sync() 方法将上述修改同步刷新到磁盘上
	if err = w.sync(); err != nil {
		return err
	}

	// 记录当前文件指针的位置，为重命名之后，重新打开文件做准备
	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 将临时文件重命名成之前得到的新日志文件名称
	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	start := time.Now()

	// 将重命名这一操作同步刷新到磁盘上，fsync 操作不仅会将文件数据刷新到磁盘上，
	// 还会将文件的元数据也刷新到磁盘上(例如，文件的长度和名称等)
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// reopen newTail with its new path so calls to Name() match the wal filename format
	// 关闭临时文件对应的句柄
	newTail.Close()

	// 打开重命名后的新日志文件
	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	// 将文件指针的位置移动到之前保存的位置
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	// 将 WAL.locks 中最后一项更新成新日志文件
	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	// 创建新日志文件对应的 encoder 实例，并更新到 WAL.encoder 字段中
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		// 使用 encoder.flush() 方法进行同步刷新
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}

	if w.unsafeNoSync {
		return nil
	}

	start := time.Now()
	// 使用操作系统的fdatasync将数据真正刷新到磁盘上
	err := fileutil.Fdatasync(w.tail().File)

	took := time.Since(start)
	// 这里会对该刷新操作的执行时间进行监控，如果刷新操作执行的时间长于指定的时间(默认值是1s)，则输出警告日志
	if took > warnSyncDuration {
		w.lg.Warn(
			"slow fdatasync",
			zap.Duration("took", took),
			zap.Duration("expected-duration", warnSyncDuration),
		)
	}
	walFsyncSec.Observe(took.Seconds())

	return err
}

func (w *WAL) Sync() error {
	return w.sync()
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			w.lg.Error("failed to close WAL", zap.Error(err))
		}
	}

	return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	// 将Entry记录序列化
	b := pbutil.MustMarshal(e)

	// 将序列化后的数据封装成 entryType 类型的 Record 记录
	rec := &walpb.Record{Type: entryType, Data: b}

	// 通过encoder.encode()方法追加日志记录，省略相关的异常处理代码
	if err := w.encoder.encode(rec); err != nil {
		return err
	}

	// 更新 WAL.enti 字段，其中保存了最后一条 Entry 记录的索引值
	w.enti = e.Index
	return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// 通过WAL.ReadAll()方法读取完全部日志记录之后，WAL.encoder字段才会被初始化，
// 自此之后，我们才能通过该WAL实例向日志文件中追加日志记录。
// WAL对外提供了追加日志的方法，分别是 Save()方法和 SaveSnapshot()方法。

func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	// 边界检查，如果待写入的HardState和Entry数组都为空，则直接返回(略)；否则就需要将修改同步到磁盘
	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	// 遍历待写入的 Entry 数组，将每个 Entry 实例序列化并封装 entryType 类型的日志记录，写入日志文件
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}

	// 将状态信息(HardState)序列化并封装成 stateType 类型的日志记录，写入日志文
	if err := w.saveState(&st); err != nil {
		return err
	}

	// 获取当前日志段文件的文件指针的位置
	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// 如果未写满预分配的空间，将新日志刷新到磁盘后，即可返回
	if curOff < SegmentSizeBytes {
		// 将上述追加的日志记录同步刷新到磁盘上
		if mustSync {
			return w.sync()
		}
		return nil
	}

	// 当前文件大小已超出了预分配的空间，则需要进行日志文件的切换
	return w.cut()
}

func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	if err := walpb.ValidateSnapshotForWrite(&e); err != nil {
		return err
	}

	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
	}
	return seq
}

func closeAll(lg *zap.Logger, rcs ...io.ReadCloser) error {
	stringArr := make([]string, 0)
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			lg.Warn("failed to close: ", zap.Error(err))
			stringArr = append(stringArr, err.Error())
		}
	}
	if len(stringArr) == 0 {
		return nil
	}
	return errors.New(strings.Join(stringArr, ", "))
}

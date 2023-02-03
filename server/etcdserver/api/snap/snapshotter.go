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

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

const snapSuffix = ".snap"

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

// 随着节点的运行，会处理客户端和集群中其他节点发来的大量请求，相应的 WAL 日志量会不断增加，会产生大量的WAL日志文件，
// 另外 etcd-raft 模块中的 raftLog 中也会存储大量的 Entry 记录，这就会导致资源浪费。
// 当节点宕机之后，如果要恢复其状态，则需要从头读取全部的WAL日志文件，这显然是非常耗时的。
// 为了解决这些问题，etcd 会定期创建快照并将其保存到本地磁盘中，在恢复节点状态时会先加载快照文件，使用该快照数据将节点恢复到对应的状态，
// 之后从快照数据之后的相应位置开始读取WAL日志文件，最终将节点恢复到正确的状态。

// 与WAL日志的管理类似，etcd 并未在 etcd-raft 模块中实现将快照管理，而是将其独立到了snap 模块中。
// 其中 SnapShotter 通过文件的方式管理快照数据，它是 snapshot 模块的核心。

type Snapshotter struct {
	lg *zap.Logger

	// 指定了存储快照文件的目录位置。
	dir string
}

func New(lg *zap.Logger, dir string) *Snapshotter {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &Snapshotter{
		lg:  lg,
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

	// 创建快照文件名，快照文件的名称由三部分组成，分别是快照所涵盖的最后一条Entry记录的Term、Index和“.snap”后缀
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)

	b := pbutil.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)

	// 将序列化后的数据和校验码封装成 snappb.Snapshot 实例，
	// 这里简单了解一下 raftpb.Snapshot 和 snappb.Snapshot 的区别，
	// 前者包含了 SnapShot 数据及一些元数据（例如，该快照数据所涵盖的最后一条Entry记录的Term和Index）；
	// 后者则是在前者序列化之后的封装，其中还记录了相应的校验码等信息
	snap := snappb.Snapshot{Crc: crc, Data: b}

	// 将快照数据进行序列化
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	snapMarshallingSec.Observe(time.Since(start).Seconds())

	spath := filepath.Join(s.dir, fname)

	fsyncStart := time.Now()
	// 将snappb.Snapshot序列化后的数据写入文件，并同步刷新到磁盘
	err = pioutil.WriteAndSyncFile(spath, d, 0666)
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

	if err != nil {
		s.lg.Warn("failed to write a snap file", zap.String("path", spath), zap.Error(err))
		rerr := os.Remove(spath)
		if rerr != nil {
			s.lg.Warn("failed to remove a broken snap file", zap.String("path", spath), zap.Error(err))
		}
		return err
	}

	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

// Load returns the newest snapshot.
// 该方法会读取指定目录下的全部快照文件，并查找其中最近的可用快照文件，
// 然后通过 Snapshotter.loadSnap() 方法加载其中的快照数据
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	return s.loadMatching(func(*raftpb.Snapshot) bool { return true })
}

// LoadNewestAvailable loads the newest snapshot available that is in walSnaps.
func (s *Snapshotter) LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.loadMatching(func(snapshot *raftpb.Snapshot) bool {
		m := snapshot.Metadata
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if m.Term == walSnaps[i].Term && m.Index == walSnaps[i].Index {
				return true
			}
		}
		return false
	})
}

// loadMatching returns the newest snapshot where matchFn returns true.
func (s *Snapshotter) loadMatching(matchFn func(*raftpb.Snapshot) bool) (*raftpb.Snapshot, error) {
	// 获取全部快照文件的名称，其中会检查文件的后缀名，过滤不合法的文件
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}

	var snap *raftpb.Snapshot
	for _, name := range names {
		// 按序读取快照文件，直到读取到第一个合法的快照文件，对于读取失败的快照文件则添加“.broken”后缀，
		// 这样在下次调用snapNames()方法获取可用快照文件时，就会将其过滤掉
		if snap, err = loadSnap(s.lg, s.dir, name); err == nil && matchFn(snap) {
			return snap, nil
		}
	}
	return nil, ErrNoSnapshot
}

func loadSnap(lg *zap.Logger, dir, name string) (*raftpb.Snapshot, error) {
	// 获取快照文件的绝对路径
	fpath := filepath.Join(dir, name)

	// 读取快照文件的内容
	snap, err := Read(lg, fpath)
	if err != nil {
		// 如果读取失败，则将该快照文件的后缀名修改成“.broken”
		brokenPath := fpath + ".broken"
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", fpath), zap.Error(err))
		}
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			if lg != nil {
				lg.Warn("failed to rename a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath), zap.Error(rerr))
			}
		} else {
			if lg != nil {
				lg.Warn("renamed to a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath))
			}
		}
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(lg *zap.Logger, snapname string) (*raftpb.Snapshot, error) {
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}

	if len(b) == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot file", zap.String("path", snapname))
		}
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	// 将读取到的数据进行反序列化
	if err = serializedSnap.Unmarshal(b); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal snappb.Snapshot", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot data", zap.String("path", snapname))
		}
		return nil, ErrEmptySnapshot
	}

	// 使用 crc32 校验码验证数据是否正确
	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		if lg != nil {
			lg.Warn("snap file is corrupt",
				zap.String("path", snapname),
				zap.Uint32("prev-crc", serializedSnap.Crc),
				zap.Uint32("new-crc", crc),
			)
		}
		return nil, ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	// 反序列化 snappb.Snapsho t实例的 Data 字段，得到 raftpb.Snapshot 实例
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal raftpb.Snapshot", zap.String("path", snapname), zap.Error(err))
		}
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	filenames, err := s.cleanupSnapdir(names)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(s.lg, filenames)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}

	// 排序
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(lg *zap.Logger, names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				if lg != nil {
					lg.Warn("found unexpected non-snap file; skipping", zap.String("path", names[i]))
				}
			}
		}
	}
	return snaps
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) (names []string, err error) {
	names = make([]string, 0, len(filenames))
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			s.lg.Info("found orphaned defragmentation file; deleting", zap.String("path", filename))
			if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
				return names, fmt.Errorf("failed to remove orphaned .snap.db file %s: %v", filename, rmErr)
			}
		} else {
			names = append(names, filename)
		}
	}
	return names, nil
}

func (s *Snapshotter) ReleaseSnapDBs(snap raftpb.Snapshot) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, ".snap.db") {
			hexIndex := strings.TrimSuffix(filepath.Base(filename), ".snap.db")
			index, err := strconv.ParseUint(hexIndex, 16, 64)
			if err != nil {
				s.lg.Error("failed to parse index from filename", zap.String("path", filename), zap.String("error", err.Error()))
				continue
			}
			if index < snap.Metadata.Index {
				s.lg.Info("found orphaned .snap.db file; deleting", zap.String("path", filename))
				if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
					s.lg.Error("failed to remove orphaned .snap.db file", zap.String("path", filename), zap.String("error", rmErr.Error()))
				}
			}
		}
	}
	return nil
}

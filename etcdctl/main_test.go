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

package main

import (
	"log"
	"os"
	"strings"
	"testing"
)

func SplitTestArgs(args []string) (testArgs, appArgs []string) {
	for i, arg := range os.Args {
		switch {
		case strings.HasPrefix(arg, "-test."):
			testArgs = append(testArgs, arg)
		case i == 0:
			appArgs = append(appArgs, arg)
			testArgs = append(testArgs, arg)
		default:
			appArgs = append(appArgs, arg)
		}
	}
	return
}

// Empty test to avoid no-tests warning.
func TestEmpty(t *testing.T) {}

/**
 * The purpose of this "test" is to run etcdctl with code-coverage
 * collection turned on. The technique is documented here:
 *
 * https://www.cyphar.com/blog/post/20170412-golang-integration-coverage
 */
func TestMain(m *testing.M) {
	// don't launch etcdctl when invoked via go test
	if strings.HasSuffix(os.Args[0], "etcdctl.test") {
		return
	}

	testArgs, appArgs := SplitTestArgs(os.Args)

	os.Args = appArgs

	err := mainWithError()
	if err != nil {
		log.Fatalf("etcdctl failed with: %v", err)
	}

	// This will generate coverage files:
	os.Args = testArgs
	m.Run()
}

//  etcdctl --endpoints=etcd.etcd-dev:2379 --user root:xxx get m
//  etcdctl --endpoints etcd.etcd-dev:2379 --user root:xx get / --prefix --keys-only

/*

写入一个key
./bin/etcdctl put foo bar

绑定一个租期
./bin/etcdctl put foo bar --lease=1234abcd

只打印 value 值
./bin/etcdctl get foo --print-value-only

读取一个范围的 key (左闭右开)
./bin/etcdctl get foo foo3

通过前缀遍历 key
./bin/etcdctl get fo --prefix

限制输出的结果
./bin/etcdctl get fo --prefix --limit=1

获取指定版本的 key
./bin/etcdctl get foo1 --rev=0

获取字典序比 foo 大的key(包含)
./bin/etcdctl get foo --from-key

删除一个key
./bin/etcdctl del m

删除一个范围内的key
 ./bin/etcdctl del m m2

删除的同时返回 key-value
./bin/etcdctl del m --prev-kv

watch 某个 key (这个 key 可以不存在), 在终端中会一直观察
./bin/etcdctl watch foo

创建一个10s的租约
./bin/etcdctl lease grant 10

*/

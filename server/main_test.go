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
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdmain"
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

func TestEmpty(t *testing.T) {}

/**
 * The purpose of this "test" is to run etcd server with code-coverage
 * collection turned on. The technique is documented here:
 *
 * 此“测试”的目的是运行具有代码覆盖率的 etcd 服务器
 * 收藏开启。 该技术记录在这里：
 *
 * https://www.cyphar.com/blog/post/20170412-golang-integration-coverage
 */
func TestMain(m *testing.M) {
	fmt.Println("TestMain function running ...")
	// 打印的结果？
	fmt.Println(os.Args[0])

	// 没搞懂什么意思，先跳过
	if time.Now().Unix() > 0 {
		m.Run()
		return
	}

	// don't launch etcd server when invoked via go test
	if strings.HasSuffix(os.Args[0], ".test") {
		log.Printf("skip launching etcd server when invoked via go test")
		return
	}

	testArgs, appArgs := SplitTestArgs(os.Args)

	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, syscall.SIGINT, syscall.SIGTERM)
	go etcdmain.Main(appArgs)
	<-notifier

	// This will generate coverage files:
	os.Args = testArgs
	m.Run()
}

// *testing.M 函数可以在测试函数执行之前做一些其他操作
// 测试程序有时需要在测试之前或之后进行额外的设置或拆卸。测试有时还需要控制在主线程上运行哪些代码。
// 为了支持这些和其他情况，如果测试文件包含一个函数：func TestMain(m *testing.M)
// 那么生成的测试将调用 TestMain（m） ，而不是直接运行测试。

func TestConn(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},

		// 创建client的首次连接超时，这里传了5秒，如果5秒都没有连接成功就会返回err；
		// 值得注意的是，一旦client创建成功，我们就不用再关心后续底层连接的状态了，client内部会重连。
		DialTimeout: 5 * time.Second,
	})
	defer func() {
		_ = cli.Close()
	}()

	if err != nil {
		t.Logf("connect failed, err: %+v", err)
		return
	}
	t.Log("connect success")

	_, err = cli.Put(context.Background(), "name", "wangxiaoming")
	if err != nil {
		log.Fatal(err)
	}
	res, err := cli.Get(context.Background(), "name")
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range res.Kvs {
		log.Printf("%s:%s", string(kv.Key), string(kv.Value))
	}
}

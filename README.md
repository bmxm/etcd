# etcd

[![Go Report Card](https://goreportcard.com/badge/github.com/etcd-io/etcd?style=flat-square)](https://goreportcard.com/report/github.com/etcd-io/etcd)
[![Coverage](https://codecov.io/gh/etcd-io/etcd/branch/master/graph/badge.svg)](https://codecov.io/gh/etcd-io/etcd)
[![Tests](https://github.com/etcd-io/etcd/actions/workflows/tests.yaml/badge.svg)](https://github.com/etcd-io/etcd/actions/workflows/tests.yaml)
[![asset-transparency](https://github.com/etcd-io/etcd/actions/workflows/asset-transparency.yaml/badge.svg)](https://github.com/etcd-io/etcd/actions/workflows/asset-transparency.yaml)
[![codeql-analysis](https://github.com/etcd-io/etcd/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/etcd-io/etcd/actions/workflows/codeql-analysis.yml)
[![self-hosted-linux-arm64-graviton2-tests](https://github.com/etcd-io/etcd/actions/workflows/self-hosted-linux-arm64-graviton2-tests.yml/badge.svg)](https://github.com/etcd-io/etcd/actions/workflows/self-hosted-linux-arm64-graviton2-tests.yml)
[![Docs](https://img.shields.io/badge/docs-latest-green.svg)](https://etcd.io/docs)
[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/etcd-io/etcd)
[![Releases](https://img.shields.io/github/release/etcd-io/etcd/all.svg?style=flat-square)](https://github.com/etcd-io/etcd/releases)
[![LICENSE](https://img.shields.io/github/license/etcd-io/etcd.svg?style=flat-square)](https://github.com/etcd-io/etcd/blob/main/LICENSE)

**Note**: The `main` branch may be in an *unstable or even broken state* during development. For stable versions, see [releases][github-release].

![etcd Logo](logos/etcd-horizontal-color.svg)

etcd is a distributed reliable key-value store for the most critical data of a distributed system, with a focus on being:

* *Simple*: well-defined, user-facing API (gRPC)
* *Secure*: automatic TLS with optional client cert authentication
* *Fast*: benchmarked 10,000 writes/sec
* *Reliable*: properly distributed using Raft

etcd is written in Go and uses the [Raft][] consensus algorithm to manage a highly-available replicated log.

etcd is used [in production by many companies](./ADOPTERS.md), and the development team stands behind it in critical deployment scenarios, where etcd is frequently teamed with applications such as [Kubernetes][k8s], [locksmith][], [vulcand][], [Doorman][], and many others. Reliability is further ensured by [**rigorous testing**](https://github.com/etcd-io/etcd/tree/main/tests/functional).

See [etcdctl][etcdctl] for a simple command line client.

[raft]: https://raft.github.io/
[k8s]: http://kubernetes.io/
[doorman]: https://github.com/youtube/doorman
[locksmith]: https://github.com/coreos/locksmith
[vulcand]: https://github.com/vulcand/vulcand
[etcdctl]: https://github.com/etcd-io/etcd/tree/main/etcdctl

## Community meetings

etcd contributors and maintainers have monthly (every four weeks) meetings at 11:00 AM (USA Pacific) on Thursday.

An initial agenda will be posted to the [shared Google docs][shared-meeting-notes] a day before each meeting, and everyone is welcome to suggest additional topics or other agendas.

[shared-meeting-notes]: https://docs.google.com/document/d/16XEGyPBisZvmmoIHSZzv__LoyOeluC5a4x353CX0SIM/edit


Time:
- [Jan 10th, 2019 11:00 AM video](https://www.youtube.com/watch?v=0Cphtbd1OSc&feature=youtu.be)
- [Feb 7th, 2019 11:00 AM video](https://youtu.be/U80b--oAlYM)
- [Mar 7th, 2019 11:00 AM video](https://youtu.be/w9TI5B7D1zg)
- [Apr 4th, 2019 11:00 AM video](https://youtu.be/oqQR2XH1L_A)
- [May 2nd, 2019 11:00 AM video](https://youtu.be/wFwQePuDWVw)
- [May 30th, 2019 11:00 AM video](https://youtu.be/2t1R5NATYG4)
- [Jul 11th, 2019 11:00 AM video](https://youtu.be/k_FZEipWD6Y)
- [Jul 25, 2019 11:00 AM video](https://youtu.be/VSUJTACO93I)
- [Aug 22, 2019 11:00 AM video](https://youtu.be/6IBQ-VxQmuM)
- [Sep 19, 2019 11:00 AM video](https://youtu.be/SqfxU9DhBOc)
- Nov 14, 2019 11:00 AM
- Dec 12, 2019 11:00 AM
- Jan 09, 2020 11:00 AM
- Feb 06, 2020 11:00 AM
- Mar 05, 2020 11:00 AM
- Apr 02, 2020 11:00 AM
- Apr 30, 2020 11:00 AM
- May 28, 2020 11:00 AM
- Jun 25, 2020 11:00 AM
- Jul 23, 2020 11:00 AM
- Aug 20, 2020 11:00 AM
- Sep 17, 2020 11:00 AM
- Oct 15, 2020 11:00 AM
- Nov 12, 2020 11:00 AM
- Dec 10, 2020 11:00 AM

Join Hangouts Meet: [meet.google.com/umg-nrxn-qvs](https://meet.google.com/umg-nrxn-qvs)

Join by phone: +1 405-792-0633‬ PIN: ‪299 906‬#


## Getting started

### Getting etcd

The easiest way to get etcd is to use one of the pre-built release binaries which are available for OSX, Linux, Windows, and Docker on the [release page][github-release].

For more installation guides, please check out [play.etcd.io](http://play.etcd.io) and [operating etcd](https://etcd.io/docs/latest/op-guide).

直接在根目录执行 `./build` 会在bin目录生成`etcd etcdctl etcdutl`三个可执行文件
For those wanting to try the very latest version, [build the latest version of etcd][dl-build] from the `main` branch. This first needs [*Go*](https://golang.org/) installed ([version 1.16+](/go.mod#L3) is required). All development occurs on `main`, including new features and bug fixes. Bug fixes are first targeted at `main` and subsequently ported to release branches, as described in the [branch management][branch-management] guide.

[github-release]: https://github.com/etcd-io/etcd/releases
[branch-management]: https://etcd.io/docs/latest/branch_management
[dl-build]: https://etcd.io/docs/latest/dl-build#build-the-latest-version

### Running etcd

First start a single-member cluster of etcd.

If etcd is installed using the [pre-built release binaries][github-release], run it from the installation location as below:

```bash
/tmp/etcd-download-test/etcd
```

The etcd command can be simply run as such if it is moved to the system path as below:

```bash
mv /tmp/etcd-download-test/etcd /usr/local/bin/
etcd
```

If etcd is [built from the main branch][dl-build], run it as below:

```bash
./bin/etcd
```

This will bring up etcd listening on port 2379 for client communication and on port 2380 for server-to-server communication.

Next, let's set a single key, and then retrieve it:

```
etcdctl put mykey "this is awesome"
etcdctl get mykey
```

etcd is now running and serving client requests. For more, please check out:

- [Interactive etcd playground](http://play.etcd.io)
- [Animated quick demo](https://etcd.io/docs/latest/demo)

### etcd TCP ports

The [official etcd ports][iana-ports] are 2379 for client requests, and 2380 for peer communication.

[iana-ports]: http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt

### Running a local etcd cluster

First install [goreman](https://github.com/mattn/goreman), which manages Procfile-based applications.

Our [Procfile script](./Procfile) will set up a local example cluster. Start it with:

```bash
goreman start
```

This will bring up 3 etcd members `infra1`, `infra2` and `infra3` and optionally etcd `grpc-proxy`, which runs locally and composes a cluster.

Every cluster member and proxy accepts key value reads and key value writes.

Follow the steps in [Procfile.learner](./Procfile.learner) to add a learner node to the cluster. Start the learner node with:

```bash
goreman -f ./Procfile.learner start
```

### Next steps

Now it's time to dig into the full etcd API and other guides.

- Read the full [documentation][].
- Explore the full gRPC [API][].
- Set up a [multi-machine cluster][clustering].
- Learn the [config format, env variables and flags][configuration].
- Find [language bindings and tools][integrations].
- Use TLS to [secure an etcd cluster][security].
- [Tune etcd][tuning].

[documentation]: https://etcd.io/docs/latest
[api]: https://etcd.io/docs/latest/learning/api
[clustering]: https://etcd.io/docs/latest/op-guide/clustering
[configuration]: https://etcd.io/docs/latest/op-guide/configuration
[integrations]: https://etcd.io/docs/latest/integrations
[security]: https://etcd.io/docs/latest/op-guide/security
[tuning]: https://etcd.io/docs/latest/tuning

## Contact

- Mailing list: [etcd-dev](https://groups.google.com/forum/?hl=en#!forum/etcd-dev)
- IRC: #[etcd](irc://irc.freenode.org:6667/#etcd) on freenode.org
- Planning/Roadmap: [milestones](https://github.com/etcd-io/etcd/milestones), [roadmap](./ROADMAP.md)
- Bugs: [issues](https://github.com/etcd-io/etcd/issues)

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## Reporting bugs

See [reporting bugs](https://etcd.io/docs/latest/reporting-bugs) for details about reporting any issues.

## Reporting a security vulnerability

See [security disclosure and release process](security/README.md) for details on how to report a security vulnerability and how the etcd team manages it.

## Issue and PR management

See [issue triage guidelines](https://etcd.io/docs/current/triage/issues/) for details on how issues are managed.

See [PR management](https://etcd.io/docs/current/triage/prs/) for guidelines on how pull requests are managed.

## etcd Emeritus Maintainers

These emeritus maintainers dedicated a part of their career to etcd and reviewed code, triaged bugs, and pushed the project forward over a substantial period of time. Their contribution is greatly appreciated.

* Fanmin Shi
* Anthony Romano

### License

etcd is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

etcd 是典型的读多写少存储，在我们实际业务场景中，读一般占据 2/3 以上的请求。

起源，CoreOS 团队需要一个协调服务来存储服务配置信息，提供分布式锁等能力。
单副本存在单点故障，多副本又会引入数据一致性的问题。
Raft 将一致性问题分解为 Leader选举、日志同步、安全性三个相对独立的子问题，只要集群一半以上的节点存活就可以提供服务。
存储引擎上，Zookeeper 使用的是 Concurrent HashMap, 而 etcd 使用的是简单内存树。
v 0.1 实现了简单的增删改查，但读数据一致性无法保证。
v 0.2 支持通过指定 consistent 模式，从 Leader 读取数据，并将 Test And Set 机制修正为 CAS(Compare And Swap),解决了原子更新的问题。

当使用 Kubernetes 声明式 API 部署服务时， Kubernetes 的控制器通过 etcd Watch 机制，会实时监听资源的变化事件，对比实际状态与期望状态是否一致，并采取协调动作使其一致。
k8s 更新数据的时候，通过 CAS 机制保证并发场景下的原子更新，并通过对 key 设置 TTL 来存储 Event 事件，提升 k8s 集群的可观测性，基于 TTL 特性，Event 事件 key 到期后可自动删除。

etcd v2 
1. 不支持范围查询和分页。
2. 不支持多 key 事务。
3. 内存型、不支持保存 key 历史版本的数据库，只在内存中使用滑动窗口保存了最近的1000条变更事件。

HTTP/1.x API 没有压缩机制，批量拉取较多的Pod时容易导致 APIServer 和 etcd 出现 CPU 高负载、OOM、丢包等问题。

redis 主备异步复制，可能丢数据，集群版本可以承载上T数据。
etcd 使用raft保证读写一致性，性能和redis差距较大，db大小一般不超过8g.
# 轮转验证记录

## 历史验证记录

以下数据只对应各自测试版本与环境。正式发布按[发布验收](release.md)执行。

- Linux：`go vet ./...`、`go test -race -count=3 ./...` 通过。
- Windows amd64、macOS arm64：交叉构建通过，未在目标系统运行。
- 故障注入覆盖封存、重开、压缩、发布、删除原件及重启后的记录核对。另有异步关闭、DST 和旧归档隔离测试；隔离测试验证 v2 不读取、迁移或清理旧归档，不是实际降级回滚演练。未执行生产灰度或断电测试。

## 性能基线

Linux amd64、Go 1.27.2、i7-14700KF、tmpfs、1 KiB 记录。普通同步写入 P99 为 0.775–0.821 µs，0 allocs/op；每次切卷并压缩的压力场景 P99 为 2.62–5.38 ms。异步并发场景计入末尾排空和同步，约 883–944 MB/s，1 alloc/op，队列峰值达到配置的 1024 条。这不是生产磁盘延迟或逐条 fsync 的测量。

~~~sh
go test -run '^$' -bench 'Benchmark(Rolling|AsyncDelivery)' -benchtime=150ms -count=3
~~~

## 异步维护复测

改造前相同 tmpfs 环境的自适应 150 ms 切卷基准 P99 为 1.52–2.07 ms。改造后固定 1,000 次 1 KiB 写入（每条触发切卷，第一条除外），三轮 P99 为 0.097–0.413 ms；写入结束时尚有 832–846 个待处理卷，Close 收尾耗时 124–189 ms。两组迭代方式不同，不据此计算加速倍数。磁盘积压无硬上限。

~~~sh
go test -run '^$' -bench 'BenchmarkRolling/roll-every-write' -benchtime=1000x -count=3
~~~

## Go 1.26 复审补强

Go 1.26.0 下 Linux vet 和 10 轮竞态测试通过；Windows amd64 与 macOS arm64 测试交叉编译通过，原生运行列入 CI 矩阵，该次复审未取得其结果。

本地磁盘（非 tmpfs），512 次操作、每种历史规模两轮，吞吐计入 Open、节奏等待和 Close：

| 历史卷 | 写入 P99 | 端到端吞吐 | 采样待处理字节峰值 | Close |
| --- | --- | --- | --- | --- |
| 32 | 1.85–1.89 ms | 13.38–13.42 MB/s | 140288–280576 B | 2.49–5.33 ms |
| 1024 | 2.33–2.34 ms | 13.30 MB/s | 561152 B | 22.59–26.79 ms |

复现：将 TMPDIR 指向目标磁盘上的独立临时目录（不是模块根目录），运行 `GOTOOLCHAIN=go1.26.0 go test -run "^$" -bench "^BenchmarkRollingPaced" -benchtime=512x -count=2`。积压为采样值，可能漏掉采样间峰值；节奏负载不是饱和吞吐测试。

## 候选验证记录

- `ad27ff5`：本地 Linux、Go 1.26.0 的 `go vet`、主模块竞态测试与 [v0.1.0 → v2 → v0.1.0 往返测试](../tests/migration/migration_test.go)通过；往返测试已纳入原生 CI。
- 同一提交用临时本地 Git 镜像提供 `v2.0.0-rc.1`，独立模块无 `replace` 安装并通过往返测试。该标签不是远端发布物。
- 仍待同一候选版本的远端原生 CI 结果与从远端安装的独立模块冒烟结果。

# logrotate

轻量 Go 日志滚动库：按大小、按天，或两者叠加。零第三方依赖。

当前写入路径始终固定为配置的 `Filename`（便于 `tail -f`）。归档命名：

| 模式 | 归档名 |
|------|--------|
| 纯按大小 | `app.log.1`、`app.log.2`、… |
| 按天 / 叠加 | `app.log.2026-07-23`、`app.log.2026-07-23.1`、… |

## 安装

```bash
go get github.com/mapleafgo/logrotate
```

本地尚未推送时可用 replace：

```go
// go.mod
require github.com/mapleafgo/logrotate v0.0.0
replace github.com/mapleafgo/logrotate => ../logrotate
```

## 用法

### 生产推荐：异步（高并发）

同步 `Writer` **非并发安全**（单 goroutine 独占）。多协程写日志请走异步：`Write` 只入队，单刷盘协程串行写底层；`Close` 排空队列再 `Sync`/`Close`。

```go
import (
    "log/slog"

    "github.com/mapleafgo/logrotate"
)

base, err := logrotate.Open(logrotate.Config{
    Filename:   "logs/app.log",
    MaxSizeMB:  50,  // 单文件上限 MiB；0 = 关闭
    MaxBackups: 3,   // 见下方语义
    Daily:      true,
    MaxAgeDays: 7,   // 按文件名日期清理；0 = 关闭
})
if err != nil {
    panic(err)
}
w, err := logrotate.OpenAsync(base, logrotate.AsyncConfig{
    QueueSize:  4096,
    FullPolicy: logrotate.FullBlock, // 或 FullDropNewest
})
if err != nil {
    panic(err)
}
defer w.Close()

slog.SetDefault(slog.New(slog.NewJSONHandler(w, nil)))
```

进程退出顺序：**停业务写日志 → `Close()`**（异步路径会 drain + fsync + 关文件）。

`Write` 返回 nil 只表示**入队成功**，不表示已落盘。刷盘错误见 `LastError()`；`Close` 也会带回刷盘过程中的错误。丢弃计数见 `Dropped()`（`FullDropNewest`）。

### 同步 Writer（单协程）

仅适合单 goroutine 写（测试或极简程序）。勿跨协程共享同一 `*Writer`。

```go
w, err := logrotate.Open(logrotate.Config{
    Filename:   "logs/app.log",
    MaxSizeMB:  50,
    MaxBackups: 3,
    Daily:      true,
    MaxAgeDays: 7,
})
if err != nil {
    panic(err)
}
defer w.Close()
slog.SetDefault(slog.New(slog.NewJSONHandler(w, nil)))
```

## 配置语义

| 字段 | 说明 |
|------|------|
| `Filename` | 必填，当前写入路径 |
| `MaxSizeMB` | `>0` 启用按大小；写前判断，单条超限仍写出 |
| `Daily` | `true` 启用按自然日轮转（使用系统/容器本地时区，由 `TZ` 环境变量控制） |
| `MaxBackups` | 纯按大小（非 Daily）：保留 `.1`…`.N` 个编号；`Daily`（含叠加）：按唯一日期保留最近 N 天（同日编号不裁） |
| `MaxAgeDays` | 仅 `Daily` 时生效，删除文件名日期早于 cutoff 的归档（非 Daily 静默不生效） |
| `SyncOnWrite` | 每次 `Write` 后 fsync（很慢） |
| `DisableSync` | 关闭轮转前与 `Close` 时的 fsync（默认开启落盘） |

`MaxSizeMB` 与 `Daily` 都关闭时，Writer 仅 append（自动建目录）。

`MaxBackups` / `MaxAgeDays` 为负时 `Open` 返回错误。

## 行为摘要

1. **纯按大小**：编号滚动；`Rename` 失败时**不会** `O_TRUNC` 当前文件，而是恢复 append 并返回错误。
2. **纯按天**：跨日将当前文件 rename 为 `Filename.YYYY-MM-DD`，再新建当前路径。
3. **叠加**：跨日与同日超大小均归档到带日期的名字；同日多次切分为 `.YYYY-MM-DD.1`、`.2`…（同日编号不裁，靠 `MaxAgeDays` 跨日清理；调大 `MaxSizeMB` 可减少切分频次）
4. **落盘**：默认轮转关文件前与 `Close` 时 fsync；可用 `Sync()` 主动刷盘。
5. **并发**：同步 `Writer` **非并发安全**；多协程请用 `OpenAsync`（单刷盘协程独占底层 Writer）。
6. **异步**：`FullBlock` 队列满时阻塞；`FullDropNewest` 丢最新条并返回 `ErrDropped`；`LastError()` 为最近一次刷盘错误。
7. **不做**：压缩、按小时/周、SIGHUP 重开。

## 可靠性边界

| 场景 | 保证 |
|------|------|
| 正常 `Write` 成功 | 字节进入内核页缓存；默认非每条 fsync |
| `Close` / 轮转成功 | 默认 fsync 后再关/归档，降低崩溃丢尾 |
| 编号轮转 `Rename` 失败 | 当前文件内容保留，返回错误，句柄恢复为 append |
| 异步 `Close` | 已成功入队的日志排空后落盘；调用中被关停的 `Write` 返回已关闭错误 |
| 异步入队成功 | 不保证已刷盘；错误见 `LastError()` / `Close` |
| 进程 `kill -9` / 断电 | 未 fsync 的尾部仍可能丢（可用 `SyncOnWrite` 换吞吐） |

## 与 codex-api-gateway

逻辑提取自 `codex-api-gateway` 的 `internal/logging/rotate.go`（按大小部分），并扩展按天、叠加、fsync 与异步包装。网关侧接入可作为后续独立变更。

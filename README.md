# logrotate

[![Go Reference](https://pkg.go.dev/badge/github.com/mapleafgo/logrotate.svg)](https://pkg.go.dev/github.com/mapleafgo/logrotate)

轻量 Go 日志滚动库：按大小或按天。零第三方依赖。

当前写入路径始终固定为配置的 `Filename`（便于 `tail -f`）。两种模式由 `Daily` 决定，完全独立：

| 模式 | 归档名 |
|------|--------|
| 纯按大小（`Daily=false`） | `app.log.1`、`app.log.2`、… |
| 按天（`Daily=true`） | `app.log.2026-07-23`、`app.log.2026-07-23.1`、… |

开启 `Compress` 后：

| 模式 | 压缩归档名 |
|------|-----------|
| 纯按大小 | `app.log.1.gz`、`app.log.2.gz`、… |
| 按天 | `app.log.2026-07-23.gz`（同日所有归档追加到一个 multi-member gzip） |

## 安装

```bash
go get github.com/mapleafgo/logrotate@v0.1.0
```

API 文档见 [pkg.go.dev](https://pkg.go.dev/github.com/mapleafgo/logrotate)。

本地联调或使用未发布版本时，可用 replace 指向本地路径：

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
    Filename:       "logs/app.log",
    MaxFileSizeMB:  50,  // 单文件上限 MiB；0 = 关闭
    MaxBackups:     3,   // 纯按大小模式：保留 .1..N 个编号
    Daily:          true,
    MaxDays:        7,   // 按天模式：保留最近 N 天；0 = 不限
    MaxTotalSizeMB: 500, // 归档总大小上限；0 = 不限
    Compress:       true, // 归档 gzip 压缩（可选）
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

`Write` 返回 nil 只表示**入队成功**，不表示已落盘。刷盘错误见 `LastError()`（成功写出后清除）；`Close` 也会带回刷盘过程中的错误。丢弃计数见 `Dropped()`（`FullDropNewest`）。`AsyncWriter.Sync()` 经刷盘协程串行执行，与并发 `Write` 安全；不排空队列。

### 同步 Writer（单协程）

仅适合单 goroutine 写（测试或极简程序）。勿跨协程共享同一 `*Writer`。

```go
w, err := logrotate.Open(logrotate.Config{
    Filename:       "logs/app.log",
    MaxFileSizeMB:  50,
    MaxBackups:     3,
    Daily:          true,
    MaxDays:        7,
    MaxTotalSizeMB: 500,
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
| `MaxFileSizeMB` | 单文件大小上限（MiB）。纯按大小触发编号轮转；按天触发同日归档。`>0` 启用；负值或过大（`<<20` 会溢出）时 `Open` 报错 |
| `Daily` | `true` 启用按自然日轮转（使用系统/容器本地时区，由 `TZ` 环境变量控制） |
| `MaxBackups` | 纯按大小模式：保留 `.1`…`.N` 个编号；按天模式不生效。0 = 不限 |
| `MaxDays` | 按天模式：保留最近 N 天的归档；纯按大小模式不生效。0 = 不限 |
| `MaxTotalSizeMB` | 所有归档总大小上限（MiB），超出时从最老归档开始删。两模式共用。0 = 不限 |
| `Compress` | `true` 时归档后 gzip 压缩。纯按大小：各归档独立 `.N.gz`；按天：同日合入一个 multi-member `.YYYY-MM-DD.gz`。压缩失败保留未压缩版本，不影响写入 |

`MaxFileSizeMB` 与 `Daily` 都关闭时，Writer 仅 append（自动建目录）。

`MaxBackups` / `MaxDays` / `MaxTotalSizeMB` 为负时 `Open` 返回错误。

## 行为摘要

1. **纯按大小**：编号滚动；`Rename` 失败时**不会** `O_TRUNC` 当前文件，而是恢复 append 并返回错误。
2. **按天**：跨日将当前文件归档为 `Filename.YYYY-MM-DD`，再新建当前路径。同日超 `MaxFileSizeMB` 时归档为 `.YYYY-MM-DD.1`、`.2`…（编号递增）。
3. **压缩**：纯按大小各归档独立 `.gz`；按天同日所有归档追加到一个 multi-member `.gz`（`gunzip`/`zcat` 可正常读取全部内容）。
4. **清理**：`MaxBackups`（纯按大小）链移位丢弃最老；`MaxDays`（按天）删早于最近 N 天的归档；`MaxTotalSizeMB`（两模式共用）从最老开始删直到低于阈值。
5. **落盘**：轮转关文件前与 `Close` 时 fsync；可用 `Sync()` 主动刷盘。
6. **并发**：同步 `Writer` **非并发安全**；多协程请用 `OpenAsync`（单刷盘协程独占底层 Writer）。
7. **异步**：`FullBlock` 队列满时阻塞；`FullDropNewest` 丢最新条并返回 `ErrDropped`；`Sync()` 与 `Write` 均经刷盘协程串行执行；`LastError()` 为当前刷盘错误状态（成功后清除）。
8. **不做**：按小时/周、SIGHUP 重开。

## 可靠性边界

| 场景 | 保证 |
|------|------|
| 正常 `Write` 成功 | 字节进入内核页缓存 |
| `Close` / 轮转成功 | fsync 后再关/归档，降低崩溃丢尾 |
| 编号轮转 `Rename` 失败 | 当前文件内容保留，返回错误，句柄恢复为 append |
| 异步 `Close` | 已成功入队的日志排空后落盘 |
| 异步入队成功 | 不保证已刷盘；错误见 `LastError()` / `Close` |
| 进程 `kill -9` / 断电 | 未 fsync 的尾部仍可能丢 |

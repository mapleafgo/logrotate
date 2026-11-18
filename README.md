# logrotate v2

轻量 Go 日志轮转库，支持按自然日和大小组合切卷，无第三方依赖。需要 Go 1.26 或更高版本。

发布支持范围为 Linux、Windows 和 macOS；不支持 Plan 9。原生 CI 和外部模块验证的要求见[发布验收](docs/logback-rolling-acceptance.md#发布验收)。

v2 发布后可用：

~~~sh
go get github.com/mapleafgo/logrotate/v2@v2.0.0
~~~

~~~go
package main

import (
    "log"
    "log/slog"
    "time"

    "github.com/mapleafgo/logrotate/v2"
)

func main() {
    w, err := logrotate.Open(logrotate.Config{
        Filename: "logs/app.log", Daily: true, Location: time.UTC,
        MaxFileMiB: 128, HistoryDays: 7,
        TotalArchiveMiB: 2048, Compress: true,
    })
    if err != nil {
        log.Fatal(err)
    }
    slog.New(slog.NewJSONHandler(w, nil)).Info("application started")
    if err := w.Close(); err != nil {
        log.Fatal(err)
    }
}
~~~

## 轮转与保留

大小配置以整数 MiB 为单位（1 MiB = 1,048,576 字节）。

- `Filename` 是固定的活动路径。它必须指向普通文件，不接受符号链接或 `..` 路径段。新文件默认权限为 `0600`；替换文件和压缩归档沿用源文件权限位，受 umask 限制。
- 非空写入前判断是否切卷：`Daily` 启用时按 `Location` 的自然日切分（nil 表示本地时区）；`MaxFileMiB` 非零时按大小切分。两者可组合。超大单条不拆分，不创建空卷。
- 归档位于 `Filename + ".archives"`，按 `YYYY-MM-DD.N` 命名；启用 `Compress` 后每卷独立 gzip。压缩成功发布前保留原件。
- `HistoryDays` 包含今天，先删除过期卷，再按从旧到新清理到 `TotalArchiveMiB` 限额。两个限制可独立启用，零值关闭；总量仅计算正式归档，不是磁盘硬配额。恢复、压缩和清理由单个后台 worker 串行执行，启动及切卷不等待维护完成。维护失败可通过底层 `Writer.MaintenanceError()` 查询，不计入 `AsyncWriter.LastError()`；`Close` 会返回尚未解决的维护错误。磁盘上的待压缩卷可在重启后继续处理。
- 活动文件重开失败时，刚封存的卷不参与删除；后续写入重开成功后，后台重新评估保留策略。若受保护及本轮无法删除的正式归档合计已超过容量上限，后台停止按容量删卷并报告错误；按日期清理不受此限制。

## 并发和退出

同步 `Writer` 由单个 goroutine 使用；多 goroutine 写入时，用 `OpenAsync(w, AsyncConfig{})` 包装并转交底层所有权。默认队列满时阻塞；`FullDropNewest` 会返回 `ErrDropped`。异步 `Write` 成功只表示已入队，`Sync` 同步此前入队的记录。`Sync` 不等待归档维护。退出时先停止生产者，再检查 `Close` 的返回值；关闭会等待已触发的归档维护收尾，积压可能延长关闭时间。后台写入失败不会被后续成功掩盖。使用相对路径时，Writer 关闭前不要改变进程工作目录。

## 从旧版升级

切换到带 `/v2` 的 import 路径，将旧大小字段改名（`MaxFileSizeMB` → `MaxFileMiB`，`MaxTotalSizeMB` → `TotalArchiveMiB`），数值不变；按下表迁移 `MaxDays`。`MaxBackups` 没有按份数的一对一替代。停止旧 writer 并关闭后，再打开同一活动路径。v2 不读取、迁移或删除旧归档；请单独安排旧归档清理并更新采集规则。活动文件和归档目录由单个 writer 独占，不与外部轮转工具混用。

| 旧版配置 | v2 配置 |
| --- | --- |
| `Daily: true`，`MaxDays: N`（`N > 0`） | `HistoryDays: N+1`。旧版保留今天及之前 N 个自然日，v2 的天数已包含今天。要保留相同的日期边界，请沿用旧版的本地时区（`Location: nil`，且进程时区不变）。 |
| `Daily: false`（纯按大小轮转） | `HistoryDays: 0`。旧版纯大小模式不应用 `MaxDays`；v2 即使关闭 `Daily`，正数 `HistoryDays` 也会按日期清理归档。 |
| `MaxDays: 0` | `HistoryDays: 0`，关闭按日期清理，不影响容量限制。 |

### 回滚到旧版

1. 停止日志生产者，关闭 v2 writer 并检查 `Close` 的返回值，确认没有 writer 仍在写入或维护归档；若报错，先核对活动文件和待处理归档。
2. 备份当前活动文件、旧归档与整个 `Filename + ".archives"` 目录（含待处理卷）。单独保留 v2 归档，不要改名为旧格式、混入旧归档或覆盖当前活动文件。
3. 恢复升级前的应用构建（原依赖版本、不带 `/v2` 的 import 路径及旧配置）和旧版采集规则。启动旧 writer 接管同一活动路径，确认新日志能写入、切卷并被采集；v2 归档通过独立规则采集或留存，不会自动转回旧格式。

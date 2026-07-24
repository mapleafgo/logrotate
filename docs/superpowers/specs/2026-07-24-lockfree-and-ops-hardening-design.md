# logrotate：无锁 Writer 与运维加固设计

日期：2026-07-24  
状态：待用户确认后实现

## 背景与目标

当前 `Writer` 用全局 `sync.Mutex` 串行化 `Write`/`Sync`/`Close`/轮转。目标场景（高并发网关日志）不希望在每条写路径上抢同一把大锁。

生产并发入口已明确为 **`AsyncWriter`**（多写者入队 + 单 goroutine 刷盘）。本设计将：

1. 去掉 `Writer` 上的 mutex，明确单所有者契约；
2. 补齐叠加模式下的磁盘裁剪；
3. 补齐异步刷盘错误可观测性；
4. 同步更新 README 与测试。

不做：压缩、按小时/SIGHUP、文件权限配置、回调式错误通知。

## 决策摘要

| 项 | 决策 |
|----|------|
| 并发方案 | **A**：`Writer` 单 goroutine 所有；多协程只写 `AsyncWriter` |
| 叠加裁剪 | `Daily` 启用且 `MaxBackups>0` 时，按**日期**保留最近 N 天（与是否 `MaxSize` 无关） |
| `LastError` | 保留刷盘过程中**最近一次**非 nil 错误 |
| 锁保留范围 | 异步层保留 `closed` channel、`writers` WaitGroup、`closeOnce`、`errMu`（保护 `writeErr`/`LastError`）；**写路径无大锁** |

## 架构

```
业务 goroutine × N
        │ Write(p) 拷贝入队
        ▼
   AsyncWriter.ch
        │ 单 loop 串行
        ▼
   *Writer（无 mutex）
        │ 轮转 / append / fsync
        ▼
     磁盘文件
```

- `*Writer`：文件系统状态（`file`/`size`/`day`）的唯一所有者是调用它的那一个 goroutine（生产路径即 async loop）。
- `*AsyncWriter`：channel 提供多生产者同步；loop 是唯一调用 `sink.Write` 的协程。

## 组件变更

### 1. `Writer` 去锁

- 删除 `mu sync.Mutex` 及所有 `Lock`/`Unlock`。
- 所有内部 `*Locked` 方法可重命名为无 Locked 后缀（实现时顺手清理命名）。
- 导出 API 文档必须写明：
  - `Write`/`Sync`/`Close` **非并发安全**；
  - 多 goroutine 写请使用 `OpenAsync`。
- **不**提供运行时竞态检测包装（避免生产开销）；单测改为不跨协程直接写 `*Writer`。

### 2. 叠加 / 按天清理语义（`cleanup`）

目标语义（统一、可文档化）：

| 配置 | 行为 |
|------|------|
| `Daily && MaxAgeDays > 0` | 删除文件名日期 `< dayString(now - MaxAgeDays)` 的归档 |
| `Daily && MaxBackups > 0` | 按**唯一日期**从旧到新，只保留最近 `MaxBackups` 个日期的全部同日归档 |
| 同日编号过多 | 仍由 `pruneSameDay`：同日内保留最新 `MaxBackups` 个文件（编号语义不变） |
| 非 Daily | 不按日期清理；纯按大小仍靠编号链 `MaxBackups` |

相对现状的关键变化：

- **以前**：跨日按个数裁剪仅在「纯按天且 `MaxAgeDays==0`」；`Daily+MaxSize` 只靠 `MaxAgeDays`。
- **现在**：只要 `Daily && MaxBackups>0`，无论是否按大小，都做「最近 N 个日期」裁剪。

`MaxAgeDays` 与 `MaxBackups` 可同时生效：先按年龄删，再按日期个数裁。

### 3. `AsyncWriter` 错误可观测

- 新增 `LastError() error`：返回后台刷盘遇到的最近一次错误；无则 nil。
- `Write` 返回 nil 只表示入队成功，不表示已落盘。
- `setWriteErr`：始终更新为**最近一次**错误（覆盖先前值）。
- `Close`：仍返回 close 过程中的首个错误；若 Sync/Close 无错而历史上有 `writeErr`，则返回该错误。
- **不**在每次 `Write` 时同步返回历史刷盘错误（避免改变「入队即成功」契约）；调用方可周期性读 `LastError`。

### 4. 文档

- README：生产推荐 `Open` + `OpenAsync`；明确 `Writer` 单线程；更新 Daily 下 `MaxBackups` 日期裁剪；说明 `LastError` / 入队≠落盘。
- 包注释与导出类型 godoc 同步。

## 错误处理与可靠性

| 场景 | 保证 |
|------|------|
| 同步路径 Rename 失败 | 仍恢复 append，不 O_TRUNC（不变） |
| 轮转前 fsync 失败 | 仍中止轮转并返回错误（不变） |
| 异步队列满 `FullBlock` | 阻塞（不变） |
| 异步 `FullDropNewest` | `ErrDropped` + `Dropped()`（不变） |
| 异步刷盘失败 | 记入 `LastError`；已入队后续条目继续尝试写 |
| 进程退出 | 先停业务写 → `AsyncWriter.Close()` drain + Sync + Close |

## 测试计划

1. **调整**：删除或改写「多协程直接写 `*Writer`」用例 → 改为多协程写 `AsyncWriter`，校验内容完整（在备份足够时）。
2. **保留**：大小/按天/叠加/MaxAge/同日空洞/Rename 失败/Sync-Close（均单线程驱动 `Writer`）。
3. **新增**：
   - `Daily+MaxSize+MaxBackups`：多日归档只留最近 N 天；
   - `LastError`：注入失败 `FileSink`，确认覆盖为最近一次，且 `Write` 入队仍成功；
   - `Close` 仍能带回刷盘错误。
4. `go test` / `go test -race` 全绿。

## 非目标

- 不给同步 `Writer` 做 lock-free 多写者。
- 不引入 `LockedWriter` 包装（YAGNI）。
- 不改默认文件权限、不做压缩。

## 实现顺序建议

1. 测试先行：叠加裁剪新语义 + `LastError` + 并发经 Async 的改写（红）。
2. `cleanup` 语义与文档字符串。
3. `Writer` 去锁 + 方法重命名清理。
4. `LastError` 实现。
5. README + 全量测试 / race。

## 成功标准

- 生产路径：业务多协程 → `AsyncWriter`，写路径无 `Writer.mu`。
- `Daily` + 大小 + `MaxBackups` 不会无限堆积跨日归档。
- 运维可查 `LastError()` / `Dropped()`。
- 测试与 race 通过；覆盖率不明显回退。

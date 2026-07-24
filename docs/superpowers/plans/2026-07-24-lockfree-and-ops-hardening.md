# 无锁 Writer 与运维加固 Implementation Plan

> **For agentic workers:** 本仓库当前无 git；跳过 commit 步骤。按任务顺序 TDD 实现。

**Goal:** 去掉 `Writer` 写路径大锁；统一 Daily 下按日期的 `MaxBackups` 裁剪；为 `AsyncWriter` 增加 `LastError()`。

**Architecture:** 生产多写者只写 `AsyncWriter`（channel 同步 + 单 loop 刷盘）；底层 `*Writer` 单所有者、无 mutex。清理语义：`Daily && MaxBackups>0` 时按唯一日期保留最近 N 天。

**Tech Stack:** Go 1.22+，仅标准库。

**Spec:** [docs/superpowers/specs/2026-07-24-lockfree-and-ops-hardening-design.md](../specs/2026-07-24-lockfree-and-ops-hardening-design.md)

## Global Constraints

- 零第三方依赖
- 不引入 `LockedWriter` / 压缩 / SIGHUP
- `Write` 入队成功 ≠ 已落盘
- `LastError` = 最近一次刷盘错误
- 无 git 时跳过 commit

## 文件职责

| 文件 | 职责 |
|------|------|
| `rotate.go` | 同步 Writer、轮转、清理；去锁；cleanup 新语义 |
| `async.go` | AsyncWriter；`LastError` |
| `rotate_test.go` | 全部测试 |
| `README.md` | 用法与语义 |

---

### Task 1: Daily+MaxSize 下 MaxBackups 按日期裁剪

**Files:**
- Modify: `rotate.go`（`cleanupLocked`）
- Test: `rotate_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestDailySizeMaxBackupsKeepsRecentDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	// 模拟多日归档（含同日编号）
	for _, name := range []string{
		"app.log.2026-07-20",
		"app.log.2026-07-21",
		"app.log.2026-07-21.1",
		"app.log.2026-07-22",
		"app.log.2026-07-23",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Daily + MaxSize + MaxBackups=2：应保留 22、23 两天，删 20、21（含 .1）
	w, err := Open(Config{
		Filename: path, Daily: true, MaxSizeMB: 1, MaxBackups: 2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	if _, err := os.Stat(path + ".2026-07-20"); !os.IsNotExist(err) {
		t.Fatal("expected 2026-07-20 removed")
	}
	if _, err := os.Stat(path + ".2026-07-21"); !os.IsNotExist(err) {
		t.Fatal("expected 2026-07-21 removed")
	}
	if _, err := os.Stat(path + ".2026-07-21.1"); !os.IsNotExist(err) {
		t.Fatal("expected 2026-07-21.1 removed")
	}
	mustExist(t, path+".2026-07-22")
	mustExist(t, path+".2026-07-23")
}
```

- [ ] **Step 2: 跑测确认失败** — `go test -run TestDailySizeMaxBackupsKeepsRecentDays -count=1`
  Expected: FAIL（旧逻辑不裁 `Daily+MaxSize` 的跨日个数）

- [ ] **Step 3: 改 `cleanup`**

将「纯按天且 MaxAgeDays==0」条件改为：`Daily` 已保证进入函数后，只要 `maxBackups > 0` 就做日期个数裁剪（`MaxAgeDays` 先执行）。

```go
// 只要 MaxBackups>0：按唯一日期保留最近 N 天（与是否 MaxSize 无关）。
if w.maxBackups > 0 {
	days := uniqueDays(archives)
	// ... 删最旧日期
}
```

- [ ] **Step 4: 全量相关测试通过** — `go test -count=1 ./...`

---

### Task 2: AsyncWriter.LastError

**Files:**
- Modify: `async.go`
- Test: `rotate_test.go`

- [ ] **Step 1: 失败测试**

```go
type errSink struct {
	n    int
	errs []error
}

func (s *errSink) Write(p []byte) (int, error) {
	if s.n < len(s.errs) {
		err := s.errs[s.n]
		s.n++
		if err != nil {
			return 0, err
		}
	}
	return len(p), nil
}
func (s *errSink) Sync() error  { return nil }
func (s *errSink) Close() error { return nil }

func TestAsyncLastErrorKeepsMostRecent(t *testing.T) {
	e1 := errors.New("first")
	e2 := errors.New("second")
	sink := &errSink{errs: []error{e1, e2}}
	aw, err := OpenAsync(sink, AsyncConfig{QueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := aw.Write([]byte("x\n")); err != nil {
			t.Fatal(err)
		}
	}
	// 等待 loop 处理
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(aw.LastError(), e2) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(aw.LastError(), e2) {
		t.Fatalf("LastError want second, got %v", aw.LastError())
	}
	_ = aw.Close()
}
```

- [ ] **Step 2: 确认红** — `go test -run TestAsyncLastError -count=1` FAIL（无 LastError 或只记首次）

- [ ] **Step 3: 实现**

```go
func (aw *AsyncWriter) setWriteErr(err error) {
	aw.errMu.Lock()
	defer aw.errMu.Unlock()
	aw.writeErr = err // 最近一次
}

// LastError 返回后台刷盘最近一次错误；无则 nil。
// Write 返回 nil 只表示入队成功。
func (aw *AsyncWriter) LastError() error {
	aw.errMu.Lock()
	defer aw.errMu.Unlock()
	return aw.writeErr
}
```

- [ ] **Step 4: 绿** — `go test -run TestAsyncLastError -count=1`

---

### Task 3: Writer 去锁 + 并发测试改走 Async

**Files:**
- Modify: `rotate.go`（删 `mu`、所有 Lock/Unlock；`*Locked` 重命名可选）
- Modify: `rotate_test.go`（并发用例改 `OpenAsync`）

- [ ] **Step 1: 改写测试**
  - `TestConcurrentWritesNoRaceAndComplete` → 经 `OpenAsync`
  - `TestConcurrentWritesPreservesAllWhenBackupsEnough` → 经 `OpenAsync`
  - 所有 `w.mu.Lock()` 测试改为直接调用内部清理方法（去锁后无 mu）

- [ ] **Step 2: 去掉 Writer.mu**
  - 删除字段与所有 `Lock`/`Unlock`
  - 更新 godoc：非并发安全；多写用 Async

- [ ] **Step 3: `go test -race -count=1 ./...` 全绿**

---

### Task 4: README 与导出差评

**Files:**
- Modify: `README.md`
- Modify: `rotate.go` / `async.go` 导出注释

- [ ] 生产推荐 `Open`+`OpenAsync`
- [ ] `Writer` 单线程契约
- [ ] Daily 下 `MaxBackups` = 最近 N 天
- [ ] `LastError` / 入队≠落盘
- [ ] `go test -race -count=1 ./...` 再跑一遍

## Spec 覆盖自检

- 无锁 Writer → Task 3
- 叠加 MaxBackups 按日裁 → Task 1
- LastError 最近一次 → Task 2
- 文档 → Task 4
- 测试调整 → Task 1–3

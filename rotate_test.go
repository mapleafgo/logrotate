package logrotate

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSizeRotationCreatesNumberedBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// 测试用：把阈值降到 64 字节，便于快速滚动
	w.maxSize = 64
	defer w.Close()

	line := strings.Repeat("a", 40) + "\n"
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()

	mustExist(t, path)
	mustExist(t, path+".1")
}

func TestSizeRotationMaxBackupsZeroDropsHistory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxSizeMB: 1, MaxBackups: 0})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxSize = 64
	defer w.Close()

	line := strings.Repeat("b", 40) + "\n"
	for i := 0; i < 3; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()

	mustExist(t, path)
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("expected no .1 backup, err=%v", err)
	}
}

func TestDailyRotationArchivesByDate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxBackups: 7})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	day1 := time.Date(2026, 7, 23, 10, 0, 0, 0, time.Local)
	day2 := time.Date(2026, 7, 24, 1, 0, 0, 0, time.Local)
	w.now = func() time.Time { return day1 }
	w.day = w.dayString(day1)

	if _, err := w.Write([]byte("day1-line\n")); err != nil {
		t.Fatalf("write day1: %v", err)
	}

	w.now = func() time.Time { return day2 }
	if _, err := w.Write([]byte("day2-line\n")); err != nil {
		t.Fatalf("write day2: %v", err)
	}
	_ = w.Close()

	mustExist(t, path)
	mustExist(t, path+".2026-07-23")

	cur, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if !strings.Contains(string(cur), "day2-line") {
		t.Fatalf("current should contain day2, got %q", cur)
	}
	old, err := os.ReadFile(path + ".2026-07-23")
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if !strings.Contains(string(old), "day1-line") {
		t.Fatalf("archive should contain day1, got %q", old)
	}
}

func TestDailyAndSizeCombined(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxSizeMB: 1, MaxBackups: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	day := time.Date(2026, 7, 23, 12, 0, 0, 0, time.Local)
	w.now = func() time.Time { return day }
	w.day = w.dayString(day)
	w.maxSize = 64

	line := strings.Repeat("c", 40) + "\n"
	for i := 0; i < 4; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()

	mustExist(t, path)
	mustExist(t, path+".2026-07-23")
}

func TestMaxAgeDaysCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	w, err := Open(Config{Filename: path, Daily: true, MaxAgeDays: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// 先 Open 再建归档，避免 Open 时用真实时钟误删
	oldPath := path + ".2026-07-01"
	keepPath := path + ".2026-07-22"
	if err := os.WriteFile(oldPath, []byte("old\n"), 0o644); err != nil {
		t.Fatalf("write old: %v", err)
	}
	if err := os.WriteFile(keepPath, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("write keep: %v", err)
	}
	w.now = func() time.Time {
		return time.Date(2026, 7, 24, 8, 0, 0, 0, time.Local)
	}
	w.cleanup()
	_ = w.Close()

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected old archive removed, err=%v", err)
	}
	mustExist(t, keepPath)
}

func TestDailyMaxBackupsKeepsRecentDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	for _, d := range []string{"2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23"} {
		if err := os.WriteFile(path+"."+d, []byte(d+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", d, err)
		}
	}

	w, err := Open(Config{Filename: path, Daily: true, MaxBackups: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Open 已 cleanup；再确认
	_ = w.Close()

	if _, err := os.Stat(path + ".2026-07-20"); !os.IsNotExist(err) {
		t.Fatalf("expected 2026-07-20 removed")
	}
	if _, err := os.Stat(path + ".2026-07-21"); !os.IsNotExist(err) {
		t.Fatalf("expected 2026-07-21 removed")
	}
	mustExist(t, path+".2026-07-22")
	mustExist(t, path+".2026-07-23")
}

// TestDailySameDayBackupsNotPruned：Daily 下 MaxBackups 只管天数，同日编号不裁。
func TestDailySameDayBackupsNotPruned(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	day := "2026-07-23"
	for _, name := range []string{
		"app.log." + day,
		"app.log." + day + ".1",
		"app.log." + day + ".2",
		"app.log." + day + ".3",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w, err := Open(Config{Filename: path, Daily: true, MaxSizeMB: 1, MaxBackups: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()
	// 同日 4 个编号归档都应保留（MaxBackups 只限制天数=1，当天内的编号不限）
	for _, suffix := range []string{"", ".1", ".2", ".3"} {
		mustExist(t, path+"."+day+suffix)
	}
}

// TestDailySizeMaxBackupsKeepsRecentDays：Daily+MaxSize 时 MaxBackups 仍按日期保留最近 N 天。
func TestDailySizeMaxBackupsKeepsRecentDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
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

func TestOpenRejectsEmptyFilename(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("expected error for empty Filename")
	}
}

func TestOpenRejectsNegativeBackups(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(Config{Filename: filepath.Join(dir, "a.log"), MaxBackups: -1}); err == nil {
		t.Fatal("expected error for negative MaxBackups")
	}
}

func TestAppendOnlyCreatesDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "app.log")
	w, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	mustExist(t, path)
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

// TestSizeRotateRenameFailureKeepsContent 模拟当前文件无法 rename 时，不得 O_TRUNC 清空。
func TestSizeRotateRenameFailureKeepsContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	// 非空目录占位 app.log.1：Remove 删不掉，Rename 到目录也会失败。
	if err := os.Mkdir(path+".1", 0o755); err != nil {
		t.Fatalf("mkdir blocker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path+".1", "x"), []byte("block"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	w, err := Open(Config{Filename: path, MaxSizeMB: 1, MaxBackups: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxSize = 64
	defer w.Close()

	payload := []byte(strings.Repeat("k", 40) + "\n")
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// 第二次应触发轮转并失败，且不得清空已有内容。
	if _, err := w.Write(payload); err == nil {
		t.Fatal("expected rotate rename failure")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("content must survive failed rotate, got %q", data)
	}
}

func TestSyncAndClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.Write([]byte("sync-me\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 关闭后再写/Sync 应明确失败
	if _, err := w.Write([]byte("x\n")); err == nil {
		t.Fatal("Write after Close should fail")
	}
	if err := w.Sync(); err == nil {
		t.Fatal("Sync after Close should fail")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "sync-me\n" {
		t.Fatalf("got %q", got)
	}
}

func TestConcurrentWritesNoRaceAndComplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxSizeMB: 1, MaxBackups: 5})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxSize = 256
	aw, err := OpenAsync(w, AsyncConfig{QueueSize: 256})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}

	const (
		goroutines = 8
		perG       = 50
	)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				line := []byte(fmt.Sprintf("g%d-%d\n", id, i))
				if _, err := aw.Write(line); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("write: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var all bytes.Buffer
	appendFile := func(p string) {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			t.Fatalf("read %s: %v", p, err)
		}
		all.Write(b)
	}
	appendFile(path)
	for i := 1; i <= 5; i++ { // 仅剩最近备份时内容不完整属预期
		appendFile(fmt.Sprintf("%s.%d", path, i))
	}
	body := all.String()
	if len(body) == 0 {
		t.Fatal("expected some log content after concurrent writes")
	}
}

// TestConcurrentWritesPreservesAllWhenBackupsEnough 在备份足够时并发写不丢行。
func TestConcurrentWritesPreservesAllWhenBackupsEnough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxSizeMB: 1, MaxBackups: 200})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxSize = 256
	aw, err := OpenAsync(w, AsyncConfig{QueueSize: 512})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}

	const (
		goroutines = 8
		perG       = 50
	)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				line := []byte(fmt.Sprintf("g%d-%d\n", id, i))
				if _, err := aw.Write(line); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("write: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var all bytes.Buffer
	appendFile := func(p string) {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			t.Fatalf("read %s: %v", p, err)
		}
		all.Write(b)
	}
	appendFile(path)
	for i := 1; i <= 200; i++ {
		p := fmt.Sprintf("%s.%d", path, i)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			break
		}
		appendFile(p)
	}
	body := all.String()
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			mark := fmt.Sprintf("g%d-%d\n", g, i)
			if !strings.Contains(body, mark) {
				t.Fatalf("missing line %q", mark)
			}
		}
	}
}

func TestAsyncWriterDrainsOnClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	aw, err := OpenAsync(base, AsyncConfig{QueueSize: 64})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}

	const n = 100
	for i := 0; i < n; i++ {
		line := []byte(fmt.Sprintf("async-%d\n", i))
		if _, err := aw.Write(line); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 底层也已关闭
	if _, err := base.Write([]byte("x\n")); err == nil {
		t.Fatal("base Write after Async Close should fail")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i := 0; i < n; i++ {
		mark := fmt.Sprintf("async-%d\n", i)
		if !strings.Contains(string(got), mark) {
			t.Fatalf("missing %q after drain", mark)
		}
	}
}

func TestAsyncWriterBlockWhenFull(t *testing.T) {
	// 用阻塞底层写，把队列堆满，验证默认 Block 策略会等待而不是丢数据。
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	gate := make(chan struct{})
	// 包装：首写阻塞，后续正常
	slow := &blockingWriter{w: base, gate: gate, blockFirst: 1}
	aw, err := OpenAsync(slow, AsyncConfig{QueueSize: 2, FullPolicy: FullBlock})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}

	// 先塞满队列（flusher 卡在第一笔）
	if _, err := aw.Write([]byte("a\n")); err != nil {
		t.Fatalf("write a: %v", err)
	}
	// 给 flusher 时间取走 a 并卡在底层
	time.Sleep(20 * time.Millisecond)
	if _, err := aw.Write([]byte("b\n")); err != nil {
		t.Fatalf("write b: %v", err)
	}
	if _, err := aw.Write([]byte("c\n")); err != nil {
		t.Fatalf("write c: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := aw.Write([]byte("d\n")) // 队列满时应阻塞
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Write should block when full, returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
		// 符合预期
	}

	close(gate) // 放行底层
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked write did not complete after ungate")
	}

	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAsyncWriterDropNewest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	gate := make(chan struct{})
	slow := &blockingWriter{w: base, gate: gate, blockFirst: 1}
	aw, err := OpenAsync(slow, AsyncConfig{QueueSize: 1, FullPolicy: FullDropNewest})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}

	if _, err := aw.Write([]byte("keep\n")); err != nil {
		t.Fatalf("write keep: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // flusher 卡在 keep

	// 填满队列
	if n, err := aw.Write([]byte("queued\n")); err != nil || n != len("queued\n") {
		t.Fatalf("queued write n=%d err=%v", n, err)
	}
	// 再写应被丢弃：返回 0 与 ErrDropped
	n, err := aw.Write([]byte("drop-me\n"))
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("want ErrDropped, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("dropped write should report 0, got %d", n)
	}
	if aw.Dropped() < 1 {
		t.Fatalf("Dropped()=%d want >=1", aw.Dropped())
	}

	close(gate)
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, _ := os.ReadFile(path)
	if strings.Contains(string(got), "drop-me") {
		t.Fatalf("dropped line should not appear: %q", got)
	}
}

type blockingWriter struct {
	w          *Writer
	gate       <-chan struct{}
	blockFirst int
	seen       atomic.Int32
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	if int(b.seen.Add(1)) <= b.blockFirst {
		<-b.gate
	}
	return b.w.Write(p)
}

func (b *blockingWriter) Sync() error  { return b.w.Sync() }
func (b *blockingWriter) Close() error { return b.w.Close() }

// errSink 按序返回预设错误，用于验证 LastError 覆盖为最近一次。
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
	for range 3 {
		if _, err := aw.Write([]byte("x\n")); err != nil {
			t.Fatal(err)
		}
	}
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
	closeErr := aw.Close()
	if !errors.Is(closeErr, e2) {
		t.Fatalf("Close want second, got %v", closeErr)
	}
}

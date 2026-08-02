package logrotate

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
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
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	defer func() { _ = w.Close() }()

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
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 0})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	defer func() { _ = w.Close() }()

	line := strings.Repeat("b", 40) + "\n"
	for i := 0; i < 3; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()
	mustExist(t, path)
	mustNotExist(t, path+".1")
}

func TestSizeRotateRenameFailureKeepsContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.Mkdir(path+".1", 0o755); err != nil {
		t.Fatalf("mkdir blocker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path+".1", "x"), []byte("block"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	defer func() { _ = w.Close() }()

	payload := []byte(strings.Repeat("k", 40) + "\n")
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("first write: %v", err)
	}
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

func TestDailyRotationArchivesByDate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxDays: 7})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

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
}

func TestDailyMaxDaysCleansOld(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.Local)
	w, err := Open(Config{Filename: path, Daily: true, MaxDays: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.now = func() time.Time { return now }
	for _, d := range []string{"2026-08-01", "2026-08-02", "2026-08-03", "2026-08-04"} {
		if err := os.WriteFile(path+"."+d, []byte(d+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w.cleanupDaily()
	_ = w.Close()
	mustNotExist(t, path+".2026-08-01")
	mustNotExist(t, path+".2026-08-02")
	mustExist(t, path+".2026-08-03")
	mustExist(t, path+".2026-08-04")
}

func TestDailySameDayMaxFileSizeCreatesNumbered(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxFileSizeMB: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()
	day := time.Date(2026, 7, 23, 12, 0, 0, 0, time.Local)
	w.now = func() time.Time { return day }
	w.day = w.dayString(day)
	w.maxFileSize = 64
	line := strings.Repeat("c", 40) + "\n"
	for i := 0; i < 4; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()
	mustExist(t, path)
	mustExist(t, path+".2026-07-23")
	mustExist(t, path+".2026-07-23.1")
}

func TestDailySameDayBackupsNotPrunedByMaxDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	day := "2026-07-23"
	w, err := Open(Config{Filename: path, Daily: true, MaxFileSizeMB: 1, MaxDays: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// 先注入 now 再创建归档，避免 Open 内部 cleanup 用真实时间误删
	w.now = func() time.Time { return time.Date(2026, 7, 23, 20, 0, 0, 0, time.Local) }
	for _, name := range []string{"app.log." + day, "app.log." + day + ".1", "app.log." + day + ".2", "app.log." + day + ".3"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w.cleanupDaily()
	_ = w.Close()
	for _, suffix := range []string{"", ".1", ".2", ".3"} {
		mustExist(t, path+"."+day+suffix)
	}
}

func TestDailySizeMaxDaysKeepsRecentDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxFileSizeMB: 1, MaxDays: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.now = func() time.Time { return time.Date(2026, 8, 6, 8, 0, 0, 0, time.Local) }
	for _, name := range []string{"app.log.2026-08-02", "app.log.2026-08-03", "app.log.2026-08-03.1", "app.log.2026-08-04", "app.log.2026-08-05"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w.cleanupDaily()
	_ = w.Close()
	mustNotExist(t, path+".2026-08-02")
	mustNotExist(t, path+".2026-08-03")
	mustNotExist(t, path+".2026-08-03.1")
	mustExist(t, path+".2026-08-04")
	mustExist(t, path+".2026-08-05")
}

func TestSizeRotationWithCompress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 2, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	defer func() { _ = w.Close() }()
	line := strings.Repeat("a", 40) + "\n"
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()
	mustExist(t, path)
	mustExist(t, path+".1.gz")
	mustNotExist(t, path+".1")
	data := readAllGzipMembers(t, path+".1.gz")
	if !bytes.Contains(data, []byte(line)) {
		t.Fatalf("gzip content missing line, got %q", data)
	}
}

func TestSizeRotationCompressChain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 2, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	defer func() { _ = w.Close() }()
	line := strings.Repeat("b", 40) + "\n"
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()
	mustExist(t, path)
	mustExist(t, path+".1.gz")
	mustNotExist(t, path+".3.gz")
}

func TestDailyRotationWithCompress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()
	day1 := time.Date(2026, 7, 23, 10, 0, 0, 0, time.Local)
	day2 := time.Date(2026, 7, 24, 1, 0, 0, 0, time.Local)
	w.now = func() time.Time { return day1 }
	w.day = w.dayString(day1)
	if _, err := w.Write([]byte("day1-content\n")); err != nil {
		t.Fatalf("write day1: %v", err)
	}
	w.now = func() time.Time { return day2 }
	if _, err := w.Write([]byte("day2-content\n")); err != nil {
		t.Fatalf("write day2: %v", err)
	}
	_ = w.Close()
	mustExist(t, path+".2026-07-23.gz")
	mustNotExist(t, path+".2026-07-23")
	mustNotExist(t, path+".2026-07-23.1")
	data := readAllGzipMembers(t, path+".2026-07-23.gz")
	if !bytes.Contains(data, []byte("day1-content\n")) {
		t.Fatalf("gzip content missing day1, got %q", data)
	}
}

func TestDailySizeCompressCombined(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, Daily: true, MaxFileSizeMB: 1, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()
	day := time.Date(2026, 7, 23, 12, 0, 0, 0, time.Local)
	w.now = func() time.Time { return day }
	w.day = w.dayString(day)
	w.maxFileSize = 64
	line := strings.Repeat("c", 40) + "\n"
	for i := 0; i < 4; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()
	mustExist(t, path)
	mustExist(t, path+".2026-07-23.gz")
	mustNotExist(t, path+".2026-07-23")
	mustNotExist(t, path+".2026-07-23.1")
	mustNotExist(t, path+".2026-07-23.1.gz")
	data := readAllGzipMembers(t, path+".2026-07-23.gz")
	if !bytes.Contains(data, []byte(line)) {
		t.Fatalf("gzip missing line, got %q", data)
	}
}

func TestDailyCompressMaxDaysCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	gf, err := os.OpenFile(path+".2026-07-01.gz", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(gf)
	_, _ = gw.Write([]byte("old\n"))
	_ = gw.Close()
	_ = gf.Close()
	w, err := Open(Config{Filename: path, Daily: true, MaxDays: 2, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.now = func() time.Time { return time.Date(2026, 7, 24, 8, 0, 0, 0, time.Local) }
	_ = w.Close()
	mustNotExist(t, path+".2026-07-01.gz")
}

func TestCompressNumberedNormal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 1, Compress: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 64
	line := strings.Repeat("d", 40) + "\n"
	if _, err := w.Write([]byte(line)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := w.Write([]byte(line)); err != nil {
		t.Fatalf("write2: %v", err)
	}
	mustExist(t, path+".1.gz")
	mustNotExist(t, path+".1")
	_ = w.Close()
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

func TestOpenRejectsNegativeMaxFileSizeMB(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(Config{Filename: filepath.Join(dir, "a.log"), MaxFileSizeMB: -1}); err == nil {
		t.Fatal("expected error for negative MaxFileSizeMB")
	}
}

func TestOpenRejectsHugeMaxFileSizeMB(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(Config{Filename: filepath.Join(dir, "a.log"), MaxFileSizeMB: 1 << 50}); err == nil {
		t.Fatal("expected error for huge MaxFileSizeMB")
	}
}

func TestOpenRejectsNegativeMaxDays(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(Config{Filename: filepath.Join(dir, "a.log"), MaxDays: -1}); err == nil {
		t.Fatal("expected error for negative MaxDays")
	}
}

func TestOpenRejectsNegativeMaxTotalSizeMB(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(Config{Filename: filepath.Join(dir, "a.log"), MaxTotalSizeMB: -1}); err == nil {
		t.Fatal("expected error for negative MaxTotalSizeMB")
	}
}

func TestAppendOnlyCreatesDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "app.log")
	w, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	mustExist(t, path)
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
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 5})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 256
	aw, err := OpenAsync(w, AsyncConfig{QueueSize: 256})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := aw.Write(fmt.Appendf(nil, "g%d-%d\n", id, i)); err != nil {
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
	for i := 1; i <= 5; i++ {
		appendFile(fmt.Sprintf("%s.%d", path, i))
	}
	if all.Len() == 0 {
		t.Fatal("expected some log content")
	}
}

func TestConcurrentWritesPreservesAllWhenBackupsEnough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 200})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.maxFileSize = 256
	aw, err := OpenAsync(w, AsyncConfig{QueueSize: 512})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := aw.Write(fmt.Appendf(nil, "g%d-%d\n", id, i)); err != nil {
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
			if !strings.Contains(body, fmt.Sprintf("g%d-%d\n", g, i)) {
				t.Fatalf("missing line g%d-%d", g, i)
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
		if _, err := aw.Write(fmt.Appendf(nil, "async-%d\n", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := base.Write([]byte("x\n")); err == nil {
		t.Fatal("base Write after Async Close should fail")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i := 0; i < n; i++ {
		if !strings.Contains(string(got), fmt.Sprintf("async-%d\n", i)) {
			t.Fatalf("missing async-%d", i)
		}
	}
}

func TestAsyncWriterBlockWhenFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	gate := make(chan struct{})
	slow := &blockingWriter{w: base, gate: gate, blockFirst: 1}
	aw, err := OpenAsync(slow, AsyncConfig{QueueSize: 2, FullPolicy: FullBlock})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}
	if _, err := aw.Write([]byte("a\n")); err != nil {
		t.Fatalf("write a: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := aw.Write([]byte("b\n")); err != nil {
		t.Fatalf("write b: %v", err)
	}
	if _, err := aw.Write([]byte("c\n")); err != nil {
		t.Fatalf("write c: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := aw.Write([]byte("d\n"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Write should block: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked write did not complete")
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
	time.Sleep(20 * time.Millisecond)
	// 填满队列
	if _, err := aw.Write([]byte("queued\n")); err != nil {
		t.Fatalf("write queued: %v", err)
	}
	n, err := aw.Write([]byte("drop-me\n"))
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("want ErrDropped, got n=%d err=%v", n, err)
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

func TestAsyncLastErrorKeepsMostRecent(t *testing.T) {
	e1 := errors.New("first")
	e2 := errors.New("second")
	sink := &errSink{errs: []error{e1, e2}}
	aw, err := OpenAsync(sink, AsyncConfig{QueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
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
	if !errors.Is(aw.Close(), e2) {
		t.Fatalf("Close want second, got %v", aw.Close())
	}
}

func TestAsyncLastErrorClearsOnSuccess(t *testing.T) {
	sink := &errSink{errs: []error{errors.New("transient")}}
	aw, err := OpenAsync(sink, AsyncConfig{QueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aw.Write([]byte("fail\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := aw.Write([]byte("ok\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if aw.LastError() == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if aw.LastError() != nil {
		t.Fatalf("LastError should clear, got %v", aw.LastError())
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAsyncSyncSerialWithWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := Open(Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 5})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	base.maxFileSize = 128
	aw, err := OpenAsync(base, AsyncConfig{QueueSize: 64})
	if err != nil {
		t.Fatalf("OpenAsync: %v", err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if _, err := aw.Write(fmt.Appendf(nil, "g%d-%d\n", id, i)); err != nil {
					errCh <- err
					return
				}
				if i%5 == 0 {
					if err := aw.Sync(); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write/sync: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("expected %s to not exist", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func readAllGzipMembers(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	var all bytes.Buffer
	for {
		gr, err := gzip.NewReader(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("gzip reader: %v", err)
		}
		if _, err := io.Copy(&all, gr); err != nil {
			t.Fatalf("read gzip member: %v", err)
		}
		_ = gr.Close()
	}
	return all.Bytes()
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

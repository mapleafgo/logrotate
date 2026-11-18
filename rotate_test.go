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

func TestConcurrentWritesPreserveAllAcrossRolls(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, err := openRolling(rollingConfig{Config: Config{Filename: path}, maxFileBytes: 256}, time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
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
	files, err := filepath.Glob(path + ".archives/*")
	if err != nil || len(files) == 0 {
		t.Fatalf("archives: %v, %v", files, err)
	}
	for _, p := range files {
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
	for !errors.Is(aw.LastError(), e2) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(aw.LastError(), e2) {
		t.Fatalf("LastError want second, got %v", aw.LastError())
	}
	if err := aw.Close(); !errors.Is(err, e1) {
		t.Fatalf("Close want first, got %v", err)
	}
}

func TestAsyncLastErrorClearsOnSuccess(t *testing.T) {
	transient := errors.New("transient")
	sink := &errSink{errs: []error{transient}}
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
	if err := aw.Sync(); !errors.Is(err, transient) {
		t.Fatalf("Sync want transient, got %v", err)
	}
	if aw.LastError() != nil {
		t.Fatalf("LastError should clear, got %v", aw.LastError())
	}
	if err := aw.Close(); !errors.Is(err, transient) {
		t.Fatalf("Close want transient, got %v", err)
	}
}

func TestAsyncSyncSerialWithWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base, err := openRolling(rollingConfig{Config: Config{Filename: path}, maxFileBytes: 128}, time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
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

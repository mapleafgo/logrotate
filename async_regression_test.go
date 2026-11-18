package logrotate

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"sync"
	"testing"
	"time"
)

type asyncRegressionSink struct {
	write func([]byte) (int, error)
	sync  func() error
	close func() error
}

func (s asyncRegressionSink) Write(p []byte) (int, error) { return s.write(p) }
func (s asyncRegressionSink) Sync() error {
	if s.sync != nil {
		return s.sync()
	}
	return nil
}
func (s asyncRegressionSink) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}

func TestAsyncBufferReuseLifecycle(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	want := [][]byte{
		bytes.Repeat([]byte{'a'}, 128),
		bytes.Repeat([]byte{'b'}, maxSpareRecordBytes+1),
		bytes.Repeat([]byte{'c'}, 64),
	}
	var got [][]byte
	var lastBuffer *byte
	aw, err := OpenAsync(asyncRegressionSink{write: func(p []byte) (int, error) {
		if len(got) == 0 {
			close(entered)
			<-release
		}
		lastBuffer = &p[0]
		got = append(got, bytes.Clone(p))
		return len(p), nil
	}}, AsyncConfig{QueueSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		if err := aw.Close(); err != nil {
			t.Error(err)
		}
	})
	write := func(record []byte) {
		t.Helper()
		p := bytes.Clone(record)
		if n, err := aw.Write(p); n != len(p) || err != nil {
			t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(p))
		}
		clear(p)
	}
	write(want[0])
	<-entered
	write(want[1])
	write(want[2])
	unblock()
	if err := aw.Sync(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("mixed-size writes changed contents or delivery order")
	}
	aw.queueMu.Lock()
	cached := len(aw.spare)
	oversized := false
	for _, p := range aw.spare {
		oversized = oversized || cap(p) > maxSpareRecordBytes
	}
	aw.queueMu.Unlock()
	if cached != 2 || oversized {
		t.Fatalf("cache contains %d records, oversized=%v; want two small records", cached, oversized)
	}

	// Sync has returned the last small record's buffer to the cache.
	previousBuffer := lastBuffer
	record := bytes.Repeat([]byte{'d'}, 64)
	write(record)
	if err := aw.Sync(); err != nil {
		t.Fatal(err)
	}
	if lastBuffer != previousBuffer {
		t.Error("next small write did not reuse the cached buffer")
	}
	if !reflect.DeepEqual(got, append(want, record)) {
		t.Error("reused buffer changed record contents or delivery order")
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}
	if aw.spare != nil {
		t.Error("Close retained the spare buffers")
	}
}

func TestAsyncRejectsNilSink(t *testing.T) {
	for _, sink := range []FileSink{nil, (*Writer)(nil)} {
		if w, err := OpenAsync(sink, AsyncConfig{}); w != nil || err == nil {
			t.Fatalf("OpenAsync(%T) accepted a nil sink", sink)
		}
	}
}

func TestAsyncDropDoesNotAllocate(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	aw, err := OpenAsync(asyncRegressionSink{write: func(p []byte) (int, error) {
		if p[0] == 'a' {
			close(entered)
			<-release
		}
		return len(p), nil
	}}, AsyncConfig{QueueSize: 1, FullPolicy: FullDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		if err := aw.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := aw.Write([]byte("active")); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err := aw.Write([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 4096)
	if allocs := testing.AllocsPerRun(20, func() {
		if n, err := aw.Write(p); n != 0 || !errors.Is(err, ErrDropped) {
			t.Fatalf("Write = %d, %v; want ErrDropped", n, err)
		}
	}); allocs != 0 {
		t.Fatalf("allocations per rejected write = %v, want 0", allocs)
	}
}

func TestAsyncRegressionRejectsUnknownPolicy(t *testing.T) {
	aw, err := OpenAsync(asyncRegressionSink{}, AsyncConfig{FullPolicy: FullPolicy(42)})
	if aw != nil {
		defer func() { _ = aw.Close() }()
	}
	if err == nil {
		t.Fatal("OpenAsync accepted an unknown policy")
	}
}

func TestAsyncRegressionRetainsFirstDeliveryError(t *testing.T) {
	failed := errors.New("sink write failed")
	for _, tc := range []struct {
		name           string
		writeErr, want error
	}{
		{"failure", failed, failed}, {"short write", nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []string
			sink := asyncRegressionSink{
				write: func(p []byte) (int, error) {
					events = append(events, string(p))
					switch string(p) {
					case "first":
						return 1, tc.writeErr
					case "second":
						return 0, errors.New("second failure")
					default:
						return len(p), nil
					}
				},
				sync:  func() error { events = append(events, "sync"); return nil },
				close: func() error { events = append(events, "close"); return nil },
			}
			aw, err := OpenAsync(sink, AsyncConfig{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = aw.Close() }()
			for _, record := range []string{"first", "second", "ok"} {
				if n, err := aw.Write([]byte(record)); n != len(record) || err != nil {
					t.Fatalf("Write(%q) = %d, %v", record, n, err)
				}
			}
			if err := aw.Sync(); !errors.Is(err, tc.want) {
				t.Errorf("Sync = %v, want %v", err, tc.want)
			}
			if err := aw.LastError(); err != nil {
				t.Errorf("LastError = %v after recovery", err)
			}
			first := aw.Close()
			if !errors.Is(first, tc.want) {
				t.Errorf("Close = %v, want %v", first, tc.want)
			}
			if again := aw.Close(); again != first {
				t.Errorf("second Close = %v, want same result %v", again, first)
			}
			if want := []string{"first", "second", "ok", "sync", "close"}; !reflect.DeepEqual(events, want) {
				t.Errorf("sink events = %q, want %q", events, want)
			}
		})
	}
}

func TestAsyncRegressionCloseWakesBlockedCalls(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := release
	var records []string
	sink := asyncRegressionSink{write: func(p []byte) (int, error) {
		if len(records) == 0 {
			close(entered)
			<-unblock
		}
		records = append(records, string(p))
		return len(p), nil
	}}
	aw, err := OpenAsync(sink, AsyncConfig{QueueSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aw.Close() }()
	defer func() {
		if release != nil {
			close(release)
		}
	}()
	if _, err := aw.Write([]byte("active")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sink did not start")
	}
	p := []byte("queued")
	if _, err := aw.Write(p); err != nil {
		t.Fatal(err)
	}
	p[0] = 'X'
	results := make(chan error, 2)
	go func() { _, err := aw.Write([]byte("blocked")); results <- err }()
	go func() { results <- aw.Sync() }()
	closed := make(chan error, 1)
	go func() { closed <- aw.Close() }()
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, fs.ErrClosed) {
				t.Errorf("interleaved call = %v, want closed", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not wake blocked call")
		}
	}
	if _, err := aw.Write(nil); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("empty Write during Close = %v, want closed", err)
	}
	close(release)
	release = nil
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not drain")
	}
	if want := []string{"active", "queued"}; !reflect.DeepEqual(records, want) {
		t.Errorf("records = %q, want %q", records, want)
	}
	if err := aw.Sync(); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("Sync after Close = %v, want closed", err)
	}
}

package logrotate

import (
	"errors"
	"io"
	"io/fs"
	"reflect"
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

func TestAsyncRegressionRejectsUnknownPolicy(t *testing.T) {
	aw, err := OpenAsync(asyncRegressionSink{}, AsyncConfig{FullPolicy: FullPolicy(42)})
	if aw != nil {
		defer aw.Close()
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
			defer aw.Close()
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
			if want := []string{"first", "second", "ok", "sync", "sync", "close"}; !reflect.DeepEqual(events, want) {
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
	defer aw.Close()
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

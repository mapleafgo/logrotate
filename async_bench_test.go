package logrotate

import (
	"path/filepath"
	"sync/atomic"
	"testing"
)

func BenchmarkAsyncBufferReuse(b *testing.B) {
	aw, err := OpenAsync(asyncRegressionSink{write: func(p []byte) (int, error) {
		return len(p), nil
	}}, AsyncConfig{QueueSize: 64})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := aw.Write(payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	if err := aw.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkAsyncDelivery(b *testing.B) {
	w, err := Open(Config{Filename: filepath.Join(b.TempDir(), "app.log")})
	if err != nil {
		b.Fatal(err)
	}
	aw, err := OpenAsync(w, AsyncConfig{QueueSize: 1024})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 1024)
	var peak atomic.Int64
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := aw.Write(payload); err != nil {
				b.Error(err)
				return
			}
			aw.queueMu.Lock()
			depth := int64(aw.count)
			aw.queueMu.Unlock()
			for previous := peak.Load(); depth > previous; previous = peak.Load() {
				if peak.CompareAndSwap(previous, depth) {
					break
				}
			}
		}
	})
	if err := aw.Close(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(peak.Load()), "peak-queued-records")
}

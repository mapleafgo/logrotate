package logrotate

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func BenchmarkRolling(b *testing.B) {
	for _, tc := range []struct {
		name  string
		limit int64
	}{
		{"ordinary", 0}, {"roll-every-write", 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			w, err := openRolling(rollingConfig{Config: Config{Filename: filepath.Join(b.TempDir(), "app.log"), Compress: true}, maxFileBytes: tc.limit, totalArchiveBytes: 64 << 10}, time.Now)
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, 1024)
			latency := make([]time.Duration, min(b.N, 10000))
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if _, err := w.Write(payload); err != nil {
					b.Fatal(err)
				}
				latency[i%len(latency)] = time.Since(start)
			}
			b.StopTimer()
			pending, err := filepath.Glob(w.dir + "/*.pending")
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(pending)), "pending-at-end")
			start := time.Now()
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(time.Since(start).Nanoseconds()), "ns/close")
			slices.Sort(latency)
			b.ReportMetric(float64(latency[(99*len(latency)-1)/100].Nanoseconds()), "p99-ns/write")
		})
	}
}

// BenchmarkRollingPaced measures from Open through Close, including pacing and
// background maintenance. Set TMPDIR to a directory on the disk under test.
// Each benchmark operation is a burst of writes, not an individual Write.
func BenchmarkRollingPaced(b *testing.B) {
	for _, history := range []int{32, 1024} {
		b.Run(fmt.Sprintf("history=%d", history), func(b *testing.B) {
			benchmarkRollingPaced(b, history)
		})
	}
}

func benchmarkRollingPaced(b *testing.B, history int) {
	const writesPerBurst = 64
	const burstInterval = 10 * time.Millisecond
	payload := rollingBenchmarkPayload()
	filename := filepath.Join(b.TempDir(), "app.log")
	dir := filename + ".archives"
	// Two rolls per burst leave both ordinary and rotating writes in the mix.
	const writesPerArchive = 32
	budget := seedRollingBenchmarkHistory(b, dir, history, bytes.Repeat(payload, writesPerArchive))
	latency := make([]time.Duration, b.N*writesPerBurst)
	b.SetBytes(int64(len(payload) * writesPerBurst))
	b.ReportAllocs()
	b.ResetTimer()
	w, err := openRolling(rollingConfig{Config: Config{Filename: filename, Compress: true}, maxFileBytes: int64(len(payload) * writesPerArchive), totalArchiveBytes: budget}, time.Now)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		// A failed write or sample must still stop the maintenance worker.
		if !w.closed {
			_ = w.Close()
		}
	}()
	var peakPending int64
	for i := range b.N {
		next := time.Now().Add(burstInterval)
		for j := range writesPerBurst {
			start := time.Now()
			if _, err := w.Write(payload); err != nil {
				b.Fatal(err)
			}
			latency[i*writesPerBurst+j] = time.Since(start)
		}
		peakPending = max(peakPending, sampleRollingPendingBytes(b, dir))
		if i+1 < b.N {
			time.Sleep(time.Until(next))
		}
	}
	closeStart := time.Now()
	err = w.Close()
	closeTime := time.Since(closeStart)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	if err := w.MaintenanceError(); err != nil {
		b.Fatal(err)
	}
	slices.Sort(latency)
	b.ReportMetric(float64(latency[(99*len(latency)-1)/100].Nanoseconds()), "p99-ns/write")
	b.ReportMetric(float64(closeTime.Nanoseconds()), "ns/close")
	b.ReportMetric(float64(peakPending), "sampled-peak-pending-B")
}

// Use fixed log text and varied request IDs rather than zero-filled buffers.
func rollingBenchmarkPayload() []byte {
	var payload bytes.Buffer
	rng := rand.New(rand.NewPCG(42, 7))
	for i := range 16 {
		fmt.Fprintf(&payload, "2026-01-15T12:08:43.%03dZ level=INFO service=checkout request=%016x route=/api/cart status=200 latency_us=%04d msg=cart_loaded\n", i, rng.Uint64(), rng.IntN(9000)+1000)
	}
	return payload.Bytes()
}

func seedRollingBenchmarkHistory(b *testing.B, dir string, count int, archive []byte) int64 {
	b.Helper()
	if err := os.Mkdir(dir, 0755); err != nil {
		b.Fatal(err)
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(archive); err != nil {
		b.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		b.Fatal(err)
	}
	// Fill the initial budget so new archives exercise retention immediately.
	for i := range count {
		name := fmt.Sprintf("2026-01-%02d.%d.gz", 1+i%28, 1+i/28)
		if err := os.WriteFile(filepath.Join(dir, name), compressed.Bytes(), 0600); err != nil {
			b.Fatal(err)
		}
	}
	return int64(compressed.Len()) * int64(count)
}

// Sample logical sizes after each burst. Files can disappear during a scan,
// and shorter-lived peaks between scans are not captured.
func sampleRollingPendingBytes(b *testing.B, dir string) int64 {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	var pending int64
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pending") {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			b.Fatal(err)
		}
		pending += info.Size()
	}
	return pending
}

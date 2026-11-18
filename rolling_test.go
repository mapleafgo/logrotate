package logrotate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRollingSizeAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	cfg := rollingConfig{Config: Config{Filename: path, Location: time.UTC}, maxFileBytes: 4}
	now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	w := mustOpen(t, cfg, now)
	writeRolling(t, w, "abc")
	writeRolling(t, w, "d")
	assertRollingContents(t, path, "abcd")
	writeRolling(t, w, "oversize")
	assertRollingContents(t, path+".archives/2026-11-18.0", "abcd")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = mustOpen(t, cfg, now)
	writeRolling(t, w, "x")
	assertRollingContents(t, path+".archives/2026-11-18.1", "oversize")
	assertRollingContents(t, path, "x")
}

func mustOpen(t *testing.T, cfg rollingConfig, now func() time.Time) *Writer {
	t.Helper()
	w, err := openRolling(cfg, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func writeRolling(t *testing.T, w *Writer, s string) {
	t.Helper()
	if n, err := w.Write([]byte(s)); n != len(s) || err != nil {
		t.Fatalf("Write(%q) = %d, %v", s, n, err)
	}
}

func assertRollingContents(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != want {
		t.Fatalf("%s = %q, %v; want %q", path, data, err, want)
	}
}

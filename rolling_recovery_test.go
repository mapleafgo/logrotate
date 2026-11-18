package logrotate

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRollingRecoverPending(t *testing.T) {
	for _, state := range []string{"original", "partial-temp", "matching-final", "conflicting-final", "truncated-final"} {
		t.Run(state, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.log")
			dir := path + ".archives"
			if err := os.Mkdir(dir, 0755); err != nil {
				t.Fatal(err)
			}
			base := filepath.Join(dir, "2026-11-18.8")
			writeRollingFixture(t, base+".pending", "record-81\n")
			switch state {
			case "partial-temp":
				writeRollingFixture(t, base+".tmp", "interrupted")
			case "matching-final":
				writeRollingGzip(t, base+".gz", "record-81\n")
			case "truncated-final":
				writeRollingGzip(t, base+".gz", "record-81\n")
				info, err := os.Stat(base + ".gz")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(base+".gz", info.Size()-1); err != nil {
					t.Fatal(err)
				}
			case "conflicting-final":
				writeRollingGzip(t, base+".gz", "record-82\n")
			}
			w, err := Open(Config{Filename: path, Compress: true})
			if err != nil {
				t.Fatal(err)
			}
			writeRolling(t, w, "record-83\n")
			if state == "conflicting-final" || state == "truncated-final" {
				if w.Close() == nil || w.MaintenanceError() == nil {
					t.Fatal("conflict was not reported")
				}
				assertRollingContents(t, base+".pending", "record-81\n")
				if state == "conflicting-final" {
					assertRollingGzip(t, base+".gz", "record-82\n")
				}
			} else {
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				assertRollingGzip(t, base+".gz", "record-81\n")
				assertRollingAbsent(t, base+".pending")
				assertRollingAbsent(t, base+".tmp")
			}
			assertRollingContents(t, path, "record-83\n")
		})
	}
}

func TestRollingRecoverPendingWithRotationDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	dir := path + ".archives"
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "2026-11-18.0")
	writeRollingFixture(t, base+".pending", "sealed")
	w, err := Open(Config{Filename: path})
	if err != nil {
		t.Fatal(err)
	}
	writeRolling(t, w, "active")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertRollingGzip(t, base+".gz", "sealed")
	assertRollingAbsent(t, base+".pending")
	assertRollingContents(t, path, "active")
}

func writeRollingGzip(t *testing.T, path, value string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	if _, err := io.WriteString(gz, value); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertRollingGzip(t *testing.T, path, want string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil || string(data) != want {
		t.Fatalf("%s = %q, %v; want %q", path, data, err, want)
	}
}

func writeRollingFixture(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0644); err != nil {
		t.Fatal(err)
	}
}

func assertRollingAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s must be absent, got %v", path, err)
	}
}

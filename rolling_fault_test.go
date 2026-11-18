package logrotate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestRollingRetriesENOSPCAfterRetention(t *testing.T) {
	for _, tc := range []struct {
		name              string
		history, attempts int
		stillFull         bool
	}{
		{"recovered", 1, 2, false}, {"still full", 1, 2, true}, {"no deletion", 0, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "active")
				now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
				w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Location: time.UTC, Compress: true, HistoryDays: tc.history}, maxFileBytes: 3}, now)
				defer w.Close()
				synctest.Wait()
				if err := os.Mkdir(w.dir, 0755); err != nil {
					t.Fatal(err)
				}
				old := filepath.Join(w.dir, "2026-11-01.0")
				writeRollingFixture(t, old, "eligible to delete")
				attempts := 0
				w.compress = func(src, tmp string) error {
					attempts++
					if attempts == 1 || tc.stillFull {
						if err := os.WriteFile(tmp, []byte("partial"), 0600); err != nil {
							return err
						}
						return syscall.ENOSPC
					}
					if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
						return errors.New("retried before retention removed old archive")
					}
					return writeRollingCompressed(src, tmp)
				}
				writeRolling(t, w, "old")
				writeRolling(t, w, "new")
				err := w.Close()
				if attempts != tc.attempts {
					t.Fatalf("compression attempts = %d, want %d (Close: %v)", attempts, tc.attempts, err)
				}
				base := filepath.Join(w.dir, "2026-11-18.0")
				if tc.stillFull || tc.history == 0 {
					if !errors.Is(err, syscall.ENOSPC) {
						t.Fatalf("Close = %v, want ENOSPC", err)
					}
					assertRollingContents(t, base+".pending", "old")
				} else {
					if err != nil {
						t.Fatal(err)
					}
					assertRollingGzip(t, base+".gz", "old")
					assertRollingAbsent(t, base+".pending")
				}
			})
		})
	}
}

func TestRollingRetriesPostUnlinkDirectorySync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "active")
		now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
		w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Compress: true, Location: time.UTC}, maxFileBytes: 3}, now)
		defer w.Close()
		synctest.Wait()
		fail := true
		calls := 0
		pending := filepath.Join(w.dir, "2026-11-18.0.pending")
		w.archiveSync = func(dir string) error {
			if _, err := os.Stat(pending); errors.Is(err, os.ErrNotExist) {
				calls++
				if fail {
					return syscall.EIO
				}
			}
			return syncRollingDir(dir)
		}
		writeRolling(t, w, "old")
		writeRolling(t, w, "new")
		synctest.Wait()
		assertRollingAbsent(t, pending)
		if err := w.MaintenanceError(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("first maintenance error: %v", err)
		}
		w.requestMaintenance("2026-11-18")
		synctest.Wait()
		if err := w.MaintenanceError(); !errors.Is(err, syscall.EIO) || calls != 2 {
			t.Fatalf("failed retry: %d sync attempts, error %v", calls, err)
		}
		fail = false
		w.requestMaintenance("2026-11-18")
		if err := w.Close(); err != nil || calls != 3 {
			t.Fatalf("successful retry: %d sync attempts, Close %v", calls, err)
		}
	})
}

func TestRollingRecoveryAfterFailure(t *testing.T) {
	for _, step := range []string{"seal", "reopen", "compress", "publish", "remove"} {
		t.Run(step, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.log")
			base := path + ".archives/2026-11-18.0"
			cfg := rollingConfig{Config: Config{Filename: path, Location: time.UTC, Compress: true}, maxFileBytes: 4}
			now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
			w := mustOpen(t, cfg, now)
			w.rename = func(src, dst string) error {
				if (step == "seal" && src == path) || (step == "publish" && strings.HasSuffix(dst, ".gz")) {
					return syscall.ENOSPC
				}
				return os.Rename(src, dst)
			}
			if step == "reopen" {
				w.reopen = func() error { return syscall.ENOSPC }
			}
			if step == "compress" {
				w.compress = func(src, tmp string) error {
					writeRollingFixture(t, tmp, "partial gzip")
					return syscall.ENOSPC
				}
			}
			if step == "remove" {
				w.remove = func(p string) error {
					if p == base+".pending" {
						return syscall.ENOSPC
					}
					return os.Remove(p)
				}
			}
			writeRolling(t, w, "ID01")
			n, err := w.Write([]byte("ID02"))
			writeFailed := step == "seal" || step == "reopen"
			if writeFailed {
				if n != 0 || !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("Write got %d, %v", n, err)
				}
			} else if n != 4 || err != nil {
				t.Fatalf("Write got %d, %v, maintenance %v", n, err, w.MaintenanceError())
			}
			if step == "seal" {
				assertRollingContents(t, path, "ID01")
			}
			if step == "reopen" {
				assertRollingAbsent(t, path)
			}
			closeErr := w.Close()
			if writeFailed && closeErr != nil || !writeFailed && !errors.Is(closeErr, syscall.ENOSPC) {
				t.Fatalf("Close: %v", closeErr)
			}
			if !writeFailed {
				assertRollingContents(t, base+".pending", "ID01")
			}
			if step == "reopen" {
				assertRollingGzip(t, base+".gz", "ID01")
			}
			restarted := mustOpen(t, cfg, now)
			if writeFailed {
				writeRolling(t, restarted, "ID02")
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}
			assertRollingGzip(t, base+".gz", "ID01")
			assertRollingContents(t, path, "ID02")
			assertRollingAbsent(t, base+".pending")
			assertRollingAbsent(t, base+".tmp")
		})
	}
}

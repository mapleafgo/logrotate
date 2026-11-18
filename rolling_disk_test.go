package logrotate

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestRollingRejectsUnsafeActivePath(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	writeRollingFixture(t, target, "sensitive")
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	for _, path := range []string{link, dir + string(os.PathSeparator) + ".." + string(os.PathSeparator) + filepath.Base(dir) + string(os.PathSeparator) + "target"} {
		w, err := Open(Config{Filename: path})
		if err == nil {
			if err := w.Close(); err != nil {
				t.Errorf("close unexpectedly opened %q: %v", path, err)
			}
			t.Errorf("accepted %s", path)
		}
	}
}
func TestRollingSealRenameErrorIncludesPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.log")
	now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path}, maxFileBytes: 3}, now)
	w.rename = func(_, _ string) error { return syscall.ENOSPC }
	writeRolling(t, w, "old")
	n, err := w.Write([]byte("new"))
	if n != 0 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Write = %d, %v; want zero bytes and ENOSPC", n, err)
	}
	archive := filepath.Join(path+".archives", "2026-11-18.0")
	for _, want := range []string{"seal active file", strconv.Quote(path), strconv.Quote(archive)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Write error %q does not include %q", err, want)
		}
	}
}

func TestRollingKeepsPrivateMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits")
	}
	path := filepath.Join(t.TempDir(), "active")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Compress: true}, maxFileBytes: 3}, now)
	writeRolling(t, w, "new")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{path, path + ".archives/2026-11-18.0.gz"} {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0077 != 0 {
			t.Errorf("%s broadened to %04o", name, info.Mode().Perm())
		}
	}
}
func TestRollingPreservesOnlySealedVolumeUntilReopen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "active")
		now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
		w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Compress: true, HistoryDays: 2}, maxFileBytes: 3, totalArchiveBytes: 1}, now)
		defer func() { _ = w.Close() }() // Fallback; the test checks the primary close result.
		synctest.Wait()
		if err := os.Mkdir(w.dir, 0755); err != nil {
			t.Fatal(err)
		}
		older := filepath.Join(w.dir, "2026-11-01.0")
		writeRollingFixture(t, older, "older")
		retained := filepath.Join(w.dir, "2026-11-17.0")
		writeRollingFixture(t, retained, "retained")
		w.reopen = func() error {
			if _, err := os.Stat(older); errors.Is(err, os.ErrNotExist) {
				return w.openRollingActive()
			}
			return syscall.ENOSPC
		}
		writeRolling(t, w, "old")
		if n, err := w.Write([]byte("new")); n != 0 || !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("write = %d %v", n, err)
		}
		archive := path + ".archives/2026-11-18.0.gz"
		synctest.Wait()
		assertRollingAbsent(t, older)
		assertRollingContents(t, retained, "retained")
		assertRollingGzip(t, archive, "old")
		if err := w.MaintenanceError(); err == nil {
			t.Fatal("maintenance did not report unreachable archive cap")
		}
		writeRolling(t, w, "new")
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		assertRollingAbsent(t, archive)
		assertRollingAbsent(t, retained)
		assertRollingContents(t, path, "new")
	})
}

package logrotate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
	_ "time/tzdata"
)

func TestRollingDailyAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	previous := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = previous })
	path := filepath.Join(t.TempDir(), "app.log")
	now := time.Date(2026, 3, 8, 0, 0, 0, 0, loc)
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Daily: true}, maxFileBytes: 4}, func() time.Time { return now.UTC() })
	writeRolling(t, w, "AB")
	now = now.Add(22 * time.Hour)
	writeRolling(t, w, "CD")
	assertRollingContents(t, path, "ABCD")
	now = now.Add(time.Hour)
	writeRolling(t, w, "")
	assertRollingContents(t, path, "ABCD")
	writeRolling(t, w, "E")
	assertRollingContents(t, path+".archives/2026-03-08.0", "ABCD")
	assertRollingAbsent(t, path+".archives/2026-03-08.1")
	now = now.Add(-time.Hour)
	writeRolling(t, w, "F")
	assertRollingContents(t, path, "EF")
}

func TestRollingRetentionAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	previous := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = previous })
	path := filepath.Join(t.TempDir(), "app.log")
	dir := path + ".archives"
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"2026-03-07.0", "2026-03-08.2", "2026-03-08.10", "2026-03-09.0", "2026-03-08.01", "2026-03-09.0.tmp"} {
		writeRollingFixture(t, filepath.Join(dir, name), "A")
	}
	writeRollingFixture(t, path+".1", "legacy")
	if err := os.Symlink(path+".1", filepath.Join(dir, "2026-03-01.0")); err != nil && !os.IsPermission(err) {
		t.Fatal(err)
	}
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path, HistoryDays: 2}, totalArchiveBytes: 2}, func() time.Time { return time.Date(2026, 3, 9, 0, 30, 0, 0, loc).UTC() })
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertRollingAbsent(t, filepath.Join(dir, "2026-03-07.0"))
	assertRollingAbsent(t, filepath.Join(dir, "2026-03-08.2"))
	for _, name := range []string{"2026-03-08.10", "2026-03-09.0", "2026-03-08.01", "2026-03-09.0.tmp"} {
		assertRollingContents(t, filepath.Join(dir, name), "A")
	}
	assertRollingContents(t, path+".1", "legacy")
}

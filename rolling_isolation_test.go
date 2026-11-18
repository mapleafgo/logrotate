package logrotate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRollingMaintenanceRetainsOnlyConflictingVolume(t *testing.T) {
	dir := t.TempDir()
	w := &archiveMaintenance{dir: dir, historyDays: 1, remove: os.Remove}
	for _, name := range []string{"2026-11-17.0", "2026-11-17.0.gz", "2026-11-17.0.pending", "2026-11-17.1"} {
		writeRollingFixture(t, filepath.Join(dir, name), name)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.maintain(entries, "2026-11-18", ""); err == nil || !strings.Contains(err.Error(), "archive conflict") {
		t.Fatalf("maintenance error = %v, want archive conflict", err)
	}
	for _, name := range []string{"2026-11-17.0", "2026-11-17.0.gz", "2026-11-17.0.pending"} {
		assertRollingContents(t, filepath.Join(dir, name), name)
	}
	if _, err := os.Stat(filepath.Join(dir, "2026-11-17.1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unrelated expired volume stat = %v, want not exist", err)
	}
}

func TestRollingMaintenanceCapAccountsForProtectedArchive(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cap         int64
		wantRemoval bool
	}{
		{"cap reachable", 6, true},
		{"cap unreachable", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w := &archiveMaintenance{dir: dir, totalArchiveBytes: tc.cap, remove: os.Remove}
			old := filepath.Join(dir, "2026-11-17.0")
			newer := filepath.Join(dir, "2026-11-18.0")
			writeRollingFixture(t, old+".gz", "bogus")
			writeRollingFixture(t, old+".pending", "source")
			writeRollingFixture(t, newer, "new")
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.maintain(entries, "2026-11-18", ""); err == nil {
				t.Fatal("maintenance succeeded despite conflicting gzip")
			}
			assertRollingContents(t, old+".gz", "bogus")
			if tc.wantRemoval {
				if _, err := os.Stat(newer); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("newer archive stat = %v, want not exist", err)
				}
			} else {
				assertRollingContents(t, newer, "new")
			}
		})
	}
}

func TestRollingMaintenanceStopsCapPruningAfterRemovalFailureMakesCapUnreachable(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "2026-11-17.0")
	newer := filepath.Join(dir, "2026-11-18.0")
	writeRollingFixture(t, old, "older")
	writeRollingFixture(t, newer, "new")
	w := &archiveMaintenance{dir: dir, totalArchiveBytes: 4, remove: func(path string) error {
		if path == old {
			return os.ErrPermission
		}
		return os.Remove(path)
	}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.maintain(entries, "2026-11-18", ""); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("maintenance error = %v, want permission error", err)
	}
	assertRollingContents(t, newer, "new")
}

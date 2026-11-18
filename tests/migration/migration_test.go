package migration_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	legacy "github.com/mapleafgo/logrotate"
	current "github.com/mapleafgo/logrotate/v2"
)

func TestReleaseMigrationAndRollback(t *testing.T) {
	const halfMiB = 1 << 19
	path := filepath.Join(t.TempDir(), "app.log")
	oldArchive := path + ".1.gz"
	legacyHistory := strings.Repeat("a", 2*halfMiB)
	legacyActive := strings.Repeat("b", halfMiB)
	v2Active := strings.Repeat("c", halfMiB)
	rollbackActive := strings.Repeat("d", 2*halfMiB)
	const upgradeMarker = "upgrade append\n"
	const rollbackMarker = "rollback append\n"

	legacyConfig := legacy.Config{Filename: path, MaxFileSizeMB: 1, MaxBackups: 2, Compress: true}
	w := opened(t, legacy.Open, legacyConfig)
	write(t, w, legacyHistory)
	write(t, w, legacyActive)
	closeWriter(t, w)
	assertContent(t, path, readFile(t, path), legacyActive)
	oldGzip := readFile(t, oldArchive)
	assertContent(t, oldArchive, gunzip(t, oldArchive, oldGzip), legacyHistory)

	v2 := opened(t, current.Open, current.Config{
		Filename: path, MaxFileMiB: 1, Location: time.UTC, Compress: true, HistoryDays: 7,
	})
	write(t, v2, upgradeMarker)
	assertContent(t, path, readFile(t, path), legacyActive+upgradeMarker)
	// Two half-MiB records fit exactly; the appended marker forces a roll.
	write(t, v2, v2Active)
	closeWriter(t, v2) // Close waits for archive maintenance; Sync does not.
	assertContent(t, path, readFile(t, path), v2Active)
	assertContent(t, oldArchive, readFile(t, oldArchive), string(oldGzip))
	archiveDir := path + ".archives"
	v2Archive := onlyV2Archive(t, archiveDir)
	v2Gzip := readFile(t, v2Archive)
	assertContent(t, v2Archive, gunzip(t, v2Archive, v2Gzip), legacyActive+upgradeMarker)

	w = opened(t, legacy.Open, legacyConfig)
	write(t, w, rollbackMarker)
	assertContent(t, path, readFile(t, path), v2Active+rollbackMarker)
	write(t, w, rollbackActive)
	closeWriter(t, w)
	assertContent(t, path, readFile(t, path), rollbackActive)
	assertContent(t, oldArchive, gunzip(t, oldArchive, readFile(t, oldArchive)), v2Active+rollbackMarker)
	previousArchive := path + ".2.gz"
	assertContent(t, previousArchive, readFile(t, previousArchive), string(oldGzip))
	if after := onlyV2Archive(t, archiveDir); after != v2Archive {
		t.Fatalf("rollback changed v2 archive path: got %s, want %s", after, v2Archive)
	}
	assertContent(t, v2Archive, readFile(t, v2Archive), string(v2Gzip))
}

func opened[C any, W io.WriteCloser](t *testing.T, open func(C) (W, error), cfg C) W {
	t.Helper()
	w, err := open(cfg)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

func closeWriter(t *testing.T, w io.Closer) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("close %T: %v", w, err)
	}
}

func write(t *testing.T, w io.Writer, s string) {
	t.Helper()
	if n, err := w.Write([]byte(s)); err != nil || n != len(s) {
		t.Fatalf("write to %T: got (%d, %v), want (%d, nil)", w, n, err, len(s))
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func gunzip(t *testing.T, path string, data []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open gzip %s: %v", path, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read gzip %s: %v", path, err)
	}
	return b
}

func assertContent(t *testing.T, path string, got []byte, want string) {
	t.Helper()
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("%s: got %d bytes (sha256 %x), want %d bytes (sha256 %x)",
			path, len(got), sha256.Sum256(got), len(want), sha256.Sum256([]byte(want)))
	}
}

func onlyV2Archive(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%s: got entries %v, want one gzip archive", dir, entries)
	}
	name := entries[0].Name()
	if !entries[0].Type().IsRegular() || !regexp.MustCompile("^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.0\\.gz$").MatchString(name) {
		t.Fatalf("%s: got %q, want a regular YYYY-MM-DD.0.gz archive", dir, name)
	}
	return filepath.Join(dir, name)
}

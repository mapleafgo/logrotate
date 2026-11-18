package logrotate_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mapleafgo/logrotate/v2"
)

func TestV2OpenRotatesAndIgnoresOldArchives(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path+".1", []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	w, err := logrotate.Open(logrotate.Config{Filename: path, MaxFileMiB: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, p := range []string{string(make([]byte, 1<<20)), "def"} {
		if _, err := w.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	files, err := filepath.Glob(path + ".archives/*.0")
	if err != nil || len(files) != 1 {
		t.Fatalf("archive paths: %v, %v", files, err)
	}
	for name, want := range map[string]string{files[0]: string(make([]byte, 1<<20)), path: "def", path + ".1": "old"} {
		got, err := os.ReadFile(name)
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
}

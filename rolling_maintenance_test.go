package logrotate

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestRollingRetriesMaintenanceWithoutAnotherWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "active")
		base := filepath.Join(path+".archives", "2026-11-18.0")
		if err := os.Mkdir(filepath.Dir(base), 0755); err != nil {
			t.Fatal(err)
		}
		writeRollingFixture(t, base+".pending", "sealed")
		attempts := 0
		w, err := openRollingWithCompressor(rollingConfig{Config: Config{Filename: path}}, time.Now, func(src, tmp string) error {
			attempts++
			if attempts == 1 {
				return os.ErrDeadlineExceeded
			}
			return writeRollingCompressed(src, tmp)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = w.Close() }()
		synctest.Wait()
		if w.MaintenanceError() == nil {
			t.Fatal("initial failure was not reported")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if attempts != 2 || w.MaintenanceError() != nil {
			t.Fatalf("attempts = %d, maintenance error = %v", attempts, w.MaintenanceError())
		}
		assertRollingGzip(t, base+".gz", "sealed")
		assertRollingAbsent(t, base+".pending")
	})
}

func TestRollingStartupDoesNotBlockOpenAndCloseWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "active")
		pending := path + ".archives/2026-11-18.0.pending"
		if err := os.Mkdir(filepath.Dir(pending), 0755); err != nil {
			t.Fatal(err)
		}
		writeRollingFixture(t, pending, "sealed before startup")
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		type openResult struct {
			w   *Writer
			err error
		}
		opened := make(chan openResult, 1)
		go func() {
			now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
			w, err := openRollingWithCompressor(rollingConfig{Config: Config{Filename: path, Compress: true}}, now, func(src, tmp string) error {
				close(entered)
				<-release
				return writeRollingCompressed(src, tmp)
			})
			opened <- openResult{w, err}
		}()
		synctest.Wait()
		select {
		case <-entered:
		default:
			t.Fatal("startup did not attempt recovery")
		}
		var w *Writer
		select {
		case result := <-opened:
			if result.err != nil {
				t.Fatal(result.err)
			}
			w = result.w
		default:
			// Release the worker and close the late writer even on regression.
			release <- struct{}{}
			if result := <-opened; result.w != nil {
				_ = result.w.Close()
			}
			t.Fatal("Open waited for startup archive recovery")
		}
		closed := make(chan error, 1)
		go func() { closed <- w.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before startup recovery: %v", err)
		default:
		}
		release <- struct{}{}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		assertRollingGzip(t, path+".archives/2026-11-18.0.gz", "sealed before startup")
		assertRollingAbsent(t, pending)
	})
}

func TestRollingMaintenanceDoesNotBlockWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active")
	now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path, Compress: true}, maxFileBytes: 3}, now)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() { close(release); _ = w.Close() }()
	w.compress = func(source, temp string) error {
		if filepath.Base(source) == "2026-11-18.0.pending" {
			close(entered)
			<-release
		}
		return writeRollingCompressed(source, temp)
	}
	writeRolling(t, w, "one")
	result := make(chan error, 1)
	go func() {
		for _, record := range []string{"two", "tri", "end"} {
			if _, err := w.Write([]byte(record)); err != nil {
				result <- err
				return
			}
		}
		result <- w.Sync()
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("compression did not start")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Error("Write or Sync waited for archive compression")
		release <- struct{}{}
		if err := <-result; err != nil {
			t.Error(err)
		}
		return
	}
	if _, err := os.Stat(path + ".archives/2026-11-18.2.pending"); err != nil {
		t.Fatal(err)
	}
	release <- struct{}{}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"one", "two", "tri"} {
		assertRollingGzip(t, path+".archives/2026-11-18."+string(rune('0'+i))+".gz", want)
	}
}

func TestRollingSameDaySequenceSurvivesRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active")
	now := func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.Local) }
	w := mustOpen(t, rollingConfig{Config: Config{Filename: path}, maxFileBytes: 3, totalArchiveBytes: 1}, now)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	removed := make(chan struct{}, 2)
	w.remove = func(name string) error {
		if err := os.Remove(name); err != nil {
			return err
		}
		removed <- struct{}{}
		return nil
	}
	sealed := make(chan string, 2)
	w.rename = func(src, dst string) error {
		if err := os.Rename(src, dst); err != nil {
			return err
		}
		sealed <- filepath.Base(dst)
		return nil
	}
	writeRolling(t, w, "one")
	writeRolling(t, w, "two")
	<-sealed
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("retention did not remove first archive")
	}
	// The worker may still reserve the deleted basename until its pass ends.
	// Wait for the published result of a sentinel error from retention.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, active, _ := w.reservedArchive("2026-11-18"); !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retention pass did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	writeRolling(t, w, "end")
	if got := <-sealed; got != "2026-11-18.1" {
		t.Errorf("second seal = %s, want 2026-11-18.1", got)
	}
}

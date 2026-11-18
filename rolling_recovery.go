package logrotate

import (
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// archiveMaintenance operates on sealed volumes, not the active file.
type archiveMaintenance struct {
	dir                 string
	historyDays         int
	totalArchiveBytes   int64
	compress, rename    func(string, string) error
	remove, archiveSync func(string) error
	unlinkSyncErr       error
}

func (s *archiveMaintenance) maintain(entries []os.DirEntry, nowDay, protected string) error {
	var result error
	recovered := make(map[string]error)
	for _, e := range entries {
		_, _, suffix, ok := parseRollingArchive(e.Name())
		if !ok || suffix != "pending" || !e.Type().IsRegular() {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".pending")
		recovered[base] = s.finishPending(filepath.Join(s.dir, base))
	}
	if s.historyDays > 0 || s.totalArchiveBytes > 0 {
		removed, retentionErr := s.applyRetention(entries, nowDay, protected, recovered)
		result = retentionErr
		if removed {
			published := false
			// Retry each space failure at most once, and only if retention made room.
			for _, e := range entries {
				base := strings.TrimSuffix(e.Name(), ".pending")
				if base == e.Name() || !errors.Is(recovered[base], syscall.ENOSPC) {
					continue
				}
				archiveBase := filepath.Join(s.dir, base)
				if exists, _ := rollingRegular(archiveBase + ".pending"); !exists {
					continue
				}
				err := s.finishPending(archiveBase)
				recovered[base] = err
				published = published || err == nil
			}
			if published {
				_, err := s.applyRetention(entries, nowDay, protected, recovered)
				result = errors.Join(result, err)
			}
		}
	}
	for _, e := range entries {
		if base, ok := strings.CutSuffix(e.Name(), ".pending"); ok {
			result = errors.Join(result, recovered[base])
		}
	}
	return result
}

func archiveEntries(dir string) ([]os.DirEntry, error) {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("logrotate: archive path %s is not a directory", dir)
	}
	return os.ReadDir(dir)
}

func (s *archiveMaintenance) finishPending(base string) error {
	source, final, temp := base+".pending", base+".gz", base+".tmp"
	if exists, err := rollingRegular(base); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("logrotate: archive conflict at %s", base)
	}
	exists, err := rollingRegular(final)
	if err != nil {
		return err
	}
	if exists {
		if err := verifyRollingGzip(source, final); err != nil {
			return fmt.Errorf("logrotate: archive conflict at %s: %w", final, err)
		}
	}
	if exists, err := rollingRegular(temp); err != nil {
		return err
	} else if exists {
		if err := s.remove(temp); err != nil {
			return err
		}
	}
	if !exists {
		if err := s.compress(source, temp); err != nil {
			return err
		}
		if err := renameUnused(s.rename, temp, final); err != nil {
			return err
		}
	}
	if err := s.archiveSync(s.dir); err != nil {
		return err
	}
	if err := s.remove(source); err != nil {
		return err
	}
	err = s.archiveSync(s.dir)
	if err != nil {
		s.unlinkSyncErr = err
	}
	return err
}

func rollingRegular(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("logrotate: not a regular archive: %s", path)
	}
	return true, nil
}

func renameUnused(rename func(string, string) error, src, dst string) error {
	// The archive directory is exclusively owned, so no other writer can
	// create the destination between this check and the rename.
	if _, err := os.Lstat(dst); err == nil {
		return &os.PathError{Op: "archive", Path: dst, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return rename(src, dst)
}

func writeRollingCompressed(source, temp string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	dst, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(dst)
	_, copyErr := io.Copy(gz, src)
	gzipErr := gz.Close()
	if err := errors.Join(copyErr, gzipErr); err != nil {
		return errors.Join(err, dst.Close())
	}
	return errors.Join(dst.Sync(), dst.Close())
}

func verifyRollingGzip(source, final string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Open(final)
	if err != nil {
		return err
	}
	defer dst.Close()
	gz, err := gzip.NewReader(dst)
	if err != nil {
		return err
	}
	defer gz.Close()
	plainDigest, gzipDigest := sha256.New(), sha256.New()
	plainSize, err := io.Copy(plainDigest, src)
	if err != nil {
		return err
	}
	// Drain to EOF so gzip validates the trailer, even when all payload
	// bytes have already been emitted.
	gzipSize, err := io.Copy(gzipDigest, gz)
	if err != nil {
		return err
	}
	if plainSize != gzipSize || string(plainDigest.Sum(nil)) != string(gzipDigest.Sum(nil)) {
		return errors.New("gzip and pending contents differ")
	}
	return nil
}

func syncRollingDir(path string) error {
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		err = nil
	}
	return errors.Join(err, f.Close())
}

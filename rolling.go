// Package logrotate provides append-only log writers with file rotation.
package logrotate

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const rollingDate = "2006-01-02"

// Config controls the daily/size format in Filename + ".archives".
// Zero limits are disabled. Calendar dates use the process local time zone.
type Config struct {
	Filename string
	Daily    bool

	// MaxFileMiB is the size-based rollover threshold in MiB (1 << 20 bytes).
	MaxFileMiB  int64
	HistoryDays int
	// TotalArchiveMiB limits the combined size of finalized archives in MiB.
	TotalArchiveMiB int64
	Compress        bool
}

type rollingConfig struct {
	Config
	maxFileBytes      int64
	totalArchiveBytes int64
}

// Writer writes to an active file and rotates it according to Config. Use Open
// to construct a Writer; the zero value is not ready for use.
// Only MaintenanceError is safe to call concurrently with other methods.
type Writer struct {
	file             *os.File
	size             int64
	day              string
	now              func() time.Time
	closed           bool
	cfg              rollingConfig
	activeMode       os.FileMode
	archiveDay       string
	archiveNext      uint64
	archiveExhausted bool
	dir              string
	mu               sync.RWMutex
	maintenanceErr   error
	maintenance      rollingMaintenance
	compress         func(string, string) error
	reopen           func() error
	rename           func(string, string) error
	remove           func(string) error
	archiveSync      func(string) error
}

// Open opens the active path for append. The caller must exclusively own
// the active file and its archive directory. The active file must be regular,
// not a symlink, and Filename must not contain parent (..) components.
// New active files start private; replacements inherit permission bits
// subject to umask. Startup archive maintenance runs in the background.
// Its errors are available via MaintenanceError and do not prevent writes.
func Open(cfg Config) (*Writer, error) {
	resolved, err := normalizeRollingConfig(cfg)
	if err != nil {
		return nil, err
	}
	return openRolling(resolved, time.Now)
}

func normalizeRollingConfig(cfg Config) (rollingConfig, error) {
	const bytesPerMiB = 1 << 20
	const maxMiB = math.MaxInt64 / bytesPerMiB
	if cfg.MaxFileMiB < 0 || cfg.MaxFileMiB > maxMiB {
		return rollingConfig{}, fmt.Errorf("logrotate: MaxFileMiB must be between 0 and %d", maxMiB)
	}
	if cfg.TotalArchiveMiB < 0 || cfg.TotalArchiveMiB > maxMiB {
		return rollingConfig{}, fmt.Errorf("logrotate: TotalArchiveMiB must be between 0 and %d", maxMiB)
	}
	switch {
	case strings.TrimSpace(cfg.Filename) == "":
		return rollingConfig{}, errors.New("logrotate: Filename is required")
	case cfg.HistoryDays < 0:
		return rollingConfig{}, errors.New("logrotate: HistoryDays must not be negative")
	}
	for _, component := range strings.FieldsFunc(cfg.Filename[len(filepath.VolumeName(cfg.Filename)):], func(r rune) bool { return r < 128 && os.IsPathSeparator(byte(r)) }) {
		if component == ".." {
			return rollingConfig{}, fmt.Errorf("logrotate: Filename must not contain parent components: %q", cfg.Filename)
		}
	}
	if os.IsPathSeparator(cfg.Filename[len(cfg.Filename)-1]) {
		return rollingConfig{}, fmt.Errorf("logrotate: Filename must name a file: %q", cfg.Filename)
	}
	cfg.Filename = filepath.Clean(cfg.Filename)
	return rollingConfig{
		Config:            cfg,
		maxFileBytes:      cfg.MaxFileMiB * bytesPerMiB,
		totalArchiveBytes: cfg.TotalArchiveMiB * bytesPerMiB,
	}, nil
}

func openRolling(cfg rollingConfig, now func() time.Time) (*Writer, error) {
	return openRollingWithCompressor(cfg, now, writeRollingCompressed)
}

func openRollingWithCompressor(cfg rollingConfig, now func() time.Time, compress func(string, string) error) (*Writer, error) {
	w := &Writer{now: now, cfg: cfg, activeMode: 0600, dir: cfg.Filename + ".archives", rename: os.Rename, remove: os.Remove, compress: compress, archiveSync: syncRollingDir}
	w.reopen = w.openRollingActive
	if err := w.reopen(); err != nil {
		return nil, err
	}
	// Pending archives can outlive the configuration that created them.
	w.startMaintenance(now().Local().Format(rollingDate))
	return w, nil
}

func (w *Writer) openRollingActive() error {
	path := w.cfg.Filename
	if _, err := rollingRegular(path); err != nil {
		return fmt.Errorf("logrotate: check active file %q: %w", path, err)
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return fmt.Errorf("logrotate: create active file directory %q: %w", parent, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.activeMode)
	if err != nil {
		return fmt.Errorf("logrotate: open active file %q: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		return errors.Join(fmt.Errorf("logrotate: stat active file %q: %w", path, err), closeRollingActive(f))
	}
	if !info.Mode().IsRegular() {
		return errors.Join(fmt.Errorf("logrotate: active path %q is not a regular file", path), closeRollingActive(f))
	}
	w.file, w.size = f, info.Size()
	w.activeMode = info.Mode().Perm()
	if w.cfg.Daily && w.size > 0 {
		day := info.ModTime().Local().Format(rollingDate)
		if day > w.day {
			w.day = day
		}
	}
	return nil
}

// MaintenanceError returns the latest archive maintenance result. It can be
// called concurrently with Write, Sync, or Close.
func (w *Writer) MaintenanceError() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return errors.Join(w.maintenanceErr, w.maintenance.sealSyncErr)
}

func (w *Writer) setMaintenanceError(err error) {
	w.mu.Lock()
	w.maintenanceErr = err
	w.mu.Unlock()
}

func (w *Writer) beforeRollingWrite(n int) error {
	if w.file == nil {
		if err := w.reopenActive(); err != nil {
			return err
		}
		w.requestMaintenance(w.now().Local().Format(rollingDate))
	}
	if n == 0 || (!w.cfg.Daily && w.cfg.maxFileBytes == 0) {
		return nil
	}
	today := w.now().Local().Format(rollingDate)
	day := today
	if w.cfg.Daily && w.day != "" {
		day = w.day
	}
	if needsRolling(w.size, int64(n), w.cfg.maxFileBytes, w.cfg.Daily && today > day) {
		if err := w.sealRolling(day, today); err != nil {
			return err
		}
	}
	if w.cfg.Daily && w.size == 0 && today > w.day {
		w.day = today
	}
	return nil
}

func (w *Writer) reopenActive() error {
	if err := w.reopen(); err != nil {
		return fmt.Errorf("logrotate: reopen active file %q: %w", w.cfg.Filename, err)
	}
	return nil
}

func needsRolling(size, n, limit int64, laterDay bool) bool {
	return size > 0 && (laterDay || (limit > 0 && n > limit-size))
}

func (w *Writer) sealRolling(day, today string) error {
	if err := w.ensureDirectory(); err != nil {
		return err
	}
	target, err := w.nextArchive(day)
	if err != nil {
		return err
	}
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("logrotate: stat active file %q before sealing: %w", w.cfg.Filename, err)
	}
	w.activeMode = info.Mode().Perm()
	if err := w.prepareCloseForRotate(); err != nil {
		if w.file == nil {
			return errors.Join(err, w.reopenActive())
		}
		return err
	}
	if w.cfg.Compress {
		target += ".pending"
	}
	w.setSealing(filepath.Base(target))
	if err := renameUnused(w.rename, w.cfg.Filename, target); err != nil {
		w.setSealing("")
		if errors.Is(err, os.ErrExist) {
			w.archiveDay = ""
		}
		return errors.Join(fmt.Errorf("logrotate: seal active file %q as %q: %w", w.cfg.Filename, target, err), w.reopenActive())
	}
	if w.archiveNext == ^uint64(0) {
		w.archiveExhausted = true
	} else {
		w.archiveNext++
	}
	w.size = 0
	if w.cfg.Daily && today > w.day {
		w.day = today
	}
	syncErr := w.syncSealDirectories()
	reopenErr := w.reopenActive()
	protected := ""
	if reopenErr != nil {
		protected = strings.TrimSuffix(filepath.Base(target), ".pending")
	}
	w.requestMaintenanceAfterSeal(today, protected, true, syncErr)
	return reopenErr
}

func (w *Writer) ensureDirectory() error {
	if err := os.Mkdir(w.dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("logrotate: create archive directory %q: %w", w.dir, err)
	}
	info, err := os.Lstat(w.dir)
	if err != nil {
		return fmt.Errorf("logrotate: check archive directory %q: %w", w.dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("logrotate: archive path %q is not a directory", w.dir)
	}
	return nil
}

func (w *Writer) nextArchive(day string) (string, error) {
	// Only this writer creates new basenames. The worker can promote or
	// delete an existing one but never invalidates the same-day high water.
	if day != w.archiveDay {
		next, err := w.scanArchiveSequence(day)
		if err != nil {
			return "", err
		}
		w.archiveDay, w.archiveNext, w.archiveExhausted = day, next, false
	}
	if w.archiveExhausted {
		return "", fmt.Errorf("logrotate: archive sequence exhausted for %s in %q", day, w.dir)
	}
	return filepath.Join(w.dir, day+"."+strconv.FormatUint(w.archiveNext, 10)), nil
}

func (w *Writer) scanArchiveSequence(day string) (uint64, error) {
	var entries []os.DirEntry
	var next uint64
	for {
		seq, found, generation := w.reservedArchive(day)
		var err error
		entries, err = os.ReadDir(w.dir)
		if err != nil {
			return 0, fmt.Errorf("logrotate: scan archive directory %q: %w", w.dir, err)
		}
		if _, _, after := w.reservedArchive(day); after != generation {
			continue
		}
		if found {
			if seq == ^uint64(0) {
				return 0, fmt.Errorf("logrotate: archive sequence exhausted for %s in %q", day, w.dir)
			}
			next = seq + 1
		}
		break
	}
	for _, e := range entries {
		d, i, _, ok := parseRollingArchive(e.Name())
		if ok && d == day && i >= next {
			if i == ^uint64(0) {
				return 0, fmt.Errorf("logrotate: archive sequence exhausted for %s in %q", day, w.dir)
			}
			next = i + 1
		}
	}
	return next, nil
}

func parseRollingArchive(name string) (day string, seq uint64, suffix string, ok bool) {
	if len(name) < 12 || name[10] != '.' {
		return
	}
	day = name[:10]
	if _, err := time.Parse(rollingDate, day); err != nil {
		return
	}
	index, suffix, _ := strings.Cut(name[11:], ".")
	switch suffix {
	case "", "gz", "pending", "tmp":
	default:
		return
	}
	seq, err := strconv.ParseUint(index, 10, 64)
	ok = err == nil && strconv.FormatUint(seq, 10) == index && name[len(name)-1] != '.'
	return
}

// Write appends p without splitting it across volumes. It returns os.ErrClosed after Close.
func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	if err := w.beforeRollingWrite(len(p)); err != nil {
		return 0, err
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, fmt.Errorf("logrotate: write active file %q: %w", w.cfg.Filename, err)
	}
	return n, nil
}

// Sync flushes the active file. It returns os.ErrClosed if no file is open.
func (w *Writer) Sync() error {
	if w.file == nil {
		return os.ErrClosed
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("logrotate: sync active file %q: %w", w.cfg.Filename, err)
	}
	return nil
}

// Close closes the active file without syncing it, then waits for scheduled
// archive maintenance and reports unresolved maintenance errors.
// Subsequent calls return nil.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var activeErr error
	if w.file != nil {
		activeErr = w.closeFile()
	}
	w.drainMaintenance()
	return errors.Join(activeErr, w.MaintenanceError())
}

func (w *Writer) prepareCloseForRotate() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.closeFile()
}

func (w *Writer) closeFile() error {
	err := closeRollingActive(w.file)
	w.file = nil
	return err
}

func closeRollingActive(f *os.File) error {
	if err := f.Close(); err != nil {
		return fmt.Errorf("logrotate: close active file %q: %w", f.Name(), err)
	}
	return nil
}

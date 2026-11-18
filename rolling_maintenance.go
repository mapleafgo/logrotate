package logrotate

import (
	"errors"
	"path/filepath"
	"time"
)

// All state except the channels is guarded by Writer.mu. No filesystem
// operations run under that lock.
type rollingMaintenance struct {
	wake, done            chan struct{}
	day                   string
	archive               archiveMaintenance
	sealing               string
	protected             string
	sealSyncErr           error
	sealGeneration        uint64
	reserved              map[string]uint64
	reservationGeneration uint64
}

func (w *Writer) startMaintenance(day string) {
	w.maintenance.wake = make(chan struct{}, 1)
	w.maintenance.done = make(chan struct{})
	w.requestMaintenance(day)
	go w.maintenanceLoop()
}

func (w *Writer) requestMaintenance(day string) {
	w.requestMaintenanceAfterSeal(day, "", false, nil)
}

func (w *Writer) requestMaintenanceAfterSeal(day, protected string, sealed bool, syncErr error) {
	m := &w.maintenance
	w.mu.Lock()
	if sealed {
		m.sealing = ""
		m.sealSyncErr = syncErr
		m.sealGeneration++
	}
	m.day, m.protected = day, protected
	// The maintenance receiver only needs archive configuration and I/O hooks.
	// Passing these by value keeps active state and test hooks caller-owned.
	m.archive = archiveMaintenance{historyDays: w.cfg.HistoryDays, totalArchiveBytes: w.cfg.totalArchiveBytes, dir: w.dir, compress: w.compress, rename: w.rename, remove: w.remove, archiveSync: w.archiveSync}
	w.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (w *Writer) maintenanceLoop() {
	m := &w.maintenance
	defer close(m.done)
	var unlinkSyncErr error
	const initialDelay = time.Second
	const maxRetries = 3
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var retry <-chan time.Time
	attempt := 0
	for {
		select {
		case _, ok := <-m.wake:
			if !ok {
				return
			}
			attempt = 0
			if retry != nil {
				timer.Stop()
				retry = nil
			}
		case <-retry:
			retry = nil
		}
		err := w.maintainSnapshot(&unlinkSyncErr)
		w.setMaintenanceError(err)
		if err != nil && attempt < maxRetries {
			delay := initialDelay << attempt
			attempt++
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}
			retry = timer.C
		}
	}
}

func (w *Writer) maintainSnapshot(unlinkSyncErr *error) error {
	entries, err := archiveEntries(w.dir)
	if err != nil {
		return errors.Join(err, *unlinkSyncErr)
	}
	m := &w.maintenance
	// Read the seal state after the scan: a seal still in progress must be
	// excluded even if its rename was already visible in the directory.
	w.mu.RLock()
	archive := m.archive
	sealing, day, protected := m.sealing, m.day, m.protected
	syncErr, generation := m.sealSyncErr, m.sealGeneration
	w.mu.RUnlock()
	// An unlinked pending file is absent from all later snapshots. Retry
	// its directory sync explicitly before allowing retention on its gzip.
	if *unlinkSyncErr != nil {
		*unlinkSyncErr = archive.archiveSync(w.dir)
		if *unlinkSyncErr != nil {
			return *unlinkSyncErr
		}
	}
	if syncErr != nil {
		if err := w.syncSealDirectories(); err != nil {
			return err
		}
		w.mu.Lock()
		if generation == m.sealGeneration {
			m.sealSyncErr = nil
		}
		w.mu.Unlock()
	}
	eligible := entries[:0]
	reserved := make(map[string]uint64)
	for _, e := range entries {
		if e.Name() == sealing {
			continue
		}
		eligible = append(eligible, e)
		if d, i, _, ok := parseRollingArchive(e.Name()); ok {
			if max, found := reserved[d]; !found || i > max {
				reserved[d] = i
			}
		}
	}
	// The scan can overlap a seal, but neither that seal nor any later one
	// uses a basename from the worker's snapshot. Keep these names reserved
	// through publication and retention even if a directory scan misses a
	// rename, or retention already unlinked a file from this snapshot.
	w.mu.Lock()
	m.reserved = reserved
	m.reservationGeneration++
	w.mu.Unlock()
	err = archive.maintain(eligible, day, protected)
	*unlinkSyncErr = archive.unlinkSyncErr
	w.mu.Lock()
	m.reserved = nil
	m.reservationGeneration++
	w.mu.Unlock()
	return err
}

func (w *Writer) setSealing(target string) {
	w.mu.Lock()
	w.maintenance.sealing = target
	w.mu.Unlock()
}

func (w *Writer) syncSealDirectories() error {
	return errors.Join(syncRollingDir(w.dir), syncRollingDir(filepath.Dir(w.cfg.Filename)))
}

func (w *Writer) reservedArchive(day string) (uint64, bool, uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	m := &w.maintenance
	seq, found := m.reserved[day]
	return seq, found, m.reservationGeneration
}

func (w *Writer) drainMaintenance() {
	m := &w.maintenance
	close(m.wake)
	<-m.done
}

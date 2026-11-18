package logrotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type rollingArchive struct {
	name, day string
	seq       uint64
	size      int64
}

func rollingRetentionPlan(archives []rollingArchive, day string, history int) (expired, retained []rollingArchive) {
	sort.Slice(archives, func(i, j int) bool {
		a, b := archives[i], archives[j]
		if a.day != b.day {
			return a.day < b.day
		}
		return a.seq < b.seq
	})
	cutoff := day
	if history == 0 {
		return nil, archives
	}
	// Treat local date labels as civil dates so even skipped local midnights
	// do not affect the arithmetic.
	current, _ := time.Parse(rollingDate, day)
	for i, a := range archives {
		stamp, _ := time.Parse(rollingDate, a.day)
		// Canonical names have four-digit years; bounded differences fit int.
		age := int((current.Unix() - stamp.Unix()) / 86400)
		if age < history || a.day >= cutoff {
			return archives[:i], archives[i:]
		}
	}
	return archives, nil
}

func (s *archiveMaintenance) applyRetention(entries []os.DirEntry, day, protected string, recovered map[string]error) (removed bool, result error) {
	var archives []rollingArchive
	var undeletable uint64
	for _, e := range entries {
		name := e.Name()
		d, seq, suffix, ok := parseRollingArchive(name)
		if !ok || !e.Type().IsRegular() {
			continue
		}
		base := name
		var info os.FileInfo
		var err error
		switch suffix {
		case "":
			info, err = os.Lstat(filepath.Join(s.dir, name))
		case "gz":
			base = strings.TrimSuffix(name, ".gz")
			if _, attempted := recovered[base]; attempted {
				continue // Account for the final gzip through its pending entry.
			}
			info, err = os.Lstat(filepath.Join(s.dir, name))
		case "pending":
			base = strings.TrimSuffix(name, ".pending")
			// Include gzip published by recovery without admitting unrelated archives
			// sealed after the maintenance snapshot.
			name = base + ".gz"
			info, err = os.Lstat(filepath.Join(s.dir, name))
		default:
			continue
		}
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if base == protected || recovered[base] != nil {
			undeletable += uint64(info.Size())
		} else {
			archives = append(archives, rollingArchive{name, d, seq, info.Size()})
		}
	}
	expired, retained := rollingRetentionPlan(archives, day, s.historyDays)
	for _, a := range expired {
		if err := s.remove(filepath.Join(s.dir, a.name)); err == nil {
			removed = true
		} else if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
			undeletable += uint64(a.size)
		}
	}
	cap := uint64(s.totalArchiveBytes)
	if cap == 0 {
		return removed, result
	}
	total := undeletable
	for _, a := range retained {
		total += uint64(a.size)
	}
	for _, a := range retained {
		// Deleting more archives cannot satisfy a cap below the known floor.
		if total <= cap || undeletable > cap {
			break
		}
		if err := s.remove(filepath.Join(s.dir, a.name)); err == nil {
			removed = true
		} else if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
			undeletable += uint64(a.size)
			continue
		}
		total -= uint64(a.size)
	}
	if undeletable > cap {
		result = errors.Join(result, fmt.Errorf("logrotate: archive cap of %d bytes is unreachable: %d bytes cannot be removed from %s", cap, undeletable, s.dir))
	}
	return removed, result
}

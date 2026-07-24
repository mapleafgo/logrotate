// Package logrotate 提供按大小和/或按天滚动的日志文件写入器。
//
// 当前写入路径始终为 Config.Filename，便于 tail；归档命名：
//   - 纯按大小：Filename.1、Filename.2、…
//   - 按天 / 叠加：Filename.YYYY-MM-DD、Filename.YYYY-MM-DD.1、…
//
// 轮转关文件前与 Close 默认 fsync，降低崩溃丢尾与归档缺尾风险。
// 不做压缩、不按小时切分；依赖仅标准库。
// Writer 非并发安全；多写者请用 OpenAsync。
package logrotate

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config 配置按大小和/或按天的日志滚动。
// MaxSizeMB 与 Daily 可单独或同时启用；都关闭时 Writer 仅做 append（自动建目录）。
type Config struct {
	// Filename 当前写入的日志路径（必填）。进程始终写此路径。
	Filename string

	// MaxSizeMB 单文件大小上限（MiB）。0 表示不按大小轮转。
	MaxSizeMB int

	// MaxBackups 保留个数策略：
	//   - 纯按大小（非 Daily）：保留 Filename.1 .. Filename.N 个编号归档
	//   - Daily（含叠加）：按唯一日期保留最近 N 天（同日编号不裁，靠 MaxAgeDays 跨日清理）
	// 0 表示不按个数裁剪。
	MaxBackups int

	// Daily 是否在自然日边界轮转。
	Daily bool

	// MaxAgeDays 仅 Daily 时生效：删除文件名日期早于 now - MaxAgeDays 的归档。
	// 非 Daily 时静默不生效；0 表示不按年龄清理。
	MaxAgeDays int

	// SyncOnWrite 为 true 时每次 Write 成功后 fsync（很慢，仅极致可靠场景）。
	SyncOnWrite bool

	// DisableSync 为 true 时：轮转前与 Close 均不 fsync（默认可落盘；测试可关以加速）。
	DisableSync bool
}

// Writer 是可滚动日志写入器，实现 io.WriteCloser。
// 非并发安全：Write / Sync / Close 仅允许单 goroutine 调用。
// 多 goroutine 写日志请用 OpenAsync（单刷盘协程独占本 Writer）。
type Writer struct {
	filename   string
	maxSize    int64 // bytes；0 表示关闭按大小
	maxBackups int
	daily      bool
	maxAgeDays int
	syncWrite  bool
	doSync     bool // 轮转前与 Close 是否 fsync

	size int64
	file *os.File
	// day 为当前文件内容所属自然日（YYYY-MM-DD），仅 daily 时有意义。
	day string

	// now 可注入，便于测试跨日；生产路径为 time.Now。
	now func() time.Time
}

// Open 打开可滚动日志文件。
// Filename 为空，或 MaxBackups / MaxAgeDays 为负时返回错误。
// 默认在轮转关文件前与 Close 时 fsync；设 DisableSync 可关闭。
func Open(cfg Config) (*Writer, error) {
	if strings.TrimSpace(cfg.Filename) == "" {
		return nil, fmt.Errorf("logrotate: Filename 不能为空")
	}
	if cfg.MaxBackups < 0 {
		return nil, fmt.Errorf("logrotate: MaxBackups 不能为负，got %d", cfg.MaxBackups)
	}
	if cfg.MaxAgeDays < 0 {
		return nil, fmt.Errorf("logrotate: MaxAgeDays 不能为负，got %d", cfg.MaxAgeDays)
	}

	var maxSize int64
	if cfg.MaxSizeMB > 0 {
		maxSize = int64(cfg.MaxSizeMB) << 20
	}

	w := &Writer{
		filename:   cfg.Filename,
		maxSize:    maxSize,
		maxBackups: cfg.MaxBackups,
		daily:      cfg.Daily,
		maxAgeDays: cfg.MaxAgeDays,
		syncWrite:  cfg.SyncOnWrite,
		doSync:     !cfg.DisableSync,
		now:        time.Now,
	}
	if err := w.openExistingOrCreate(); err != nil {
		return nil, err
	}
	// 启动时顺带清理超龄归档，避免只在滚动时才删。
	w.cleanup()
	return w, nil
}

func (w *Writer) openExistingOrCreate() error {
	if dir := filepath.Dir(w.filename); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("logrotate: 无法创建日志目录 %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logrotate: 无法打开日志文件 %s: %w", w.filename, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logrotate: 无法 stat 日志文件 %s: %w", w.filename, err)
	}
	w.file = f
	w.size = info.Size()
	if w.daily {
		// 已有内容时用 mtime 的日期，重启跨日后首次 Write 会触发按天滚动。
		if info.Size() > 0 {
			w.day = w.dayString(info.ModTime())
		} else {
			w.day = w.dayString(w.now())
		}
	}
	return nil
}

// Write 写入日志。写前按需按天/按大小滚动；单条超过阈值仍写出，避免吞日志。
// 轮转失败时尽量以 append 方式恢复当前文件句柄，避免 O_TRUNC 清空未归档内容。
// 非并发安全；多写者请用 AsyncWriter。
func (w *Writer) Write(p []byte) (int, error) {
	if w.file == nil {
		return 0, fmt.Errorf("logrotate: 日志文件已关闭")
	}
	if w.daily {
		today := w.dayString(w.now())
		if w.day != "" && today != w.day {
			if err := w.rotateToArchive(w.day); err != nil {
				return 0, err
			}
			w.day = today
		}
	}
	// 写前滚动：当前 size + 本次写入会超过阈值时先轮转。
	if w.maxSize > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxSize {
		if err := w.rotateSize(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, err
	}
	if w.syncWrite {
		if err := w.file.Sync(); err != nil {
			return n, fmt.Errorf("logrotate: Write 后 fsync 失败: %w", err)
		}
	}
	return n, nil
}

// Sync 将已写入数据刷到存储设备。文件已关闭时返回错误。
// 非并发安全。
func (w *Writer) Sync() error {
	if w.file == nil {
		return fmt.Errorf("logrotate: 日志文件已关闭")
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("logrotate: fsync 失败: %w", err)
	}
	return nil
}

// Close 关闭底层文件；默认先 fsync 再关。重复调用安全（后续为 no-op）。
// 非并发安全。
func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	var first error
	if w.doSync {
		if err := w.file.Sync(); err != nil {
			first = fmt.Errorf("logrotate: Close 前 fsync 失败: %w", err)
		}
	}
	if err := w.file.Close(); err != nil && first == nil {
		first = err
	}
	w.file = nil
	return first
}

func (w *Writer) rotateSize() error {
	if w.daily {
		day := w.day
		if day == "" {
			day = w.dayString(w.now())
		}
		return w.rotateToArchive(day)
	}
	return w.rotateNumbered()
}

// rotateNumbered 纯按大小：Filename -> .1 -> .2 ...
// Rename 失败时恢复为 append 打开，绝不在原路径 O_TRUNC。
func (w *Writer) rotateNumbered() error {
	if err := w.prepareCloseForRotate(); err != nil {
		return err
	}

	var rotateErr error
	if w.maxBackups > 0 {
		oldest := fmt.Sprintf("%s.%d", w.filename, w.maxBackups)
		_ = os.Remove(oldest)
		for i := w.maxBackups - 1; i >= 1; i-- {
			src := fmt.Sprintf("%s.%d", w.filename, i)
			dst := fmt.Sprintf("%s.%d", w.filename, i+1)
			if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
				// 备份链失败不立即返回：仍尝试归档当前文件；最终一并报告。
				if rotateErr == nil {
					rotateErr = fmt.Errorf("logrotate: 备份链 rename %s -> %s: %w", src, dst, err)
				}
			}
		}
		if err := os.Rename(w.filename, w.filename+".1"); err != nil {
			rotateErr = fmt.Errorf("logrotate: 编号轮转失败 %s -> %s.1: %w", w.filename, w.filename, err)
		}
	} else {
		if err := os.Remove(w.filename); err != nil && !os.IsNotExist(err) {
			rotateErr = fmt.Errorf("logrotate: 删除当前日志失败 %s: %w", w.filename, err)
		}
	}

	if rotateErr != nil {
		if err := w.openExistingOrCreate(); err != nil {
			return fmt.Errorf("logrotate: %v; 且无法恢复打开: %w", rotateErr, err)
		}
		return rotateErr
	}
	return w.reopenTrunc()
}

// rotateToArchive 将当前文件归档到 Filename.YYYY-MM-DD[.N]，再重开当前路径。
func (w *Writer) rotateToArchive(day string) error {
	if err := w.prepareCloseForRotate(); err != nil {
		return err
	}
	// 空文件无需归档，直接重开即可。
	if info, err := os.Stat(w.filename); err == nil && info.Size() == 0 {
		return w.reopenTrunc()
	} else if os.IsNotExist(err) {
		return w.reopenTrunc()
	}

	target, err := w.nextDatedArchive(day)
	if err != nil {
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: %v; 且无法恢复打开: %w", err, rerr)
		}
		return err
	}
	if err := os.Rename(w.filename, target); err != nil {
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 归档失败 %s -> %s: %v; 且无法恢复打开: %w", w.filename, target, err, rerr)
		}
		return fmt.Errorf("logrotate: 归档失败 %s -> %s: %w", w.filename, target, err)
	}
	w.cleanup()
	return w.reopenTrunc()
}

// prepareCloseForRotate 轮转前可选 fsync 再关文件。
func (w *Writer) prepareCloseForRotate() error {
	if w.file == nil {
		return nil
	}
	if w.doSync {
		if err := w.file.Sync(); err != nil {
			// fsync 失败仍继续关文件并尝试轮转，但把错误带回会阻断写；
			// 落盘失败时继续 rename 可能归档不完整，直接返回更安全。
			return fmt.Errorf("logrotate: 轮转前 fsync 失败: %w", err)
		}
	}
	return w.closeFile()
}

func (w *Writer) closeFile() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) reopenTrunc() error {
	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		// 尝试 append 恢复，避免后续写全部失败。
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 滚动后无法打开日志文件 %s: %v; 恢复 append 也失败: %w", w.filename, err, rerr)
		}
		return fmt.Errorf("logrotate: 滚动后无法以 TRUNC 打开 %s: %w", w.filename, err)
	}
	w.file = f
	w.size = 0
	return nil
}

func (w *Writer) nextDatedArchive(day string) (string, error) {
	base := fmt.Sprintf("%s.%s", w.filename, day)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return base, nil
	} else if err != nil {
		return "", fmt.Errorf("logrotate: stat 归档 %s: %w", base, err)
	}
	for i := 1; i < 10000; i++ {
		name := fmt.Sprintf("%s.%s.%d", w.filename, day, i)
		if _, err := os.Stat(name); os.IsNotExist(err) {
			return name, nil
		} else if err != nil {
			return "", fmt.Errorf("logrotate: stat 归档 %s: %w", name, err)
		}
	}
	return "", fmt.Errorf("logrotate: 同日归档编号耗尽（%s）", day)
}

// cleanup Daily 下按 MaxAgeDays 删超龄归档，再按 MaxBackups 保留最近 N 个日期。
func (w *Writer) cleanup() {
	if !w.daily {
		return
	}
	archives := w.listDatedArchives()
	if len(archives) == 0 {
		return
	}

	now := w.now()
	if w.maxAgeDays > 0 {
		cutoff := w.dayString(now.AddDate(0, 0, -w.maxAgeDays))
		for _, a := range archives {
			if a.day < cutoff {
				_ = os.Remove(a.path)
			}
		}
		// 重新列举，供后续 MaxBackups 日级裁剪
		archives = w.listDatedArchives()
	}

	// Daily 下 MaxBackups 表示保留最近 N 个「日期」（与是否按大小无关）。
	if w.maxBackups > 0 {
		// 按日期去重，从旧到新
		days := uniqueDays(archives)
		if len(days) > w.maxBackups {
			drop := make(map[string]struct{}, len(days)-w.maxBackups)
			for _, d := range days[:len(days)-w.maxBackups] {
				drop[d] = struct{}{}
			}
			for _, a := range archives {
				if _, ok := drop[a.day]; ok {
					_ = os.Remove(a.path)
				}
			}
		}
	}
}

type datedArchive struct {
	path string
	day  string
	seq  int // 0 = Filename.YYYY-MM-DD，>0 = .N
}

func (w *Writer) listDatedArchives() []datedArchive {
	dir := filepath.Dir(w.filename)
	if dir == "" {
		dir = "."
	}
	base := filepath.Base(w.filename)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	prefix := base + "."
	var out []datedArchive
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(name, prefix)
		day, seq, ok := parseDatedSuffix(rest)
		if !ok {
			continue
		}
		out = append(out, datedArchive{
			path: filepath.Join(dir, name),
			day:  day,
			seq:  seq,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].day != out[j].day {
			return out[i].day < out[j].day
		}
		return out[i].seq < out[j].seq
	})
	return out
}

// parseDatedSuffix 解析 "2006-01-02" 或 "2006-01-02.3"。
func parseDatedSuffix(rest string) (day string, seq int, ok bool) {
	if len(rest) < 10 {
		return "", 0, false
	}
	day = rest[:10]
	if _, err := time.Parse("2006-01-02", day); err != nil {
		return "", 0, false
	}
	if len(rest) == 10 {
		return day, 0, true
	}
	if rest[10] != '.' {
		return "", 0, false
	}
	n, err := strconv.Atoi(rest[11:])
	if err != nil || n < 1 {
		return "", 0, false
	}
	return day, n, true
}

func uniqueDays(archives []datedArchive) []string {
	seen := make(map[string]struct{})
	var days []string
	for _, a := range archives {
		if _, ok := seen[a.day]; ok {
			continue
		}
		seen[a.day] = struct{}{}
		days = append(days, a.day)
	}
	return days
}

func (w *Writer) dayString(t time.Time) string {
	t = t.Local()
	return t.Format("2006-01-02")
}

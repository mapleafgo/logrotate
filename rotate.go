// Package logrotate 提供按大小或按天滚动的日志文件写入器。
//
// 当前写入路径始终为 Config.Filename，便于 tail。
// 两种模式由 Daily 决定：
//   - 纯按大小（Daily=false）：归档为 Filename.1、Filename.2、…
//   - 按天（Daily=true）：归档为 Filename.YYYY-MM-DD、Filename.YYYY-MM-DD.1、…
//
// 轮转关文件前与 Close 均 fsync，降低崩溃丢尾与归档缺尾风险。
// 不按小时切分；依赖仅标准库。
// Writer 非并发安全；多写者请用 OpenAsync。
package logrotate

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config 配置日志滚动。Daily 决定模式：false 为纯按大小，true 为按天。
type Config struct {
	// Filename 当前写入的日志路径（必填）。进程始终写此路径。
	Filename string

	// MaxFileSizeMB 单文件大小上限（MiB）。纯按大小模式触发编号轮转；
	// 按天模式触发同日归档。0 表示不按大小轮转。
	// 负值或过大（左移 20 位会溢出）时 Open 报错。
	MaxFileSizeMB int

	// MaxBackups 纯按大小模式下保留编号归档个数（.1..N）。
	// 按天模式下不生效。0 表示不限。
	MaxBackups int

	// MaxDays 按天模式下保留最近 N 天的归档。
	// 纯按大小模式下不生效。0 表示不限。
	MaxDays int

	// MaxTotalSizeMB 所有归档总大小上限（MiB），超出时从最老归档开始删除。
	// 两模式共用。0 表示不限。
	MaxTotalSizeMB int

	// Daily 是否按自然日轮转。
	Daily bool

	// Compress 归档后是否 gzip 压缩。
	//   - 纯按大小：每个归档独立压缩为 .N.gz。
	//   - 按天：同日所有归档追加到一个 multi-member .YYYY-MM-DD.gz。
	// 压缩失败不影响当前写入。
	Compress bool
}

// Writer 是可滚动日志写入器，实现 io.WriteCloser。
// 非并发安全：Write / Sync / Close 仅允许单 goroutine 调用。
// 多 goroutine 写日志请用 OpenAsync（单刷盘协程独占本 Writer）。
type Writer struct {
	filename     string
	maxFileSize  int64 // bytes；0 表示关闭按大小
	maxBackups   int
	maxDays      int
	maxTotalSize int64 // bytes；0 表示不限
	daily        bool
	compress     bool

	size int64
	file *os.File

	// day 为当前文件内容所属自然日（YYYY-MM-DD），仅 daily 时有意义。
	day string

	// now 可注入，便于测试跨日；生产路径为 time.Now。
	now func() time.Time
}

// Open 打开可滚动日志文件。Filename 为空或数值配置为负时返回错误；
// MaxFileSizeMB / MaxTotalSizeMB 过大（左移 20 位会溢出）时也返回错误。
// 启动时立即清理超限归档。返回的 Writer 非并发安全。
func Open(cfg Config) (*Writer, error) {
	if strings.TrimSpace(cfg.Filename) == "" {
		return nil, fmt.Errorf("logrotate: Filename 不能为空")
	}
	if cfg.MaxFileSizeMB < 0 {
		return nil, fmt.Errorf("logrotate: MaxFileSizeMB 不能为负，got %d", cfg.MaxFileSizeMB)
	}
	if cfg.MaxBackups < 0 {
		return nil, fmt.Errorf("logrotate: MaxBackups 不能为负，got %d", cfg.MaxBackups)
	}
	if cfg.MaxDays < 0 {
		return nil, fmt.Errorf("logrotate: MaxDays 不能为负，got %d", cfg.MaxDays)
	}
	if cfg.MaxTotalSizeMB < 0 {
		return nil, fmt.Errorf("logrotate: MaxTotalSizeMB 不能为负，got %d", cfg.MaxTotalSizeMB)
	}

	// int64 最大值右移 20 位 ≈ 8 PiB 的 MiB 数，远超合理日志阈值。
	const sizeMBLimit = int64(^uint64(0)>>1) >> 20

	var maxFileSize int64
	if cfg.MaxFileSizeMB > 0 {
		if int64(cfg.MaxFileSizeMB) > sizeMBLimit {
			return nil, fmt.Errorf("logrotate: MaxFileSizeMB 过大，got %d", cfg.MaxFileSizeMB)
		}
		maxFileSize = int64(cfg.MaxFileSizeMB) << 20
	}

	var maxTotalSize int64
	if cfg.MaxTotalSizeMB > 0 {
		if int64(cfg.MaxTotalSizeMB) > sizeMBLimit {
			return nil, fmt.Errorf("logrotate: MaxTotalSizeMB 过大，got %d", cfg.MaxTotalSizeMB)
		}
		maxTotalSize = int64(cfg.MaxTotalSizeMB) << 20
	}

	w := &Writer{
		filename:     cfg.Filename,
		maxFileSize:  maxFileSize,
		maxBackups:   cfg.MaxBackups,
		maxDays:      cfg.MaxDays,
		maxTotalSize: maxTotalSize,
		daily:        cfg.Daily,
		compress:     cfg.Compress,
		now:          time.Now,
	}
	if err := w.openExistingOrCreate(); err != nil {
		return nil, err
	}
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
		if info.Size() > 0 {
			w.day = w.dayString(info.ModTime())
		} else {
			w.day = w.dayString(w.now())
		}
	}
	return nil
}

// Write 写入日志。写前按需滚动；单条超过阈值仍写出，避免吞日志。
func (w *Writer) Write(p []byte) (int, error) {
	if w.file == nil {
		return 0, fmt.Errorf("logrotate: 日志文件已关闭")
	}
	if w.daily {
		today := w.dayString(w.now())
		if w.day != "" && today != w.day {
			if err := w.rotateDaily(w.day); err != nil {
				return 0, err
			}
			w.day = today
		}
	}
	if w.maxFileSize > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxFileSize {
		if w.daily {
			if err := w.rotateDaily(w.day); err != nil {
				return 0, err
			}
		} else {
			if err := w.rotateNumbered(); err != nil {
				return 0, err
			}
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

// Sync 将已写入数据刷到存储设备。
func (w *Writer) Sync() error {
	if w.file == nil {
		return fmt.Errorf("logrotate: 日志文件已关闭")
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("logrotate: fsync 失败: %w", err)
	}
	return nil
}

// Close 关闭底层文件；先 fsync 再关。重复调用安全。
func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	var first error
	if err := w.file.Sync(); err != nil {
		first = fmt.Errorf("logrotate: Close 前 fsync 失败: %w", err)
	}
	if err := w.file.Close(); err != nil && first == nil {
		first = err
	}
	w.file = nil
	return first
}

// ==================== 纯按大小 ====================

func (w *Writer) rotateNumbered() error {
	if err := w.prepareCloseForRotate(); err != nil {
		return err
	}

	var rotateErr error
	var archivePath string

	if w.maxBackups > 0 {
		suffix := ""
		if w.compress {
			suffix = ".gz"
		}
		oldest := fmt.Sprintf("%s.%d%s", w.filename, w.maxBackups, suffix)
		_ = os.Remove(oldest)
		for i := w.maxBackups - 1; i >= 1; i-- {
			src := fmt.Sprintf("%s.%d%s", w.filename, i, suffix)
			dst := fmt.Sprintf("%s.%d%s", w.filename, i+1, suffix)
			if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
				if rotateErr == nil {
					rotateErr = fmt.Errorf("logrotate: 备份链 rename %s -> %s: %w", src, dst, err)
				}
			}
		}
		archivePath = w.filename + ".1"
		if err := os.Rename(w.filename, archivePath); err != nil {
			rotateErr = fmt.Errorf("logrotate: 编号轮转失败 %s -> %s: %w", w.filename, archivePath, err)
			archivePath = ""
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
	if w.compress && archivePath != "" {
		// 压缩失败不阻断 reopenTrunc；归档以未压缩形式保留，不影响写入。
		_ = w.compressArchive(archivePath)
	}
	w.cleanupTotalSize()
	return w.reopenTrunc()
}

// ==================== 按天 ====================

func (w *Writer) rotateDaily(day string) error {
	if err := w.prepareCloseForRotate(); err != nil {
		return err
	}
	if info, err := os.Stat(w.filename); (err == nil && info.Size() == 0) || os.IsNotExist(err) {
		return w.reopenTrunc()
	}

	if w.compress {
		if err := w.rotateToDatedGz(day); err != nil {
			return err
		}
	} else {
		target := w.nextDatedArchive(day)
		if err := os.Rename(w.filename, target); err != nil {
			if rerr := w.openExistingOrCreate(); rerr != nil {
				return fmt.Errorf("logrotate: 归档失败 %s -> %s: %v; 且无法恢复打开: %w", w.filename, target, err, rerr)
			}
			return fmt.Errorf("logrotate: 归档失败 %s -> %s: %w", w.filename, target, err)
		}
	}

	w.cleanupDaily()
	return w.reopenTrunc()
}

func (w *Writer) rotateToDatedGz(day string) error {
	gzPath := fmt.Sprintf("%s.%s.gz", w.filename, day)

	src, err := os.Open(w.filename)
	if err != nil {
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 打开待归档 %s: %v; 恢复失败: %w", w.filename, err, rerr)
		}
		return fmt.Errorf("logrotate: 打开待归档 %s: %w", w.filename, err)
	}
	defer func() { _ = src.Close() }()

	// O_APPEND 追加：每次 NewWriter 生成一个独立 gzip member，
	// 同日多次归档合入同一文件，gunzip/zcat 可顺序读取全部 member。
	dst, err := os.OpenFile(gzPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 创建/打开 %s: %v; 恢复失败: %w", gzPath, err, rerr)
		}
		return fmt.Errorf("logrotate: 创建/打开 %s: %w", gzPath, err)
	}

	gw := gzip.NewWriter(dst)
	if _, err := io.Copy(gw, src); err != nil {
		_ = gw.Close()
		_ = dst.Close()
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: gzip 写入 %s: %v; 恢复失败: %w", gzPath, err, rerr)
		}
		return fmt.Errorf("logrotate: gzip 写入 %s: %w", gzPath, err)
	}
	if err := gw.Close(); err != nil {
		_ = dst.Close()
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: gzip 关闭 %s: %v; 恢复失败: %w", gzPath, err, rerr)
		}
		return fmt.Errorf("logrotate: gzip 关闭 %s: %w", gzPath, err)
	}
	if err := dst.Close(); err != nil {
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 关闭 %s: %v; 恢复失败: %w", gzPath, err, rerr)
		}
		return fmt.Errorf("logrotate: 关闭 %s: %w", gzPath, err)
	}
	if err := os.Remove(w.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logrotate: 删除已归档文件 %s: %w", w.filename, err)
	}
	return nil
}

// ==================== 工具方法 ====================

func (w *Writer) prepareCloseForRotate() error {
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("logrotate: 轮转前 fsync 失败: %w", err)
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
		if rerr := w.openExistingOrCreate(); rerr != nil {
			return fmt.Errorf("logrotate: 滚动后无法打开日志文件 %s: %v; 恢复 append 也失败: %w", w.filename, err, rerr)
		}
		return fmt.Errorf("logrotate: 滚动后无法以 TRUNC 打开 %s: %w", w.filename, err)
	}
	w.file = f
	w.size = 0
	return nil
}

func (w *Writer) nextDatedArchive(day string) string {
	base := fmt.Sprintf("%s.%s", w.filename, day)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return base
	}
	for i := 1; ; i++ {
		name := fmt.Sprintf("%s.%s.%d", w.filename, day, i)
		if _, err := os.Stat(name); os.IsNotExist(err) {
			return name
		}
	}
}

// ==================== 清理 ====================

func (w *Writer) cleanup() {
	if w.daily {
		w.cleanupDaily()
	} else {
		w.cleanupTotalSize()
	}
}

func (w *Writer) cleanupDaily() {
	archives := w.listDatedArchives()
	if len(archives) == 0 {
		return
	}

	if w.maxDays > 0 {
		cutoff := w.dayString(w.now().AddDate(0, 0, -w.maxDays))
		var keep []datedArchive
		for _, a := range archives {
			if a.day >= cutoff {
				keep = append(keep, a)
			} else {
				_ = os.Remove(a.path)
			}
		}
		archives = keep
	}

	if w.maxTotalSize > 0 {
		var total int64
		for _, a := range archives {
			total += a.size
		}
		for _, a := range archives {
			if total <= w.maxTotalSize {
				break
			}
			_ = os.Remove(a.path)
			total -= a.size
		}
	}
}

func (w *Writer) cleanupTotalSize() {
	if w.maxTotalSize <= 0 {
		return
	}
	archives := w.listNumberedArchives()
	if len(archives) == 0 {
		return
	}
	var total int64
	for _, a := range archives {
		total += a.size
	}
	for _, a := range archives {
		if total <= w.maxTotalSize {
			break
		}
		_ = os.Remove(a.path)
		total -= a.size
	}
}

// ==================== 归档列表 ====================

type datedArchive struct {
	path string
	day  string
	seq  int
	size int64
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
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, datedArchive{
			path: filepath.Join(dir, name),
			day:  day,
			seq:  seq,
			size: info.Size(),
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

type numberedArchive struct {
	path string
	n    int
	size int64
}

func (w *Writer) listNumberedArchives() []numberedArchive {
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
	var out []numberedArchive
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(name, prefix)
		n, ok := parseNumberedSuffix(rest)
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, numberedArchive{
			path: filepath.Join(dir, name),
			n:    n,
			size: info.Size(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		// 编号大的更旧（链移位时被推向高编号），降序使最老归档排首，
		// 供 cleanupTotalSize 从最老开始删。
		return out[i].n > out[j].n
	})
	return out
}

func parseDatedSuffix(rest string) (day string, seq int, ok bool) {
	rest = strings.TrimSuffix(rest, ".gz")
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

func parseNumberedSuffix(rest string) (n int, ok bool) {
	rest = strings.TrimSuffix(rest, ".gz")
	n, err := strconv.Atoi(rest)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// ==================== 压缩 ====================

func (w *Writer) compressArchive(path string) error {
	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("logrotate: 压缩打开 %s: %w", path, err)
	}
	defer func() { _ = src.Close() }()

	dstPath := path + ".gz"
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("logrotate: 压缩创建 %s: %w", dstPath, err)
	}

	gw := gzip.NewWriter(dst)
	if _, err := io.Copy(gw, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)
		return fmt.Errorf("logrotate: 压缩写入 %s: %w", dstPath, err)
	}
	if err := gw.Close(); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)
		return fmt.Errorf("logrotate: 压缩关闭 gzip %s: %w", dstPath, err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(dstPath)
		return fmt.Errorf("logrotate: 压缩关闭 %s: %w", dstPath, err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("logrotate: 压缩后删除原文件 %s: %w", path, err)
	}
	return nil
}

func (w *Writer) dayString(t time.Time) string {
	t = t.Local()
	return t.Format("2006-01-02")
}

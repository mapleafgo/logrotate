package logrotate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"sync"
	"sync/atomic"
)

// ErrDropped 表示异步队列已满且策略为 FullDropNewest，当前写入被丢弃。
var ErrDropped = errors.New("logrotate: async queue is full; record dropped")

// ErrClosed 表示异步写入器已停止接收；也可用 errors.Is 匹配 fs.ErrClosed。
var ErrClosed = fmt.Errorf("logrotate: async writer is closed: %w", fs.ErrClosed)

// FullPolicy 异步队列满时的行为。
type FullPolicy int

const (
	// FullBlock 阻塞直到队列有空位（默认）。不丢数据，可能放大调用方延迟。
	FullBlock FullPolicy = iota
	// FullDropNewest 立即丢弃本条，Write 返回 (0, ErrDropped)。
	FullDropNewest
)

// defaultQueueSize 是 AsyncConfig.QueueSize 未指定或为零时的默认缓冲条数。
const defaultQueueSize = 1024

// AsyncConfig 异步写入配置。
type AsyncConfig struct {
	// QueueSize 缓冲条数（按 Write 调用计，非字节）。<=0 时默认 defaultQueueSize。
	QueueSize int
	// FullPolicy 队列满时策略，默认 FullBlock。
	FullPolicy FullPolicy
}

// FileSink 是 AsyncWriter 独占的底层写入目标，各方法由异步层串行调用。
// Write 遵守 io.Writer 契约；短写且无错误时按 io.ErrShortWrite 处理。
// Close 在已接收的数据交付后调用，不会隐式调用 Sync。
type FileSink interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// AsyncWriter 在单刷盘 goroutine 上串行写底层，调用方 Write 只入队。
// Close 会拒绝新写入、唤醒等待入队的调用、排空队列并关闭底层，不隐式调用 Sync。
// 停机请先停止业务写日志，再 Close，以保证“写完再退”。
// Write 成功仅表示入队；需要确认持久化时，请显式调用并检查 Sync。
type AsyncWriter struct {
	sink   FileSink
	policy FullPolicy

	queueMu  sync.Mutex
	readable *sync.Cond
	writable *sync.Cond
	queue    []asyncMsg
	head     int
	count    int
	spare    [][]byte // guarded by queueMu
	closed   bool

	loopWG    sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	dropped atomic.Uint64

	errMu       sync.Mutex
	lastErr     error
	deliveryErr error // 仅由 loop 写，Close 在 loop 结束后读取
}

// asyncMsg 保证 sink 的 Write/Sync 只在 loop 协程执行，不破坏底层 *Writer 单所有者契约。
type asyncMsg struct {
	data     []byte
	syncDone chan error
}

// OpenAsync 启动后台刷盘。sink 不可为 nil，且成功后由 AsyncWriter 独占并关闭。
// Write 会拷贝 p，调用方可复用缓冲区。
func OpenAsync(sink FileSink, cfg AsyncConfig) (*AsyncWriter, error) {
	if nilSink(sink) {
		return nil, errors.New("logrotate: async sink is nil")
	}
	cfg, err := resolveAsyncConfig(cfg)
	if err != nil {
		return nil, err
	}
	aw := &AsyncWriter{sink: sink, policy: cfg.FullPolicy, queue: make([]asyncMsg, cfg.QueueSize)}
	aw.readable = sync.NewCond(&aw.queueMu)
	aw.writable = sync.NewCond(&aw.queueMu)
	aw.loopWG.Add(1)
	go aw.loop()
	return aw, nil
}

func nilSink(sink FileSink) bool {
	v := reflect.ValueOf(sink)
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func resolveAsyncConfig(cfg AsyncConfig) (AsyncConfig, error) {
	if cfg.FullPolicy != FullBlock && cfg.FullPolicy != FullDropNewest {
		return AsyncConfig{}, fmt.Errorf("logrotate: invalid async queue full policy %d", cfg.FullPolicy)
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	return cfg, nil
}

func (aw *AsyncWriter) loop() {
	defer aw.loopWG.Done()
	var delivered []byte
	for {
		msg, ok := aw.dequeue(delivered)
		delivered = nil
		if !ok {
			return
		}
		if msg.syncDone != nil {
			err := aw.sink.Sync()
			aw.setLastError(err)
			if err == nil {
				err = aw.deliveryErr
			} else if aw.deliveryErr != nil {
				err = errors.Join(aw.deliveryErr, err)
			}
			msg.syncDone <- err
			continue
		}
		n, err := aw.sink.Write(msg.data)
		delivered = msg.data
		if err == nil && n != len(msg.data) {
			err = io.ErrShortWrite
		}
		if aw.deliveryErr == nil {
			aw.deliveryErr = err
		}
		aw.setLastError(err)
	}
}

func (aw *AsyncWriter) setLastError(err error) {
	aw.errMu.Lock()
	aw.lastErr = err
	aw.errMu.Unlock()
}

func (aw *AsyncWriter) enqueue(msg asyncMsg, drop bool) error {
	aw.queueMu.Lock()
	defer aw.queueMu.Unlock()
	for !aw.closed && aw.count == len(aw.queue) {
		if drop {
			aw.dropped.Add(1)
			return ErrDropped
		}
		aw.writable.Wait()
	}
	if aw.closed {
		return ErrClosed
	}
	// Copy only after admission; the caller may reuse its input after Write.
	msg.data = aw.copyRecord(msg.data)
	aw.queue[(aw.head+aw.count)%len(aw.queue)] = msg
	aw.count++
	aw.readable.Signal()
	return nil
}

// Cap idle retention at 256 KiB per writer, even with a large queue.
const (
	maxSpareRecordBytes = 4 << 10
	maxSpareRecords     = 64
)

// copyRecord is called with queueMu held.
func (aw *AsyncWriter) copyRecord(p []byte) []byte {
	if len(p) == 0 || len(p) > maxSpareRecordBytes || len(aw.spare) == 0 {
		return bytes.Clone(p)
	}
	i := len(aw.spare) - 1
	buf := aw.spare[i]
	if cap(buf) < len(p) {
		return bytes.Clone(p)
	}
	aw.spare[i] = nil
	aw.spare = aw.spare[:i]
	return append(buf[:0], p...)
}

func (aw *AsyncWriter) dequeue(delivered []byte) (asyncMsg, bool) {
	aw.queueMu.Lock()
	defer aw.queueMu.Unlock()
	// Reclaim only after the sink returns, with no extra lock acquisition.
	if cap(delivered) > 0 && cap(delivered) <= maxSpareRecordBytes && len(aw.spare) < min(len(aw.queue), maxSpareRecords) {
		aw.spare = append(aw.spare, delivered[:0])
	}
	for aw.count == 0 && !aw.closed {
		aw.readable.Wait()
	}
	if aw.count == 0 {
		aw.spare = nil
		return asyncMsg{}, false
	}
	msg := aw.queue[aw.head]
	aw.queue[aw.head] = asyncMsg{}
	aw.head = (aw.head + 1) % len(aw.queue)
	aw.count--
	aw.writable.Signal()
	return msg, true
}

// Write 将日志入队。默认队列满时阻塞；FullDropNewest 时可能返回 ErrDropped。
// 停止接收后返回 ErrClosed。成功入队时拷贝 p。并发安全。
func (aw *AsyncWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		aw.queueMu.Lock()
		defer aw.queueMu.Unlock()
		if aw.closed {
			return 0, ErrClosed
		}
		return 0, nil
	}
	if err := aw.enqueue(asyncMsg{data: p}, aw.policy == FullDropNewest); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Dropped 返回因 FullDropNewest 丢弃的条数。
func (aw *AsyncWriter) Dropped() uint64 {
	return aw.dropped.Load()
}

// LastError 返回后台刷盘的当前错误状态：最近一次失败，或成功后清除为 nil。
// Write 返回 nil 只表示入队成功，不表示已落盘；请周期性读本方法做健康检查。
func (aw *AsyncWriter) LastError() error {
	aw.errMu.Lock()
	defer aw.errMu.Unlock()
	return aw.lastErr
}

// Sync 在队列中放入 FIFO 屏障；先前已入队的记录写完后同步底层。
// 返回首个后台写入错误及本次同步错误（若有）。停止接收后返回 ErrClosed。并发安全。
func (aw *AsyncWriter) Sync() error {
	done := make(chan error, 1)
	if err := aw.enqueue(asyncMsg{syncDone: done}, false); err != nil {
		return err
	}
	return <-done
}

// Close 停止接收、唤醒等待入队的调用、排空队列并关闭底层。
// 不隐式调用 Sync，也不设超时。返回首个后台写入错误及底层关闭错误（若有）。
// 重复调用返回同一结果。
func (aw *AsyncWriter) Close() error {
	aw.closeOnce.Do(func() {
		aw.queueMu.Lock()
		aw.closed = true
		aw.readable.Signal()
		aw.writable.Broadcast()
		aw.queueMu.Unlock()
		aw.loopWG.Wait()

		aw.closeErr = errors.Join(aw.deliveryErr, aw.sink.Close())
	})
	return aw.closeErr
}

package logrotate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
)

// ErrDropped 表示异步队列已满且策略为 FullDropNewest，当前写入被丢弃。
var ErrDropped = errors.New("logrotate: 异步队列已满，已丢弃本条日志")

// ErrClosed 表示异步写入器已停止接收；也可用 errors.Is 匹配 fs.ErrClosed。
var ErrClosed = fmt.Errorf("logrotate: 异步日志已关闭: %w", fs.ErrClosed)

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

// FileSink 是异步层底层的文件写入抽象。*Writer 已实现；测试可注入包装类型。
type FileSink interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// AsyncWriter 在单刷盘 goroutine 上串行写底层，调用方 Write 只入队。
// Close 会拒绝新写入、等待在途 Write、排空队列、Sync 再 Close 底层。
// 停机请先停止业务写日志，再 Close，以保证“写完再退”。
// Write 成功仅表示入队；刷盘错误见 LastError 与 Close。
type AsyncWriter struct {
	sink    FileSink
	ch      chan asyncMsg
	closeCh chan struct{} // Close 时先关闭，解除阻塞中的 Write
	policy  FullPolicy

	admission sync.Mutex
	senders   sync.WaitGroup // 已准入、可能发送到 ch 的调用
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
	if sink == nil {
		return nil, fmt.Errorf("logrotate: async sink 不能为 nil")
	}
	if cfg.FullPolicy != FullBlock && cfg.FullPolicy != FullDropNewest {
		return nil, fmt.Errorf("logrotate: 未知的异步队列满策略 %d", cfg.FullPolicy)
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	aw := &AsyncWriter{
		sink:    sink,
		ch:      make(chan asyncMsg, cfg.QueueSize),
		closeCh: make(chan struct{}),
		policy:  cfg.FullPolicy,
	}
	aw.loopWG.Add(1)
	go aw.loop()
	return aw, nil
}

func (aw *AsyncWriter) loop() {
	defer aw.loopWG.Done()
	for msg := range aw.ch {
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

// admit registers a sender before Close can seal admission and wait.
func (aw *AsyncWriter) admit() bool {
	aw.admission.Lock()
	defer aw.admission.Unlock()
	select {
	case <-aw.closeCh:
		return false
	default:
		aw.senders.Add(1)
		return true
	}
}

// Write 将日志入队。默认队列满时阻塞；FullDropNewest 时可能返回 ErrDropped。
// 停止接收后返回 ErrClosed。会拷贝 p。并发安全。
func (aw *AsyncWriter) Write(p []byte) (int, error) {
	if !aw.admit() {
		return 0, ErrClosed
	}
	defer aw.senders.Done()
	if len(p) == 0 {
		return 0, nil
	}
	msg := asyncMsg{data: bytes.Clone(p)}
	if aw.policy == FullDropNewest {
		select {
		case <-aw.closeCh:
			return 0, ErrClosed
		case aw.ch <- msg:
			return len(p), nil
		default:
			aw.dropped.Add(1)
			return 0, ErrDropped
		}
	}

	select {
	case <-aw.closeCh:
		return 0, ErrClosed
	case aw.ch <- msg:
		return len(p), nil
	}
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
	if !aw.admit() {
		return ErrClosed
	}
	done := make(chan error, 1)
	select {
	case <-aw.closeCh:
		aw.senders.Done()
		return ErrClosed
	case aw.ch <- asyncMsg{syncDone: done}:
		aw.senders.Done()
		return <-done
	}
}

// Close 停止接收、唤醒等待入队的调用、排空队列、同步并关闭底层。
// 返回首个后台写入错误以及最终同步和关闭错误（若有）。重复调用返回同一结果。
func (aw *AsyncWriter) Close() error {
	aw.closeOnce.Do(func() {
		aw.admission.Lock()
		close(aw.closeCh)
		aw.admission.Unlock()
		aw.senders.Wait()
		close(aw.ch)
		aw.loopWG.Wait()

		syncErr := aw.sink.Sync()
		closeErr := aw.sink.Close()
		aw.closeErr = errors.Join(aw.deliveryErr, syncErr, closeErr)
	})
	return aw.closeErr
}

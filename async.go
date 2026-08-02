package logrotate

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrDropped 表示异步队列已满且策略为 FullDropNewest，当前写入被丢弃。
var ErrDropped = errors.New("logrotate: 异步队列已满，已丢弃本条日志")

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

	writers   sync.WaitGroup // 在途 Write（避免 close(ch) 时仍有 send）
	loopWG    sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	dropped atomic.Uint64

	errMu    sync.Mutex
	writeErr error
}

// asyncMsg 保证 sink 的 Write/Sync 只在 loop 协程执行，不破坏底层 *Writer 单所有者契约。
type asyncMsg struct {
	data     []byte
	syncDone chan error
}

// OpenAsync 启动后台刷盘。sink 不可为 nil。
// Write 会拷贝 p，调用方可复用缓冲区。
func OpenAsync(sink FileSink, cfg AsyncConfig) (*AsyncWriter, error) {
	if sink == nil {
		return nil, fmt.Errorf("logrotate: async sink 不能为 nil")
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
			// 成功则清除，失败则更新为最近一次，便于运维看当前健康状态。
			aw.setWriteErr(err)
			msg.syncDone <- err
			continue
		}
		if _, err := aw.sink.Write(msg.data); err != nil {
			aw.setWriteErr(err)
		} else {
			aw.setWriteErr(nil)
		}
	}
}

func (aw *AsyncWriter) setWriteErr(err error) {
	aw.errMu.Lock()
	defer aw.errMu.Unlock()
	// nil 表示恢复健康；非 nil 为最近一次刷盘错误。
	aw.writeErr = err
}

// Write 将日志入队。默认队列满时阻塞；FullDropNewest 时可能返回 ErrDropped。
// 已 Close 后返回错误。会拷贝 p。并发安全。
func (aw *AsyncWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	msg := asyncMsg{data: cp}

	// 先登记在途，再判断 closed，避免 Close 的 Wait 漏掉并发 Write。
	// closeCh 已关闭时，接收立即就绪，不会走到对 ch 的 send。
	aw.writers.Add(1)
	defer aw.writers.Done()

	select {
	case <-aw.closeCh:
		return 0, fmt.Errorf("logrotate: 异步日志已关闭")
	default:
	}

	if aw.policy == FullDropNewest {
		select {
		case <-aw.closeCh:
			return 0, fmt.Errorf("logrotate: 异步日志已关闭")
		case aw.ch <- msg:
			return len(p), nil
		default:
			aw.dropped.Add(1)
			return 0, ErrDropped
		}
	}

	select {
	case <-aw.closeCh:
		return 0, fmt.Errorf("logrotate: 异步日志已关闭")
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
	return aw.writeErr
}

// Sync 将已由 loop 写出的数据刷到存储（不排空队列）。请求会入队，由刷盘协程串行执行，
// 与 Write 并发安全。停机请用 Close。已 Close 后返回错误。
// 注意：队列中尚未写出的条目不会被本方法刷盘。
func (aw *AsyncWriter) Sync() error {
	// 与 Write 一样登记在途，避免 Close 在 Sync 仍占着 channel 时 close(ch)。
	aw.writers.Add(1)
	defer aw.writers.Done()

	done := make(chan error, 1)
	msg := asyncMsg{syncDone: done}
	// Sync 始终阻塞入队（即使 FullDropNewest），丢弃 fsync 请求没有意义。
	select {
	case <-aw.closeCh:
		return fmt.Errorf("logrotate: 异步日志已关闭")
	case aw.ch <- msg:
		return <-done
	}
}

// Close 停止接收、等待在途 Write、排空队列、Sync 并关闭底层。可重复调用。
// 返回刷盘过程中遇到的第一个错误（若有）。
func (aw *AsyncWriter) Close() error {
	aw.closeOnce.Do(func() {
		// 1) 广播关闭，阻塞中的 Write 退出且不再 send
		close(aw.closeCh)
		// 2) 等所有在途 Write 结束，再 close 队列，避免 send on closed channel
		aw.writers.Wait()
		close(aw.ch)
		// 3) 排空并退出 loop
		aw.loopWG.Wait()

		if err := aw.sink.Sync(); err != nil {
			aw.closeErr = err
		}
		if err := aw.sink.Close(); err != nil && aw.closeErr == nil {
			aw.closeErr = err
		}
		aw.errMu.Lock()
		if aw.writeErr != nil && aw.closeErr == nil {
			aw.closeErr = aw.writeErr
		}
		aw.errMu.Unlock()
	})
	return aw.closeErr
}

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

// ErrDropped reports that the async queue is full under FullDropNewest and the
// current record was dropped.
var ErrDropped = errors.New("logrotate: async queue is full; record dropped")

// ErrClosed reports that the async writer no longer accepts writes; errors.Is
// can also match fs.ErrClosed.
var ErrClosed = fmt.Errorf("logrotate: async writer is closed: %w", fs.ErrClosed)

// FullPolicy selects the behavior when the async queue is full.
type FullPolicy int

const (
	// FullBlock blocks until the queue has room (the default). It never drops
	// data, but can inflate caller latency.
	FullBlock FullPolicy = iota
	// FullDropNewest drops the current record immediately; Write returns
	// (0, ErrDropped).
	FullDropNewest
)

// defaultQueueSize is the default buffer depth used when AsyncConfig.QueueSize
// is unset or not positive.
const defaultQueueSize = 1024

// AsyncConfig configures the async writer.
type AsyncConfig struct {
	// QueueSize is the buffer depth in records (one per Write call), not bytes.
	// Values <= 0 use defaultQueueSize.
	QueueSize int
	// FullPolicy selects the policy applied when the queue is full; it defaults
	// to FullBlock.
	FullPolicy FullPolicy
}

// FileSink is the underlying destination owned exclusively by AsyncWriter;
// the async layer calls its methods serially.
// Write follows the io.Writer contract; a short write without an error is
// reported as io.ErrShortWrite.
// Close is called after all accepted data is delivered and never implies Sync.
type FileSink interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// AsyncWriter serializes writes to the underlying sink on a single flusher
// goroutine; caller Write calls only enqueue.
// Close rejects new writes, wakes callers waiting to enqueue, drains the queue,
// and closes the sink without calling Sync implicitly.
// To shut down, stop producers first and then call Close so delivery completes
// before exit.
// A successful Write only means the record was enqueued; call and check Sync
// explicitly when persistence must be confirmed.
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
	deliveryErr error // written only by loop; read by Close after the loop exits
}

// asyncMsg keeps sink Write/Sync on the loop goroutine so the single-owner
// contract of the underlying *Writer is preserved.
type asyncMsg struct {
	data     []byte
	syncDone chan error
}

// OpenAsync starts background flushing. sink must not be nil; on success the
// AsyncWriter owns and closes it.
// Write copies p, so the caller may reuse the buffer.
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

// Write enqueues p. It blocks when the queue is full by default and may
// return ErrDropped under FullDropNewest.
// It returns ErrClosed after Close. p is copied once admitted. It is safe for
// concurrent use.
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

// Dropped returns the number of records dropped by FullDropNewest.
func (aw *AsyncWriter) Dropped() uint64 {
	return aw.dropped.Load()
}

// LastError reports the current background flush state: the most recent
// failure, or nil once a later operation succeeds.
// A nil return from Write only means the record was enqueued, not that it
// reached disk; poll this method for health checks.
func (aw *AsyncWriter) LastError() error {
	aw.errMu.Lock()
	defer aw.errMu.Unlock()
	return aw.lastErr
}

// Sync places a FIFO barrier in the queue: it syncs the sink after all
// previously enqueued records are written.
// It returns the first background write error joined with the sync error, if
// any. It returns ErrClosed after Close. It is safe for concurrent use.
func (aw *AsyncWriter) Sync() error {
	done := make(chan error, 1)
	if err := aw.enqueue(asyncMsg{syncDone: done}, false); err != nil {
		return err
	}
	return <-done
}

// Close stops accepting writes, wakes callers waiting to enqueue, drains the
// queue, and closes the sink.
// It does not call Sync implicitly and sets no timeout. It returns the first
// background write error joined with the sink close error, if any.
// Repeated calls return the same result.
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

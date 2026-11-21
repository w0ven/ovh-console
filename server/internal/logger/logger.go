package logger

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
)

const (
	maxLogs           = 1000
	defaultFlushDelay = 2 * time.Second
	maxRetryBackoff   = 30 * time.Second
)

// Logger 内存累积 + 后台批量异步刷盘 + 控制台输出。
// Add 保证应用日志文件零同步 I/O（2 秒窗口批量落盘，ERROR 立即触发异步落盘）；
// 控制台 slog 沿用原有同步输出。刷盘失败时后台按有界退避自动重试。
// 生产启动时调用 StartAutoFlush 开启后台异步刷盘，退出时调用 Close / CloseWithErr 停机并落盘。
type Logger struct {
	flushMu    sync.Mutex
	mu         sync.Mutex
	entries    []types.LogEntry
	dirty      bool
	logsFile   string
	stdlog     *slog.Logger
	flushDelay time.Duration
	wakeCh     chan struct{}
	urgentCh   chan struct{}
	stopCh     chan struct{}
	stoppedCh  chan struct{}
	started    bool
	closed     bool
	writeFn    func(path string, v interface{}) error
}

// New 创建 logger（默认不启动后台 goroutine，避免短生命周期测试泄漏协程；
// 服务端运行期调用 StartAutoFlush 开启后台防抖刷盘）。
func New(logsFile string, console *slog.Logger) *Logger {
	return newLogger(logsFile, console, defaultFlushDelay, storage.WriteJSONCompact)
}

// NewWithWriter 供集成测试注入自定义落盘函数（如阻塞写入器），生产代码请使用 New。
func NewWithWriter(logsFile string, console *slog.Logger, delay time.Duration, writeFn func(string, interface{}) error) *Logger {
	return newLogger(logsFile, console, delay, writeFn)
}

func newLogger(logsFile string, console *slog.Logger, delay time.Duration, writeFn func(string, interface{}) error) *Logger {
	if console == nil {
		console = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if delay <= 0 {
		delay = defaultFlushDelay
	}
	if writeFn == nil {
		writeFn = storage.WriteJSONCompact
	}
	l := &Logger{
		entries:    make([]types.LogEntry, 0, maxLogs),
		logsFile:   logsFile,
		stdlog:     console,
		flushDelay: delay,
		wakeCh:     make(chan struct{}, 1),
		urgentCh:   make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
		writeFn:    writeFn,
	}
	l.Load()
	return l
}

// StartAutoFlush 启动后台异步防抖刷盘循环（幂等）。
func (l *Logger) StartAutoFlush() {
	l.mu.Lock()
	if l.started || l.closed {
		l.mu.Unlock()
		return
	}
	l.started = true
	dirty := l.dirty
	l.mu.Unlock()

	go l.flushLoop()
	if dirty {
		select {
		case l.wakeCh <- struct{}{}:
		default:
		}
	}
}

func (l *Logger) flushLoop() {
	defer close(l.stoppedCh)
	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	defer stopTimer()
	retryBackoff := l.flushDelay
	scheduleFlush := func(d time.Duration) {
		if timer == nil {
			timer = time.NewTimer(d)
			timerC = timer.C
		}
	}
	runFlush := func() {
		if err := l.flushIfDirty(); err != nil {
			// 写盘失败且后续无新日志时，按有界退避自动重试，避免脏数据滞留内存
			next := retryBackoff
			retryBackoff *= 2
			if retryBackoff > maxRetryBackoff {
				retryBackoff = maxRetryBackoff
			}
			scheduleFlush(next)
			return
		}
		retryBackoff = l.flushDelay
	}

	for {
		select {
		case <-l.stopCh:
			stopTimer()
			_ = l.flushIfDirty()
			return
		case <-l.urgentCh:
			stopTimer()
			runFlush()
		case <-l.wakeCh:
			scheduleFlush(l.flushDelay)
		case <-timerC:
			timer = nil
			timerC = nil
			runFlush()
		}
	}
}

// Close 停止后台刷盘协程并确保剩余脏日志已落盘（幂等）。
func (l *Logger) Close() {
	_ = l.CloseWithErr()
}

// CloseWithErr 停止后台刷盘协程并执行最终落盘，返回末次写入错误以便退出协调者上报。
func (l *Logger) CloseWithErr() error {
	l.mu.Lock()
	started := l.started
	alreadyClosed := l.closed
	l.closed = true
	if started && !alreadyClosed {
		close(l.stopCh)
	}
	l.mu.Unlock()

	if started {
		<-l.stoppedCh
		// 若在停机边缘或 Close 后仍有零星日志写入，仍尝试最终落盘并向上游回报错误
		return l.flushIfDirty()
	}
	return l.flushIfDirty()
}

// Load 启动时读取已有日志（最多保留 maxLogs 条），兼容旧缩进格式与新紧凑格式。
func (l *Logger) Load() {
	l.mu.Lock()
	defer l.mu.Unlock()

	var existing []types.LogEntry
	if err := storage.ReadJSON(l.logsFile, &existing); err != nil {
		l.stdlog.Warn("read logs file", "err", err)
		return
	}
	if len(existing) > maxLogs {
		existing = existing[len(existing)-maxLogs:]
	}
	l.entries = existing
}

// Add 添加一条日志（非阻塞通知后台防抖刷盘，ERROR 立即触发异步刷盘）。
func (l *Logger) Add(level, message, source string) {
	if source == "" {
		source = "system"
	}
	entry := types.LogEntry{
		ID:        uuid.NewString(),
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     level,
		Message:   message,
		Source:    source,
	}

	l.mu.Lock()
	l.entries = append(l.entries, entry)
	if len(l.entries) > maxLogs {
		l.entries = l.entries[len(l.entries)-maxLogs:]
	}
	l.dirty = true
	started := l.started && !l.closed
	l.mu.Unlock()

	if started {
		if level == "ERROR" {
			select {
			case l.urgentCh <- struct{}{}:
			default:
			}
		} else {
			select {
			case l.wakeCh <- struct{}{}:
			default:
			}
		}
	}

	// 控制台输出
	msg := fmt.Sprintf("[%s] %s", source, message)
	switch level {
	case "ERROR":
		l.stdlog.Error(msg)
	case "WARNING", "WARN":
		l.stdlog.Warn(msg)
	case "DEBUG":
		l.stdlog.Debug(msg)
	default:
		l.stdlog.Info(msg)
	}
}

// Info/Warn/Error/Debug 便捷方法
func (l *Logger) Info(msg, source string)  { l.Add("INFO", msg, source) }
func (l *Logger) Warn(msg, source string)  { l.Add("WARNING", msg, source) }
func (l *Logger) Error(msg, source string) { l.Add("ERROR", msg, source) }
func (l *Logger) Debug(msg, source string) { l.Add("DEBUG", msg, source) }

func (l *Logger) flushIfDirty() error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	if !l.dirty {
		l.mu.Unlock()
		return nil
	}
	snapshot := make([]types.LogEntry, len(l.entries))
	copy(snapshot, l.entries)
	l.dirty = false
	l.mu.Unlock()

	if err := l.writeFn(l.logsFile, snapshot); err != nil {
		l.stdlog.Error("write logs file", "err", err)
		l.mu.Lock()
		l.dirty = true
		l.mu.Unlock()
		return err
	}
	return nil
}

// Flush 强制同步刷盘，保持原有显式刷盘与优雅退出语义不变。
func (l *Logger) Flush() {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	snapshot := make([]types.LogEntry, len(l.entries))
	copy(snapshot, l.entries)
	wasDirty := l.dirty
	l.dirty = false
	l.mu.Unlock()

	if err := l.writeFn(l.logsFile, snapshot); err != nil {
		l.stdlog.Error("write logs file", "err", err)
		if wasDirty {
			l.mu.Lock()
			l.dirty = true
			l.mu.Unlock()
		}
	}
}

// Snapshot 取当前内存中的日志副本
func (l *Logger) Snapshot() []types.LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]types.LogEntry, len(l.entries))
	copy(cp, l.entries)
	return cp
}

// Clear 清空所有日志（含文件）。
// 仅在短临界区内清空内存并置脏标志，绝不持 l.mu 执行阻塞文件 I/O，
// 防止日志写盘阻塞时连带卡死 Add / Info / Error 等上层调用与退出信号路径。
func (l *Logger) Clear() error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	prev := l.entries
	prevDirty := l.dirty
	l.entries = make([]types.LogEntry, 0, maxLogs)
	l.dirty = false
	l.mu.Unlock()

	if err := l.writeFn(l.logsFile, []types.LogEntry{}); err != nil {
		l.mu.Lock()
		// 保留 Clear 期间并发新增的日志，并恢复原条目顺序（如果未被覆盖）
		if len(prev) > 0 {
			restored := make([]types.LogEntry, 0, len(prev)+len(l.entries))
			restored = append(restored, prev...)
			restored = append(restored, l.entries...)
			if len(restored) > maxLogs {
				restored = restored[len(restored)-maxLogs:]
			}
			l.entries = restored
		}
		if prevDirty || len(l.entries) > 0 {
			l.dirty = true
		}
		l.mu.Unlock()
		return err
	}
	return nil
}

// MarshalEntries 调试用：序列化全部条目
func (l *Logger) MarshalEntries() string {
	b, _ := json.MarshalIndent(l.Snapshot(), "", "  ")
	return string(b)
}

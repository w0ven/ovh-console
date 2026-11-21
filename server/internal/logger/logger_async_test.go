package logger

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
)

func TestAsyncDebouncedFlushReducesWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log.json")
	var writes atomic.Int32
	writeFn := func(p string, v interface{}) error {
		writes.Add(1)
		return storage.WriteJSONCompact(p, v)
	}
	l := newLogger(path, slog.New(slog.NewTextHandler(io.Discard, nil)), 80*time.Millisecond, writeFn)
	l.StartAutoFlush()
	defer l.Close()

	for i := 0; i < 1000; i++ {
		l.Info(fmt.Sprintf("msg-%d", i), "queue")
	}
	// 等待一次防抖窗口完成落盘
	time.Sleep(160 * time.Millisecond)

	if got := writes.Load(); got == 0 || got > 2 {
		t.Fatalf("expected 1..2 debounced writes for 1000 Add calls, got %d", got)
	}
	l.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted logs: %v", err)
	}
	var entries []types.LogEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("invalid compact JSON: %v", err)
	}
	if len(entries) != maxLogs {
		t.Fatalf("expected %d entries, got %d", maxLogs, len(entries))
	}
}

func TestErrorTriggersUrgentAsyncFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "urgent.log.json")
	flushed := make(chan struct{}, 4)
	writeFn := func(p string, v interface{}) error {
		err := storage.WriteJSONCompact(p, v)
		select {
		case flushed <- struct{}{}:
		default:
		}
		return err
	}
	l := newLogger(path, slog.New(slog.NewTextHandler(io.Discard, nil)), 5*time.Second, writeFn)
	l.StartAutoFlush()
	defer l.Close()

	l.Error("urgent failure", "system")
	select {
	case <-flushed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ERROR log did not trigger immediate async flush")
	}
}

func TestLoggerLoadsLegacyIndentedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.log.json")
	seed := []types.LogEntry{
		{ID: "1", Timestamp: "2026-11-22T00:00:00Z", Level: "INFO", Message: "legacy-one", Source: "system"},
		{ID: "2", Timestamp: "2026-11-22T00:00:01Z", Level: "ERROR", Message: "legacy-two", Source: "queue"},
	}
	if err := storage.WriteJSON(path, seed); err != nil {
		t.Fatal(err)
	}

	l := New(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer l.Close()

	snap := l.Snapshot()
	if len(snap) != 2 || snap[0].Message != "legacy-one" || snap[1].Message != "legacy-two" {
		t.Fatalf("legacy indented file not loaded cleanly: %+v", snap)
	}
}

func TestAsyncFlushRetriesAfterInitialFailureWithoutNewLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry.log.json")
	var attempts atomic.Int32
	succeeded := make(chan struct{}, 1)
	writeFn := func(p string, v interface{}) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary disk busy")
		}
		if err := storage.WriteJSONCompact(p, v); err != nil {
			return err
		}
		select {
		case succeeded <- struct{}{}:
		default:
		}
		return nil
	}

	l := newLogger(path, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Millisecond, writeFn)
	l.StartAutoFlush()
	defer l.Close()

	// 只写 1 条日志，之后不再有新日志到达；首次写失败后后台必须按退避自动重试落盘
	l.Info("must-persist-after-retry", "system")

	select {
	case <-succeeded:
	case <-time.After(time.Second):
		t.Fatalf("expected retry flush to succeed without new logs, attempts=%d", attempts.Load())
	}
	if attempts.Load() < 2 {
		t.Fatalf("expected at least 2 write attempts, got %d", attempts.Load())
	}
}

func TestCloseWithErrReportsFinalWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fail-close.log.json")
	writeErr := errors.New("disk full")
	writeFn := func(string, interface{}) error {
		return writeErr
	}

	l := newLogger(path, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, writeFn)
	l.Info("unsaved", "system")
	if err := l.CloseWithErr(); !errors.Is(err, writeErr) {
		t.Fatalf("CloseWithErr() = %v, want %v", err, writeErr)
	}
}

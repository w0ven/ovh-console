package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/handlers"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
)

// closeSpy 记录 Close 有没有被调用、调用次数，以及可选的调用当刻回调与阻塞通道
type closeSpy struct {
	closed  bool
	count   atomic.Int32
	err     error
	onClose func()
	blockCh <-chan struct{}
}

func (c *closeSpy) Close() error {
	c.closed = true
	c.count.Add(1)
	if c.onClose != nil {
		c.onClose()
	}
	if c.blockCh != nil {
		<-c.blockCh
	}
	return c.err
}

// 收到 SIGTERM 时必须做两件**可观察**的事:把缓冲区里的日志写进文件、关掉数据库。
func TestGracefulShutdownFlushesLogsAndClosesDB(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log.json")
	lg := newTestLogger(t, logPath)

	lg.Info("抢购任务 t1 开始", "queue")
	lg.Info("抢购任务 t1 无货,等待下一轮", "queue")

	if _, err := os.Stat(logPath); err == nil {
		t.Log("注意:日志在收尾之前就已落盘,这条测试的前提变弱了(缓冲策略变了?)")
	}

	db := &closeSpy{}
	gracefulShutdown(nil, db, lg, nil, "terminated", time.Second)

	if !db.closed {
		t.Error("数据库没有被关闭")
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("日志没有落盘(这正是修复前的表现): %v", err)
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("日志文件不是合法 JSON: %v", err)
	}
	joined := string(raw)
	for _, want := range []string{"抢购任务 t1 开始", "正在优雅退出", "正在关闭数据库"} {
		if !strings.Contains(joined, want) {
			t.Errorf("日志里缺少 %q —— 收尾时这条没写进去", want)
		}
	}
}

func TestGracefulShutdownRecordsCloseFailure(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log.json")
	lg := newTestLogger(t, logPath)

	db := &closeSpy{err: errors.New("database is locked")}
	gracefulShutdown(nil, db, lg, nil, "interrupt", time.Second)

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("日志没有落盘: %v", err)
	}
	if !strings.Contains(string(raw), "database is locked") {
		t.Error("关库失败没有记进日志,用户无从知道数据可能没写完")
	}
}

func newTestLogger(t *testing.T, path string) *logger.Logger {
	t.Helper()
	return logger.New(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// 验证 SIGTERM 先到、自更新后到：只执行一次关库，自更新被拒且绝不触发 restart/spawn。
func TestShutdownCoordinatorSigtermFirstBlocksRestart(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })

	bgCtx, bgCancel := context.WithCancel(context.Background())
	bgEntered := make(chan struct{})
	releaseBG := make(chan struct{})
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		<-bgCtx.Done()
		close(bgEntered)
		<-releaseBG
	}()
	registerBackgroundLoops(bgCancel, bgDone)

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "sigterm-first.log.json"))

	won, deadline := coord.beginShutdown("terminated", 2*time.Second)
	if !won {
		t.Fatal("sigterm should win lifecycle")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gracefulShutdownWithCoordinator(coord, nil, db, lg, nil, "terminated", deadline)
	}()

	<-bgEntered

	var restartCalls atomic.Int32
	exitCode, completed := executeGracefulRestart(coord, nil, db, lg, nil, "/bin/fake", time.Second,
		func(string) error { restartCalls.Add(1); return nil },
		func(string) error { restartCalls.Add(1); return nil },
	)
	if completed || exitCode != 0 || restartCalls.Load() != 0 {
		t.Fatalf("restart must not execute after sigterm won: completed=%v code=%d calls=%d", completed, exitCode, restartCalls.Load())
	}

	close(releaseBG)
	_, done, _, _, _ := coord.state()
	<-done
	wg.Wait()

	if db.count.Load() != 1 {
		t.Fatalf("db must be closed exactly once, got %d", db.count.Load())
	}
}

// 验证自更新先到、SIGTERM 在排空窗口到达：
//  1. SIGTERM 不会直接抢占正在排空的协程（beginShutdown 返回 won=false），而是原子登记延迟停机信号；
//  2. 当后台循环排空完成、准备进入不可逆替换进程前，executeGracefulRestart 必须识别并优先执行延迟停机：
//     不调用 restartFn/spawnFn，转入优雅退出并只关库一次、正常关闭 done 通道。
func TestShutdownCoordinatorRestartFirstHonorsDeferredSigtermEvenWhenDrained(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })

	bgCtx, bgCancel := context.WithCancel(context.Background())
	bgEntered := make(chan struct{})
	releaseBG := make(chan struct{})
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		<-bgCtx.Done()
		close(bgEntered)
		<-releaseBG
	}()
	registerBackgroundLoops(bgCancel, bgDone)

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "restart-first-sigterm.log.json"))

	var restartCalls atomic.Int32
	var completed atomic.Bool
	restartDone := make(chan struct{})
	go func() {
		defer close(restartDone)
		_, ok := executeGracefulRestart(coord, nil, db, lg, nil, "/bin/fake", 2*time.Second,
			func(string) error { restartCalls.Add(1); return nil },
			func(string) error { restartCalls.Add(1); return nil },
		)
		completed.Store(ok)
	}()

	<-bgEntered

	won, _ := coord.beginShutdown("terminated", time.Second)
	if won {
		t.Fatal("sigterm must register as deferred rather than racing active restarting")
	}

	close(releaseBG)
	<-restartDone

	if completed.Load() || restartCalls.Load() != 0 {
		t.Fatalf("deferred SIGTERM during restart drain must prevent restartFn/spawnFn: completed=%v calls=%d", completed.Load(), restartCalls.Load())
	}
	if db.count.Load() != 1 {
		t.Fatalf("db must be closed exactly once by deferred shutdown, got %d", db.count.Load())
	}
	mode, done, _, code, _ := coord.state()
	if mode != lifecycleShuttingDown || code != 0 {
		t.Fatalf("expected clean lifecycleShuttingDown exit 0, got mode=%v code=%d", mode, code)
	}
	select {
	case <-done:
	default:
		t.Fatal("expected coordinator done channel to be closed after deferred shutdown")
	}
}

// 验证后台循环超时时：自更新中止且不关库不 exec，转入可观测维护模式并经 writeGateMiddleware 返回 503 拒绝新写请求；
// 若在超时窗口内收到 SIGTERM，中止后必须自动无缝转交优雅退出并关闭数据库（绝不丢信号）。
func TestRestartTimeoutEntersMaintenanceAndRejectsNewWritesAndHonorsDeferredSigterm(t *testing.T) {
	t.Run("enters_maintenance_and_rejects_writes", func(t *testing.T) {
		t.Cleanup(func() { registerBackgroundLoops(nil) })

		_, bgCancel := context.WithCancel(context.Background())
		stuckDone := make(chan struct{})
		defer close(stuckDone)
		registerBackgroundLoops(bgCancel, stuckDone)

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "restart-maint.log.json"))

		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.Use(coord.writeGateMiddleware())
		r.POST("/api/queue", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
		r.GET("/api/version", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

		var restartCalls atomic.Int32
		_, completed := executeGracefulRestart(coord, nil, db, lg, nil, "/bin/fake", 150*time.Millisecond,
			func(string) error { restartCalls.Add(1); return nil },
			func(string) error { restartCalls.Add(1); return nil },
		)
		if completed || restartCalls.Load() != 0 || db.count.Load() != 0 {
			t.Fatalf("timed-out restart must abort without closing DB or exec: completed=%v calls=%d db=%d", completed, restartCalls.Load(), db.count.Load())
		}
		mode, _, _, _, reason := coord.state()
		if mode != lifecycleMaintenance || reason == "" {
			t.Fatalf("expected lifecycleMaintenance with reason, got mode=%v reason=%q", mode, reason)
		}

		// 验证维护态下提交新写请求返回 503，而只读查询仍可用以观察状态
		wPost := httptest.NewRecorder()
		r.ServeHTTP(wPost, httptest.NewRequest(http.MethodPost, "/api/queue", nil))
		if wPost.Code != http.StatusServiceUnavailable {
			t.Fatalf("POST /api/queue in maintenance mode = %d, want 503", wPost.Code)
		}
		wGet := httptest.NewRecorder()
		r.ServeHTTP(wGet, httptest.NewRequest(http.MethodGet, "/api/version", nil))
		if wGet.Code != http.StatusOK {
			t.Fatalf("GET /api/version in maintenance mode = %d, want 200", wGet.Code)
		}
	})

	t.Run("deferred_sigterm_handed_off_on_abort", func(t *testing.T) {
		t.Cleanup(func() { registerBackgroundLoops(nil) })

		bgCtx, bgCancel := context.WithCancel(context.Background())
		bgEntered := make(chan struct{})
		stuckDone := make(chan struct{})
		go func() {
			<-bgCtx.Done()
			close(bgEntered)
			// 先超过自更新排空窗口（150ms），但在后续延迟 SIGTERM 1s 预算内干净收尾，
			// 验证延迟 SIGTERM 交接后若排空干净仍正常关闭 DB 并以 0 退出。
			time.Sleep(240 * time.Millisecond)
			close(stuckDone)
		}()
		registerBackgroundLoops(bgCancel, stuckDone)

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "restart-deferred-sig.log.json"))

		restartDone := make(chan struct{})
		go func() {
			defer close(restartDone)
			_, _ = executeGracefulRestart(coord, nil, db, lg, nil, "/bin/fake", 150*time.Millisecond,
				func(string) error { t.Error("restartFn must not be called"); return nil },
				func(string) error { t.Error("spawnFn must not be called"); return nil },
			)
		}()

		<-bgEntered
		// 在重启超时窗口内发 SIGTERM：单次持锁登记延迟信号
		if won, _ := coord.beginShutdown("terminated", time.Second); won {
			t.Fatal("sigterm during restarting should be deferred, not immediately win")
		}

		<-restartDone
		// 重启中止后应自动转交优雅退出，等待后台 loop 在 SIGTERM 预算内干净收尾后关闭 DB 恰好一次，且退出码为 0
		if db.count.Load() != 1 {
			t.Fatalf("deferred sigterm must hand off to graceful shutdown and close DB once after clean drain, got %d", db.count.Load())
		}
		if _, _, _, code, _ := coord.state(); code != 0 {
			t.Fatalf("clean deferred sigterm shutdown exitCode = %d, want 0", code)
		}
	})
}

// 验证真实被屏障挂起的在途 HTTP 写 handler 未完成时：
// executeGracefulRestart 不会关闭 DB，不会调用 restart 或 spawn，并拒绝新写请求。
func TestRestartAbortsWithoutClosingDBWhileInFlightWriteHandlerBlocked(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	coord := newShutdownCoordinator()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(coord.writeGateMiddleware())

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	r.POST("/api/write-slow", func(c *gin.Context) {
		close(handlerEntered)
		<-releaseHandler
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		close(releaseHandler)
		_ = srv.Close()
	}()

	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, err := http.Post("http://"+ln.Addr().String()+"/api/write-slow", "application/json", nil)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-handlerEntered

	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "inflight-write.log.json"))
	var restartCalls atomic.Int32

	_, completed := executeGracefulRestart(coord, srv, db, lg, nil, "/bin/fake", 180*time.Millisecond,
		func(string) error { restartCalls.Add(1); return nil },
		func(string) error { restartCalls.Add(1); return nil },
	)
	if completed {
		t.Fatal("restart must abort while in-flight write handler is still running")
	}
	if db.count.Load() != 0 {
		t.Fatalf("DB must NOT be closed while in-flight write handler is still active, got %d", db.count.Load())
	}
	if restartCalls.Load() != 0 {
		t.Fatalf("restart/spawn must NOT run while write handler is still active, got %d", restartCalls.Load())
	}
}

// 验证并发排空与关库顺序：用通道屏障证明后台 loop 和 HTTP 都在同一排空窗口内并发运行，
// 且在 closeSpy.Close() 调用的精确瞬间，两者完成标志均已置 true。
func TestGracefulShutdownSharedDeadlineDrainsConcurrentlyBeforeClosingDB(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })

	bgCtx, bgCancel := context.WithCancel(context.Background())
	bgInDrain := make(chan struct{})
	httpInDrain := make(chan struct{})
	releaseBoth := make(chan struct{})
	bgDone := make(chan struct{})
	var bgExited atomic.Bool
	go func() {
		defer close(bgDone)
		<-bgCtx.Done()
		close(bgInDrain)
		<-releaseBoth
		bgExited.Store(true)
	}()
	registerBackgroundLoops(bgCancel, bgDone)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var httpDone atomic.Bool
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(httpInDrain)
			<-releaseBoth
			httpDone.Store(true)
			w.WriteHeader(http.StatusOK)
		}),
	}
	go func() { _ = srv.Serve(ln) }()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		resp, err := http.Get("http://" + ln.Addr().String() + "/slow")
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-httpInDrain

	var bgDoneAtClose, httpDoneAtClose atomic.Bool
	db := &closeSpy{
		onClose: func() {
			bgDoneAtClose.Store(bgExited.Load())
			httpDoneAtClose.Store(httpDone.Load())
		},
	}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "shared-deadline.log.json"))

	shutDone := make(chan struct{})
	go func() {
		defer close(shutDone)
		gracefulShutdown(srv, db, lg, nil, "terminated", 2*time.Second)
	}()

	// 严格屏障：只有 bg 已经进入排空、HTTP 也在途时，才同时放行两者
	<-bgInDrain
	close(releaseBoth)
	<-shutDone
	<-clientDone

	if !bgDoneAtClose.Load() || !httpDoneAtClose.Load() {
		t.Fatalf("at DB Close() moment both bg and HTTP must already be drained: bg=%v http=%v", bgDoneAtClose.Load(), httpDoneAtClose.Load())
	}
	if db.count.Load() != 1 {
		t.Fatalf("db close count = %d, want 1", db.count.Load())
	}
}

// 验证总截止时间对阻塞的 io.Closer（慢/卡死 DB Close）生效，不会无限挂死退出路径。
func TestGracefulShutdownBoundedByUnifiedDeadlineOnBlockedCloser(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	neverClose := make(chan struct{})
	defer close(neverClose)
	db := &closeSpy{blockCh: neverClose}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "blocked-closer.log.json"))

	start := time.Now()
	gracefulShutdown(nil, db, lg, nil, "terminated", 250*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 700*time.Millisecond {
		t.Fatalf("gracefulShutdown with blocked closer exceeded unified deadline: %v", elapsed)
	}
	// 先显式等待/收敛测试内 logger 的末次刷盘，避免超时后遗留的 runBounded(CloseWithErr) goroutine 撞上 t.TempDir() 清理
	lg.Flush()
	_ = lg.CloseWithErr()
}

// 验证在阻塞日志写入器存在的情况下：
// 1. 自更新中止时必须先完成维护态切换与延迟 SIGTERM 交接，绝不因日志写盘挂死而停在 lifecycleRestarting 或阻断退出；
// 2. 自更新正常路径若最终日志落盘超过统一 deadline，必须中止且绝不调用 restartFn。
func TestRestartWithBlockedLoggerWriterEntersMaintenanceAndBoundsShutdown(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })

	blockWrite := make(chan struct{})
	defer close(blockWrite)
	blockedLogger := logger.NewWithWriter(
		filepath.Join(t.TempDir(), "blocked.log.json"),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		10*time.Millisecond,
		func(string, interface{}) error {
			<-blockWrite
			return nil
		},
	)

	t.Run("abort_enters_maintenance_and_honors_sigterm_despite_blocked_logger", func(t *testing.T) {
		neverStop := make(chan struct{})
		defer close(neverStop)
		registerBackgroundLoops(func() {}, neverStop)

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		var restartCalls atomic.Int32

		start := time.Now()
		done := make(chan struct{})
		go func() {
			defer close(done)
			executeGracefulRestart(
				coord, nil, db, blockedLogger, nil, "/bin/fake",
				300*time.Millisecond,
				func(string) error { restartCalls.Add(1); return nil },
				func(string) error { restartCalls.Add(1); return nil },
			)
		}()

		// 等待进入重启态后立刻发送延迟 SIGTERM
		time.Sleep(40 * time.Millisecond)
		if won, _ := coord.beginShutdown("terminated", 250*time.Millisecond); won {
			t.Fatal("SIGTERM during restarting should be deferred, not win immediately")
		}

		select {
		case <-done:
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("abortToMaintenance hung on blocked logger instead of handing off deferred SIGTERM within deadline")
		}
		if elapsed := time.Since(start); elapsed > 1200*time.Millisecond {
			t.Fatalf("blocked logger abort+deferred shutdown took too long: %v", elapsed)
		}
		if restartCalls.Load() != 0 {
			t.Fatalf("restart must not run on abort, got %d", restartCalls.Load())
		}
		if db.count.Load() != 0 {
			t.Fatalf("dirty deferred shutdown must skip DB close while bg loop still blocked, got %d", db.count.Load())
		}
		if _, _, _, code, _ := coord.state(); code == 0 {
			t.Fatalf("dirty deferred shutdown must exit non-zero, got %d", code)
		}
	})

	t.Run("success_path_aborts_before_exec_when_logger_close_times_out", func(t *testing.T) {
		registerBackgroundLoops(nil)
		coord := newShutdownCoordinator()
		db := &closeSpy{}
		var restartCalls atomic.Int32

		start := time.Now()
		exitCode, completed := executeGracefulRestart(
			coord, nil, db, blockedLogger, nil, "/bin/fake",
			250*time.Millisecond,
			func(string) error { restartCalls.Add(1); return nil },
			func(string) error { restartCalls.Add(1); return nil },
		)
		if completed || exitCode == 0 {
			t.Fatalf("expected non-zero aborted restart on logger close timeout, got code=%d completed=%v", exitCode, completed)
		}
		if restartCalls.Load() != 0 {
			t.Fatalf("restartFn must NOT be called after logger close timeout, got %d", restartCalls.Load())
		}
		if time.Since(start) > 800*time.Millisecond {
			t.Fatalf("restart with blocked logger exceeded deadline: %v", time.Since(start))
		}
	})
}

// 验证 SIGTERM 排空超时（仍有持续阻塞的写 handler 在运行）时：
// 必须跳过显式关闭数据库（db.count == 0）并以非零退出码在预算内收口。
func TestGracefulShutdownSkipsDBCloseAndExitsNonZeroWhenDrainTimesOut(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	coord := newShutdownCoordinator()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(coord.writeGateMiddleware())

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	defer close(releaseHandler)
	r.POST("/api/stuck-write", func(c *gin.Context) {
		close(handlerEntered)
		<-releaseHandler
		c.Status(http.StatusOK)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r}
	go func() { _ = srv.Serve(ln) }()

	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/stuck-write", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-handlerEntered

	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "dirty-shutdown.log.json"))

	won, dl := coord.beginShutdown("terminated", 280*time.Millisecond)
	if !won {
		t.Fatal("expected beginShutdown to win")
	}
	start := time.Now()
	gracefulShutdownWithCoordinator(coord, srv, db, lg, nil, "terminated", dl)
	elapsed := time.Since(start)

	if db.count.Load() != 0 {
		t.Fatalf("DB must NOT be closed when in-flight write failed to drain before deadline, got %d", db.count.Load())
	}
	if _, _, _, code, _ := coord.state(); code == 0 {
		t.Fatalf("dirty shutdown must record non-zero exitCode, got %d", code)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("dirty shutdown exceeded unified deadline: %v", elapsed)
	}
}

// 验证“先发生关键落库失败，随后再请求自更新”的场景：
// 即使排空期间没有新的失败，只要 State 存在未成功持久化的关键队列代次，协调器也必须中止重启并进入维护模式。
func TestRestartAbortsWhenCriticalSaveFailedBeforeRestartRequested(t *testing.T) {
	origState := appStateForShutdown.Load()
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(origState)
	})
	registerBackgroundLoops(nil)

	st := &app.State{}
	appStateForShutdown.Store(st)
	// 先在重启之前推进未落库的关键队列代次（不依赖排空期间的新增失败）
	st.MarkQueueCriticalDirty()
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("expected pending critical queue save generation to be set")
	}

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "pre-fail-restart.log.json"))
	var restartCalls atomic.Int32

	_, completed := executeGracefulRestart(
		coord, nil, db, lg, nil, "/bin/fake",
		500*time.Millisecond,
		func(string) error { restartCalls.Add(1); return nil },
		func(string) error { restartCalls.Add(1); return nil },
	)
	if completed {
		t.Fatal("restart must abort when a prior critical save failure is still pending")
	}
	if restartCalls.Load() != 0 || db.count.Load() != 0 {
		t.Fatalf("must neither close DB nor exec on prior critical save failure: db=%d restart=%d", db.count.Load(), restartCalls.Load())
	}
	if mode, _, _, _, _ := coord.state(); mode != lifecycleMaintenance {
		t.Fatalf("expected lifecycleMaintenance, got %v", mode)
	}
}

// blockedConsoleWriter 模拟 stdout/stderr 管道阻塞的控制台输出目标，并可通过 entered 通道屏障确认已真正阻塞在 Write。
type blockedConsoleWriter struct {
	blockCh <-chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (b *blockedConsoleWriter) Write(p []byte) (int, error) {
	if b.entered != nil {
		b.once.Do(func() { close(b.entered) })
	}
	<-b.blockCh
	return len(p), nil
}

// 验证 Clear() 正在阻塞写盘 + 控制台 stdout 已真正阻塞在 Write + QueueMu 被持有时：
// 经由真实 startSignalHandler 信号入口发送 SIGTERM，并经由 waitMainLifecycleExit 兜底收口，
// 全链路必须在严格受控的截止时间内完成交接与退出，绝不被 Clear、阻塞 stdout 或 QueueMu 卡死。
func TestRestartAndSigtermUnblockedDuringBlockedClearAndBlockedConsole(t *testing.T) {
	origState := appStateForShutdown.Load()
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(origState)
	})

	clearEntered := make(chan struct{})
	releaseClear := make(chan struct{})
	defer close(releaseClear)
	var enteredOnce sync.Once

	consoleEntered := make(chan struct{})
	blockStdout := make(chan struct{})
	defer close(blockStdout)
	blockedConsole := slog.New(slog.NewTextHandler(&blockedConsoleWriter{
		blockCh: blockStdout,
		entered: consoleEntered,
	}, nil))

	lg := logger.NewWithWriter(
		filepath.Join(t.TempDir(), "clear-block.log.json"),
		blockedConsole,
		10*time.Millisecond,
		func(string, interface{}) error {
			enteredOnce.Do(func() { close(clearEntered) })
			<-releaseClear
			return nil
		},
	)

	// 1. 屏障确认 Clear() 已进入阻塞的 writeFn
	go func() { _ = lg.Clear() }()
	select {
	case <-clearEntered:
	case <-time.After(time.Second):
		t.Fatal("Clear() never entered blocked writeFn")
	}

	// 2. 屏障确认控制台 stdout 已经真正进入阻塞 Write
	go func() { blockedConsole.Info("probe-block") }()
	select {
	case <-consoleEntered:
	case <-time.After(time.Second):
		t.Fatal("blockedConsole never entered blocked Write")
	}

	// 3. 模拟在途任务正持有 QueueMu，验证排空超时分支直接交接而不卡在 QueueMu
	st := &app.State{}
	st.QueueMu.Lock()
	defer st.QueueMu.Unlock()
	appStateForShutdown.Store(st)

	neverStop := make(chan struct{})
	defer close(neverStop)
	registerBackgroundLoops(func() {}, neverStop)

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	var restartCalls atomic.Int32

	// 经过真实的 main 信号处理入口 startSignalHandler 发送 SIGTERM
	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	startSignalHandler(sigCh, coord, nil, db, lg, blockedConsole, 200*time.Millisecond)

	start := time.Now()
	restartDone := make(chan struct{})
	go func() {
		defer close(restartDone)
		executeGracefulRestart(
			coord, nil, db, lg, blockedConsole, "/bin/fake",
			200*time.Millisecond,
			func(string) error { restartCalls.Add(1); return nil },
			func(string) error { restartCalls.Add(1); return nil },
		)
	}()

	deadlineWait := time.Now().Add(500 * time.Millisecond)
	for {
		if m, _, _, _, _ := coord.state(); m == lifecycleRestarting {
			break
		}
		if time.Now().After(deadlineWait) {
			t.Fatal("coordinator never entered lifecycleRestarting")
		}
		time.Sleep(5 * time.Millisecond)
	}
	sigCh <- syscall.SIGTERM

	// 经过真实的 waitMainLifecycleExit 兜底收口，验证严格截止时间（200ms 预算 + 少量调度余量 <= 550ms）
	exitCode := waitMainLifecycleExit(coord)
	<-restartDone
	elapsed := time.Since(start)

	if elapsed > 550*time.Millisecond {
		t.Fatalf("coordinator exceeded tight deadline under blocked Clear()+console+QueueMu: %v (want <=550ms)", elapsed)
	}
	if restartCalls.Load() != 0 {
		t.Fatalf("restartFn must not be called, got %d", restartCalls.Load())
	}
	if exitCode == 0 {
		t.Fatalf("expected non-zero exitCode from waitMainLifecycleExit, got %d", exitCode)
	}
	if db.count.Load() != 0 {
		t.Fatalf("dirty shutdown must skip DB close while bg loop still blocked, got %d", db.count.Load())
	}

	// 4. 独立验证 waitMainLifecycleExit 的超时兜底分支在 stdout 永久阻塞时也能立即返回 1 而不挂死
	stuckCoord := newShutdownCoordinator()
	_, _ = stuckCoord.beginShutdown("terminated", 1*time.Millisecond)
	// 不调 stuckCoord.finish，并把 deadline 设为早已过去以立刻触发 waitLimit=50ms 超时兜底
	stuckCoord.mu.Lock()
	stuckCoord.deadline = time.Now().Add(-3 * time.Second)
	stuckCoord.mu.Unlock()
	wdStart := time.Now()
	wdCode := waitMainLifecycleExit(stuckCoord)
	if wdCode != 1 || time.Since(wdStart) > 300*time.Millisecond {
		t.Fatalf("waitMainLifecycleExit watchdog failed: code=%d elapsed=%v", wdCode, time.Since(wdStart))
	}
}

// 验证 SIGTERM 在关键队列状态仍未落库（worker 3 次保存失败但正常结束）时：
// 排空虽干净，关库前补救 SaveQueue 仍失败时绝不以 exitCode=0 伪装正常退出；覆盖直接 SIGTERM 与延迟交接双入口。
func TestSigtermExitsNonZeroWhenPendingCriticalQueueSaveFailsBeforeCloseDB(t *testing.T) {
	origState := appStateForShutdown.Load()
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(origState)
	})

	newFailingQueueState := func(t *testing.T) *app.State {
		t.Helper()
		st := &app.State{}
		st.MarkQueueCriticalDirty()
		st.SetReplaceQueueFunc(func([]types.QueueItem) error {
			return errors.New("injected sqlite write failure on shutdown")
		})
		return st
	}

	t.Run("direct_sigterm", func(t *testing.T) {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(newFailingQueueState(t))

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "sigterm-pending-save.log.json"))

		won, dl := coord.beginShutdown("terminated", time.Second)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		gracefulShutdownWithCoordinator(coord, nil, db, lg, nil, "terminated", dl)

		if _, _, _, code, _ := coord.state(); code == 0 {
			t.Fatalf("direct SIGTERM with unpersisted critical queue save must exit non-zero, got %d", code)
		}
		if db.count.Load() != 1 {
			t.Fatalf("expected db close once after clean drain, got %d", db.count.Load())
		}
	})

	t.Run("deferred_sigterm_via_abort_to_maintenance", func(t *testing.T) {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(newFailingQueueState(t))

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "deferred-pending-save.log.json"))

		// 排空后检测到待落库关键代次 -> abortToMaintenance；先人工登记一个延迟 SIGTERM（模拟重启过程中收到 SIGTERM）
		coord.mu.Lock()
		coord.mode = lifecycleRunning
		coord.mu.Unlock()

		bgCtx, bgCancel := context.WithCancel(context.Background())
		bgEntered := make(chan struct{})
		bgDone := make(chan struct{})
		go func() {
			defer close(bgDone)
			<-bgCtx.Done()
			close(bgEntered)
			time.Sleep(40 * time.Millisecond)
		}()
		registerBackgroundLoops(bgCancel, bgDone)

		done := make(chan struct{})
		go func() {
			defer close(done)
			executeGracefulRestart(
				coord, nil, db, lg, nil, "/bin/fake",
				500*time.Millisecond,
				func(string) error { t.Error("restartFn must not run"); return nil },
				func(string) error { t.Error("spawnFn must not run"); return nil },
			)
		}()

		<-bgEntered
		if won, _ := coord.beginShutdown("terminated", time.Second); won {
			t.Fatal("SIGTERM during restarting must be deferred")
		}
		<-done

		if _, _, _, code, _ := coord.state(); code == 0 {
			t.Fatalf("deferred SIGTERM with unpersisted critical queue save must exit non-zero, got %d", code)
		}
	})

	// 覆盖补救 SaveQueue 在 ReplaceQueue 阻塞超时的场景：
	// 严禁在保存协程仍在写库时并发调用 sqlDB.Close()，必须在统一预算内跳过关库、记录 ERROR 日志并设置 exitCode=1。
	t.Run("remedial_save_timeout_skips_db_close_and_exits_nonzero", func(t *testing.T) {
		registerBackgroundLoops(nil)
		saveEntered := make(chan struct{})
		releaseSave := make(chan struct{})
		defer close(releaseSave)

		st := &app.State{}
		st.MarkQueueCriticalDirty()
		st.SetReplaceQueueFunc(func([]types.QueueItem) error {
			close(saveEntered)
			<-releaseSave
			return nil
		})
		appStateForShutdown.Store(st)

		var logBuf bytes.Buffer
		console := slog.New(slog.NewTextHandler(&logBuf, nil))
		coord := newShutdownCoordinator()
		db := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "remedial-save-timeout.log.json"))

		won, dl := coord.beginShutdown("terminated", 220*time.Millisecond)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		start := time.Now()
		gracefulShutdownWithCoordinator(coord, nil, db, lg, console, "terminated", dl)
		elapsed := time.Since(start)

		select {
		case <-saveEntered:
		default:
			t.Fatal("expected remedial SaveQueue to enter ReplaceQueue")
		}
		// 严格断言阻塞期间（在 defer releaseSave 之前）DB.Close 调用次数必须为 0
		if db.count.Load() != 0 {
			t.Fatalf("DB must NOT be closed while remedial SaveQueue is still blocked in ReplaceQueue, got %d", db.count.Load())
		}
		// 严格断言超时 ERROR 日志真正写入了 console（未被已过期的 finalCtx 静默吞掉）
		if !strings.Contains(logBuf.String(), "shutdown critical queue save timed out") {
			t.Fatalf("expected console ERROR log on remedial save timeout, got: %q", logBuf.String())
		}
		if _, _, _, code, _ := coord.state(); code != 1 {
			t.Fatalf("remedial save timeout must exit with code 1, got %d", code)
		}
		if elapsed > 320*time.Millisecond {
			t.Fatalf("remedial save timeout exceeded tight total budget (220ms + 100ms scheduling margin): %v", elapsed)
		}
	})
}

// 真实挂住前置 Flush（使用 logger.NewWithWriter 阻塞落盘）并覆盖短预算：
//  1. 子用例 blocked_preflush_still_allows_remedial_save_to_succeed：
//     48ms 极短预算 + 前置 Flush 彻底卡死，验证补救 SaveQueue 依然分到子预算并成功落盘（saveEntered 命中且 HasPendingCriticalQueueSave()==false）；
//  2. 子用例 blocked_preflush_and_blocked_save_respects_total_deadline：
//     前置 Flush 与 ReplaceQueue 双双卡死，验证两者均真实进入（flushEntered & saveEntered）、总耗时严格不突破 deadline、
//     不并发关库（db.count==0）、预留日志预算写出 ERROR 日志且退出码为 1。
func TestGracefulShutdownRemedialSaveRespectsTotalDeadlineWithSlowPreFlush(t *testing.T) {
	origState := appStateForShutdown.Load()
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(origState)
	})

	t.Run("blocked_preflush_short_budget_still_executes_remedial_save", func(t *testing.T) {
		registerBackgroundLoops(nil)
		flushEntered := make(chan struct{})
		releaseFlush := make(chan struct{})
		defer close(releaseFlush)
		var flushOnce sync.Once

		lg := logger.NewWithWriter(filepath.Join(t.TempDir(), "blocked-flush-short.log.json"), nil, time.Hour, func(string, interface{}) error {
			flushOnce.Do(func() { close(flushEntered) })
			<-releaseFlush
			return nil
		})
		lg.Info("seed dirty log so pre-flush actually writes", "system")

		var saveCalls atomic.Int32
		var preFlushAlreadyEnteredAtSave atomic.Bool
		st := &app.State{}
		st.MarkQueueCriticalDirty()
		st.SetReplaceQueueFunc(func([]types.QueueItem) error {
			// 在 ReplaceQueue 入口瞬间断言前置 Flush 已经真实进入，彻底排除“靠末尾 CloseWithErr 触发”的假阳性
			select {
			case <-flushEntered:
				preFlushAlreadyEnteredAtSave.Store(true)
			default:
			}
			saveCalls.Add(1)
			return nil
		})
		appStateForShutdown.Store(st)

		coord := newShutdownCoordinator()
		db := &closeSpy{}
		// 模拟极短剩余预算（48ms）：前置 Flush 卡死后，必须严格按比例切分预算，给补救 SaveQueue 留出窗口并实际执行成功
		budget := 48 * time.Millisecond
		won, dl := coord.beginShutdown("terminated", budget)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		gracefulShutdownWithCoordinator(coord, nil, db, lg, nil, "terminated", dl)

		if !preFlushAlreadyEnteredAtSave.Load() {
			t.Fatal("pre-flush must have already entered BEFORE remedial SaveQueue started")
		}
		if saveCalls.Load() != 1 {
			t.Fatalf("remedial SaveQueue must still execute even when pre-flush blocks on 48ms short budget, got calls=%d", saveCalls.Load())
		}
		if st.HasPendingCriticalQueueSave() {
			t.Fatal("remedial SaveQueue should have cleared pending critical queue save")
		}
	})

	t.Run("blocked_preflush_and_blocked_save_respects_total_deadline", func(t *testing.T) {
		registerBackgroundLoops(nil)
		flushEntered := make(chan struct{})
		releaseFlush := make(chan struct{})
		defer close(releaseFlush)
		var flushOnce sync.Once

		lg := logger.NewWithWriter(filepath.Join(t.TempDir(), "blocked-flush-and-save.log.json"), nil, time.Hour, func(string, interface{}) error {
			flushOnce.Do(func() { close(flushEntered) })
			<-releaseFlush
			return nil
		})
		lg.Info("seed dirty log", "system")

		saveEntered := make(chan struct{})
		releaseSave := make(chan struct{})
		defer close(releaseSave)
		var saveOnce sync.Once
		var preFlushAlreadyEnteredAtSave atomic.Bool

		st := &app.State{}
		st.MarkQueueCriticalDirty()
		st.SetReplaceQueueFunc(func([]types.QueueItem) error {
			select {
			case <-flushEntered:
				preFlushAlreadyEnteredAtSave.Store(true)
			default:
			}
			saveOnce.Do(func() { close(saveEntered) })
			<-releaseSave
			return nil
		})
		appStateForShutdown.Store(st)

		var logBuf bytes.Buffer
		console := slog.New(slog.NewTextHandler(&logBuf, nil))
		coord := newShutdownCoordinator()
		db := &closeSpy{}

		budget := 240 * time.Millisecond
		won, dl := coord.beginShutdown("terminated", budget)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		start := time.Now()
		gracefulShutdownWithCoordinator(coord, nil, db, lg, console, "terminated", dl)
		elapsed := time.Since(start)

		if !preFlushAlreadyEnteredAtSave.Load() {
			t.Fatal("pre-flush must have already entered BEFORE remedial SaveQueue started")
		}
		select {
		case <-saveEntered:
		default:
			t.Fatal("expected remedial SaveQueue to actually enter after blocked pre-flush")
		}
		if elapsed > budget+60*time.Millisecond {
			t.Fatalf("gracefulShutdown exceeded tight total budget %v: elapsed=%v", budget, elapsed)
		}
		if db.count.Load() != 0 {
			t.Fatalf("DB must NOT be closed when remedial save timed out, got %d", db.count.Load())
		}
		if !strings.Contains(logBuf.String(), "shutdown critical queue save timed out") {
			t.Fatalf("expected console ERROR log within reserved final budget, got: %q", logBuf.String())
		}
		if _, _, _, code, _ := coord.state(); code != 1 {
			t.Fatalf("expected exit code 1, got %d", code)
		}
	})
}

// blockingSignal 精确复现“真实 startSignalHandler 已从 sigCh 取走信号、正在调用 sig.String() 尚未登记 beginShutdown”时的竞态窗口。
type blockingSignal struct {
	name    string
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *blockingSignal) Signal() {}
func (s *blockingSignal) String() string {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.name
}

// 确定性覆盖生产完整信号架构（含真实 startSignalHandler + 模拟 main() 外层 defer coord.closeDB）：
//  1. 等 sqlDB.Close() 进入后，向 sigCh 投递带门闸的 blockingSignal；
//  2. 明确等待 startSignalHandler 真正从 sigCh 取走该信号并卡在 String() 内部（尚未调用 beginShutdown）；
//  3. 在 String() 仍暂停时放行 sqlDB.Close()：由于 commitRestartOrTakeDeferredShutdown 交由 startSignalHandler 单协程串行裁决，
//     重启提交必须等待信号完成登记后被拦截，绝不能偷跑执行 restartFn/spawnFn；
//  4. 放行 String() 后，验证不调用 restartFn/spawnFn，且外层 defer 返回后数据库严格只关闭 1 次！
func TestRestartSigtermDuringDBCloseCancelsExecAndClosesDBOnce(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	db := &closeSpy{}
	var restartCalls, spawnCalls atomic.Int32
	var completed atomic.Bool
	var exitCode atomic.Int32

	runSimulatedMain := func() {
		coord := newShutdownCoordinator()
		defer func() {
			_, _ = coord.closeDB(db)
		}()

		sigCh := make(chan os.Signal, 1)
		coord.attachSignalChannel(sigCh)
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "sigterm-during-db-close.log.json"))
		sigHandlerDone := startSignalHandler(sigCh, coord, nil, db, lg, nil, 2*time.Second)

		closeEntered := make(chan struct{})
		releaseClose := make(chan struct{})
		db.onClose = func() { close(closeEntered) }
		db.blockCh = releaseClose

		restartDone := make(chan struct{})
		go func() {
			defer close(restartDone)
			code, ok := executeGracefulRestart(
				coord, nil, db, lg, nil, "/bin/fake", 2*time.Second,
				func(string) error { restartCalls.Add(1); return nil },
				func(string) error { spawnCalls.Add(1); return nil },
			)
			exitCode.Store(int32(code))
			completed.Store(ok)
		}()

		// 1) 等 DB Close 真正进入
		<-closeEntered
		// 安装提交入口屏障：严格确认重启协程已经真实抵达 commitRestartOrTakeDeferredShutdown()（而非仍停留在关日志阶段）
		commitEntered := make(chan struct{})
		var commitOnce sync.Once
		enterFn := func() { commitOnce.Do(func() { close(commitEntered) }) }
		commitRestartEnterHook.Store(&enterFn)
		defer commitRestartEnterHook.Store(nil)

		// 2) 投递带门闸的信号，并确定性等待 startSignalHandler 已经从 sigCh 取出它、正停在 String() 内（beginShutdown 尚未执行）
		sigEntered := make(chan struct{})
		releaseSig := make(chan struct{})
		sigCh <- &blockingSignal{name: "terminated", entered: sigEntered, release: releaseSig}
		<-sigEntered

		// 3) 保持 sig.String() 暂停的同时放行 DB Close，严格等待重启协程真正抵达提交点（<-commitEntered），
		//    再断言其在单协程裁决完成前绝不能偷跑调用 restartFn/spawnFn
		close(releaseClose)
		<-commitEntered
		time.Sleep(25 * time.Millisecond)
		if restartCalls.Load() != 0 || spawnCalls.Load() != 0 {
			t.Fatalf("restart must wait for in-flight signal handler arbitration instead of racing ahead: restart=%d spawn=%d", restartCalls.Load(), spawnCalls.Load())
		}

		// 4) 放行 sig.String()，让串行裁决完成
		close(releaseSig)
		<-restartDone
		close(sigCh)
		<-sigHandlerDone

		mode, done, _, code, _ := coord.state()
		if mode != lifecycleShuttingDown || code != 0 || exitCode.Load() != 0 {
			t.Fatalf("expected clean lifecycleShuttingDown code 0, got mode=%v coordCode=%d retCode=%d", mode, code, exitCode.Load())
		}
		select {
		case <-done:
		default:
			t.Fatal("expected coordinator done channel to be closed")
		}
	}

	runSimulatedMain()

	if completed.Load() || restartCalls.Load() != 0 || spawnCalls.Load() != 0 {
		t.Fatalf("in-flight SIGTERM read by startSignalHandler during DB close must cancel exec/spawn: completed=%v restart=%d spawn=%d", completed.Load(), restartCalls.Load(), spawnCalls.Load())
	}
	if db.count.Load() != 1 {
		t.Fatalf("DB must be closed exactly once even after main defer returns, got %d", db.count.Load())
	}
}

// 覆盖每个辅助组件的独立并发扇出停止：
// 即使首个辅助组件（poller）彻底卡死，后续的 monitor、VPS、OrderPrewarm 依然全部独立收到停止请求并进入执行。
func TestStopAllBackgroundLoopsInvokesAuxStopsEvenWhenRegisteredLoopStuck(t *testing.T) {
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		auxStopOverride.Store(nil)
		auxStopHook.Store(nil)
	})

	_, bgCancel := context.WithCancel(context.Background())
	stuckDone := make(chan struct{})
	defer close(stuckDone)
	registerBackgroundLoops(bgCancel, stuckDone)

	pollerEntered := make(chan struct{})
	releasePoller := make(chan struct{})
	defer close(releasePoller)
	var pollerOnce sync.Once
	var monitorStops, vpsStops, prewarmStops atomic.Int32

	auxStopOverride.Store(&auxStopFns{
		poller: func() {
			pollerOnce.Do(func() { close(pollerEntered) })
			<-releasePoller
		},
		monitor: func() { monitorStops.Add(1) },
		vps:     func(*app.State) { vpsStops.Add(1) },
		prewarm: func() { prewarmStops.Add(1) },
	})

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "aux-stop-stuck.log.json"))

	_, completed := executeGracefulRestart(
		coord, nil, db, lg, nil, "/bin/fake", 150*time.Millisecond,
		func(string) error { return nil },
		func(string) error { return nil },
	)
	if completed {
		t.Fatal("expected restart to abort on stuck registered loop / poller")
	}
	if mode, _, _, _, _ := coord.state(); mode != lifecycleMaintenance {
		t.Fatalf("expected lifecycleMaintenance, got %v", mode)
	}
	select {
	case <-pollerEntered:
	default:
		t.Fatal("expected first aux component (poller) to enter")
	}
	if monitorStops.Load() < 1 || vpsStops.Load() < 1 || prewarmStops.Load() < 1 {
		t.Fatalf("all sibling aux components must receive stop independently when poller blocks: monitor=%d vps=%d prewarm=%d",
			monitorStops.Load(), vpsStops.Load(), prewarmStops.Load())
	}
}

// 覆盖“已通过写门禁的在途写请求在第一轮 stopAllBackgroundLoops 之后重新启动监控”的竞态窗口：
// 写排空结束后必须再次停止后台组件，断言在 DB.Close() 被调用的精确瞬间该监控组件处于已停止状态。
func TestShutdownAndRestartReStopComponentsStartedByInFlightWriteBeforeDBClose(t *testing.T) {
	for _, tc := range []struct {
		name      string
		isRestart bool
	}{
		{name: "sigterm_shutdown", isRestart: false},
		{name: "graceful_restart", isRestart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				registerBackgroundLoops(nil)
				auxStopOverride.Store(nil)
			})
			registerBackgroundLoops(nil)

			var componentRunning atomic.Bool
			var stopCalls atomic.Int32
			firstStopDone := make(chan struct{})
			var firstOnce sync.Once
			auxStopOverride.Store(&auxStopFns{
				poller:  func() {},
				vps:     func(*app.State) {},
				prewarm: func() {},
				monitor: func() {
					stopCalls.Add(1)
					componentRunning.Store(false)
					firstOnce.Do(func() { close(firstStopDone) })
				},
			})

			coord := newShutdownCoordinator()
			// 在进入关停/重启之前模拟一个已通过写门禁的 POST /monitor/start 请求
			releaseWrite, allowed, _ := coord.tryAcquireWrite()
			if !allowed {
				t.Fatal("expected write gate to allow request before shutdown")
			}

			var runningAtDBClose atomic.Bool
			db := &closeSpy{
				onClose: func() {
					runningAtDBClose.Store(componentRunning.Load())
				},
			}
			lg := newTestLogger(t, filepath.Join(t.TempDir(), tc.name+"-restop.log.json"))

			// 协程模拟在途写请求：刻意等到第一轮 stopAllBackgroundLoops 完成后才启动组件并释放写请求
			go func() {
				<-firstStopDone
				componentRunning.Store(true)
				releaseWrite()
			}()

			if tc.isRestart {
				_, ok := executeGracefulRestart(
					coord, nil, db, lg, nil, "/bin/fake", 2*time.Second,
					func(string) error { return nil },
					func(string) error { return nil },
				)
				if !ok {
					t.Fatal("expected clean restart to succeed")
				}
			} else {
				won, dl := coord.beginShutdown("terminated", 2*time.Second)
				if !won {
					t.Fatal("expected beginShutdown to win")
				}
				gracefulShutdownWithCoordinator(coord, nil, db, lg, nil, "terminated", dl)
			}

			if db.count.Load() != 1 {
				t.Fatalf("expected DB close once, got %d", db.count.Load())
			}
			if runningAtDBClose.Load() {
				t.Fatal("component restarted by in-flight write MUST be stopped again before DB.Close()!")
			}
			if stopCalls.Load() < 2 {
				t.Fatalf("expected second post-write-drain stop pass, got stopCalls=%d", stopCalls.Load())
			}
		})
	}
}

// 确定性覆盖：在构造旧 timer 后、进入 select 之前，先让 SIGTERM 交接广播新 deadline 且旧 timer 彻底到期（双通道同时就绪），
// 重复验证锁内 deadlineGen 二次复核绝不会被 Go select 随机选中旧 timer.C 误返回 1，直到延长后的新预算内 finish(0) 正常返回 0！
func TestWaitMainLifecycleExitUpdatesDeadlineOnDeferredSigtermHandoff(t *testing.T) {
	t.Cleanup(func() { waitMainLifecycleExitBeforeSelectHook.Store(nil) })

	coord := newShutdownCoordinator()
	// 初始进入重启，并把重启 deadline 设成 now-1950ms（使旧 waitLimit 贴着 50ms 最小下限）
	won, _ := coord.beginRestart(100 * time.Millisecond)
	if !won {
		t.Fatal("expected beginRestart to win")
	}
	coord.mu.Lock()
	coord.deadline = time.Now().Add(-1950 * time.Millisecond)
	firstGen := coord.deadlineGen
	coord.mu.Unlock()

	// 严格门闸：在 waitMainLifecycleExit 构造完旧 timer 后、进入 select 前精准暂停，
	// 在此期间完成 SIGTERM 交接（推进 deadlineGen 并关闭旧 deadlineChanged），并且刻意等到旧 50ms timer 也彻底到期！
	// 随后放行，使 select 面对 `deadlineChanged` 与 `timer.C` 双双已就绪的最坏竞态。
	beforeSelectHit := make(chan struct{})
	releaseBeforeSelect := make(chan struct{})
	var hookOnce sync.Once
	hook := func(obsGen uint64) {
		if obsGen == firstGen {
			hookOnce.Do(func() {
				close(beforeSelectHit)
				<-releaseBeforeSelect
			})
		}
	}
	waitMainLifecycleExitBeforeSelectHook.Store(&hook)

	waitRes := make(chan int, 1)
	go func() {
		waitRes <- waitMainLifecycleExit(coord)
	}()

	<-beforeSelectHit
	coord.beginShutdown("terminated", 800*time.Millisecond)
	sig, _ := coord.takeDeferredShutdown()
	if sig != "terminated" {
		t.Fatalf("expected deferred sig 'terminated', got %q", sig)
	}
	// 等旧的 50ms timer 彻底触发（保证 timer.C 与 deadlineChanged 同时就绪）
	time.Sleep(80 * time.Millisecond)
	close(releaseBeforeSelect)

	// 再等 120ms，验证即便 select 选中了旧 timer.C，锁内 deadlineGen 复核也能正确继续等待新 deadline 而绝不返回 1
	select {
	case code := <-waitRes:
		t.Fatalf("waitMainLifecycleExit erroneously timed out on stale timer.C race with code %d!", code)
	case <-time.After(120 * time.Millisecond):
	}

	coord.finish(0)
	select {
	case code := <-waitRes:
		if code != 0 {
			t.Fatalf("expected clean exit code 0 after extended SIGTERM deadline, got %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("waitMainLifecycleExit did not return after coord.finish(0)")
	}

	// 验证即便 select 明确选中了 timer.C，也在“已进 timer.C、尚未锁内复核”瞬间推进代次：
	// 若删掉锁内复核，该测试 100% 必然失败（彻底杜绝 select 选 deadlineChanged 造成的假绿）！
	t.Run("TimerBranchSelectedBeforeRecheck", func(t *testing.T) {
		t.Cleanup(func() {
			waitMainLifecycleExitBeforeSelectHook.Store(nil)
			waitMainLifecycleExitOnTimerBeforeRecheckHook.Store(nil)
		})
		coord2 := newShutdownCoordinator()
		won, _ := coord2.beginRestart(100 * time.Millisecond)
		if !won {
			t.Fatal("expected beginRestart to win")
		}
		coord2.mu.Lock()
		coord2.deadline = time.Now().Add(-1950 * time.Millisecond)
		gen0 := coord2.deadlineGen
		coord2.mu.Unlock()

		// 关键：此刻绝不提前触发 deadlineChanged，让 timer.C 成为唯一先就绪的分支，100% 确定性进入 `case <-timer.C:`！
		// 然后在 OnTimerBeforeRecheckHook 内部（已选中 timer.C、尚未执行 isDeadlineStillCurrentAndExpired）推进 SIGTERM 新 deadline！
		enteredTimerBranch := make(chan struct{})
		var once sync.Once
		onTimer := func(obsGen uint64) {
			if obsGen == gen0 {
				once.Do(func() {
					coord2.beginShutdown("terminated", 800*time.Millisecond)
					sig, _ := coord2.takeDeferredShutdown()
					if sig != "terminated" {
						t.Errorf("expected deferred sig 'terminated', got %q", sig)
					}
					close(enteredTimerBranch)
				})
			}
		}
		waitMainLifecycleExitOnTimerBeforeRecheckHook.Store(&onTimer)

		resCh := make(chan int, 1)
		go func() { resCh <- waitMainLifecycleExit(coord2) }()

		<-enteredTimerBranch
		// 已经 100% 走入 timer.C 分支：等待 120ms，必须没有提前返回 1
		select {
		case code := <-resCh:
			t.Fatalf("timer.C branch must not return %d after deadlineGen advanced!", code)
		case <-time.After(120 * time.Millisecond):
		}

		coord2.finish(0)
		select {
		case code := <-resCh:
			if code != 0 {
				t.Fatalf("expected clean exit code 0 after timer.C recheck continued waiting, got %d", code)
			}
		case <-time.After(time.Second):
			t.Fatal("waitMainLifecycleExit did not return after finish(0)")
		}
	})
}

// 确定性覆盖两条 HTTP 超时路径（严格借助 srv.RegisterOnShutdown 与 restartHTTPTimeoutBranchHook 杜绝走早分支假绿）：
//  1. Subtest HandOff: 用 RegisterOnShutdown 确定性等到首次 srv.Shutdown 真正启动后再登记更长预算的 SIGTERM，
//     并断言精确走入 httpErr != nil 后的原子交接分支（handedOff=true），待慢 GET 在新预算内结束后关库退出 0。
//  2. Subtest AbortAtomicallyLocksOutLateSigterm: 当无延迟信号时，httpErr != nil 原子切换至 lifecycleRestartAborted；
//     即使在写日志/finish(1) 窗口内后到 SIGTERM 调用 beginShutdown，也绝不再被当作 deferred 吞掉（won=false 且 sigtermDeferred 保持空），
//     并验证启动鉴权策略 validateStartupAuthPolicy 拒绝默认密钥 123456 + 非回环监听地址。
func TestRestartHTTPTimeoutHandsOffToDeferredSigtermAndDrainsCleanly(t *testing.T) {
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		restartHTTPTimeoutBranchHook.Store(nil)
	})
	registerBackgroundLoops(nil)

	t.Run("HandOffConfirmedInHTTPTimeoutBranch", func(t *testing.T) {
		t.Cleanup(func() { restartHTTPTimeoutBranchHook.Store(nil) })

		getEntered := make(chan struct{})
		releaseGet := make(chan struct{})
		shutdownStarted := make(chan struct{})
		var handlerDone atomic.Bool
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(getEntered)
				<-releaseGet
				w.WriteHeader(http.StatusOK)
				handlerDone.Store(true)
			}),
		}
		var shutdownStartedOnce sync.Once
		srv.RegisterOnShutdown(func() {
			shutdownStartedOnce.Do(func() { close(shutdownStarted) })
		})
		go func() { _ = srv.Serve(ln) }()

		clientDone := make(chan struct{})
		go func() {
			defer close(clientDone)
			resp, err := http.Get("http://" + ln.Addr().String() + "/slow-get")
			if err == nil {
				resp.Body.Close()
			}
		}()
		<-getEntered

		var observedHTTPTimeoutBranch atomic.Bool
		var observedHandedOff atomic.Bool
		branchFn := func(handedOff bool) {
			observedHTTPTimeoutBranch.Store(true)
			observedHandedOff.Store(handedOff)
		}
		restartHTTPTimeoutBranchHook.Store(&branchFn)

		coord := newShutdownCoordinator()
		var closedWhileHandlerRunning atomic.Bool
		dbSpy := &closeSpy{
			onClose: func() {
				if !handlerDone.Load() {
					closedWhileHandlerRunning.Store(true)
				}
			},
		}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "http-timeout-sigterm.log.json"))

		var restartCalls atomic.Int32
		retCh := make(chan int, 1)
		go func() {
			// 给重启极短预算（120ms，确保首次 srv.Shutdown 在慢 GET 阻塞期间超时返回 context.DeadlineExceeded）
			code, _ := executeGracefulRestart(
				coord, srv, dbSpy, lg, nil, "/bin/fake", 120*time.Millisecond,
				func(string) error { restartCalls.Add(1); return nil },
				func(string) error { restartCalls.Add(1); return nil },
			)
			retCh <- code
		}()

		// 确定性屏障：严格等到首次 srv.Shutdown 已经真实启动（保证已经跨过 L1726 早期检查！）再投递延迟 SIGTERM
		<-shutdownStarted
		won, _ := coord.beginShutdown("terminated", 1500*time.Millisecond)
		if won {
			t.Fatal("expected SIGTERM to register as deferred during in-progress srv.Shutdown")
		}

		// 刻意等慢 GET 跨越 120ms 重启预算并命中 httpErr != nil 的交接分支，再放行 GET
		time.Sleep(180 * time.Millisecond)
		close(releaseGet)
		<-clientDone

		select {
		case code := <-retCh:
			if code != 0 {
				t.Fatalf("expected deferred SIGTERM handoff on HTTP timeout to finish cleanly with code 0, got %d", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("executeGracefulRestart did not complete after deferred SIGTERM handoff")
		}
		if !observedHTTPTimeoutBranch.Load() || !observedHandedOff.Load() {
			t.Fatalf("expected execution through httpErr!=nil handoff branch (observed=%v handedOff=%v)", observedHTTPTimeoutBranch.Load(), observedHandedOff.Load())
		}
		if restartCalls.Load() != 0 {
			t.Fatalf("must not invoke restartFn after SIGTERM handoff, got %d", restartCalls.Load())
		}
		if dbSpy.count.Load() != 1 {
			t.Fatalf("DB must be closed exactly once after GET drained cleanly under SIGTERM budget, got %d", dbSpy.count.Load())
		}
		if closedWhileHandlerRunning.Load() {
			t.Fatal("DB was closed BEFORE the in-flight GET handler completed!")
		}
	})

	t.Run("AbortLocksOutLateSigtermBetweenCheckAndFinish", func(t *testing.T) {
		t.Cleanup(func() { restartHTTPTimeoutBranchHook.Store(nil) })

		getEntered := make(chan struct{})
		releaseGet := make(chan struct{})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(getEntered)
				<-releaseGet
				w.WriteHeader(http.StatusOK)
			}),
		}
		go func() { _ = srv.Serve(ln) }()

		clientDone := make(chan struct{})
		go func() {
			defer close(clientDone)
			resp, err := http.Get("http://" + ln.Addr().String() + "/slow-get-abort")
			if err == nil {
				resp.Body.Close()
			}
		}()
		<-getEntered

		coord := newShutdownCoordinator()
		dbSpy := &closeSpy{}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "http-timeout-abort.log.json"))

		// 确定性屏障：恰好在 httpErr != nil 的原子裁决选择 abort（handedOff=false）后、日志和 finish(1) 之前暂停，
		// 此时投递 SIGTERM，断言 coord 已经原子切入 lifecycleRestartAborted，绝不再把该信号静默存入 sigtermDeferred 吞掉！
		abortHookHit := make(chan struct{})
		releaseAbortHook := make(chan struct{})
		branchFn := func(handedOff bool) {
			if !handedOff {
				close(abortHookHit)
				<-releaseAbortHook
			}
		}
		restartHTTPTimeoutBranchHook.Store(&branchFn)

		retCh := make(chan int, 1)
		go func() {
			code, _ := executeGracefulRestart(
				coord, srv, dbSpy, lg, nil, "/bin/fake", 120*time.Millisecond,
				func(string) error { return nil },
				func(string) error { return nil },
			)
			retCh <- code
		}()

		<-abortHookHit
		won, _ := coord.beginShutdown("terminated", 1500*time.Millisecond)
		if won {
			t.Fatal("late SIGTERM after terminal abort lock must not start parallel shutdown")
		}
		coord.mu.Lock()
		modeNow := coord.mode
		deferredNow := coord.sigtermDeferred
		coord.mu.Unlock()
		if modeNow != lifecycleRestartAborted {
			t.Fatalf("expected coordinator mode lifecycleRestartAborted, got %v", modeNow)
		}
		if deferredNow != "" {
			t.Fatalf("late SIGTERM must NOT be accepted into sigtermDeferred after abort decision, got %q", deferredNow)
		}

		close(releaseAbortHook)
		select {
		case code := <-retCh:
			if code != 1 {
				t.Fatalf("expected abort exit code 1, got %d", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("executeGracefulRestart did not finish after abort")
		}
		if dbSpy.count.Load() != 0 {
			t.Fatalf("DB must NOT be closed while HTTP handler is still running on abort, got %d", dbSpy.count.Load())
		}
		close(releaseGet)
		<-clientDone
	})
}

func TestValidateStartupAuthPolicyRejectsDefaultKeyOnNonLoopback(t *testing.T) {
	// 启用鉴权 + 默认密钥 123456 + 空 LISTEN_HOST（监听所有网卡）或 0.0.0.0 -> 必须拒绝启动
	for _, badHost := range []string{"", "0.0.0.0", "::", "192.168.1.10"} {
		if err := validateStartupAuthPolicy(true, "123456", badHost, false); err == nil {
			t.Fatalf("expected startup policy to reject default key 123456 on non-loopback LISTEN_HOST=%q", badHost)
		}
	}
	// 仅监听回环地址 -> 允许启动
	for _, localHost := range []string{"127.0.0.1", "::1", "[::1]", "localhost"} {
		if err := validateStartupAuthPolicy(true, "123456", localHost, false); err != nil {
			t.Fatalf("expected loopback LISTEN_HOST=%q to be allowed, got %v", localHost, err)
		}
	}
	// 设置自定义强密钥 -> 允许监听所有网卡
	if err := validateStartupAuthPolicy(true, "strong-random-key-999", "", false); err != nil {
		t.Fatalf("expected custom key on all interfaces to be allowed, got %v", err)
	}
	// 显式开启 OVH_ALLOW_DEFAULT_KEY_REMOTE -> 允许放行
	if err := validateStartupAuthPolicy(true, "123456", "", true); err != nil {
		t.Fatalf("expected explicit opt-in override to be allowed, got %v", err)
	}
}

// 覆盖真实 SQLite 磁盘持久化场景：
//  1. 关键状态 SaveQueue 失败后，EnqueueItems 整表落盘清零脏标志；
//  2. 再次关键状态 SaveQueue 失败后，UpdateQueueItem 整表落盘清零脏标志；
//  3. 用真实 SQLite 触发器制造 PauseMonitorQueues 写库失败：内存变为 paused、磁盘仍为 pending，
//     严格断言 HasPendingCriticalQueueSave() == true（阻断自更新重启），随后修复触发器后补救落盘成功并清零。
func TestRealSQLiteEnqueueClearsPendingCriticalQueueSaveAndPersistsToDisk(t *testing.T) {
	dir := t.TempDir()
	sqliteDB, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer sqliteDB.Close()

	lg := newTestLogger(t, filepath.Join(dir, "app.log.json"))
	st := app.NewState(storage.Paths{DataDir: dir}, config.New(sqliteDB), lg, sqliteDB)

	// (1) EnqueueItems 整表落盘清零脏代次
	st.QueueMu.Lock()
	st.Queue = []types.QueueItem{{
		ID:         "crit-1",
		PlanCode:   "24sk202",
		Datacenter: "gra",
		Status:     "completed",
		StopReason: "order_placed",
		CreatedAt:  types.NowISO(),
		UpdatedAt:  types.NowISO(),
	}}
	st.MarkQueueCriticalDirtyLocked()
	st.QueueMu.Unlock()

	st.SetReplaceQueueFunc(func([]types.QueueItem) error { return errors.New("transient disk busy") })
	if err := st.SaveQueue(); err == nil {
		t.Fatal("expected SaveQueue to fail under injected error")
	}
	st.SetReplaceQueueFunc(nil)
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("expected HasPendingCriticalQueueSave=true after failed SaveQueue")
	}

	if err := st.EnqueueItems([]types.QueueItem{{
		ID:         "next-2",
		PlanCode:   "24sk202",
		Datacenter: "rbx",
		Status:     "pending",
		CreatedAt:  types.NowISO(),
		UpdatedAt:  types.NowISO(),
	}}, false); err != nil {
		t.Fatalf("EnqueueItems: %v", err)
	}
	if st.HasPendingCriticalQueueSave() {
		t.Fatal("HasPendingCriticalQueueSave must be false once EnqueueItems persisted the full queue to SQLite")
	}

	// (2) UpdateQueueItem 整表落盘清零脏代次
	st.MarkQueueCriticalDirty()
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("expected dirty flag true before UpdateQueueItem")
	}
	if err := st.UpdateQueueItem("next-2", "sbg", nil, 30, false, "", nil); err != nil {
		t.Fatalf("UpdateQueueItem: %v", err)
	}
	if st.HasPendingCriticalQueueSave() {
		t.Fatal("HasPendingCriticalQueueSave must be false once UpdateQueueItem persisted the full queue to SQLite")
	}

	// 校验真实 SQLite 磁盘内容包含 crit-1 与更新后的 next-2(sbg)
	items, err := sqliteDB.ListQueue()
	if err != nil {
		t.Fatalf("ListQueue: %v", err)
	}
	foundCrit, foundUpdated := false, false
	for _, it := range items {
		if it.ID == "crit-1" && it.Status == "completed" && it.StopReason == "order_placed" {
			foundCrit = true
		}
		if it.ID == "next-2" && it.Datacenter == "sbg" {
			foundUpdated = true
		}
	}
	if !foundCrit || !foundUpdated {
		t.Fatalf("expected both crit-1 and updated next-2(sbg) in SQLite, got %+v", items)
	}

	// (3) PauseMonitorQueues 写库失败路径：在 SQLite 上加 BEFORE INSERT 触发器拦截 queue 写入，
	// 确认内存 next-2 已变成 paused、磁盘仍是 pending，且 HasPendingCriticalQueueSave() 必须为 true！
	if _, err := sqliteDB.Exec(`CREATE TRIGGER block_queue_insert BEFORE INSERT ON queue BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	pausedCount, pauseErr := st.PauseMonitorQueues([]app.MonitorQueueScope{{PlanCode: "24sk202"}})
	if pauseErr == nil || pausedCount != 1 {
		t.Fatalf("expected PauseMonitorQueues to pause 1 item in memory and fail write, got count=%d err=%v", pausedCount, pauseErr)
	}
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("PauseMonitorQueues write failure MUST mark HasPendingCriticalQueueSave() == true so restart is blocked and shutdown runs remedial save!")
	}
	// 故障现场断言（删除触发器前）：内存中 next-2 确已变为 paused，而磁盘 SQLite 中 next-2 仍存在且状态保持为 pending！
	st.QueueMu.Lock()
	memPaused := false
	for _, it := range st.Queue {
		if it.ID == "next-2" && it.Status == "paused" && it.StopReason == app.StopReasonMonitorPaused {
			memPaused = true
		}
	}
	st.QueueMu.Unlock()
	if !memPaused {
		t.Fatal("expected next-2 in memory to be paused right after PauseMonitorQueues failure")
	}
	diskDuringFailure, err := sqliteDB.ListQueue()
	if err != nil {
		t.Fatalf("ListQueue during failure: %v", err)
	}
	diskStillPending := false
	for _, it := range diskDuringFailure {
		if it.ID == "next-2" && it.Status == "pending" {
			diskStillPending = true
		}
	}
	if !diskStillPending {
		t.Fatalf("expected next-2 on disk to still be pending while trigger blocks writes, got %+v", diskDuringFailure)
	}
	// 移除故障触发器后补救保存成功，脏代次清零且磁盘落为 paused
	if _, err := sqliteDB.Exec(`DROP TRIGGER block_queue_insert;`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	if err := st.SaveQueue(); err != nil {
		t.Fatalf("remedial SaveQueue after trigger drop: %v", err)
	}
	if st.HasPendingCriticalQueueSave() {
		t.Fatal("expected dirty flag cleared after remedial SaveQueue")
	}
	itemsAfter, err := sqliteDB.ListQueue()
	if err != nil {
		t.Fatalf("ListQueue after remedial save: %v", err)
	}
	pausedOnDisk := false
	for _, it := range itemsAfter {
		if it.ID == "next-2" && it.Status == "paused" && it.StopReason == app.StopReasonMonitorPaused {
			pausedOnDisk = true
		}
	}
	if !pausedOnDisk {
		t.Fatalf("expected next-2 persisted as paused on disk, got %+v", itemsAfter)
	}
}

// 验证“所有已登记后台循环和 HTTP 写请求均已干净排空，但 QueueMu 仍被其他只读/并发操作持续持有”时：
// HasPendingCriticalQueueSave() 不依赖 QueueMu，绝不卡住 executeGracefulRestart 或延迟 SIGTERM 交接。
func TestRestartDrainedCleanlyDoesNotBlockOnHeldQueueMu(t *testing.T) {
	origState := appStateForShutdown.Load()
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		appStateForShutdown.Store(origState)
	})
	registerBackgroundLoops(nil)

	st := &app.State{}
	// 先登记一次未落库的关键队列代次，随后持续锁住 QueueMu（模拟未计入 inFlightWrites 的 GET 请求等持锁）
	st.MarkQueueCriticalDirty()
	st.QueueMu.Lock()
	defer st.QueueMu.Unlock()
	appStateForShutdown.Store(st)

	coord := newShutdownCoordinator()
	db := &closeSpy{}
	lg := newTestLogger(t, filepath.Join(t.TempDir(), "held-queuemu-restart.log.json"))
	var restartCalls atomic.Int32

	start := time.Now()
	_, completed := executeGracefulRestart(
		coord, nil, db, lg, nil, "/bin/fake",
		400*time.Millisecond,
		func(string) error { restartCalls.Add(1); return nil },
		func(string) error { restartCalls.Add(1); return nil },
	)
	elapsed := time.Since(start)

	if completed {
		t.Fatal("restart must abort into maintenance mode when pending critical queue save exists")
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("executeGracefulRestart must not block on held QueueMu after clean drain: %v", elapsed)
	}
	if mode, _, _, _, _ := coord.state(); mode != lifecycleMaintenance {
		t.Fatalf("expected lifecycleMaintenance, got %v", mode)
	}
	if restartCalls.Load() != 0 || db.count.Load() != 0 {
		t.Fatalf("must neither close DB nor restart: db=%d restart=%d", db.count.Load(), restartCalls.Load())
	}
}

// 验证排空干净后，若最终收尾阶段 sqlDB.Close() 返回错误或 lg.CloseWithErr() 返回错误/超时，
// 均必须将失败反映到非零退出码（exitCode == 1），绝不返回 0 伪装成功退出。
func TestGracefulShutdownReflectsFinalCloseErrorsInExitCode(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	t.Run("db_close_error_sets_nonzero_exit_code", func(t *testing.T) {
		coord := newShutdownCoordinator()
		db := &closeSpy{err: errors.New("sqlite checkpoint I/O error")}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "db-close-err.log.json"))

		won, dl := coord.beginShutdown("terminated", time.Second)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		gracefulShutdownWithCoordinator(coord, nil, db, lg, nil, "terminated", dl)

		if db.count.Load() != 1 {
			t.Fatalf("expected db close once, got %d", db.count.Load())
		}
		if _, _, _, code, _ := coord.state(); code == 0 {
			t.Fatalf("db.Close error must set non-zero exitCode, got %d", code)
		}
	})

	t.Run("logger_close_error_sets_nonzero_exit_code", func(t *testing.T) {
		coord := newShutdownCoordinator()
		db := &closeSpy{}
		failingLogger := logger.NewWithWriter(
			filepath.Join(t.TempDir(), "lg-close-err.log.json"),
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			10*time.Millisecond,
			func(string, interface{}) error { return errors.New("disk full on final log flush") },
		)
		failingLogger.Info("trigger dirty entry", "system")

		won, dl := coord.beginShutdown("terminated", time.Second)
		if !won {
			t.Fatal("expected beginShutdown to win")
		}
		gracefulShutdownWithCoordinator(coord, nil, db, failingLogger, nil, "terminated", dl)

		if _, _, _, code, _ := coord.state(); code == 0 {
			t.Fatalf("lg.CloseWithErr error must set non-zero exitCode, got %d", code)
		}
	})
}

//  1. 缓冲信号预算测试：重启 deadline 已接近过期（20ms），sigCh 缓冲里刚进入一个 SIGTERM；
//     transitionRestartTerminalDirect 读取该缓冲信号时，返回的 deadline 必须来自完整停机预算（2s）而非旧重启的 20ms！
func TestBufferedSigtermInTerminalTransitionUsesFullShutdownBudget(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	coord := newShutdownCoordinator()
	coord.attachSignalChannel(sigCh, 2*time.Second)

	won, restartDL := coord.beginRestart(20 * time.Millisecond)
	if !won {
		t.Fatal("expected beginRestart to win")
	}
	time.Sleep(30 * time.Millisecond) // 旧重启 deadline 已过期

	sigCh <- syscall.SIGTERM
	transitioned, sig, dl := coord.transitionRestartTerminalDirect(lifecycleRestartAborted)
	if transitioned {
		t.Fatal("expected buffered SIGTERM to supersede abort transition")
	}
	if sig != syscall.SIGTERM.String() {
		t.Fatalf("expected SIGTERM, got %q", sig)
	}
	rem := time.Until(dl)
	if rem < time.Second || !dl.After(restartDL.Add(time.Second)) {
		t.Fatalf("buffered SIGTERM inherited stale restart deadline! remaining=%v restartDL=%v newDL=%v", rem, restartDL, dl)
	}
}

//  2. 启动屏障同步测试：主协程进入 beginStartupPhase 后，SIGTERM 抢先接管并启动 gracefulShutdownWithCoordinator；
//     停机排空必须等待 finishStartupPhase 完成后才执行 stopAllBackgroundLoops，且停机开始后 runIfRunning 拒绝后续新启动项！
func TestStartupPhaseBarrierSynchronizesWithEarlySigterm(t *testing.T) {
	t.Cleanup(func() {
		registerBackgroundLoops(nil)
		auxStopOverride.Store(nil)
	})
	registerBackgroundLoops(nil)

	coord := newShutdownCoordinator()
	if !coord.beginStartupPhase() {
		t.Fatal("expected beginStartupPhase to succeed in running mode")
	}

	var stopCalledBeforeStartupFinished atomic.Bool
	var startupFinished atomic.Bool
	var monitorStartedAfterShutdown atomic.Bool
	auxStopOverride.Store(&auxStopFns{
		poller: func() {},
		monitor: func() {
			if !startupFinished.Load() {
				stopCalledBeforeStartupFinished.Store(true)
			}
		},
		vps:     func(*app.State) {},
		prewarm: func() {},
	})

	// 停机在启动阶段尚未结束时切入
	won, dl := coord.beginShutdown("terminated", 2*time.Second)
	if !won {
		t.Fatal("expected beginShutdown to win")
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		gracefulShutdownWithCoordinator(coord, nil, &closeSpy{}, nil, nil, "terminated", dl)
	}()

	time.Sleep(40 * time.Millisecond)
	// 停机已开始：后续试图通过 runIfRunning 启动的 monitor 必须被原子跳过！
	if coord.runIfRunning(func() { monitorStartedAfterShutdown.Store(true) }) {
		t.Fatal("runIfRunning must reject starting new background tasks after shutdown began")
	}
	if stopCalledBeforeStartupFinished.Load() {
		t.Fatal("stopAllBackgroundLoops ran BEFORE startup phase finished!")
	}

	// 结束启动阶段，放行停机排空
	startupFinished.Store(true)
	coord.finishStartupPhase()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("gracefulShutdownWithCoordinator did not finish after finishStartupPhase")
	}
	if stopCalledBeforeStartupFinished.Load() {
		t.Fatal("stopAllBackgroundLoops must wait for finishStartupPhase")
	}
	if monitorStartedAfterShutdown.Load() {
		t.Fatal("monitor must not start after shutdown began")
	}
}

//  3. 关库/关日志失败窗口延迟 SIGTERM 终态裁决测试：
//     在 sqlDB.Close() / lg.CloseWithErr() 阻塞返回错误期间投递 SIGTERM，断言错误出口通过 abortRestartOrTakeDeferredShutdown 消费了延迟信号、
//     不重复调用 sqlDB.Close()、不调用 restartFn，并保留非零退出码 1！
func TestRestartCloseErrorsArbitrateDeferredSigtermWithoutDoubleClose(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	registerBackgroundLoops(nil)

	t.Run("db_close_error_consumes_deferred_sigterm_and_exits_1", func(t *testing.T) {
		coord := newShutdownCoordinator()
		closeEntered := make(chan struct{})
		releaseClose := make(chan struct{})
		dbSpy := &closeSpy{
			err:     errors.New("sqlite io error"),
			onClose: func() { close(closeEntered) },
			blockCh: releaseClose,
		}
		lg := newTestLogger(t, filepath.Join(t.TempDir(), "db-err-sigterm.log.json"))

		var restartCalls atomic.Int32
		retCh := make(chan int, 1)
		go func() {
			code, _ := executeGracefulRestart(
				coord, nil, dbSpy, lg, nil, "/bin/fake", time.Second,
				func(string) error { restartCalls.Add(1); return nil },
				func(string) error { restartCalls.Add(1); return nil },
			)
			retCh <- code
		}()

		<-closeEntered
		if won, _ := coord.beginShutdown("terminated", 2*time.Second); won {
			t.Fatal("expected SIGTERM during DB close to register as deferred")
		}
		close(releaseClose)

		select {
		case code := <-retCh:
			if code != 1 {
				t.Fatalf("expected non-zero exit code 1 on DB close failure, got %d", code)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("executeGracefulRestart did not finish")
		}
		coord.mu.Lock()
		deferredLeft := coord.sigtermDeferred
		modeNow := coord.mode
		coord.mu.Unlock()
		if deferredLeft != "" {
			t.Fatalf("deferred SIGTERM must be consumed on DB close error exit, left=%q", deferredLeft)
		}
		if modeNow != lifecycleShuttingDown && modeNow != lifecycleRestartAborted {
			t.Fatalf("expected terminal mode after arbitration, got %v", modeNow)
		}
		if dbSpy.count.Load() != 1 {
			t.Fatalf("DB Close must not be called twice, got %d", dbSpy.count.Load())
		}
		if restartCalls.Load() != 0 {
			t.Fatalf("restartFn must not be called after DB close failure, got %d", restartCalls.Load())
		}
	})

	t.Run("logger_close_error_consumes_deferred_sigterm_and_exits_1", func(t *testing.T) {
		coord := newShutdownCoordinator()
		dbSpy := &closeSpy{}
		writeEntered := make(chan struct{})
		releaseWrite := make(chan struct{})
		var enteredOnce sync.Once
		failingLg := logger.NewWithWriter(
			filepath.Join(t.TempDir(), "lg-err-sigterm.log.json"),
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			5*time.Second,
			func(string, interface{}) error {
				enteredOnce.Do(func() { close(writeEntered) })
				<-releaseWrite
				return errors.New("disk full on logger close")
			},
		)

		var restartCalls atomic.Int32
		retCh := make(chan int, 1)
		go func() {
			code, _ := executeGracefulRestart(
				coord, nil, dbSpy, failingLg, nil, "/bin/fake", time.Second,
				func(string) error { restartCalls.Add(1); return nil },
				func(string) error { restartCalls.Add(1); return nil },
			)
			retCh <- code
		}()

		<-writeEntered
		if won, _ := coord.beginShutdown("terminated", 2*time.Second); won {
			t.Fatal("expected SIGTERM during logger close to register as deferred")
		}
		close(releaseWrite)

		select {
		case code := <-retCh:
			if code != 1 {
				t.Fatalf("expected non-zero exit code 1 on logger close failure, got %d", code)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("executeGracefulRestart did not finish")
		}
		coord.mu.Lock()
		deferredLeft := coord.sigtermDeferred
		coord.mu.Unlock()
		if deferredLeft != "" {
			t.Fatalf("deferred SIGTERM must be consumed on logger close error exit, left=%q", deferredLeft)
		}
		if dbSpy.count.Load() != 1 {
			t.Fatalf("DB Close must remain exactly 1, got %d", dbSpy.count.Load())
		}
		if restartCalls.Load() != 0 {
			t.Fatalf("restartFn must not be called after logger close failure, got %d", restartCalls.Load())
		}
	})
}

//  4. Poller 真实发布运行态准入互斥回归测试：
//     已派发的 poller 在锁外完成 deleteWebhook 网络准备后、进入持锁准入前暂停；
//     此时停机协程调用 beginShutdown 完成接管并切入 lifecycleShuttingDown，再放行 poller：
//     严格断言 poller 实际启动次数（loopLaunches）为 0，且 poller.Running() == false！
func TestDispatchedPollerBlockedFromLaunchingAfterBeginShutdown(t *testing.T) {
	dir := t.TempDir()
	sqliteDB, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer sqliteDB.Close()

	cfgMgr := config.New(sqliteDB)
	cfg := cfgMgr.Get()
	cfg.TgToken = "123456:TEST_TOKEN"
	if err := cfgMgr.Set(cfg); err != nil {
		t.Fatalf("config set: %v", err)
	}
	lg := newTestLogger(t, filepath.Join(dir, "poller-admit.log.json"))
	st := app.NewState(storage.Paths{DataDir: dir}, cfgMgr, lg, sqliteDB)

	poller := handlers.InitPoller(st, nil)
	t.Cleanup(func() {
		handlers.StopPoller()
		poller.SetTestHooks(nil, nil, nil)
	})

	prepDone := make(chan struct{})
	releaseBeforeAdmit := make(chan struct{})
	var loopLaunches atomic.Int32
	poller.SetTestHooks(
		func(*app.State) (bool, string) { return true, "" }, // 无外网桩模拟 deleteWebhook 完成
		func() {
			close(prepDone)
			<-releaseBeforeAdmit
		},
		func(int64) {
			loopLaunches.Add(1)
		},
	)

	coord := newShutdownCoordinator()
	pollerDone := make(chan struct{})
	// 1) 主协程在 running 状态下已经完成 poller goroutine 的派发
	if !coord.runIfRunning(func() {
		go func() {
			defer close(pollerDone)
			handlers.StartPollerIfEnabledWithAdmit(st, coord.runIfRunning)
		}()
	}) {
		t.Fatal("expected dispatch in running mode")
	}

	// 2) 确定性等待 poller 已完成 deleteWebhook 准备并停在 admit 之前
	<-prepDone

	// 3) 停机信号接管：beginShutdown 完成状态迁移到 lifecycleShuttingDown
	won, _ := coord.beginShutdown("terminated", time.Second)
	if !won {
		t.Fatal("expected beginShutdown to win")
	}

	// 4) 放行已派发的 poller，等待其结束并断言实际启动次数严格为 0 且 Running() 为 false
	close(releaseBeforeAdmit)
	select {
	case <-pollerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatched poller did not return after release")
	}
	if loopLaunches.Load() != 0 {
		t.Fatalf("poller must NEVER launch its polling loop after beginShutdown, got %d launches", loopLaunches.Load())
	}
	if poller.Running() {
		t.Fatal("poller.Running() must remain false when admitted after beginShutdown")
	}
}

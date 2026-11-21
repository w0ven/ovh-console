package purchase

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
)

// 验证优雅退出：正在结账的在途任务（模拟 WithoutCancel 结账）在 loopCtx 取消后仍能完成并落库，
// 而未结账的 running 任务在被取消后数据库中保持 running 不被改成失败或暂停。
func TestProcessQueueLoopGracefulCancelPreservesInflightAndRunningState(t *testing.T) {
	st := queueCompletionState(t)
	st.Queue = []types.QueueItem{
		{ID: "inflight-checkout", PlanCode: "plan-a", Status: "running", RetryInterval: 1},
		{ID: "idle-running", PlanCode: "plan-b", Status: "running", RetryInterval: 60},
	}
	if err := st.SaveQueue(); err != nil {
		t.Fatal(err)
	}

	checkoutStarted := make(chan struct{})
	var checkoutStartedOnce atomic.Bool
	idleEntered := make(chan struct{})
	var idleEnteredOnce atomic.Bool

	loopCtx, loopCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		processQueueLoop(loopCtx, st, func(ctx context.Context, _ *app.State, item *types.QueueItem) Outcome {
			if item.ID == "inflight-checkout" {
				if checkoutStartedOnce.CompareAndSwap(false, true) {
					close(checkoutStarted)
				}
				// 模拟 PurchaseServer 已进入 context.WithoutCancel 结账窗口：
				// 即使 loopCtx 取消也继续完成结账并返回成功。
				time.Sleep(120 * time.Millisecond)
				return Outcome{Success: true}
			}
			if idleEnteredOnce.CompareAndSwap(false, true) {
				close(idleEntered)
			}
			// 模拟等待响应时响应取消，按取消语义返回不改任务状态
			select {
			case <-ctx.Done():
				return Outcome{Cancelled: true}
			case <-time.After(5 * time.Second):
				return Outcome{}
			}
		})
	}()

	select {
	case <-checkoutStarted:
	case <-time.After(3 * time.Second):
		loopCancel()
		t.Fatal("inflight checkout never started")
	}
	select {
	case <-idleEntered:
	case <-time.After(3 * time.Second):
		loopCancel()
		t.Fatal("idle running task never started")
	}

	loopCancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processQueueLoop did not exit within deadline after cancel")
	}

	rows, err := st.DB.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	statusByID := map[string]string{}
	for _, r := range rows {
		statusByID[r.ID] = r.Status
	}
	if statusByID["inflight-checkout"] != "completed" {
		t.Fatalf("inflight checkout should complete and persist, got %q", statusByID["inflight-checkout"])
	}
	if statusByID["idle-running"] != "running" {
		t.Fatalf("cancelled running task must stay running in DB, got %q", statusByID["idle-running"])
	}
}

func TestRetryNoStockLogThrottling(t *testing.T) {
	now := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	th := &retryNoStockLogThrottle{
		tasks: map[string]*retryNoStockLogState{},
		now:   func() time.Time { return now },
	}

	emit, count := th.shouldLog("task-1")
	if !emit || count != 0 {
		t.Fatalf("first retry should emit with count=0, got emit=%v count=%d", emit, count)
	}

	for i := 0; i < 9; i++ {
		now = now.Add(time.Minute)
		emit, _ = th.shouldLog("task-1")
		if emit {
			t.Fatalf("minute %d should be suppressed within 10m window", i+1)
		}
	}

	now = now.Add(time.Minute) // elapsed == 10m
	emit, count = th.shouldLog("task-1")
	if !emit || count != 10 {
		t.Fatalf("at 10m boundary should emit summary count=10, got emit=%v count=%d", emit, count)
	}

	// 验证 runQueueAttempt 接入节流后确实减少重复日志
	st := queueCompletionState(t)
	defer st.Logger.Close()
	item := types.QueueItem{ID: "throttle-task", PlanCode: "27skle06-v1", Status: "running", RetryInterval: 60}
	st.Queue = []types.QueueItem{item}
	retryNoStockLogs.clear(item.ID)
	t.Cleanup(func() { retryNoStockLogs.clear(item.ID) })

	for i := 0; i < 6; i++ {
		runQueueAttempt(context.Background(), st, item, func(context.Context, *app.State, *types.QueueItem) Outcome {
			return Outcome{WaitingForStock: true}
		})
	}
	var noStockLines int
	for _, entry := range st.Logger.Snapshot() {
		if strings.Contains(entry.Message, "27skle06-v1") {
			noStockLines++
		}
	}
	// 第 1 轮输出"首次尝试..."，第 2 轮输出首次"重试..."，第 3~6 轮被节流，共 2 条
	if noStockLines != 2 {
		t.Fatalf("expected 2 log lines across 6 no-stock attempts, got %d", noStockLines)
	}
}

// 端到端故障注入测试：走真实 PurchaseServer 生产结账与 ClaimQueueOrder 路径，
// 对成功后的 SaveQueue 注入连续 3 次写库失败，然后关闭 SQLite 并基于同一磁盘文件重新打开构建全新 State；
// 再次走真实 PurchaseServer 路径，断言远端 checkout 总调用次数严格为 1，且任务恢复为 paused(checkout_uncertain)。
func TestCheckoutClaimPreventsDuplicateOrderAfterSaveFailure(t *testing.T) {
	st1, _, checkouts, stock := hotFixture(t)
	stock.Store(true)
	dataDir := st1.Paths.DataDir
	accounts := append([]types.OVHAccount(nil), st1.Accounts...)

	item := types.QueueItem{
		ID:            "claim-guard-e2e",
		AccountID:     "hot-eu",
		PlanCode:      "plan",
		Datacenter:    "gra",
		Status:        "running",
		RetryInterval: 60,
	}
	st1.Queue = []types.QueueItem{item}
	if err := st1.SaveQueue(); err != nil {
		t.Fatal(err)
	}

	// 对成功后的终态 SaveQueue 底层 ReplaceQueue 注入连续 3 次持久化失败
	var failCalls atomic.Int32
	st1.SetReplaceQueueFunc(func(items []types.QueueItem) error {
		if failCalls.Add(1) <= 3 {
			return errors.New("injected sqlite write failure")
		}
		return st1.DB.ReplaceQueue(items)
	})
	t.Cleanup(func() { st1.SetReplaceQueueFunc(nil) })

	out1 := runQueueAttempt(context.Background(), st1, item, PurchaseServer)
	if !out1.Success {
		t.Fatalf("first production checkout should succeed, got %+v", out1)
	}
	if checkouts.Load() != 1 {
		t.Fatalf("expected 1 remote checkout on first run, got %d", checkouts.Load())
	}
	if failCalls.Load() != 3 || !st1.HasPendingCriticalQueueSave() {
		t.Fatalf("expected 3 failed SaveQueue retries and pending critical generation, got calls=%d pending=%v", failCalls.Load(), st1.HasPendingCriticalQueueSave())
	}

	// 恢复正常持久化并关闭原 SQLite 连接，模拟进程崩溃后全新冷启动
	st1.SetReplaceQueueFunc(nil)
	if err := st1.DB.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}

	reopenedDB, err := db.Open(dataDir)
	if err != nil {
		t.Fatalf("reopen sqlite from disk: %v", err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	lg2 := logger.New(filepath.Join(dataDir, "restart.log.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(lg2.Close)
	st2 := app.NewState(storage.Paths{DataDir: dataDir}, config.New(reopenedDB), lg2, reopenedDB)
	st2.Accounts = accounts

	loaded, err := reopenedDB.ListQueue()
	if err != nil || len(loaded) != 1 || loaded[0].Status != "running" {
		t.Fatalf("reopened DB should still show running before reconciliation, got err=%v rows=%+v", err, loaded)
	}
	st2.Queue = loaded

	// 重启后再次走真实生产 PurchaseServer 路径：必须被持久化的 ClaimQueueOrder 拦截，绝不再发起第二次 checkout
	out2 := runQueueAttempt(context.Background(), st2, loaded[0], PurchaseServer)
	if out2.Success || !out2.PauseTask {
		t.Fatalf("restarted attempt must pause instead of reordering, got %+v", out2)
	}
	if checkouts.Load() != 1 {
		t.Fatalf("remote checkout count must stay 1 across restart, got %d", checkouts.Load())
	}
	after, err := reopenedDB.ListQueue()
	if err != nil || len(after) != 1 {
		t.Fatalf("reload queue after reconciliation failed: err=%v rows=%+v", err, after)
	}
	if after[0].Status != "paused" || after[0].StopReason != app.StopReasonCheckoutUncertain {
		t.Fatalf("expected paused(%s), got status=%q reason=%q", app.StopReasonCheckoutUncertain, after[0].Status, after[0].StopReason)
	}
	if st2.HasPendingCriticalQueueSave() {
		t.Fatal("pending critical save generation should be cleared after a successful SaveQueue")
	}
}

// 并发持久化代次回归测试：
// 1. Worker A 先把任务 A 推进到 completed 并拿到旧快照 gen=1，暂停在 ReplaceQueue 写库屏障处；
// 2. Worker B 随后把任务 B 推进到 completed（gen=2）等待写库；
// 3. 放行 Worker A 写库成功（SQLite 中仅落入 gen=1，B 仍为 running），而 Worker B 的 3 次重试全部失败；
// 4. 断言：A 的旧快照成功绝不会清除 B 的未落库标记（HasPendingCriticalQueueSave() == true）且磁盘中 B 仍为 running；
// 5. 随后任意直接调用 state.SaveQueue() 成功覆盖 gen=2 后，HasPendingCriticalQueueSave() 立即归 false 且磁盘中 B 变为 completed。
func TestConcurrentStaleSaveCannotClearNewerCriticalFailure(t *testing.T) {
	st := queueCompletionState(t)
	defer st.Logger.Close()

	taskA := types.QueueItem{ID: "task-A", PlanCode: "plan-a", Datacenter: "gra", Status: "running", RetryInterval: 60}
	taskB := types.QueueItem{ID: "task-B", PlanCode: "plan-b", Datacenter: "gra", Status: "running", RetryInterval: 60}
	st.Queue = []types.QueueItem{taskA, taskB}
	if err := st.SaveQueue(); err != nil {
		t.Fatal(err)
	}

	aWriting := make(chan struct{})
	releaseA := make(chan struct{})
	bDirtyReady := make(chan struct{})
	var callNum atomic.Int32
	var bSaveAttempts atomic.Int32

	st.SetReplaceQueueFunc(func(items []types.QueueItem) error {
		n := callNum.Add(1)
		if n == 1 {
			// 第 1 次写入属于 Worker A 的旧快照（此时 B 仍为 running）
			close(aWriting)
			<-releaseA
			return st.DB.ReplaceQueue(items)
		}
		if n <= 4 {
			// 接下来的 3 次写入全部来自 Worker B 的 saveQueueCritical 重试，注入写库失败
			bSaveAttempts.Add(1)
			return errors.New("injected sqlite write failure for B")
		}
		return st.DB.ReplaceQueue(items)
	})
	t.Cleanup(func() { st.SetReplaceQueueFunc(nil) })

	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		runQueueAttempt(context.Background(), st, taskA, func(context.Context, *app.State, *types.QueueItem) Outcome {
			return Outcome{Success: true}
		})
	}()

	// 等 Worker A 已拿到旧快照并进入 ReplaceQueue 屏障
	<-aWriting

	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		runQueueAttempt(context.Background(), st, taskB, func(context.Context, *app.State, *types.QueueItem) Outcome {
			return Outcome{Success: true}
		})
	}()

	// 轮询等待 B 已经把内存状态改为 completed 并推进了 critical dirty gen（此时 B 正在等 saveQueueMu）
	deadline := time.Now().Add(2 * time.Second)
	for {
		st.QueueMu.Lock()
		bStatus := ""
		for _, q := range st.Queue {
			if q.ID == "task-B" {
				bStatus = q.Status
			}
		}
		st.QueueMu.Unlock()
		if bStatus == "completed" {
			close(bDirtyReady)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker B did not transition to completed in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	<-bDirtyReady

	// 现在放行 Worker A：A 写库成功（覆盖 gen=1），紧接着 B 获锁并连续 3 次写库失败
	close(releaseA)
	<-aDone
	<-bDone

	if bSaveAttempts.Load() != 3 {
		t.Fatalf("expected 3 failed retry attempts for worker B, got %d", bSaveAttempts.Load())
	}
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("stale snapshot success from worker A MUST NOT clear newer pending critical failure from worker B")
	}

	// 核对 SQLite 磁盘上的真实状态：A 已 completed，B 仍停留在 running（未落库）
	rows, err := st.DB.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	statusByID := map[string]string{}
	for _, r := range rows {
		statusByID[r.ID] = r.Status
	}
	if statusByID["task-A"] != "completed" || statusByID["task-B"] != "running" {
		t.Fatalf("unexpected sqlite rows after A succeeded & B failed: %+v", statusByID)
	}

	// 反向验证：直接调用 state.SaveQueue() 成功覆盖包含 gen=2 的最新快照后，待落库标记自动清零且 B 落盘为 completed
	if err := st.SaveQueue(); err != nil {
		t.Fatalf("direct state.SaveQueue() should succeed: %v", err)
	}
	if st.HasPendingCriticalQueueSave() {
		t.Fatal("direct state.SaveQueue() covering latest critical gen must clear pending failure")
	}
	rowsAfter, err := st.DB.ListQueue()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rowsAfter {
		statusByID[r.ID] = r.Status
	}
	if statusByID["task-B"] != "completed" {
		t.Fatalf("expected task-B completed in sqlite after direct SaveQueue, got %+v", statusByID)
	}
}

// 验证确定性失败（Outcome.Fatal）或达到 MaxRetries 自动置为 failed 终态时：
// 同样必须登记持久化代次并调用 saveQueueCritical；若保存失败则 HasPendingCriticalQueueSave() == true，
// 直至后续成功 SaveQueue 后才清零且 SQLite 中状态落盘为 failed。
func TestFatalFailedTerminalStateRegistersCriticalDirtyGeneration(t *testing.T) {
	st := queueCompletionState(t)
	defer st.Logger.Close()

	task := types.QueueItem{ID: "task-fatal-1", PlanCode: "bad-plan", Datacenter: "gra", Status: "running", RetryInterval: 60}
	st.Queue = []types.QueueItem{task}
	if err := st.SaveQueue(); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	st.SetReplaceQueueFunc(func(items []types.QueueItem) error {
		attempts.Add(1)
		return errors.New("injected sqlite write failure on failed terminal state")
	})

	runQueueAttempt(context.Background(), st, task, func(context.Context, *app.State, *types.QueueItem) Outcome {
		return Outcome{Fatal: true, Reason: "plan does not exist in zone"}
	})

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 saveQueueCritical retries for failed terminal state, got %d", attempts.Load())
	}
	if !st.HasPendingCriticalQueueSave() {
		t.Fatal("failed terminal state with SaveQueue failure must keep HasPendingCriticalQueueSave() == true")
	}

	// 此时磁盘上仍为 running（未落库）；恢复持久化并保存后应清零且落盘为 failed
	rowsBefore, err := st.DB.ListQueue()
	if err != nil || len(rowsBefore) != 1 || rowsBefore[0].Status != "running" {
		t.Fatalf("expected sqlite still running before recovery save, got err=%v rows=%+v", err, rowsBefore)
	}

	st.SetReplaceQueueFunc(nil)
	if err := st.SaveQueue(); err != nil {
		t.Fatalf("recovery SaveQueue failed: %v", err)
	}
	if st.HasPendingCriticalQueueSave() {
		t.Fatal("pending critical save generation should clear after recovery SaveQueue")
	}
	rowsAfter, err := st.DB.ListQueue()
	if err != nil || len(rowsAfter) != 1 || rowsAfter[0].Status != "failed" {
		t.Fatalf("expected sqlite failed after recovery save, got err=%v rows=%+v", err, rowsAfter)
	}
}

// 验证 RefreshOrderStatusesWithContext 在真实上游 HTTP I/O 已经开始后响应 ctx 取消并立刻返回。
func TestRefreshOrderStatusesWithContextCancelsInFlightHTTP(t *testing.T) {
	st := queueCompletionState(t)
	defer st.Logger.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/auth/time") {
			_, _ = w.Write([]byte("1700000000"))
			return
		}
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	st.Accounts = []types.OVHAccount{{ID: "acc-1", Endpoint: srv.URL, Zone: "IE", AppKey: "k", AppSecret: "s", ConsumerKey: "c"}}
	st.History = []types.PurchaseHistoryEntry{
		{ID: "h1", OrderID: "10001", AccountID: "acc-1", Status: "success", PurchaseTime: types.NowISO()},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- RefreshOrderStatusesWithContext(ctx, st, true)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("order status HTTP request never started")
	}

	cancel()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("expected 0 updates after in-flight cancel, got %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("RefreshOrderStatusesWithContext did not abort promptly on in-flight HTTP cancel")
	}
}

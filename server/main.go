package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/auth"
	"github.com/ovh-buy/server/internal/catalog"
	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/handlers"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/monitor"
	"github.com/ovh-buy/server/internal/netfp"
	"github.com/ovh-buy/server/internal/proxyguard"
	"github.com/ovh-buy/server/internal/purchase"
	"github.com/ovh-buy/server/internal/secret"
	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/telegram"
	"github.com/ovh-buy/server/internal/updater"
	"github.com/ovh-buy/server/internal/vps"
)

var monitorState *monitor.Monitor
var appStateForShutdown atomic.Pointer[app.State]

type lifecycleMode uint8

const (
	lifecycleRunning lifecycleMode = iota
	lifecycleShuttingDown
	lifecycleRestarting
	lifecycleRestartCommitted
	lifecycleRestartAborted // HTTP排空失败等终态中止：拒绝新的延迟信号登记，不再误吃 SIGTERM
	lifecycleMaintenance
)

const (
	defaultShutdownBudget = 25 * time.Second
	defaultRestartBudget  = 25 * time.Second
	persistReserveBudget  = 3 * time.Second
)

// shutdownCoordinator 是 SIGTERM 优雅退出与自更新重启的唯一生命周期状态所有者：
// - 同一把锁内完成 running -> shuttingDown / restarting / maintenance 迁移以及信号登记，杜绝双锁竞态；
// - 唯一负责跟踪在途写请求（trackWrite），在维护态/排空态立刻拒绝新写操作；
// - 唯一负责记录最终退出码并通过 done 通道交给 main 退出，杜绝退出码竞态；
// - closeDBOnce 保证数据库最多只关闭一次。
type shutdownCoordinator struct {
	mu                sync.Mutex
	mode              lifecycleMode
	maintenanceReason string
	done              chan struct{}
	deadline          time.Time
	deadlineGen       uint64
	deadlineUpdated   chan struct{}
	sigCh             <-chan os.Signal
	shutdownBudget    time.Duration
	restartArbiter    func(target lifecycleMode) (transitioned bool, sig string, deadline time.Time)
	sigtermDeferred   string
	sigtermDeadline   time.Time
	exitCode          int
	inFlightWrites    int
	writesDrained     chan struct{}
	startupActive     bool
	startupDone       chan struct{}
	finishOnce        sync.Once
	closeDBOnce       sync.Once
	closeDBErr        error
}

func newShutdownCoordinator() *shutdownCoordinator {
	return &shutdownCoordinator{
		mode:            lifecycleRunning,
		done:            make(chan struct{}),
		deadlineUpdated: make(chan struct{}),
		shutdownBudget:  defaultShutdownBudget,
	}
}

// notifyDeadlineUpdatedLocked 在持锁更新 deadline 时递增代次并广播唤醒 waitMainLifecycleExit 重新计时。
func (c *shutdownCoordinator) notifyDeadlineUpdatedLocked() {
	c.deadlineGen++
	if c.deadlineUpdated != nil {
		close(c.deadlineUpdated)
	}
	c.deadlineUpdated = make(chan struct{})
}

// beginShutdown 在单次持锁内完成 SIGTERM/SIGINT 归属判定：
// 若当前为 running 或 maintenance，直接赢得退出权；若当前为 restarting，在同一次持锁内登记延迟信号，杜绝丢信号。
func (c *shutdownCoordinator) beginShutdown(sig string, budget time.Duration) (won bool, deadline time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.mode {
	case lifecycleRunning, lifecycleMaintenance:
		c.mode = lifecycleShuttingDown
		c.deadline = time.Now().Add(budget)
		c.notifyDeadlineUpdatedLocked()
		return true, c.deadline
	case lifecycleRestarting:
		if c.sigtermDeferred == "" {
			c.sigtermDeferred = sig
			c.sigtermDeadline = time.Now().Add(budget)
		}
		return false, c.deadline
	default:
		return false, c.deadline
	}
}

func (c *shutdownCoordinator) beginRestart(budget time.Duration) (won bool, deadline time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRunning {
		return false, c.deadline
	}
	c.mode = lifecycleRestarting
	c.deadline = time.Now().Add(budget)
	c.notifyDeadlineUpdatedLocked()
	return true, c.deadline
}

// abortRestartOrTakeDeferredShutdown 在 HTTP 排空超时后准备进入不可逆失败退出前执行原子裁决：
// 若已注册 startSignalHandler 单协程裁决器，交由该唯一信号协程串行裁决；否则直接锁内裁决。
// - 若此前已登记延迟 SIGTERM/SIGINT，切入 lifecycleShuttingDown 并交出信号继续排空；
// - 否则切入 lifecycleRestartAborted，封死后续一切延迟信号登记窗口，保证“已登记的信号必有消费者，未登记则不会再被登记”。
func (c *shutdownCoordinator) abortRestartOrTakeDeferredShutdown() (aborted bool, sig string, deadline time.Time) {
	c.mu.Lock()
	arbiter := c.restartArbiter
	c.mu.Unlock()
	if arbiter != nil {
		return arbiter(lifecycleRestartAborted)
	}
	return c.transitionRestartTerminalDirect(lifecycleRestartAborted)
}

// isShutdownRequested 判断协调器是否已离开正常启动/运行态（用于主协程启动每个后台 worker 前检查是否已有停机信号抢先接管）。
func (c *shutdownCoordinator) isShutdownRequested() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode != lifecycleRunning
}

// beginStartupPhase 与 finishStartupPhase 构成“后台服务启动阶段”和“优雅停机排空”之间的严格同步屏障：
//   - 主协程仅当 coord.mode == lifecycleRunning 时获准进入启动阶段；若 SIGTERM 已经提前接管，直接返回 false 跳过所有启动。
//   - 一旦获准进入，gracefulShutdownWithCoordinator 必须先等待 waitStartupPhaseDone(drainCtx) 再执行 stopAllBackgroundLoops，
//     彻底消除“主协程刚读完 running、信号协程切入 shuttingDown 并跑完 Stop，主协程再调用 mon.Start()/vps.Start()/StartPollerIfEnabled”的并发窗口！
func (c *shutdownCoordinator) beginStartupPhase() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRunning {
		return false
	}
	c.startupActive = true
	if c.startupDone == nil {
		c.startupDone = make(chan struct{})
	}
	return true
}

func (c *shutdownCoordinator) finishStartupPhase() {
	c.mu.Lock()
	if c.startupActive {
		c.startupActive = false
		if c.startupDone != nil {
			close(c.startupDone)
			c.startupDone = nil
		}
	}
	c.mu.Unlock()
}

func (c *shutdownCoordinator) waitStartupPhaseDone(ctx context.Context) bool {
	c.mu.Lock()
	if !c.startupActive || c.startupDone == nil {
		c.mu.Unlock()
		return true
	}
	ch := c.startupDone
	c.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// runStartupUnitLocked 在 coord.mu 与启动屏障共同保护下串行执行单个短启动登记（如 mon.Start / vps.Start / 派发后台协程）：
// 仅当此刻仍处于 lifecycleRunning 时才允许启动；一旦停机开始，后续任意未开始的启动单元全部被原子跳过。
func (c *shutdownCoordinator) runIfRunning(fn func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRunning {
		return false
	}
	fn()
	return true
}

// validateStartupAuthPolicy 校验生产启动时的鉴权与监听地址组合：
// 当启用 API Key 鉴权且使用公开默认密钥 "123456" 并暴露到非回环监听地址时，直接拒绝启动（除非显式设置 OVH_ALLOW_DEFAULT_KEY_REMOTE=true）。
func validateStartupAuthPolicy(enableAuth bool, apiKey, listenHost string, allowDefaultRemote bool) error {
	if !enableAuth || apiKey != "123456" || allowDefaultRemote {
		return nil
	}
	h := strings.TrimSpace(listenHost)
	h = strings.Trim(h, "[]")
	if h == "127.0.0.1" || h == "::1" || strings.EqualFold(h, "localhost") {
		return nil
	}
	return fmt.Errorf("拒绝启动：启用鉴权时正在使用公开默认 API_SECRET_KEY=123456 且 LISTEN_HOST=%q 非本机回环地址；请在 .env 或环境变量中设置强随机 API_SECRET_KEY（或仅将 LISTEN_HOST 绑定到 127.0.0.1；本地调试如确需放行请显式设置 OVH_ALLOW_DEFAULT_KEY_REMOTE=true）", listenHost)
}

// takeDeferredShutdown 在重启准备进入不可逆关停（关 HTTP / 关库 / 替换进程）前原子检查是否有延迟登记的 SIGTERM/SIGINT。
// 若存在，在同一把锁内直接切入 lifecycleShuttingDown、广播新 deadline 并交出信号名与截止时间，防止优雅停机信号被重启流程吞掉。
func (c *shutdownCoordinator) takeDeferredShutdown() (sig string, deadline time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRestarting || c.sigtermDeferred == "" {
		return "", time.Time{}
	}
	sig = c.sigtermDeferred
	dl := c.sigtermDeadline
	if dl.IsZero() {
		dl = c.deadline
	}
	c.sigtermDeferred = ""
	c.sigtermDeadline = time.Time{}
	c.mode = lifecycleShuttingDown
	c.deadline = dl
	c.notifyDeadlineUpdatedLocked()
	return sig, dl
}

// setRestartArbiter 由 startSignalHandler 注册单协程裁决器；
// 绑定后，commitRestartOrTakeDeferredShutdown 会将最终提交请求交由该唯一信号协程串行执行，
// 彻底消灭“信号已被另一协程从 sigCh 读走、正在调用 String() 尚未登记，重启却同时提交”的竞态窗口。
func (c *shutdownCoordinator) setRestartArbiter(fn func(target lifecycleMode) (bool, string, time.Time)) {
	c.mu.Lock()
	c.restartArbiter = fn
	c.mu.Unlock()
}

func (c *shutdownCoordinator) clearRestartArbiter() {
	c.mu.Lock()
	c.restartArbiter = nil
	c.mu.Unlock()
}

// attachSignalChannel 将进程信号通道与停机预算绑定到协调器（供统一裁决路径对缓冲信号使用完整停机预算）。
func (c *shutdownCoordinator) attachSignalChannel(ch <-chan os.Signal, budget ...time.Duration) {
	c.mu.Lock()
	c.sigCh = ch
	if len(budget) > 0 && budget[0] > 0 {
		c.shutdownBudget = budget[0]
	}
	c.mu.Unlock()
}

// setShutdownBudget 记录本次运行的停机信号预算，保证所有裁决点（含从 sigCh 直接读取缓冲信号）均授予完整停机预算。
func (c *shutdownCoordinator) setShutdownBudget(budget time.Duration) {
	if budget <= 0 {
		return
	}
	c.mu.Lock()
	c.shutdownBudget = budget
	c.mu.Unlock()
}

// commitRestartEnterHook 仅供单测确认重启协程已实际抵达提交入口（生产环境为 nil）。
var commitRestartEnterHook atomic.Pointer[func()]

// commitRestartOrTakeDeferredShutdown 是进入不可逆 exec/spawn 前的最终原子提交点：
// 若已注册 startSignalHandler 裁决器，交由唯一的信号协程串行裁决（零竞态窗口）；否则直接锁内裁决。
func (c *shutdownCoordinator) commitRestartOrTakeDeferredShutdown() (committed bool, sig string, deadline time.Time) {
	if h := commitRestartEnterHook.Load(); h != nil && *h != nil {
		(*h)()
	}
	c.mu.Lock()
	arbiter := c.restartArbiter
	c.mu.Unlock()
	if arbiter != nil {
		return arbiter(lifecycleRestartCommitted)
	}
	return c.transitionRestartTerminalDirect(lifecycleRestartCommitted)
}

// transitionRestartTerminalDirect 在单把 coord.mu 锁内完成重启向指定终态（lifecycleRestartCommitted / lifecycleRestartAborted）的原子切换或延迟停机交接。
// 关键修复：若在两步读取之间 sigCh 恰好缓冲到了新信号，使用完整停机预算（time.Now().Add(c.shutdownBudget)），绝不继承即将耗尽的旧重启 deadline！
func (c *shutdownCoordinator) transitionRestartTerminalDirect(target lifecycleMode) (transitioned bool, sig string, deadline time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRestarting {
		return false, "", time.Time{}
	}
	if c.sigtermDeferred == "" && c.sigCh != nil {
		select {
		case s, ok := <-c.sigCh:
			if ok && s != nil {
				c.sigtermDeferred = s.String()
				b := c.shutdownBudget
				if b <= 0 {
					b = defaultShutdownBudget
				}
				c.sigtermDeadline = time.Now().Add(b)
			}
		default:
		}
	}
	if c.sigtermDeferred != "" {
		sig = c.sigtermDeferred
		dl := c.sigtermDeadline
		if dl.IsZero() {
			b := c.shutdownBudget
			if b <= 0 {
				b = defaultShutdownBudget
			}
			dl = time.Now().Add(b)
		}
		c.sigtermDeferred = ""
		c.sigtermDeadline = time.Time{}
		c.mode = lifecycleShuttingDown
		c.deadline = dl
		c.notifyDeadlineUpdatedLocked()
		return false, sig, dl
	}
	c.mode = target
	return true, "", time.Time{}
}

// enterMaintenance 在自更新排空超时或关键落库异常时转入可观测维护态（拒绝新的副作用写请求）。
// 若等待期间已登记 SIGTERM，则在同一次持锁内直接切入 shuttingDown、广播新 deadline 并返回信号名。
func (c *shutdownCoordinator) enterMaintenance(reason string) (deferredSig string, deadline time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != lifecycleRestarting {
		return "", time.Time{}
	}
	c.maintenanceReason = reason
	if c.sigtermDeferred != "" {
		sig := c.sigtermDeferred
		dl := c.sigtermDeadline
		if dl.IsZero() {
			dl = c.deadline
		}
		c.sigtermDeferred = ""
		c.sigtermDeadline = time.Time{}
		c.mode = lifecycleShuttingDown
		c.deadline = dl
		c.notifyDeadlineUpdatedLocked()
		return sig, dl
	}
	c.mode = lifecycleMaintenance
	c.deadline = time.Time{}
	c.notifyDeadlineUpdatedLocked()
	return "", time.Time{}
}

// tryAcquireWrite 供 HTTP 写请求中间件调用：仅在 lifecycleRunning 状态下放行并登记在途写请求；
// 进入重启/退出/维护态后立即拒绝新写请求（返回 503），同时等待已进入的在途写请求全部完成。
func (c *shutdownCoordinator) tryAcquireWrite() (release func(), allowed bool, reason string) {
	c.mu.Lock()
	if c.mode != lifecycleRunning {
		r := c.maintenanceReason
		if r == "" {
			r = "服务正在退出或更新重启中，暂不接受新写入请求"
		}
		c.mu.Unlock()
		return nil, false, r
	}
	c.inFlightWrites++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.inFlightWrites--
			if c.inFlightWrites == 0 && c.writesDrained != nil {
				close(c.writesDrained)
				c.writesDrained = nil
			}
			c.mu.Unlock()
		})
	}, true, ""
}

// waitWritesDrained 等待所有已放行的在途 HTTP 写请求收尾，返回是否全部在 ctx 截止前完成。
func (c *shutdownCoordinator) waitWritesDrained(ctx context.Context) bool {
	c.mu.Lock()
	if c.inFlightWrites == 0 {
		c.mu.Unlock()
		return true
	}
	ch := c.writesDrained
	if ch == nil {
		ch = make(chan struct{})
		c.writesDrained = ch
	}
	c.mu.Unlock()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *shutdownCoordinator) writeGateMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		switch ctx.Request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			ctx.Next()
			return
		}
		// 仅拦截业务写操作；鉴权放行后已到达这里
		release, ok, reason := c.tryAcquireWrite()
		if !ok {
			ctx.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"success":     false,
				"error":       reason,
				"maintenance": true,
			})
			return
		}
		defer release()
		ctx.Next()
	}
}

func (c *shutdownCoordinator) state() (mode lifecycleMode, done <-chan struct{}, deadline time.Time, exitCode int, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode, c.done, c.deadline, c.exitCode, c.maintenanceReason
}

func (c *shutdownCoordinator) stateWithDeadlineSignal() (mode lifecycleMode, done <-chan struct{}, deadline time.Time, deadlineGen uint64, deadlineChanged <-chan struct{}, exitCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode, c.done, c.deadline, c.deadlineGen, c.deadlineUpdated, c.exitCode
}

// isDeadlineStillCurrentAndExpired 在锁内二次复核本轮观察到的 deadlineGen 是否仍是当前有效代次且确已超过 deadline+grace；
// 消除“旧 timer.C 与新 deadlineChanged 同时就绪时 Go select 随机选到旧 timer.C 误返回 1”的竞态窗口。
func (c *shutdownCoordinator) isDeadlineStillCurrentAndExpired(observedGen uint64, grace time.Duration) (expired bool, finished bool, exitCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return false, true, c.exitCode
	default:
	}
	if c.mode != lifecycleShuttingDown && c.mode != lifecycleRestarting && c.mode != lifecycleRestartCommitted && c.mode != lifecycleRestartAborted {
		return false, true, 0
	}
	if c.deadlineGen != observedGen {
		return false, false, 0
	}
	return !time.Now().Before(c.deadline.Add(grace)), false, 0
}

func (c *shutdownCoordinator) finish(exitCode int) {
	c.finishOnce.Do(func() {
		c.mu.Lock()
		c.exitCode = exitCode
		c.mu.Unlock()
		close(c.done)
	})
}

func (c *shutdownCoordinator) closeDB(sqlDB io.Closer) (performed bool, err error) {
	if sqlDB == nil {
		return false, nil
	}
	c.closeDBOnce.Do(func() {
		performed = true
		c.closeDBErr = sqlDB.Close()
	})
	return performed, c.closeDBErr
}

// runBounded 在截止时间 ctx 约束下执行可能阻塞的收尾步骤（如关库/刷盘），
// 若 ctx 在调用前已过期则直接返回 (false, ctx.Err())，绝不额外拉起注定超时的孤儿协程（防止并发 Close 等副作用）。
func runBounded(ctx context.Context, fn func() error) (completed bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ch := make(chan error, 1)
	go func() {
		ch <- fn()
	}()
	select {
	case e := <-ch:
		return true, e
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

var (
	bgLoopsMu     sync.Mutex
	bgLoopsCancel context.CancelFunc
	bgLoopsDone   []<-chan struct{}
)

func registerBackgroundLoops(cancel context.CancelFunc, done ...<-chan struct{}) {
	bgLoopsMu.Lock()
	bgLoopsCancel = cancel
	bgLoopsDone = append([]<-chan struct{}(nil), done...)
	bgLoopsMu.Unlock()
}

// stopRegisteredBackgroundLoops 取消并等待全部已注册后台循环退出，返回是否全部在 ctx 截止前干净退出。
// 不提前清空 bgLoopsDone，避免并发或重入调用误判为“已完成”。
func stopRegisteredBackgroundLoops(ctx context.Context, lg *logger.Logger) bool {
	bgLoopsMu.Lock()
	cancel := bgLoopsCancel
	doneChans := append([]<-chan struct{}(nil), bgLoopsDone...)
	bgLoopsMu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, ch := range doneChans {
		if ch == nil {
			continue
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func buildTrustedProxyChecker(trusted []string) func(string) bool {
	var ips []net.IP
	var nets []*net.IPNet
	for _, entry := range trusted {
		if _, cidr, err := net.ParseCIDR(entry); err == nil {
			nets = append(nets, cidr)
			continue
		}
		if ip := net.ParseIP(entry); ip != nil {
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 && len(nets) == 0 {
		return nil
	}
	return func(remoteIP string) bool {
		ip := net.ParseIP(strings.TrimSpace(remoteIP))
		if ip == nil {
			return false
		}
		for _, allowed := range ips {
			if allowed.Equal(ip) {
				return true
			}
		}
		for _, cidr := range nets {
			if cidr.Contains(ip) {
				return true
			}
		}
		return false
	}
}

func main() {
	// envPath 就是 godotenv 读的那个文件。密钥自动生成时会追加到这里,
	// 所以路径必须和 Load() 用的完全一致 —— 分叉了就会出现
	// "写进了 A、下次从 B 读"的情况,而那意味着重新生成一把新密钥。
	envPath := envFilePath()
	// 空值的环境变量要当成"没设"。
	//
	// godotenv.Load 不覆盖**已存在**的环境变量 —— 哪怕它是空串。
	// 而容器编排很容易设出空变量:docker compose 里写
	// `OVH_DB_KEY: ${OVH_DB_KEY:-}`,宿主机没定义时容器里就是一个空的 OVH_DB_KEY。
	//
	// 后果是灾难性的且完全无声:文件里明明有密钥,却因为环境变量"已存在"而读不进来,
	// 于是每次启动都判定"没有密钥"、重新生成一把、再追加进 .env ——
	// 之前加密的 OVH 凭据和 Telegram Token 从此永久解不开,
	// 而用户看到的只是"账户怎么没了"。
	//
	// 实测踩到过:容器重启两次,/data/.env 里就有两行 OVH_DB_KEY。
	clearEmptyEnv(
		"OVH_DB_KEY", "API_SECRET_KEY", "TG_ALLOWED_USER_IDS",
		"CORS_ALLOWED_ORIGINS", "TRUSTED_PROXIES", "OVH_UPDATE_API",
		"LISTEN_HOST", "PORT", "DATA_DIR", "CACHE_DIR", "LOGS_DIR",
	)
	_ = godotenv.Load(envPath)

	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("DEBUG"), "true") {
		level = slog.LevelDebug
	}
	console := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	paths := storage.DefaultPaths()
	if err := paths.EnsureDirs(); err != nil {
		console.Error("create dirs", "err", err)
		os.Exit(1)
	}

	// 落盘加密:必须在打开数据库**之前**初始化,否则第一次读账户时解不开。
	// 密钥读取失败时停止启动,避免用空凭据或明文写入覆盖已有密文。
	if err := secret.Init(paths.DataDir, envPath); err != nil {
		console.Error("无法初始化数据库加密密钥,已停止启动；请检查密钥文件及权限", "err", err)
		os.Exit(1)
	} else if secret.KeyWasGenerated() {
		console.Info("已自动生成数据库加密密钥并写入 " + secret.KeySource())
		console.Info("请把它连同配置文件一起备份 —— 丢了这把钥匙，已保存的 OVH 凭据和 Telegram Token 就再也解不开了")
	} else {
		console.Info("凭据加密已启用（密钥来自 " + secret.KeySource() + "）")
	}

	sqliteDB, err := db.Open(paths.DataDir)
	if err != nil {
		console.Error("open sqlite", "err", err)
		os.Exit(1)
	}

	// 统一由 coord 唯一拥有数据库关闭权：无论正常返回、早期启动失败还是 SIGTERM/自更新收尾，均通过 coord.closeDB(sqliteDB) 的 sync.Once 保证只关闭一次。
	coord := newShutdownCoordinator()
	exitProcess := func(code int) {
		_, _ = coord.closeDB(sqliteDB)
		os.Exit(code)
	}
	defer func() {
		_, _ = coord.closeDB(sqliteDB)
	}()

	// 密钥是刚生成的、库里却已经有密文 —— 说明原来那把钥匙丢了。
	// 直接起来的话程序看着一切正常，只是每次调 OVH 都报签名错误，
	// 没人猜得到是密钥问题；更糟的是用户会重新录入凭据把旧密文覆盖掉，
	// 最后一点恢复余地也没了。所以这里宁可不起来，把话说清楚。
	if secret.KeyWasGenerated() && !isTrue(os.Getenv("OVH_DB_KEY_RESET")) {
		if has, herr := sqliteDB.HasEncryptedSecrets(); herr == nil && has {
			console.Error("数据库里有加密过的凭据，但找不到对应的密钥，已停止启动")
			console.Error("这通常是配置文件被覆盖/重置了，或者换机器时只拷了数据库没拷配置")
			console.Error("怎么办：把原来的 " + secret.KeyEnv + " 那一行放回 " + envPath +
				"（或用环境变量传进来）；确实找不回来就设 OVH_DB_KEY_RESET=1 启动，" +
				"那些账户需要重新录入 OVH 凭据")
			exitProcess(1)
		}
	}

	// 老库升级:把已有的明文凭据就地加密。幂等,不迁移也能跑(读的时候兼容明文)
	if n, merr := sqliteDB.EncryptExistingSecrets(); merr != nil {
		console.Warn("迁移明文凭据时出错（不影响启动）", "err", merr)
	} else if n > 0 {
		console.Info("已把数据库里的明文凭据加密", "账户数", n)
	}

	lg := logger.New(paths.LogFile("app.log.json"), console)
	lg.StartAutoFlush()
	cfgStore := config.New(sqliteDB)
	state := app.NewState(paths, cfgStore, lg, sqliteDB)
	state.OrderPrewarm = purchase.NewCartPool(state)
	apiKey, generatedAPIKey, err := auth.ResolveAPIKey(paths.DataDir, os.Getenv("API_SECRET_KEY"))
	if err != nil {
		console.Error("无法安全初始化 API 密钥,已停止启动", "err", err)
		exitProcess(1)
	}
	state.APIKey = apiKey
	if generatedAPIKey {
		console.Info("已生成随机 API 密钥,请从数据目录的 .api-secret 文件读取后登录", "dataDir", paths.DataDir)
	}
	state.Port = os.Getenv("PORT")
	if state.Port == "" {
		state.Port = "19998"
	}
	state.LoadAll()

	// 密钥对不上时凭据会全部解成空串。只静默显示"空账户"的话,用户看到的现象是
	// "账户还在、但连不上 OVH",完全猜不到根因。这里大声说清楚,并给出可执行的下一步。
	if n := secret.DecryptFailures(); n > 0 {
		console.Error("有 " + fmt.Sprintf("%d", n) + " 个加密字段解不开 —— 密钥与数据库对不上。" +
			"常见原因:只恢复了 sniper.db 却没恢复 .dbkey,或换过 " + secret.KeyEnv + "。" +
			"把原来的密钥放回去即可;找不回来的话需要在设置页重新录入 OVH 凭据。")
		state.Logger.Error(fmt.Sprintf("[加密] %d 个字段解密失败:密钥与数据库不匹配", n), "system")
	} else if secret.KeyWasGenerated() && len(state.Accounts) > 0 {
		console.Warn("新生成了加密密钥,但库里已有账户 —— 如果这些账户的凭据是空的,说明原密钥丢了")
	}

	// 上一次更新留下的残骸(中断的临时文件)在这里清掉
	updater.CleanupStale()

	// 上一次更新之后没能正常启动?换回更新前的版本再跑。
	// 判据是"有备份 + 有待验证标记":新版本活到对外服务那一刻会把标记删掉,
	// 标记还在说明它没撑到那一步(配置不兼容、平台问题、启动即崩)。
	// 抢购服务停机就是错过补货,不能让一次坏更新把机器撂在那儿。
	if updater.RollbackIfStale(state) {
		if exe, err := os.Executable(); err == nil {
			console.Warn("已回滚到更新前的版本，正在用它重启")
			_, _ = coord.closeDB(sqliteDB)
			if err := updater.Restart(exe); err != nil {
				console.Error("回滚后重启失败", "err", err)
				exitProcess(1)
			}
		}
	}

	// gracefulRestart 由 SelfUpdate 在替换完二进制后调用。
	// 必须先关监听端口和 SQLite 再 exec:端口不放新进程会撞 "address already in use",
	// SQLite 不干净关闭会留下 -wal / -shm。
	var gracefulRestart = func(exe string) {}

	// 监控器
	mon := monitor.New(state)
	monitorState = mon
	appStateForShutdown.Store(state)
	// 删账户的级联清理需要摸到监控内存(SQL 清了、内存不清会被 SaveToDB 写回)
	handlers.SetMonitorRef(mon)
	mon.LoadFromDB()
	console.Info("监控就绪", "checkInterval", mon.CheckInterval())

	// Gin
	if mode := os.Getenv("GIN_MODE"); mode != "" {
		gin.SetMode(mode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	// 默认不信任任何代理。gin 默认信任所有代理 + 读 X-Forwarded-For,
	// 于是 c.ClientIP() 直接返回请求头里的值 —— 而 /api/internal/monitor/price
	// 的「仅限本地」判定就是拿 ClientIP 做的,任何人加一个
	// X-Forwarded-For: 127.0.0.1 就能绕过去,驱动服务端用真实凭据建/删 OVH 购物车
	// (消耗 API 配额,补货窗口被限流就是错过抢购)。
	//
	// 真的在反向代理后面跑,用 TRUSTED_PROXIES 显式声明(逗号分隔 IP/CIDR)。
	trusted := []string{}
	for _, p := range strings.Split(os.Getenv("TRUSTED_PROXIES"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			trusted = append(trusted, p)
		}
	}
	if err := r.SetTrustedProxies(trusted); err != nil {
		_ = r.SetTrustedProxies(nil)
		handlers.SetTrustedProxyChecker(nil)
		console.Warn("SetTrustedProxies 失败,将按不信任代理处理", "err", err)
	} else {
		handlers.SetTrustedProxyChecker(buildTrustedProxyChecker(trusted))
	}
	r.Use(gin.Recovery())
	r.Use(auth.SecurityHeaders())
	r.Use(coord.writeGateMiddleware())
	r.Use(auth.LimitRequestBody(4 << 20))
	// CORS 只放行同机来源。
	//
	// 以前是 AllowAllOrigins:true,配上「API_SECRET_KEY 未设时默认 123456」和
	// 「GET /api/accounts 下发解密后的凭据明文」,构成一条完整的窃取链:
	// 用户开着控制台时访问任意网页,那个页面只要
	//   fetch('http://127.0.0.1:19998/api/accounts', {headers:{'X-API-Key':'123456'}})
	// 就能读走全部 OVH 凭据 —— 浏览器本身就是攻击载体,「只监听本地」挡不住。
	//
	// 前端和后端同源部署(单二进制 embed)时根本不需要 CORS;
	// 分开跑时只有本机的 dev server 需要。所以白名单化,
	// 额外来源用 CORS_ALLOWED_ORIGINS 显式声明(逗号分隔)。
	r.Use(cors.New(cors.Config{
		AllowOrigins: allowedOrigins(state.Port),
		AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowHeaders: []string{"Content-Type", "Authorization", "X-API-Key", "X-Request-Time"},
		// X-Partial-Failures:部分明细拉取失败的计数(账单/退款/邮件等走响应头下发),
		// 跨源部署时不列进 ExposeHeaders 浏览器就读不到,前端的"部分失败"提示会恒不显示
		ExposeHeaders:    []string{"X-Cache-Warning", "X-Partial-Failures", "X-Cache-Age-Seconds", "X-Subsidiary-Mismatch"},
		AllowCredentials: false,
	}))

	enableAuth := !strings.EqualFold(os.Getenv("ENABLE_API_KEY_AUTH"), "false")
	r.Use(auth.Middleware(auth.Config{
		APIKey:         state.APIKey,
		Enabled:        enableAuth,
		WhitelistPaths: auth.DefaultWhitelist(),
		// App 设备令牌校验(Bearer)。nil 安全:配对表不存在时走不到这条分支
		DeviceTokenValid: state.DB.DeviceTokenValid,
	}))

	// 健康检查
	r.GET("/health", handlers.Health())

	api := r.Group("/api")
	{
		api.GET("/health", handlers.Health())

		// Settings
		api.GET("/settings", handlers.GetSettings(state))
		api.POST("/settings", handlers.SaveSettings(state))
		api.POST("/verify-auth", handlers.VerifyAuth(state))
		api.GET("/endpoint-config", handlers.EndpointConfig(state))

		// Logs / stats
		api.GET("/logs", handlers.GetLogs(state))
		api.POST("/logs/flush", handlers.FlushLogs(state))
		api.DELETE("/logs", handlers.ClearLogs(state))
		api.GET("/stats", handlers.GetStats(state, mon))

		// Queue
		api.GET("/queue", handlers.GetQueue(state))
		api.GET("/queue/timings", handlers.GetPurchaseTimings(state))
		api.POST("/queue", handlers.AddQueueItem(state))
		api.GET("/purchase/durations", handlers.GetOrderDurations(state))
		api.POST("/purchase/prewarm", handlers.PrepareOrderCart(state))
		api.GET("/purchase/prewarm", handlers.GetOrderPrewarm(state))
		api.DELETE("/purchase/prewarm/:id", handlers.CancelOrderPrewarm(state))
		api.DELETE("/queue/clear", handlers.ClearQueue(state))
		api.DELETE("/queue/:id", handlers.RemoveQueueItem(state))
		api.PUT("/queue/:id", handlers.UpdateQueueItem(state))
		api.PUT("/queue/:id/status", handlers.UpdateQueueStatus(state))
		api.PUT("/queue/:id/interval", handlers.UpdateQueueInterval(state))

		// Purchase history
		api.GET("/purchase-history", handlers.GetPurchaseHistory(state))
		api.DELETE("/purchase-history", handlers.ClearPurchaseHistory(state))
		// 订单支付状态:下单成功≠已付款,得去 OVH 问
		api.POST("/purchase-history/refresh-status", handlers.RefreshOrderStatuses(state))

		// Monitor
		api.GET("/monitor/subscriptions", handlers.GetSubscriptions(state, mon))
		api.POST("/monitor/subscriptions", handlers.AddSubscription(state, mon))
		api.PUT("/monitor/subscriptions/:planCode", handlers.UpdateSubscription(state, mon))
		api.POST("/monitor/subscriptions/:planCode/pause", handlers.PauseSubscription(state, mon))
		api.POST("/monitor/subscriptions/batch-add-all", handlers.BatchAddAll(state, mon))
		api.DELETE("/monitor/subscriptions/clear", handlers.ClearSubscriptions(state, mon))
		api.DELETE("/monitor/subscriptions/:planCode", handlers.RemoveSubscription(state, mon))
		api.GET("/monitor/subscriptions/:planCode/history", handlers.GetSubscriptionHistory(state, mon))
		api.POST("/monitor/start", handlers.StartMonitor(state, mon))
		api.POST("/monitor/stop", handlers.StopMonitor(state, mon))
		api.GET("/monitor/status", handlers.GetMonitorStatus(state, mon))
		api.PUT("/monitor/interval", handlers.SetMonitorInterval(state, mon))
		api.POST("/monitor/test-notification", handlers.TestNotification(state))
		// 通知通道体检:哪条配了、哪条能用。?verify=true 会真的去调远端
		api.GET("/notify/channels", handlers.GetNotifyChannels(state))
		api.GET("/telegram/verify", handlers.VerifyTelegram(state))

		// Telegram
		// 收 update 只有长轮询一条路。这个端点只读状态,给设置页显示"收到没收到"。
		api.GET("/telegram/poller", handlers.GetTelegramPollerStatus(state))

		// 按账户的出站代理:查真实出口 IP(确认隔离生效的唯一可靠手段)+ 健康状况
		api.POST("/accounts/:id/proxy-test", handlers.TestAccountProxy(state))
		// 出站链路体检:出口 IP + 各目标连通性与延迟(抢购对延迟直接敏感)
		api.POST("/accounts/:id/proxy-check", handlers.CheckAccountProxy(state))
		api.GET("/accounts/proxy-status", handlers.ProxyStatus(state))

		// Servers / availability / cache
		api.GET("/servers", handlers.GetServers(state))
		api.GET("/availability/*planCode", availabilityHandler(handlers.GetAvailability(state)))
		api.POST("/availability/*planCode", availabilityHandler(handlers.GetAvailability(state)))
		api.POST("/internal/monitor/price", handlers.MonitorPrice(state))
		api.POST("/servers/:planCode/price", handlers.ServerPrice(state))
		api.GET("/cache/info", handlers.CacheInfo(state))
		api.POST("/cache/clear", handlers.ClearCache(state))
		api.GET("/catalog", handlers.GetCatalog(state))
		api.GET("/system/metrics", handlers.GetSystemMetrics(state))
		api.GET("/version", handlers.GetVersion(state))

		// App 配对体系:兑换在鉴权白名单(凭 2 分钟一次性码),管理端仅限主 API 密钥（防止被盗设备签发持久化后门新设备）
		api.POST("/app/pair", handlers.RedeemPairingCode(state))
		api.POST("/app/pairing-codes", auth.RequireMasterKey(), handlers.CreatePairingCode(state))
		api.GET("/app/devices", auth.RequireMasterKey(), handlers.ListAppDevices(state))
		api.DELETE("/app/devices/:id", auth.RequireMasterKey(), handlers.RevokeAppDevice(state))
		api.GET("/version/check-update", handlers.CheckUpdate(state))
		// 在线更新:下载 → 校验 → 替换自己 → 自动重启。gracefulRestart 在下面赋值,
		// 这里用闭包间接引用,避免"路由要在 server 之前注册、server 又要在路由之后创建"的鸡生蛋
		api.POST("/version/update", handlers.SelfUpdate(state, func(exe string) { gracefulRestart(exe) }))
		api.GET("/version/update/status", handlers.GetUpdateStatus(state))

		// Accounts (多账户管理)
		api.GET("/accounts", handlers.ListAccounts(state))
		api.GET("/accounts/:id", handlers.GetAccountByID(state))
		api.POST("/accounts", handlers.CreateAccount(state))
		api.PUT("/accounts/:id", handlers.UpdateAccount(state))
		api.DELETE("/accounts/:id", handlers.DeleteAccountByID(state))
		api.POST("/accounts/:id/set-default", handlers.SetDefaultAccountByID(state))
		api.POST("/accounts/:id/verify", handlers.VerifyAccount(state))

		// 快速下单端点 (监控的 auto-order 通过 HTTP 自调走它,前端也可直接调)
		api.POST("/queue/quick-order", handlers.QuickOrder(state))

		// Server control - basic
		sc := api.Group("/server-control")
		// :service_name 会被直接拼进 OVH 的请求路径,先在组上统一挡一道 ——
		// 名字里带 # 或 ? 会让请求被静默发到另一个端点,见 ValidateServiceName
		sc.Use(handlers.ValidateServiceName())
		{
			sc.GET("/list", handlers.ListMyServers(state))
			// 服务器本地别名:纯本地显示用,不下发 OVH
			sc.GET("/aliases", handlers.ListServerAliases(state))
			sc.PUT("/:service_name/alias", handlers.SetServerAlias(state))
			sc.DELETE("/:service_name/alias", handlers.DeleteServerAlias(state))
			sc.GET("/order-mapping", handlers.GetOrderMapping(state))
			// 14 天无理由撤单:GET 判断这台机器还能不能退(依据 OVH 的 retractionDate,
			// 不是自己算 14 天),POST 真正提交申请(不可逆,要求 confirm:true)
			sc.GET("/:service_name/retraction", handlers.GetRetraction(state))
			sc.POST("/:service_name/retraction", handlers.PostRetraction(state))
			sc.POST("/:service_name/reboot", handlers.Reboot(state))
			sc.GET("/:service_name/templates", handlers.GetOSTemplates(state))
			sc.POST("/:service_name/install", handlers.InstallOS(state))
			sc.GET("/:service_name/install/status", handlers.GetInstallStatus(state))
			sc.GET("/:service_name/tasks", handlers.GetServerTasks(state))
			sc.GET("/:service_name/tasks/:task_id/available-timeslots", handlers.GetTaskAvailableTimeslots(state))
			sc.POST("/:service_name/tasks/:task_id/schedule", handlers.ScheduleTaskTimeslot(state))

			// boot/monitoring
			// 一键救援:改 netboot → 设收信邮箱 → 重启,三步合一。
			// 手动做要在 OVH 后台点四步,而漏掉最后的重启是最常见的错误。
			sc.GET("/:service_name/rescue", handlers.GetRescueStatus(state))
			sc.GET("/:service_name/boot-state", handlers.GetBootState(state))
			sc.POST("/:service_name/rescue", handlers.EnterRescue(state))
			sc.POST("/:service_name/rescue/exit", handlers.ExitRescue(state))
			sc.GET("/:service_name/boot", handlers.GetBootConfig(state))
			sc.PUT("/:service_name/boot/:boot_id", handlers.SetBootConfig(state))
			sc.GET("/:service_name/monitoring", handlers.GetMonitoringStatus(state))
			sc.PUT("/:service_name/monitoring", handlers.SetMonitoringStatus(state))
			sc.GET("/:service_name/boot-mode", handlers.GetBootModes(state))
			sc.PUT("/:service_name/boot-mode", handlers.ChangeBootMode(state))

			// hardware/network/dns
			sc.GET("/:service_name/hardware", handlers.GetHardwareInfo(state))
			sc.GET("/:service_name/network-specs", handlers.GetNetworkSpecs(state))
			sc.GET("/:service_name/ips", handlers.GetServerIPs(state))
			sc.GET("/:service_name/reverse", handlers.GetReverseDNS(state))
			sc.POST("/:service_name/reverse", handlers.SetReverseDNS(state))
			sc.DELETE("/:service_name/reverse/:ip", handlers.DeleteReverseDNS(state))
			sc.GET("/:service_name/serviceinfo", handlers.GetServiceInfo(state))
			sc.GET("/:service_name/pricing", handlers.GetServerPricing(state))
			sc.PUT("/:service_name/serviceinfo/renewal", handlers.UpdateServiceRenewal(state))

			// engagement(合同期切换)
			sc.GET("/:service_name/engagement", handlers.GetEngagement(state))
			sc.GET("/:service_name/engagement/available", handlers.GetEngagementAvailable(state))
			sc.GET("/:service_name/engagement/request", handlers.GetEngagementRequest(state))
			sc.POST("/:service_name/engagement/request", handlers.CreateEngagementRequest(state))
			sc.DELETE("/:service_name/engagement/request", handlers.DeleteEngagementRequest(state))
			sc.PUT("/:service_name/engagement/end-rule", handlers.UpdateEngagementEndRule(state))

			// DDoS mitigation
			sc.GET("/:service_name/mitigation", handlers.GetMitigation(state))
			sc.POST("/:service_name/mitigation/:ip", handlers.EnableMitigation(state))
			sc.DELETE("/:service_name/mitigation/:ip", handlers.DisableMitigation(state))
			sc.POST("/:service_name/change-contact", handlers.ChangeContact(state))
			sc.GET("/:service_name/interventions", handlers.GetInterventions(state))
			sc.GET("/:service_name/interventions/:intervention_id", handlers.GetInterventionDetail(state))
			sc.GET("/:service_name/planned-interventions", handlers.GetPlannedInterventions(state))
			sc.GET("/:service_name/planned-interventions/:intervention_id", handlers.GetPlannedInterventionDetail(state))
			sc.POST("/:service_name/hardware/replace", handlers.HardwareReplace(state))
			sc.GET("/:service_name/hardware-raid-profiles", handlers.GetHardwareRaidProfiles(state))
			sc.GET("/:service_name/hardware-disk-info", handlers.GetHardwareDiskInfo(state))
			sc.GET("/:service_name/partition-schemes", handlers.GetPartitionSchemes(state))

			// network
			sc.GET("/:service_name/network-interfaces", handlers.GetNetworkInterfaces(state))
			sc.GET("/:service_name/mrtg", handlers.GetMRTGData(state))
			sc.POST("/:service_name/ola/aggregation", handlers.ConfigureOLAAggregation(state))
			sc.POST("/:service_name/ola/reset", handlers.ResetOLAConfiguration(state))
			sc.POST("/:service_name/ola/group", handlers.OLAGroup(state))
			sc.POST("/:service_name/ola/ungroup", handlers.OLAUngroup(state))
			sc.GET("/:service_name/console", handlers.GetIPMIConsole(state))
			// 只查支持哪几种控制台类型(HTML5 / Java KVM / SOL),不申请会话
			sc.GET("/:service_name/ipmi-types", handlers.GetIPMIAccessTypes(state))
			sc.GET("/:service_name/statistics", handlers.GetTrafficStatistics(state))
			sc.GET("/:service_name/network-stats", handlers.GetNetworkInterfaceStats(state))

			// features
			sc.GET("/:service_name/burst", handlers.GetBurst(state))
			sc.PUT("/:service_name/burst", handlers.UpdateBurst(state))
			sc.GET("/:service_name/firewall", handlers.GetFirewall(state))
			sc.PUT("/:service_name/firewall", handlers.UpdateFirewall(state))
			sc.GET("/:service_name/backup-ftp", handlers.GetBackupFTP(state))
			sc.POST("/:service_name/backup-ftp", handlers.ActivateBackupFTP(state))
			sc.DELETE("/:service_name/backup-ftp", handlers.DeleteBackupFTP(state))
			sc.GET("/:service_name/backup-ftp/access", handlers.GetBackupFTPAccess(state))
			sc.POST("/:service_name/backup-ftp/access", handlers.AddBackupFTPAccess(state))
			// ipBlock 是带掩码的 CIDR(如 37.59.1.0/28)。gin 默认 UseRawPath=false,%2F 会被还原成 "/",
			// 把 URL 撑成多一段,:ip_block 永远匹配不上(实测编码与否都 404)。
			// 所以主用 query 形式 ?ipBlock=...,旧的路径形式保留做兼容(handler 三级兜底取值)。
			sc.DELETE("/:service_name/backup-ftp/access", handlers.DeleteBackupFTPAccess(state))
			sc.DELETE("/:service_name/backup-ftp/access/:ip_block", handlers.DeleteBackupFTPAccess(state))
			sc.POST("/:service_name/backup-ftp/password", handlers.ChangeBackupFTPPassword(state))
			sc.GET("/:service_name/backup-ftp/authorizable-blocks", handlers.GetBackupFTPAuthorizableBlocks(state))
			sc.GET("/:service_name/backup-cloud", handlers.GetBackupCloud(state))
			sc.GET("/:service_name/backup-cloud/offer-details", handlers.GetBackupCloudOfferDetails(state))
			// 云备份的写操作(官方 /features/backupCloud POST/DELETE 与 /password POST),
			// 原来只实现了只读,用户无法在控制台激活/停用/重置密码
			sc.POST("/:service_name/backup-cloud", handlers.ActivateBackupCloud(state))
			sc.DELETE("/:service_name/backup-cloud", handlers.DeleteBackupCloud(state))
			sc.POST("/:service_name/backup-cloud/password", handlers.ChangeBackupCloudPassword(state))

			// misc
			sc.GET("/:service_name/secondary-dns", handlers.GetSecondaryDNS(state))
			sc.POST("/:service_name/secondary-dns", handlers.AddSecondaryDNS(state))
			sc.DELETE("/:service_name/secondary-dns/:domain", handlers.DeleteSecondaryDNS(state))
			sc.GET("/:service_name/virtual-mac", handlers.GetVirtualMACList(state))
			sc.POST("/:service_name/virtual-mac", handlers.CreateVirtualMAC(state))
			sc.GET("/:service_name/virtual-network-interface", handlers.GetVirtualNetworkInterfaces(state))
			// virtual-network-interface/:uuid/{enable,disable} 已删:底层 OVH 端点三区 DEPRECATED,
			// 按约定废弃端点不再调用(见 server_control_misc.go 里删除处的说明)
			sc.GET("/:service_name/vrack", handlers.GetVRackList(state))
			sc.DELETE("/:service_name/vrack/:vrack", handlers.RemoveFromVRack(state))
			sc.GET("/:service_name/orderable/bandwidth", handlers.GetOrderableBandwidth(state))
			sc.GET("/:service_name/orderable/traffic", handlers.GetOrderableTraffic(state))
			sc.GET("/:service_name/orderable/ip", handlers.GetOrderableIP(state))
			sc.GET("/:service_name/options", handlers.GetServerOptions(state))
			sc.GET("/:service_name/ip-specs", handlers.GetIPSpecs(state))
			sc.GET("/:service_name/ip/can-be-moved-to", handlers.GetIPCanBeMovedTo(state))
			sc.GET("/:service_name/ip/country-available", handlers.GetIPCountryAvailable(state))
			sc.POST("/:service_name/ip/move", handlers.MoveIP(state))
			sc.GET("/:service_name/ongoing", handlers.GetOngoingTasks(state))
			sc.GET("/:service_name/license/windows/compliant", handlers.GetCompliantWindowsVersions(state))
			sc.GET("/:service_name/license/windows-sql/compliant", handlers.GetCompliantWindowsSqlVersions(state))
			// 到期终止走 terminationPolicy,不是 /terminate ——
			// 后者是立即终止,提交即暂停服务器
			sc.PUT("/:service_name/termination-policy", handlers.UpdateTerminationPolicy(state))
			sc.POST("/:service_name/terminate", handlers.TerminateService(state))
			sc.POST("/:service_name/confirm-termination", handlers.ConfirmTermination(state))
			sc.GET("/:service_name/spla", handlers.GetSPLAList(state))
			sc.POST("/:service_name/spla", handlers.CreateSPLA(state))
			sc.GET("/:service_name/bios-settings", handlers.GetBIOSSettings(state))
			sc.GET("/:service_name/bios-settings/sgx", handlers.GetBIOSSettingsSGX(state))
		}

		// VPS control(已购 VPS 管理)
		vc := api.Group("/vps-control")
		// :service_name 会被直接拼进 OVH 的请求路径,先在组上统一挡一道 ——
		// 名字里带 # 或 ? 会让请求被静默发到另一个端点,见 ValidateServiceName
		vc.Use(handlers.ValidateServiceName())
		{
			vc.GET("/list", handlers.ListVps(state))
			vc.GET("/:service_name/info", handlers.GetVpsInfo(state))
			vc.GET("/:service_name/status", handlers.GetVpsServiceStatus(state))
			vc.GET("/:service_name/serviceinfo", handlers.GetVpsServiceInfo(state))
			vc.PUT("/:service_name/serviceinfo/renewal", handlers.UpdateVpsRenewal(state))
			vc.GET("/:service_name/ips", handlers.GetVpsIps(state))
			vc.PUT("/:service_name/ips/:ip/reverse", handlers.SetVpsIpReverse(state))
			vc.GET("/:service_name/datacenter", handlers.GetVpsDatacenter(state))
			// 注:OVH 已废弃 /vps/{name}/monitoring (2024-07) 和 /statistics (2023-11),
			// 不提供替代的 VPS 级监控端点,前端不再展示监控视图。

			// 电源
			vc.POST("/:service_name/start", handlers.VpsStart(state))
			vc.POST("/:service_name/stop", handlers.VpsStop(state))
			vc.POST("/:service_name/reboot", handlers.VpsReboot(state))
			vc.POST("/:service_name/console", handlers.VpsGetConsoleUrl(state))
			vc.POST("/:service_name/password", handlers.VpsSetPassword(state))

			// 重装系统
			vc.GET("/:service_name/current-os", handlers.GetVpsCurrentOS(state))
			vc.GET("/:service_name/templates", handlers.GetVpsTemplates(state))
			vc.POST("/:service_name/reinstall", handlers.ReinstallVps(state))

			// 任务
			vc.GET("/:service_name/tasks", handlers.GetVpsTasks(state))
			vc.GET("/:service_name/tasks/:task_id", handlers.GetVpsTaskDetail(state))

			// 快照
			vc.GET("/:service_name/snapshot", handlers.GetVpsSnapshot(state))
			vc.POST("/:service_name/snapshot", handlers.CreateVpsSnapshot(state))
			vc.PUT("/:service_name/snapshot", handlers.UpdateVpsSnapshotDescription(state))
			vc.POST("/:service_name/snapshot/revert", handlers.RevertVpsSnapshot(state))
			vc.DELETE("/:service_name/snapshot", handlers.DeleteVpsSnapshot(state))

			// 杂项
			vc.POST("/:service_name/change-contact", handlers.ChangeVpsContact(state))
			vc.PUT("/:service_name/termination-policy", handlers.UpdateVpsTerminationPolicy(state))
			vc.POST("/:service_name/terminate", handlers.TerminateVps(state))
			vc.POST("/:service_name/confirm-termination", handlers.ConfirmVpsTermination(state))
			vc.GET("/:service_name/secondary-dns", handlers.GetVpsSecondaryDns(state))
			vc.POST("/:service_name/secondary-dns", handlers.AddVpsSecondaryDns(state))
			vc.DELETE("/:service_name/secondary-dns/:domain", handlers.DeleteVpsSecondaryDns(state))
			vc.GET("/:service_name/options", handlers.GetVpsOptions(state))
			vc.DELETE("/:service_name/options/:option", handlers.DeleteVpsOption(state))
			vc.GET("/:service_name/automated-backup", handlers.GetVpsAutomatedBackup(state))

			// 合同期(engagement)
			vc.GET("/:service_name/engagement", handlers.GetVpsEngagement(state))
			vc.GET("/:service_name/engagement/available", handlers.GetVpsEngagementAvailable(state))
			vc.GET("/:service_name/engagement/request", handlers.GetVpsEngagementRequest(state))
			vc.POST("/:service_name/engagement/request", handlers.CreateVpsEngagementRequest(state))
			vc.DELETE("/:service_name/engagement/request", handlers.DeleteVpsEngagementRequest(state))
			vc.PUT("/:service_name/engagement/end-rule", handlers.UpdateVpsEngagementEndRule(state))

			// DDoS mitigation(IP 级别,但 IP 列表从 /vps/{svc}/ips 取)
			vc.GET("/:service_name/mitigation", handlers.GetVpsMitigation(state))
			vc.POST("/:service_name/mitigation/:ip", handlers.EnableVpsMitigation(state))
			vc.DELETE("/:service_name/mitigation/:ip", handlers.DisableVpsMitigation(state))
		}

		// VPS monitor
		// 在售型号来自 OVH 实时目录 —— 写死过的那份已经整代停售了
		api.GET("/vps-monitor/models", handlers.GetVPSModels(state))
		api.GET("/vps-monitor/subscriptions", handlers.GetVPSSubscriptions(state))
		api.POST("/vps-monitor/subscriptions", handlers.AddVPSSubscription(state))
		api.PUT("/vps-monitor/subscriptions/:subscription_id", handlers.UpdateVPSSubscription(state))
		api.DELETE("/vps-monitor/subscriptions/clear", handlers.ClearVPSSubscriptions(state))
		api.DELETE("/vps-monitor/subscriptions/:subscription_id", handlers.RemoveVPSSubscription(state))
		api.GET("/vps-monitor/subscriptions/:subscription_id/history", handlers.GetVPSSubscriptionHistory(state))
		api.POST("/vps-monitor/start", handlers.StartVPSMonitor(state))
		api.POST("/vps-monitor/stop", handlers.StopVPSMonitor(state))
		api.GET("/vps-monitor/status", handlers.GetVPSMonitorStatus(state))
		api.PUT("/vps-monitor/interval", handlers.SetVPSMonitorInterval(state))
		api.POST("/vps-monitor/check/:plan_code", handlers.ManualCheckVPS(state))

		// Account
		api.GET("/ovh/account/info", handlers.GetAccountInfo(state))
		api.GET("/ovh/account/refunds", handlers.GetAccountRefunds(state))
		api.POST("/ovh/support/chat/session", handlers.OfficialChatSession(state))
		api.GET("/ovh/support/options", handlers.GetSupportOptions(state))
		api.GET("/ovh/support/services", handlers.GetSupportServices(state))
		api.GET("/ovh/support/tickets", handlers.ListSupportTickets(state))
		api.POST("/ovh/support/tickets", handlers.CreateSupportTicket(state))
		api.GET("/ovh/support/tickets/:ticket_id", handlers.GetSupportTicket(state))
		api.POST("/ovh/support/tickets/:ticket_id", handlers.UpdateSupportTicket(state))
		api.GET("/ovh/account/orders", handlers.GetAccountOrders(state))
		api.GET("/ovh/account/orders/:order_id/payment", handlers.GetOrderPaymentOptions(state))
		api.POST("/ovh/account/orders/:order_id/pay", handlers.PayAccountOrder(state))
		api.POST("/ovh/account/orders/:order_id/pay-balance", handlers.PayAccountOrderWithBalance(state))
		api.GET("/ovh/account/credit-balance", handlers.GetCreditBalance(state))
		api.GET("/ovh/account/topup", handlers.GetTopupInfo(state))
		api.POST("/ovh/account/topup", handlers.CreateTopupOrder(state))
		api.POST("/ovh/account/credit-code", handlers.RedeemCreditCode(state))
		api.GET("/ovh/account/default-payment-method", handlers.GetDefaultPaymentMethod(state))
		api.GET("/ovh/account/email-history", handlers.GetEmailHistory(state))
		api.GET("/ovh/contact-change-requests", handlers.GetContactChangeRequests(state))
		api.GET("/ovh/contact-change-requests/:task_id", handlers.GetContactChangeRequestDetail(state))
		api.POST("/ovh/contact-change-requests/:task_id/accept", handlers.AcceptContactChangeRequest(state))
		api.POST("/ovh/contact-change-requests/:task_id/refuse", handlers.RefuseContactChangeRequest(state))
		api.POST("/ovh/contact-change-requests/:task_id/resend-email", handlers.ResendContactChangeEmail(state))
		api.GET("/ovh/account/sub-accounts", handlers.GetSubAccounts(state))
		api.GET("/ovh/account/bills", handlers.GetAccountBills(state))

		// System health / DB maintenance / Backups（仅允许主密钥 X-API-Key，显式拒绝 App 配对设备令牌）
		sysOps := api.Group("/system")
		sysOps.Use(auth.RequireMasterKey())
		{
			sysOps.GET("/status", handlers.GetSystemStatus(state))
			sysOps.POST("/db/maintain", handlers.MaintainDatabase(state))
			sysOps.GET("/backups", handlers.ListSystemBackups(state))
			sysOps.POST("/backups", handlers.CreateSystemBackup(state))
			sysOps.POST("/backups/:id/download-token", handlers.IssueBackupDownloadToken(state))
			sysOps.POST("/backups/:id/download", handlers.DownloadSystemBackup(state))
		}
	}

	// 前端静态文件（仅 `-tags ui` 构建时生效）
	mountEmbeddedUI(r)

	// 1. 先做启动安全策略校验与端口监听，再安装信号处理器，最后才启动任何有副作用的后台抢购/监控/轮询协程！
	//    - 启用鉴权 + 默认密钥 123456 + 非回环监听地址（如 LISTEN_HOST=""）时直接拒绝启动，彻底消灭“公网默认密码接管 OVH 账户”窗口；
	//    - 若端口绑定重试耗尽失败，此时后台 worker 尚未启动、没有活跃写库协程，可直接安全关库并退出。
	host := os.Getenv("LISTEN_HOST")
	addr := host + ":" + state.Port
	if !enableAuth {
		console.Error("⚠️  API 密钥校验已关闭(ENABLE_API_KEY_AUTH=false):任何人都能调用全部接口,包括下单和重装。仅限本地调试")
	} else if err := validateStartupAuthPolicy(enableAuth, state.APIKey, host, isTrue(os.Getenv("OVH_ALLOW_DEFAULT_KEY_REMOTE"))); err != nil {
		console.Error(err.Error())
		state.Logger.Error(err.Error(), "system")
		state.Logger.Flush()
		exitProcess(1)
	} else if state.APIKey == "123456" {
		console.Warn("⚠️  当前仅监听本机回环地址并使用了默认 API 密钥 123456 —— 仍建议尽快在 .env 中设置随机 API_SECRET_KEY")
	}
	srv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}

	ln, err := listenWithRetry(addr, state)
	if err != nil {
		console.Error("启动失败:端口没能绑上", "addr", addr, "err", err)
		state.Logger.Error("启动失败,端口 "+addr+" 没能绑上: "+err.Error(), "system")
		state.Logger.Flush()
		exitProcess(1)
	}

	// 2. 在启动任何有副作用的后台抢购/监控/轮询协程之前，先注册停机信号处理器与优雅重启钩子！
	//    若此刻恰好收到 SIGTERM/SIGINT，协调器立刻切入 lifecycleShuttingDown，后续将跳过所有后台 worker 启动并走完整优雅收尾。
	gracefulRestart = func(exe string) {
		executeGracefulRestart(coord, srv, sqliteDB, state.Logger, console, exe, defaultRestartBudget, updater.Restart, updater.Spawn)
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	coord.attachSignalChannel(sigCh, defaultShutdownBudget)
	startSignalHandler(sigCh, coord, srv, sqliteDB, state.Logger, console, defaultShutdownBudget)

	// 3. 端口与信号通道双双就绪后，通过 coord.beginStartupPhase() / finishStartupPhase() 与停机排空严格同步：
	//    - 若停机已开始，beginStartupPhase() 返回 false，直接跳过所有后台启动；
	//    - 一旦进入启动阶段，所有后台协程/poller/monitor/vps 的派发均在启动屏障与 coord.runIfRunning 保护下执行，
	//      且 gracefulShutdownWithCoordinator 必须等待 waitStartupPhaseDone 后才执行 stopAllBackgroundLoops，
	//      彻底杜绝“停机 Stop 已执行完，主协程才刚调用 StartPollerIfEnabled / mon.Start / vps.Start”的乱序竞态！
	proxyguard.Init(state)
	proxyguard.SetDisableAutoOrder(mon.DisableAutoOrderForAccount)
	proxyguard.SetReload(func() {
		mon.LoadFromDB()
	})
	state.OVH.SetProxyErrorHook(proxyguard.Report)
	state.SetProxyErrorHook(proxyguard.Report)
	applySharedProxy(state)
	state.OVH.SetLogf(func(format string, args ...interface{}) {
		state.Logger.Warn(fmt.Sprintf(format, args...), "proxy")
	})

	if !coord.beginStartupPhase() {
		_ = ln.Close()
		if code := waitMainLifecycleExit(coord); code != 0 {
			os.Exit(code)
		}
		return
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	queueLoopDone := make(chan struct{})
	orderStatusDone := make(chan struct{})
	warmRegionDone := make(chan struct{})
	cleanupPairingDone := make(chan struct{})
	updateCheckDone := make(chan struct{})
	dbMaintDone := make(chan struct{})
	autoBackupDone := make(chan struct{})
	pollerBootDone := make(chan struct{})
	tgCmdBootDone := make(chan struct{})
	registerBackgroundLoops(bgCancel, queueLoopDone, orderStatusDone, warmRegionDone, cleanupPairingDone, updateCheckDone, dbMaintDone, autoBackupDone, pollerBootDone, tgCmdBootDone)

	// 预注册可选后台循环（仅在尚未存在时补建 stopped 记录，绝不覆盖已在运行的循环，且持锁读取 VPSCheckInterval）
	if state.Heartbeats != nil {
		state.VPSSubsMu.Lock()
		vpsIv := state.VPSCheckInterval
		state.VPSSubsMu.Unlock()
		state.Heartbeats.RegisterIfAbsent("server_monitor", mon.CheckInterval(), false)
		state.Heartbeats.RegisterIfAbsent("vps_monitor", vpsIv, false)
		state.Heartbeats.RegisterIfAbsent("telegram_poller", telegram.PollTimeoutSeconds+20, false)
	}

	go func() {
		defer close(queueLoopDone)
		if coord.isShutdownRequested() {
			return
		}
		purchase.ProcessQueueLoopWithContext(bgCtx, state)
	}()
	go func() {
		defer close(orderStatusDone)
		if coord.isShutdownRequested() {
			return
		}
		purchase.OrderStatusLoopWithContext(bgCtx, state)
	}()
	go func() {
		defer close(warmRegionDone)
		if coord.isShutdownRequested() {
			return
		}
		catalog.WarmRegionCacheWithContext(bgCtx, state)
	}()
	go func() {
		defer close(cleanupPairingDone)
		if coord.isShutdownRequested() {
			return
		}
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-bgCtx.Done():
				return
			case <-ticker.C:
				state.DB.CleanupPairingCodesContext(bgCtx)
			}
		}
	}()
	go func() {
		defer close(updateCheckDone)
		if coord.isShutdownRequested() {
			return
		}
		handlers.StartUpdateCheckLoopWithContext(bgCtx, state)
	}()
	go func() {
		defer close(dbMaintDone)
		if coord.isShutdownRequested() {
			return
		}
		handlers.StartDBMaintenanceLoopWithContext(bgCtx, state)
	}()
	go func() {
		defer close(autoBackupDone)
		if coord.isShutdownRequested() {
			return
		}
		handlers.StartAutoBackupLoopWithContext(bgCtx, state)
	}()

	handlers.InitPoller(state, mon)
	if !coord.runIfRunning(func() {
		go func() {
			defer close(pollerBootDone)
			if coord.isShutdownRequested() {
				return
			}
			// 锁外完成 deleteWebhook 网络准备，发布运行态并拉起轮询循环的瞬间在 coord.runIfRunning 锁内与 beginShutdown 严格互斥：
			// 若停机已在准备期间接管，admit 返回 false，绝不进入运行态也绝不启动轮询循环！
			handlers.StartPollerIfEnabledWithAdmit(state, coord.runIfRunning)
		}()
		go func() {
			defer close(tgCmdBootDone)
			if coord.isShutdownRequested() {
				return
			}
			telegram.RegisterCommands(state)
		}()
	}) {
		close(pollerBootDone)
		close(tgCmdBootDone)
	}

	coord.runIfRunning(func() {
		if len(mon.Snapshot()) > 0 && mon.AutoStartAllowed() {
			mon.Start()
			state.Logger.Info("自动启动服务器监控", "system")
		}
	})
	coord.runIfRunning(func() {
		if len(state.VPSSubscriptions) > 0 && vps.AutoStartAllowed(state) {
			if vps.Start(state) {
				state.Logger.Info("自动启动 VPS 监控", "system")
			}
		}
	})

	// 解除启动屏障：自此之后绝不会再有新的后台组件启动，停机排空即可安全执行 stopAllBackgroundLoops
	coord.finishStartupPhase()

	if coord.isShutdownRequested() {
		bgCancel()
		_ = ln.Close()
		if code := waitMainLifecycleExit(coord); code != 0 {
			os.Exit(code)
		}
		return
	}

	state.Logger.Info("Server started", "system")
	console.Info("Listening", "addr", addr, "auth", enableAuth, "ui", hasUI(), "dataDir", paths.DataDir)
	state.Logger.Info("已监听 "+addr+",开始对外服务", "system")
	updater.MarkHealthy(state)

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		// 运行期 Serve 异常时后台 worker 已在运行：绝不直接关库，必须经协调器走统一有界排空流程再退出！
		console.Error("server run", "err", err)
		if won, deadline := coord.beginShutdown("serve_error", defaultShutdownBudget); won {
			gracefulShutdownWithCoordinator(coord, srv, sqliteDB, state.Logger, console, "serve_error", deadline)
		}
		_ = waitMainLifecycleExit(coord)
		os.Exit(1)
	}

	// main 是进程唯一退出码所有者：经可测收口函数 waitMainLifecycleExit 等待统一 coord.done 并动态跟随 deadline 更新；
	// 注意：若退出码非 0（如活跃写库超时跳过关库），直接 os.Exit(code) 不跑外层 defer closeDB，保留“活跃写库超时不并发关库”的安全语义！
	if code := waitMainLifecycleExit(coord); code != 0 {
		os.Exit(code)
	}
}

// waitMainLifecycleExitBeforeSelectHook 仅供单测在构造旧 timer 后、进入 select 前插入屏障，
// 确定性复现“旧 timer.C 与新 deadlineChanged 同时就绪”的竞态窗口（生产环境为 nil）。
var waitMainLifecycleExitBeforeSelectHook atomic.Pointer[func(observedGen uint64)]

// waitMainLifecycleExitOnTimerBeforeRecheckHook 仅供单测在 select 明确选中 timer.C 之后、调用锁内二次复核之前插入屏障，
// 确保如果删掉锁内复核，测试必然失败（杜绝假绿）。
var waitMainLifecycleExitOnTimerBeforeRecheckHook atomic.Pointer[func(observedGen uint64)]

// waitMainLifecycleExit 是 main 在 srv.Serve 返回后的统一收尾与兜底等待逻辑（独立抽离便于单测直接覆盖）。
// 动态监听 coord.deadlineUpdated 并带 deadlineGen 锁内二次复核：
// 即使旧 timer.C 与新 deadlineChanged 同时就绪且 Go select 选中旧 timer.C，锁内复核发现代次已变或未超新截止点也会重新等待，绝不提前返回 1！
func waitMainLifecycleExit(coord *shutdownCoordinator) int {
	const grace = 2 * time.Second
	for {
		mode, done, deadline, observedGen, deadlineChanged, _ := coord.stateWithDeadlineSignal()
		if mode != lifecycleShuttingDown && mode != lifecycleRestarting && mode != lifecycleRestartCommitted && mode != lifecycleRestartAborted {
			return 0
		}
		waitLimit := time.Until(deadline.Add(grace))
		if waitLimit < 50*time.Millisecond {
			waitLimit = 50 * time.Millisecond
		}
		timer := time.NewTimer(waitLimit)
		if hook := waitMainLifecycleExitBeforeSelectHook.Load(); hook != nil && *hook != nil {
			(*hook)(observedGen)
		}
		select {
		case <-done:
			timer.Stop()
			_, _, _, exitCode, _ := coord.state()
			return exitCode
		case <-deadlineChanged:
			timer.Stop()
		case <-timer.C:
			if hook := waitMainLifecycleExitOnTimerBeforeRecheckHook.Load(); hook != nil && *hook != nil {
				(*hook)(observedGen)
			}
			// 锁内复核：防止旧 timer 与新 deadlineChanged 同时就绪时 Go 随机选到旧 timer.C 误杀有效 SIGTERM 收尾
			expired, finished, code := coord.isDeadlineStillCurrentAndExpired(observedGen, grace)
			if finished {
				return code
			}
			if expired {
				return 1
			}
			// 代次已推进或未到期：继续下一轮按新 deadline 等待
		}
	}
}

// startSignalHandler 是 main 的信号接收与退出分发入口（独立抽离便于单测端到端验证信号入口 + 兜底退出）。
// 关键并发不变量：唯一持有 sigCh 读取权的单协程同时充当重启最终提交的同步裁决器（commitReqCh）。
// 信号被此协程取出、登记进 coord 与响应重启最终提交请求严格串行执行，数学上彻底消灭“信号已被取走但尚未登记，重启却同时提交”的竞态窗口。
func startSignalHandler(
	sigCh <-chan os.Signal,
	coord *shutdownCoordinator,
	srv *http.Server,
	sqlDB io.Closer,
	lg *logger.Logger,
	console *slog.Logger,
	budget time.Duration,
) <-chan struct{} {
	type arbiterReply struct {
		transitioned bool
		sig          string
		deadline     time.Time
	}
	type arbiterReq struct {
		target lifecycleMode
		reply  chan arbiterReply
	}
	coord.setShutdownBudget(budget)
	arbiterReqCh := make(chan arbiterReq)
	exited := make(chan struct{})
	coord.setRestartArbiter(func(target lifecycleMode) (bool, string, time.Time) {
		replyCh := make(chan arbiterReply, 1)
		select {
		case arbiterReqCh <- arbiterReq{target: target, reply: replyCh}:
			rep := <-replyCh
			return rep.transitioned, rep.sig, rep.deadline
		case <-exited:
			return coord.transitionRestartTerminalDirect(target)
		case <-coord.done:
			return false, "", time.Time{}
		}
	})

	go func() {
		defer close(exited)
		defer coord.clearRestartArbiter()
		for {
			select {
			case sig, ok := <-sigCh:
				if !ok {
					return
				}
				sigName := sig.String()
				won, deadline := coord.beginShutdown(sigName, budget)
				if !won {
					continue
				}
				logCtx, cancel := context.WithDeadline(context.Background(), drainDeadlineFrom(deadline))
				logLifecycleBounded(logCtx, func() {
					if console != nil {
						console.Info("shutdown", "signal", sigName)
					}
				})
				cancel()
				gracefulShutdownWithCoordinator(coord, srv, sqlDB, lg, console, sigName, deadline)
				return
			case req := <-arbiterReqCh:
				// 单协程严格串行裁决（同时覆盖最终重启提交 lifecycleRestartCommitted 与 HTTP 超时中止 lifecycleRestartAborted）：
				// 先优先非阻塞排空 sigCh（即便 sig 与 req 同刻就绪，也 100% 优先信号），
				// 绝不存在“信号已被另一协程读出但尚未调用 beginShutdown”的并发窗口！
				select {
				case sig, ok := <-sigCh:
					if ok && sig != nil {
						_, _ = coord.beginShutdown(sig.String(), budget)
					}
				default:
				}
				transitioned, sigName, dl := coord.transitionRestartTerminalDirect(req.target)
				req.reply <- arbiterReply{transitioned: transitioned, sig: sigName, deadline: dl}
				if transitioned {
					return
				}
			}
		}
	}()
	return exited
}

// drainDeadlineFrom 依据统一截止时间推导排空阶段截止点，为日志刷盘与 SQLite 关闭预留缓冲。
func drainDeadlineFrom(deadline time.Time) time.Time {
	remaining := time.Until(deadline)
	reserve := persistReserveBudget
	if remaining <= reserve+500*time.Millisecond {
		reserve = remaining / 5
	}
	return deadline.Add(-reserve)
}

// logLifecycleBounded 在有界上下文内输出生命周期日志与控制台日志；
// 单次日志输出再加 50ms 硬顶，防止阻塞的 stdout/stderr 管道吃掉整段排空或信号交接预算。
func logLifecycleBounded(ctx context.Context, fn func()) {
	if fn == nil {
		return
	}
	logCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, _ = runBounded(logCtx, func() error {
		fn()
		return nil
	})
}

// auxStopHooks 仅供单测注入和观测各独立辅助停止项的并发扇出行为（生产环境全部走真实组件函数）。
type auxStopFns struct {
	poller  func()
	monitor func()
	vps     func(*app.State)
	prewarm func()
}

var auxStopOverride atomic.Pointer[auxStopFns]

// auxStopHook 保留给现有测试观测辅助停止触发。
var auxStopHook atomic.Pointer[func()]

func stopAllBackgroundLoops(ctx context.Context, lg *logger.Logger) bool {
	// 先停后台循环再关资源:下单/监控的副作用(OVH 侧订单/通知)先于持久化,
	// 中途被杀 → 重启后重放(重复下单/重复通知)。停循环让它们走到下一个
	// 安全点(状态已落库)再退出;HTTP 收尾和 DB 关闭紧随其后。
	// 重要：已注册循环与每个同步辅助组件（TG 轮询 / 服务器监控 / VPS 监控 / 预热池）均独立并发派发停止请求，
	// 并在同一 ctx 下统一等待，彻底避免任一组件阻塞卡住导致其他组件收不到停止请求！
	ov := auxStopOverride.Load()
	st := appStateForShutdown.Load()

	pollerFn := func() { handlers.StopPoller() }
	monitorFn := func() {
		if monitorState != nil {
			monitorState.Stop()
		}
	}
	vpsFn := func() { vps.Stop(st) }
	prewarmFn := func() {
		if st != nil && st.OrderPrewarm != nil {
			st.OrderPrewarm.Close()
		}
	}
	if ov != nil {
		if ov.poller != nil {
			pollerFn = ov.poller
		}
		if ov.monitor != nil {
			monitorFn = ov.monitor
		}
		if ov.vps != nil {
			vpsFn = func() { ov.vps(st) }
		}
		if ov.prewarm != nil {
			prewarmFn = ov.prewarm
		}
	}

	tasks := []func() bool{
		func() bool { return stopRegisteredBackgroundLoops(ctx, lg) },
		func() bool { ok, _ := runBounded(ctx, func() error { pollerFn(); return nil }); return ok },
		func() bool { ok, _ := runBounded(ctx, func() error { monitorFn(); return nil }); return ok },
		func() bool { ok, _ := runBounded(ctx, func() error { vpsFn(); return nil }); return ok },
		func() bool { ok, _ := runBounded(ctx, func() error { prewarmFn(); return nil }); return ok },
	}
	if hook := auxStopHook.Load(); hook != nil && *hook != nil {
		h := *hook
		tasks = append(tasks, func() bool { ok, _ := runBounded(ctx, func() error { h(); return nil }); return ok })
	}

	var (
		wg       sync.WaitGroup
		allClean atomic.Bool
	)
	allClean.Store(true)
	wg.Add(len(tasks))
	for _, fn := range tasks {
		fn := fn
		go func() {
			defer wg.Done()
			if !fn() {
				allClean.Store(false)
			}
		}()
	}
	wg.Wait()

	clean := allClean.Load()
	if lg != nil {
		logLifecycleBounded(ctx, func() {
			if clean {
				lg.Info("后台循环已停止(抢购队列/订单状态/区域预热/更新检查/TG 轮询/服务器监控/VPS 监控)", "system")
			} else {
				lg.Warn("优雅退出:部分后台循环或监听未在期限内完全结束", "system")
			}
		})
	}
	return clean
}

// gracefulShutdown 保留单测兼容入口，委托给统一协调器与统一截止时间实现。
func gracefulShutdown(srv *http.Server, sqlDB io.Closer, lg *logger.Logger, console *slog.Logger, reason string, wait time.Duration) {
	coord := newShutdownCoordinator()
	_, deadline := coord.beginShutdown(reason, wait)
	gracefulShutdownWithCoordinator(coord, srv, sqlDB, lg, console, reason, deadline)
}

func gracefulShutdownWithCoordinator(
	coord *shutdownCoordinator,
	srv *http.Server,
	sqlDB io.Closer,
	lg *logger.Logger,
	console *slog.Logger,
	reason string,
	deadline time.Time,
) {
	exitCode := 0
	defer func() { coord.finish(exitCode) }()

	// 从收到信号起共享统一绝对截止时间：后台循环、在途写请求与 HTTP 并发排空，预留缓冲给刷盘与关库。
	drainCtx, drainCancel := context.WithDeadline(context.Background(), drainDeadlineFrom(deadline))
	defer drainCancel()

	if lg != nil {
		logLifecycleBounded(drainCtx, func() {
			lg.Info("收到 "+reason+",正在优雅退出", "system")
		})
	}

	// 阶段 0：若主协程正处于后台组件启动阶段（beginStartupPhase .. finishStartupPhase），先等待其完成启动登记再统一执行 Stop，
	// 杜绝“Stop 先跑完、主协程随后又调用 mon.Start / vps.Start / StartPollerIfEnabled”的乱序启动窗口！
	startupClean := coord.waitStartupPhaseDone(drainCtx)

	// 阶段 1：并发排空在途写请求 + HTTP 服务 + 第一轮后台循环停止（尽早取消在途轮询/抢购任务）。
	// 关键正确性保护：已通过写门禁的在途写请求（如 POST /monitor/start、/vps-monitor/start）
	// 仍可能在第一轮 stopAllBackgroundLoops 返回后才刚刚拉起监控，因此在所有写请求彻底排空后，
	// 必须再次兜底执行一次 stopAllBackgroundLoops 确保关库瞬间无任何刚被重启的后台组件仍在写库！
	var (
		wg          sync.WaitGroup
		bgClean     bool
		writesClean bool
		httpErr     error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		bgClean = startupClean && stopAllBackgroundLoops(drainCtx, lg)
	}()
	go func() {
		defer wg.Done()
		writesClean = coord.waitWritesDrained(drainCtx)
	}()
	if srv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			httpErr = srv.Shutdown(drainCtx)
		}()
	}
	wg.Wait()
	// 写请求已全部排空且新写已被 writeGateMiddleware 永久拒绝后，做最终一次后台停止收口，确保无任何被在途写请求重新拉起的组件
	if writesClean && bgClean {
		bgClean = stopAllBackgroundLoops(drainCtx, lg)
	}

	// 收尾持久化、日志输出与关库全部受剩余统一 deadline 约束，确保阻塞写入器、阻塞控制台或慢 Close 不会无限挂死进程。
	finalCtx, finalCancel := context.WithDeadline(context.Background(), deadline)
	defer finalCancel()

	if httpErr != nil {
		logLifecycleBounded(finalCtx, func() {
			if console != nil {
				console.Warn("shutdown", "err", httpErr)
			}
			if lg != nil {
				lg.Warn("优雅退出:仍有请求未在期限内收尾 - "+httpErr.Error(), "system")
			}
		})
	}
	if !writesClean && console != nil {
		logLifecycleBounded(finalCtx, func() {
			console.Warn("shutdown", "err", "仍有在途写请求未在排空截止前结束")
		})
	}
	if !bgClean && console != nil {
		logLifecycleBounded(finalCtx, func() {
			console.Warn("shutdown", "err", "仍有后台循环未在排空截止前结束")
		})
	}

	drainClean := bgClean && writesClean && httpErr == nil
	if !drainClean {
		// 排空未完全收尾时，在途结账或 HTTP handler 仍可能持有/写入数据库，
		// 此时跳过显式 sqlDB.Close 以避免在活跃操作下破坏 DB 句柄，并明确以非零码在预算内退出。
		exitCode = 1
		if lg != nil {
			logLifecycleBounded(finalCtx, func() {
				lg.Error("优雅退出:排空超时，跳过显式关闭数据库并以非零码退出", "system")
			})
			if ok, err := runBounded(finalCtx, func() error { return lg.CloseWithErr() }); (!ok || err != nil) && console != nil {
				logLifecycleBounded(finalCtx, func() {
					console.Error("close logger on dirty shutdown", "completed", ok, "err", err)
				})
			}
		}
		return
	}

	// 统一在最终收尾窗口内提前切分预算：
	// - 为日志落盘/超时错误记录预留 logReserve（deadline 之前），
	// - 前置 Flush、补救 SaveQueue 均严格继承 finalCtx 且绝不晚于 workDeadline = deadline - logReserve，
	//   前置 Flush 与其超时告警均严格限制在 workDeadline 前半段，无论预算长短绝不挤占补救 SaveQueue 的执行窗口。
	rem := time.Until(deadline)
	logReserve := 150 * time.Millisecond
	if rem/4 < logReserve {
		logReserve = rem / 4
	}
	if logReserve < 30*time.Millisecond && rem > 60*time.Millisecond {
		logReserve = 30 * time.Millisecond
	}
	workDeadline := deadline.Add(-logReserve)
	if !workDeadline.After(time.Now()) {
		workDeadline = deadline
	}

	preFlushTimedOut := false
	if lg != nil {
		// 只要还有剩余工作窗口，就至多把一半交给前置 Flush，严格为后续补救 SaveQueue 留出另一半预算（短预算同样按比例切分）
		flushDeadline := workDeadline
		if remWork := time.Until(workDeadline); remWork > 0 {
			flushDeadline = time.Now().Add(remWork / 2)
		}
		flushCtx, flushCancel := context.WithDeadline(finalCtx, flushDeadline)
		logLifecycleBounded(flushCtx, func() {
			lg.Info("优雅退出:请求已收尾,正在关闭数据库", "system")
		})
		if ok, _ := runBounded(flushCtx, func() error { lg.Flush(); return nil }); !ok {
			preFlushTimedOut = true
		}
		flushCancel()
	}

	// 排空完成后、关库前：若仍有未成功持久化的关键队列状态（例如 worker 三次重试均失败），
	// 在继承自 finalCtx 且不晚于 workDeadline 的子上下文内执行一次补救保存：
	// - 绝对不会突破总截止时间 deadline（父 ctx 为 finalCtx）；
	// - 若补救保存超时未返回（!saveOK），保存协程仍可能在执行 ReplaceQueue，此时严禁并发关库，
	//   直接跳过 sqlDB.Close、用尚在预算内的 finalCtx 写出明确 ERROR 日志，并以非零码退出。
	saveInFlightTimedOut := false
	if st := appStateForShutdown.Load(); st != nil && st.HasPendingCriticalQueueSave() {
		saveCtx, saveCancel := context.WithDeadline(finalCtx, workDeadline)
		saveOK, saveErr := runBounded(saveCtx, func() error {
			return st.SaveQueue()
		})
		saveCancel()
		if !saveOK {
			saveInFlightTimedOut = true
			exitCode = 1
			logLifecycleBounded(finalCtx, func() {
				if console != nil {
					console.Error("shutdown critical queue save timed out; skipping DB close", "err", saveErr)
				}
				if lg != nil {
					lg.Error("优雅退出:关键订单队列补救保存超时，为避免并发破坏活跃写库跳过显式关闭数据库并以非零码退出", "system")
				}
			})
		} else if saveErr != nil || st.HasPendingCriticalQueueSave() {
			exitCode = 1
			logLifecycleBounded(finalCtx, func() {
				if console != nil {
					console.Error("shutdown critical queue save failed", "err", saveErr)
				}
				if lg != nil {
					lg.Error(fmt.Sprintf("优雅退出:关键订单队列状态仍有未落库代次(err=%v)，将以非零码退出", saveErr), "system")
				}
			})
		}
	}
	if preFlushTimedOut && console != nil {
		// 前置 Flush 的告警放到补救 SaveQueue 之后再用 finalCtx 记录，绝不让慢控制台挤占补救保存窗口
		logLifecycleBounded(finalCtx, func() {
			console.Warn("shutdown", "err", "关库前日志刷盘超过子预算")
		})
	}
	if sqlDB != nil && !saveInFlightTimedOut {
		ok, err := runBounded(finalCtx, func() error {
			_, cerr := coord.closeDB(sqlDB)
			return cerr
		})
		if !ok || err != nil {
			exitCode = 1
			logLifecycleBounded(finalCtx, func() {
				if console != nil {
					console.Error("close sqlite failed or timed out", "completed", ok, "err", err)
				}
				if lg != nil && err != nil {
					lg.Error("关闭数据库失败: "+err.Error(), "system")
				}
			})
		}
	}
	if lg != nil {
		// finalCtx 已耗尽时（如 sqlDB.Close 刚刚超期），无需再启动必然立刻超时的 CloseWithErr 孤儿协程
		if finalCtx.Err() != nil {
			exitCode = 1
		} else {
			ok, err := runBounded(finalCtx, func() error { return lg.CloseWithErr() })
			if !ok || err != nil {
				exitCode = 1
				if console != nil {
					logLifecycleBounded(finalCtx, func() {
						console.Error("close logger failed or timed out", "completed", ok, "err", err)
					})
				}
			}
		}
	}
}

// hasPendingCriticalQueueSave 检查全局运行态是否存在尚未被 SaveQueue 覆盖的关键队列状态。
func hasPendingCriticalQueueSave() bool {
	st := appStateForShutdown.Load()
	return st != nil && st.HasPendingCriticalQueueSave()
}

// restartHTTPTimeoutBranchHook 仅供单测精确验证本次调用确实走入了 httpErr != nil 后的原子裁决分支（生产环境为 nil）。
var restartHTTPTimeoutBranchHook atomic.Pointer[func(handedOffToSigterm bool)]

// executeGracefulRestart 执行受控自更新重启，返回 (exitCode, completedRestart)。
//  1. 进入 lifecycleRestarting 立即经 writeGateMiddleware 拒绝一切新写操作；
//  2. 并发等待后台循环 + 在途写请求全部干净收尾，并检查是否存在任何未落库的关键队列代次（含触发重启前遗留的失败）；
//  3. 所有生命周期日志与控制台输出均纳入有界截止时间，绝不因阻塞日志或 stdout 卡死状态机或截断延迟 SIGTERM 交接；
//  4. 若任一排空超时或存在未落库关键代次，先完成维护态/延迟 SIGTERM 原子状态切换，绝不关库、绝不替换进程；
//  5. 全部干净后，等待 srv.Shutdown 无错完成、在剩余预算内关库并落盘日志，全部成功后才调用 exec/spawn。
func executeGracefulRestart(
	coord *shutdownCoordinator,
	srv *http.Server,
	sqlDB io.Closer,
	lg *logger.Logger,
	console *slog.Logger,
	exe string,
	budget time.Duration,
	restartFn func(string) error,
	spawnFn func(string) error,
) (exitCode int, completedRestart bool) {
	won, deadline := coord.beginRestart(budget)
	if !won {
		if lg != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			logLifecycleBounded(ctx, func() {
				lg.Warn("[更新] 已有退出、维护或重启流程进行中，忽略本次自更新重启", "version")
			})
			cancel()
		}
		return 0, false
	}

	bgDeadline := deadline.Add(-8 * time.Second)
	if !bgDeadline.After(time.Now()) {
		bgDeadline = drainDeadlineFrom(deadline)
	}
	bgCtx, bgCancel := context.WithDeadline(context.Background(), bgDeadline)

	if lg != nil {
		logLifecycleBounded(bgCtx, func() {
			lg.Info("[更新] 正在优雅关闭以完成重启（已停止接纳新写操作）", "version")
		})
	}

	var (
		wg          sync.WaitGroup
		bgClean     bool
		writesClean bool
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		bgClean = stopAllBackgroundLoops(bgCtx, lg)
	}()
	go func() {
		defer wg.Done()
		writesClean = coord.waitWritesDrained(bgCtx)
	}()
	wg.Wait()
	// 所有在途写请求排空且新写已被拒绝后，再次执行一次后台停止收口，彻底堵住“已放行写请求在首轮停后台之后重新 Start 监控”的窗口
	if writesClean && bgClean {
		bgClean = stopAllBackgroundLoops(bgCtx, lg)
	}
	bgCancel()

	flushBounded := func(ctx context.Context) (bool, error) {
		if lg == nil {
			return true, nil
		}
		return runBounded(ctx, func() error { lg.Flush(); return nil })
	}

	abortToMaintenance := func(reason string) {
		// 先完成可观测状态与维护态/延迟信号交接，绝不在状态迁移前同步调用可能阻塞的日志/控制台 I/O
		handlers.SetUpdateFailed(reason)
		deferredSig, sigDeadline := coord.enterMaintenance(reason)
		if deferredSig != "" {
			sigCtx, sigCancel := context.WithDeadline(context.Background(), sigDeadline)
			logLifecycleBounded(sigCtx, func() {
				if lg != nil {
					lg.Error("[更新] "+reason, "version")
				}
				if console != nil {
					console.Error("restart aborted; handing off deferred signal", "signal", deferredSig, "reason", reason)
				}
			})
			sigCancel()
			gracefulShutdownWithCoordinator(coord, srv, sqlDB, lg, console, deferredSig, sigDeadline)
			return
		}
		flushCtx, flushCancel := context.WithDeadline(context.Background(), deadline)
		logLifecycleBounded(flushCtx, func() {
			if lg != nil {
				lg.Error("[更新] "+reason, "version")
			}
			if console != nil {
				console.Error("restart aborted; entered maintenance mode", "reason", reason)
			}
		})
		_, _ = flushBounded(flushCtx)
		flushCancel()
	}

	// 排空超时分支优先直接交接维护态/延迟 SIGTERM，不额外触碰可能被占用的 QueueMu；
	// 仅在排空已干净完成时，再检查是否存在尚未持久化的关键队列代次。
	if !bgClean || !writesClean {
		abortToMaintenance("后台任务或在途写请求未在期限内安全收尾，已中止自动重启并进入维护模式（已拒绝新写操作，不关闭数据库）；请核对订单状态后手动重启")
		return 0, false
	}
	if hasPendingCriticalQueueSave() {
		abortToMaintenance("检测到存在未成功持久化的关键订单队列状态，已中止自动重启并进入维护模式；请核对 OVH 订单后手动重启")
		return 0, false
	}

	// 进入不可逆关 HTTP / 关库 / 替换进程映像前，若等待期间已登记 SIGTERM/SIGINT，
	// 优先遵循运维停机信号（避免容器/systemd 正在停止服务时被自更新 exec 强行拉起新进程）。
	if deferredSig, sigDeadline := coord.takeDeferredShutdown(); deferredSig != "" {
		handlers.SetUpdateFailed("等待重启排空期间收到停机信号(" + deferredSig + ")，已取消替换进程并转入优雅停机")
		sigCtx, sigCancel := context.WithDeadline(context.Background(), sigDeadline)
		logLifecycleBounded(sigCtx, func() {
			if lg != nil {
				lg.Info("[更新] 重启排空期间收到停机信号("+deferredSig+")，放弃替换进程映像并执行优雅退出", "version")
			}
			if console != nil {
				console.Info("restart superseded by deferred shutdown signal", "signal", deferredSig)
			}
		})
		sigCancel()
		gracefulShutdownWithCoordinator(coord, srv, sqlDB, lg, console, deferredSig, sigDeadline)
		_, _, _, code, _ := coord.state()
		return code, false
	}

	httpCtx, httpCancel := context.WithDeadline(context.Background(), drainDeadlineFrom(deadline))
	var httpErr error
	if srv != nil {
		httpErr = srv.Shutdown(httpCtx)
	}
	httpCancel()
	finalCtx, finalCancel := context.WithDeadline(context.Background(), deadline)
	defer finalCancel()

	if httpErr != nil {
		// 锁内原子裁决：“接管延迟 SIGTERM/SIGINT” 或 “锁定终态中止 lifecycleRestartAborted” 二选一，
		// 彻底封死检查延迟信号之后、日志/finish(1) 之前登记信号被吞掉的竞态窗口！
		_, deferredSig, sigDeadline := coord.abortRestartOrTakeDeferredShutdown()
		if deferredSig != "" {
			if h := restartHTTPTimeoutBranchHook.Load(); h != nil && *h != nil {
				(*h)(true)
			}
			handlers.SetUpdateFailed("HTTP 排空期间收到停机信号(" + deferredSig + ")，已转入优雅停机并继续排空在途请求")
			gracefulShutdownWithCoordinator(coord, srv, sqlDB, lg, console, deferredSig, sigDeadline)
			_, _, _, code, _ := coord.state()
			return code, false
		}
		if h := restartHTTPTimeoutBranchHook.Load(); h != nil && *h != nil {
			(*h)(false)
		}
		// HTTP 排空超时且已锁定终态中止（后续不再接受延迟信号登记）：仍有请求在执行，绝不关库或替换进程映像
		reason := "HTTP 在途请求未在期限内排空(" + httpErr.Error() + ")，已中止自动重启且未关闭数据库，请核对后手动重启"
		handlers.SetUpdateFailed(reason)
		logLifecycleBounded(finalCtx, func() {
			if lg != nil {
				lg.Error("[更新] "+reason, "version")
			}
			if console != nil {
				console.Error("restart aborted: http shutdown timed out", "err", httpErr)
			}
		})
		_, _ = flushBounded(finalCtx)
		coord.finish(1)
		return 1, false
	}

	// 再次在关库/替换进程前检查一次延迟停机信号（覆盖 srv.Shutdown 期间到达的 SIGTERM）
	if deferredSig, sigDeadline := coord.takeDeferredShutdown(); deferredSig != "" {
		handlers.SetUpdateFailed("HTTP 排空期间收到停机信号(" + deferredSig + ")，已取消替换进程并转入优雅停机")
		gracefulShutdownWithCoordinator(coord, nil, sqlDB, lg, console, deferredSig, sigDeadline)
		_, _, _, code, _ := coord.state()
		return code, false
	}

	if sqlDB != nil {
		ok, err := runBounded(finalCtx, func() error {
			_, cerr := coord.closeDB(sqlDB)
			return cerr
		})
		if !ok || err != nil {
			// 关库失败或超时前，先做终态裁决：锁死后续延迟信号登记并消费窗口内可能已登记的 SIGTERM，
			// 同时保留原始失败退出码 1 且绝不重复关闭仍在关/已失败的数据库句柄！
			_, deferredSig, _ := coord.abortRestartOrTakeDeferredShutdown()
			reason := fmt.Sprintf("关闭数据库失败或超时(completed=%v err=%v)，已中止替换进程映像", ok, err)
			if deferredSig != "" {
				reason += "；期间收到停机信号(" + deferredSig + ")，已跳过重复关库并以非零码结束"
			}
			handlers.SetUpdateFailed(reason)
			logLifecycleBounded(finalCtx, func() {
				if lg != nil {
					lg.Error("[更新] "+reason, "version")
				}
				if console != nil {
					console.Error("restart aborted: close sqlite failed", "completed", ok, "err", err, "deferredSignal", deferredSig)
				}
			})
			if lg != nil && finalCtx.Err() == nil {
				_, _ = runBounded(finalCtx, func() error { return lg.CloseWithErr() })
			}
			coord.finish(1)
			return 1, false
		}
	}

	if lg != nil {
		logLifecycleBounded(finalCtx, func() {
			lg.Info("[更新] 准备用新二进制替换进程映像: "+exe, "version")
		})
		ok, err := runBounded(finalCtx, func() error { return lg.CloseWithErr() })
		if !ok || err != nil {
			// 关日志失败或超时前，先做终态裁决：消费窗口内可能已登记的 SIGTERM 并锁死后续登记，
			// 保留原始失败退出码 1 且绝不重复关闭已关的 DB 或仍在关的 logger！
			_, deferredSig, _ := coord.abortRestartOrTakeDeferredShutdown()
			reason := fmt.Sprintf("最终日志落盘失败或超过统一截止时间(completed=%v err=%v)，已中止替换进程映像", ok, err)
			if deferredSig != "" {
				reason += "；期间收到停机信号(" + deferredSig + ")，已跳过重复收尾并以非零码结束"
			}
			handlers.SetUpdateFailed(reason)
			if console != nil {
				logLifecycleBounded(finalCtx, func() {
					console.Error("restart aborted: logger close failed or timed out", "completed", ok, "err", err, "deferredSignal", deferredSig)
				})
			}
			coord.finish(1)
			return 1, false
		}
	}

	// 最终原子提交点：与 beginShutdown 共用同一把锁检查关库/关日志窗口内是否登记了 SIGTERM/SIGINT。
	// 若已登记，绝不执行 restartFn/spawnFn，直接在 DB/日志均已关闭的状态下完成收口并返回退出码 0；
	// 否则原子切换为 lifecycleRestartCommitted，封死一切后续延迟信号窗口后立即执行进程替换。
	committed, deferredSig, _ := coord.commitRestartOrTakeDeferredShutdown()
	if !committed {
		if deferredSig != "" {
			handlers.SetUpdateFailed("关库/收尾期间收到停机信号(" + deferredSig + ")，已取消替换进程并完成优雅停机")
			if console != nil {
				logLifecycleBounded(finalCtx, func() {
					console.Info("restart superseded by deferred shutdown signal before exec", "signal", deferredSig)
				})
			}
		}
		coord.finish(0)
		return 0, false
	}

	if err := restartFn(exe); err != nil {
		logLifecycleBounded(finalCtx, func() {
			if lg != nil {
				lg.Error("[更新] 替换进程映像失败: "+err.Error()+"，改用启动新进程的方式", "version")
			}
		})
		_, _ = flushBounded(finalCtx)
		if serr := spawnFn(exe); serr != nil {
			logLifecycleBounded(finalCtx, func() {
				if lg != nil {
					lg.Error("[更新] 启动新进程也失败了: "+serr.Error()+"。请手动重启程序", "version")
				}
				if console != nil {
					console.Error("restart", "err", err, "spawn", serr)
				}
			})
			_, _ = flushBounded(finalCtx)
			coord.finish(1)
			return 1, false
		}
		logLifecycleBounded(finalCtx, func() {
			if lg != nil {
				lg.Info("[更新] 新进程已拉起,当前进程退出", "version")
			}
		})
		_, _ = flushBounded(finalCtx)
		coord.finish(0)
		return 0, true
	}
	coord.finish(0)
	return 0, true
}

// listenWithRetry 绑端口,短暂重试。
//
// 自更新是 execve:旧进程的监听 fd 虽然在 Shutdown 里关了,但内核回收、
// 以及仍处于 TIME_WAIT 的连接,都可能让紧接着的 bind 撞上
// "address already in use"。这是个几百毫秒的窗口,重试几次就过去了 ——
// 而不重试的话,自更新会以"新版本起不来"收场,然后被回滚。
//
// 真的是别的进程占着端口时,重试几秒也还是失败,那时候才该报错退出。
func listenWithRetry(addr string, state *app.State) (net.Listener, error) {
	var lastErr error
	for i := 0; i < 10; i++ {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, nil
		}
		lastErr = err
		if i == 0 {
			state.Logger.Warn("端口 "+addr+" 暂时绑不上,重试中: "+err.Error(), "system")
		}
		time.Sleep(300 * time.Millisecond)
	}
	return nil, lastErr
}

// mountEmbeddedUI 把嵌入的前端挂到根路径。
// 没启用 -tags ui 时 hasUI() 为 false，不注册任何 NoRoute；
// 启用时：/api/* 未匹配 → 404 JSON；命中具体文件 → 直接 serve；其余 → 返回 index.html 让 SPA 路由接管。
//
// 注意：index.html 不能交给 http.FileServer 去 serve，否则它会把 /index.html 301 重定向到 /，
// 触发与我们 SPA fallback 的相互重定向死循环（ERR_TOO_MANY_REDIRECTS）。
// 直接读出来缓存到内存，命中 SPA 路径时手工写回，绕开 FileServer 的内部行为。
func mountEmbeddedUI(r *gin.Engine) {
	if !hasUI() {
		return
	}
	registerEmbeddedUI(r, webDistFS())
}

func registerEmbeddedUI(r *gin.Engine, distFS fs.FS) {
	if distFS == nil {
		return
	}
	indexHTML, err := fs.ReadFile(distFS, "index.html")
	if err != nil {
		// 没构出 index.html，等于没 UI；退化为纯 API
		return
	}
	fileServer := http.FileServer(http.FS(distFS))

	serveIndex := func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.Header("Cache-Control", "no-cache")
		c.Status(http.StatusOK)
		_, _ = c.Writer.Write(indexHTML)
	}

	r.NoRoute(func(c *gin.Context) {
		reqPath := c.Request.URL.Path
		if strings.HasPrefix(reqPath, "/api/") {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		clean := strings.TrimPrefix(path.Clean("/"+reqPath), "/")
		// 根路径或显式访问 index.html：直接写 index.html，绕开 FileServer 的 301 重定向
		if clean == "" || clean == "index.html" {
			serveIndex(c)
			return
		}
		// 命中具体文件 → FileServer 处理；/assets/* 为 Vite 带内容哈希的构建产物，赋予长缓存
		if info, err := fs.Stat(distFS, clean); err == nil && !info.IsDir() {
			if strings.HasPrefix(clean, "assets/") {
				c.Header("Cache-Control", "public, max-age=31536000, immutable")
			}
			fileServer.ServeHTTP(c.Writer, c.Request)
			return
		}
		// 缺失的 /assets/ 目录、/assets/* 或显式带扩展名的静态资源路径必须返回 404，
		// 绝不能回落为 200 text/html（否则发版后旧 chunk 请求会拿到 HTML 并可能被 CDN 缓存）。
		if clean == "assets" || strings.HasPrefix(clean, "assets/") || path.Ext(clean) != "" {
			c.Header("Cache-Control", "no-store")
			c.String(http.StatusNotFound, "404 page not found")
			return
		}
		// SPA 客户端路由：写 index.html，让前端 router 接管
		serveIndex(c)
	})
}

// availabilityHandler 用 *planCode 通配符处理像 "/api/availability/24sk20-ram-64g" 这样的路径
func availabilityHandler(h gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		pc := c.Param("planCode")
		pc = strings.TrimPrefix(pc, "/")
		c.Params = append(c.Params[:0], gin.Param{Key: "planCode", Value: pc})
		h(c)
	}
}

// envFilePath 配置文件的位置。
// 默认是工作目录下的 .env（godotenv 的默认行为），允许用 OVH_ENV_FILE 覆盖 ——
// systemd / docker 里工作目录未必是程序所在目录，把路径写死会让密钥"下次启动就找不到"。
func envFilePath() string {
	if p := strings.TrimSpace(os.Getenv("OVH_ENV_FILE")); p != "" {
		return p
	}
	return ".env"
}

// isTrue 环境变量的宽松真值判断
func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// allowedOrigins CORS 白名单:本机的前端来源 + 用户显式声明的。
//
// 默认只有 localhost / 127.0.0.1 的后端端口和 Vite dev 端口。
// 反向代理或跨机访问的场景用 CORS_ALLOWED_ORIGINS 加(逗号分隔完整来源,
// 如 https://ovh.example.com)。
func allowedOrigins(port string) []string {
	if port == "" {
		port = "19998"
	}
	out := []string{}
	for _, host := range []string{"localhost", "127.0.0.1"} {
		for _, p := range []string{port, "19997"} { // 19997 = Vite dev server
			out = append(out, "http://"+host+":"+p)
		}
	}
	for _, o := range strings.Split(os.Getenv("CORS_ALLOWED_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// applySharedProxy 把默认账户的代理设为"公开请求"的统一出口。
// 账户增删改之后需要重调 —— 默认账户可能换了。
func applySharedProxy(state *app.State) {
	acc, ok := state.FindAccount("")
	if !ok {
		return
	}
	if err := netfp.SetSharedProxy(acc.ProxyURL); err != nil {
		state.Logger.Warn("公开请求的统一出口设置失败,将走直连: "+err.Error(), "proxy")
		return
	}
	if p := netfp.SharedProxy(); p != "" {
		state.Logger.Info("公开目录/区域探测统一走: "+p, "proxy")
	}
}

// clearEmptyEnv 把"设了但是空串"的环境变量彻底 unset。
//
// 只有 unset 之后 godotenv 才会用配置文件里的值填上 ——
// 它对已存在的变量一律跳过，不看是不是空的。
// 容器编排（compose 的 ${VAR:-}、k8s 的空 value）很容易设出这种变量。
func clearEmptyEnv(names ...string) {
	for _, n := range names {
		if v, ok := os.LookupEnv(n); ok && strings.TrimSpace(v) == "" {
			_ = os.Unsetenv(n)
		}
	}
}

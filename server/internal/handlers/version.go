package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/notify"
	"github.com/ovh-buy/server/internal/updater"
)

// Version 当前二进制版本。构建时通过 -ldflags "-X github.com/ovh-buy/server/internal/handlers.Version=x.y.z" 注入。
// 默认 "dev" 给 go run / 未注入的 build。
var Version = "dev"

// GetVersion GET /api/version  无需鉴权,前端启动时拿来显示
func GetVersion(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"version": Version,
		})
	}
}

// 上游仓库与更新源都在 internal/updater 里定义(updater.Repo / updater.LatestReleaseURL),
// 这里不再另存一份 —— 两处各写一份的话,换源时漏改一边就会出现"检查到的版本"和
// "实际装上的版本"不是同一个的诡异现象。
const (
	updateUserAgent         = "OVH-Console-UpdateChecker"
	updateCacheSuccessTT    = 15 * time.Minute
	updateCacheFailureTT    = 60 * time.Second
	updatePollInterval      = 5 * time.Minute
	kvUpdateLastNotifiedTag = "update_last_notified_tag"
	updateNotifyMaxBackoff  = 6 * time.Hour
)

// updateNotifyBroadcastFn 与 updateNotifyAnyAvailableFn 允许测试注入无外网桩；生产环境直接走 notify 包。
var (
	updateNotifyBroadcastFn    = notify.BroadcastWithStatus
	updateNotifyAnyAvailableFn = notify.AnyAvailable
	updateNotifyNowFn          = time.Now
	updateNotifyMu             sync.Mutex
	updateNotifyMemNotifiedTag string
	updateNotifyFailTag        string
	updateNotifyFailCount      int
	updateNotifyNextAttemptAt  time.Time
)

func resetUpdateNotifyStateForTest() {
	updateNotifyMu.Lock()
	updateNotifyMemNotifiedTag = ""
	updateNotifyFailTag = ""
	updateNotifyFailCount = 0
	updateNotifyNextAttemptAt = time.Time{}
	updateNotifyMu.Unlock()
}

// sanitizeReleaseBodyPlainText 剥离外部 Release body 中的 HTML 标签和控制字符并截断，确保纯文本安全展示。
func sanitizeReleaseBodyPlainText(raw string, maxRunes int) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	var out strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			if inTag {
				inTag = false
				continue
			}
		}
		if inTag {
			continue
		}
		if r < 32 && r != '\n' && r != '\t' {
			continue
		}
		out.WriteRune(r)
	}
	clean := strings.TrimSpace(out.String())
	runes := []rune(clean)
	if maxRunes > 0 && len(runes) > maxRunes {
		clean = string(runes[:maxRunes]) + "…"
	}
	return clean
}

// isTrustedReleaseURL 仅允许官方仓库 https://github.com/<Repo>/releases/... 链接出现在通知中，防止第三方镜像钓鱼。
func isTrustedReleaseURL(u string) bool {
	u = strings.TrimSpace(u)
	if u == "" {
		return false
	}
	prefix := "https://github.com/" + updater.Repo + "/releases/"
	return strings.HasPrefix(u, prefix)
}

// formatUpdateNotifyMessage 构造简洁清晰的纯文本新版本通知文案（按容器/裸机区分升级动作）。
func formatUpdateNotifyMessage(current, latest, tag, relURL, body string, inContainer bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("🆕 OVH 控制台发现新版本：v%s → %s\n", strings.TrimPrefix(current, "v"), tag))
	if summary := sanitizeReleaseBodyPlainText(body, 400); summary != "" {
		b.WriteString("\n更新说明：\n")
		b.WriteString(summary)
		b.WriteString("\n")
	}
	if isTrustedReleaseURL(relURL) {
		b.WriteString("\n发布页：")
		b.WriteString(strings.TrimSpace(relURL))
		b.WriteString("\n")
	}
	if inContainer {
		b.WriteString("\n容器升级命令：\ndocker compose pull && docker compose up -d")
	} else {
		b.WriteString("\n可在 Web 控制台右上角点击版本入口查看并更新。")
	}
	return b.String()
}

// maybeNotifyNewRelease 按“状态驱动”规则决定是否发送新版本通知：
//   - 无论本次结果来自新 200、304 复用还是已有缓存快照，只要满足 Version != "dev"、非 prerelease/draft、
//     semverGreater(latest, Version) 且未通知过该 tag，即触发检查；
//   - 若用户未配置任何通知通道，静默跳过；
//   - 若发送全部失败，按指数退避（5m → 10m → 20m … 上限 6h）避免重试风暴；
//   - 只要至少 1 个通道实际送达成功（sent > 0），立即在内存和 SQLite kv 中记为已通知（即使 SetKV 失败，本进程也不重复刷屏）。
var strictReleaseTagRe = regexp.MustCompile(`^v?\d+\.\d+\.\d+$`)

func maybeNotifyNewRelease(state *app.State, res updateCheckResult) bool {
	if state == nil || state.DB == nil || !res.ok {
		return false
	}
	tag := strings.TrimSpace(res.release.TagName)
	if tag == "" || !strictReleaseTagRe.MatchString(tag) || res.release.Prerelease || res.release.Draft {
		return false
	}
	latest := strings.TrimPrefix(tag, "v")
	if Version == "dev" || !semverGreater(latest, Version) {
		return false
	}

	updateNotifyMu.Lock()
	defer updateNotifyMu.Unlock()

	if updateNotifyMemNotifiedTag == tag {
		return false
	}
	var lastNotified string
	if ok, err := state.DB.GetKV(kvUpdateLastNotifiedTag, &lastNotified); err == nil && ok {
		if strings.TrimSpace(lastNotified) == tag {
			updateNotifyMemNotifiedTag = tag
			return false
		}
	}

	now := updateNotifyNowFn()
	if updateNotifyFailTag == tag && now.Before(updateNotifyNextAttemptAt) {
		return false
	}

	avail, _ := updateNotifyAnyAvailableFn(state, false)
	if !avail {
		return false
	}

	msg := formatUpdateNotifyMessage(Version, latest, tag, res.release.HTMLURL, res.release.Body, updater.InContainer())
	sent, _ := updateNotifyBroadcastFn(state, msg, nil)
	if sent <= 0 {
		if updateNotifyFailTag != tag {
			updateNotifyFailTag = tag
			updateNotifyFailCount = 1
		} else {
			updateNotifyFailCount++
		}
		shift := updateNotifyFailCount - 1
		if shift > 8 {
			shift = 8
		}
		backoff := updatePollInterval * time.Duration(1<<shift)
		if backoff > updateNotifyMaxBackoff {
			backoff = updateNotifyMaxBackoff
		}
		updateNotifyNextAttemptAt = now.Add(backoff)
		if state.Logger != nil {
			state.Logger.Warn(fmt.Sprintf("新版本 %s 通知未成功送达任何通道，将在 %s 后重试", tag, backoff), "version")
		}
		return false
	}

	// 只要已成功送达，先落内存去重，防止 SetKV 瞬时失败引发每 5 分钟重复推送
	updateNotifyMemNotifiedTag = tag
	updateNotifyFailTag = ""
	updateNotifyFailCount = 0
	updateNotifyNextAttemptAt = time.Time{}

	if err := state.DB.SetKV(kvUpdateLastNotifiedTag, tag); err != nil {
		if state.Logger != nil {
			state.Logger.Warn("记录已通知版本标签失败（已在内存去重）: "+err.Error(), "version")
		}
	} else if state.Logger != nil {
		state.Logger.Info(fmt.Sprintf("已通过 %d 个通道推送新版本通知: %s", sent, tag), "version")
	}
	return true
}

// githubRelease 只挑要用的字段反序列化
type githubRelease struct {
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	HTMLURL     string `json:"html_url"`
	PublishedAt string `json:"published_at"`
	Body        string `json:"body"`
	Prerelease  bool   `json:"prerelease"`
	Draft       bool   `json:"draft"`
}

type updateCheckResult struct {
	ok        bool
	release   githubRelease
	checkedAt string
	errMsg    string
	etag      string
	lastError string
	expiresAt time.Time
}

type updateInFlight struct {
	done chan struct{}
	res  updateCheckResult
}

type updateCheckCache struct {
	mu          sync.Mutex
	entries     map[string]updateCheckResult
	lastSuccess map[string]updateCheckResult
	inflight    map[string]*updateInFlight
	now         func() time.Time
}

var releaseCheckCache = newUpdateCheckCache()

func newUpdateCheckCache() *updateCheckCache {
	return &updateCheckCache{
		entries:     map[string]updateCheckResult{},
		lastSuccess: map[string]updateCheckResult{},
		inflight:    map[string]*updateInFlight{},
		now:         time.Now,
	}
}

// ResetUpdateCheckCache 清空更新检查缓存（仅供测试使用）。
func ResetUpdateCheckCache() {
	releaseCheckCache.mu.Lock()
	releaseCheckCache.entries = map[string]updateCheckResult{}
	releaseCheckCache.lastSuccess = map[string]updateCheckResult{}
	releaseCheckCache.inflight = map[string]*updateInFlight{}
	releaseCheckCache.mu.Unlock()
}

// trimV 去掉 tag 的 v 前缀
func trimV(s string) string { return strings.TrimPrefix(strings.TrimSpace(s), "v") }

// parseSemver 把 "v0.0.2" / "0.1.10" 切成 [0 0 2] / [0 1 10],
// 解析失败的段当 0,简单稳定的字典序比较够用
func parseSemver(s string) [3]int {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimPrefix(s, "V")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.SplitN(s, ".", 3)
	var v [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		n, _ := strconv.Atoi(parts[i])
		v[i] = n
	}
	return v
}

// semverGreater latest > current ?
func semverGreater(latest, current string) bool {
	l := parseSemver(latest)
	c := parseSemver(current)
	for i := 0; i < 3; i++ {
		if l[i] > c[i] {
			return true
		}
		if l[i] < c[i] {
			return false
		}
	}
	return false
}

// fetch 合并并发拉取并按成功/失败分别缓存（ETag 与成功快照严格绑定），避免免鉴权接口刷爆上游限额。
// upstreamCtx 决定上游 HTTP 的生命周期（后台轮询传服务 bgCtx，接口触发传独立 15s 生命周期避免被单个客户端断开拖垮）。
func (uc *updateCheckCache) fetch(reqCtx context.Context, upstreamCtx context.Context, url string, state *app.State, force bool) (updateCheckResult, bool) {
	uc.mu.Lock()
	now := uc.now()
	prev, hasPrev := uc.entries[url]
	if !force && hasPrev && now.Before(prev.expiresAt) {
		uc.mu.Unlock()
		return prev, true
	}
	if call, ok := uc.inflight[url]; ok {
		uc.mu.Unlock()
		select {
		case <-call.done:
			return call.res, true
		case <-reqCtx.Done():
			return updateCheckResult{}, false
		}
	}
	prevOK, hasPrevOK := uc.lastSuccess[url]
	call := &updateInFlight{done: make(chan struct{})}
	uc.inflight[url] = call
	uc.mu.Unlock()

	defer func() {
		uc.mu.Lock()
		if uc.inflight[url] == call {
			delete(uc.inflight, url)
			close(call.done)
		}
		uc.mu.Unlock()
	}()

	res := fetchLatestReleaseFromUpstream(upstreamCtx, url, state, uc.now, prevOK, hasPrevOK)

	// 后台周期刷新遇到临时上游失败时保留最近一次成功快照并附带 lastError，不把既有成功状态抹成 502。
	if !res.ok && hasPrevOK && force {
		kept := prevOK
		kept.lastError = res.errMsg
		kept.expiresAt = uc.now().Add(updateCacheFailureTT)
		res = kept
	}

	uc.mu.Lock()
	if res.ok {
		uc.lastSuccess[url] = res
	}
	uc.entries[url] = res
	call.res = res
	delete(uc.inflight, url)
	close(call.done)
	uc.mu.Unlock()

	return res, true
}

func fetchLatestReleaseFromUpstream(parentCtx context.Context, url string, state *app.State, nowFn func() time.Time, prevOK updateCheckResult, hasPrevOK bool) updateCheckResult {
	fail := func(publicErr, internalDetail string) updateCheckResult {
		if internalDetail != "" && state != nil && state.Logger != nil {
			state.Logger.Warn("update check 失败: "+internalDetail, "version")
		}
		now := nowFn()
		return updateCheckResult{
			ok:        false,
			errMsg:    publicErr,
			checkedAt: now.UTC().Format(time.RFC3339),
			expiresAt: now.Add(updateCacheFailureTT),
		}
	}

	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancel()

	client := &http.Client{
		Timeout: 15 * time.Second,
		// 禁止跟随重定向：上游非 200（含首次 3xx）统一映射为 502，且避免更新源跳转到意外地址
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fail("invalid update upstream configuration", "更新源地址无效: "+err.Error())
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", updateUserAgent)
	sentETag := ""
	if hasPrevOK && prevOK.etag != "" {
		sentETag = prevOK.etag
		req.Header.Set("If-None-Match", sentETag)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fail("failed to reach update upstream", err.Error())
	}
	defer resp.Body.Close()

	// 仅当本次确实携带了匹配该成功快照的 If-None-Match 验证器时，才接受 304 复用并续期
	if resp.StatusCode == http.StatusNotModified && hasPrevOK && sentETag != "" {
		now := nowFn()
		refreshed := prevOK
		refreshed.lastError = ""
		refreshed.checkedAt = now.UTC().Format(time.RFC3339)
		refreshed.expiresAt = now.Add(updateCacheSuccessTT)
		if newTag := strings.TrimSpace(resp.Header.Get("ETag")); newTag != "" {
			refreshed.etag = newTag
		}
		return refreshed
	}

	const maxReleaseResponse = 2 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReleaseResponse+1))
	if err != nil {
		return fail("failed to read update upstream response", err.Error())
	}
	if len(body) > maxReleaseResponse {
		return fail("update upstream response too large", "响应超过 2MiB 上限")
	}
	if resp.StatusCode != http.StatusOK {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 256 {
			snippet = snippet[:256]
		}
		return fail("upstream returned "+strconv.Itoa(resp.StatusCode),
			"HTTP "+strconv.Itoa(resp.StatusCode)+" "+snippet)
	}

	var rel githubRelease
	if err := json.Unmarshal(body, &rel); err != nil {
		return fail("invalid update upstream response", "JSON 解析失败: "+err.Error())
	}
	if strings.TrimSpace(rel.TagName) == "" {
		return fail("invalid update upstream response", "响应缺少 tag_name 字段")
	}

	now := nowFn()
	// 新 200 响应的 ETag 严格与该次快照绑定（若上游未返 ETag 则置空，绝不沿用旧 ETag）
	return updateCheckResult{
		ok:        true,
		release:   rel,
		checkedAt: now.UTC().Format(time.RFC3339),
		etag:      strings.TrimSpace(resp.Header.Get("ETag")),
		expiresAt: now.Add(updateCacheSuccessTT),
	}
}

// StartUpdateCheckLoopWithContext 仿照 OpenBear 更新策略，后台按 5 分钟周期（携带 If-None-Match ETag 条件请求）
// 探测最新 Release，缓存热数据供前端秒开读取，并在发现未通知过的新正式版本时主动推送通知；
// 将服务生命周期 ctx 传入上游 HTTP 以在退出时即时中止在途请求。
func StartUpdateCheckLoopWithContext(ctx context.Context, state *app.State) {
	pollSec := int(updatePollInterval.Seconds())
	var epoch uint64
	if state != nil && state.Heartbeats != nil {
		epoch = state.Heartbeats.Register("update_check", pollSec, true)
		defer state.Heartbeats.SetStoppedIf("update_check", epoch)
	}
	// 启动稍作延迟，避免和初始化阶段的 OVH 请求撞在一起
	initTimer := time.NewTimer(15 * time.Second)
	defer initTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-initTimer.C:
	}

	runCheck := func() {
		if ctx.Err() != nil {
			return
		}
		url := updater.LatestReleaseURL()
		res, ok := releaseCheckCache.fetch(ctx, ctx, url, state, true)
		if !ok {
			if ctx.Err() == nil && state != nil && state.Heartbeats != nil {
				state.Heartbeats.BeatError("update_check", pollSec, "更新检查请求被取消或超时")
			}
			return
		}
		if state != nil && state.Heartbeats != nil {
			if res.ok && res.lastError == "" {
				state.Heartbeats.BeatOK("update_check", pollSec)
			} else {
				errMsg := res.errMsg
				if errMsg == "" {
					errMsg = res.lastError
				}
				state.Heartbeats.BeatError("update_check", pollSec, errMsg)
			}
		}
		if res.ok {
			maybeNotifyNewRelease(state, res)
		}
	}

	runCheck()
	ticker := time.NewTicker(updatePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCheck()
		}
	}
}

// CheckUpdate GET /api/version/check-update
// 收到请求 → 带服务端缓存、ETag 条件请求与并发合并（singleflight）向更新源拉取最新 release，与本地 Version 比对。
// 成功缓存 15 分钟（后台每 5 分钟 ETag 条件刷新），失败缓存 60 秒，上游非 200（含 3xx）与无效载荷统一映射为 502 Bad Gateway。
// 本接口为免鉴权公开端点，不访问也不暴露运行中抢购队列信息，完全独立于购买队列锁。
func CheckUpdate(state *app.State) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 与真正执行更新的 updater 用同一个来源:否则设了 OVH_UPDATE_API(私有镜像)之后,
		// 这里显示"有 v1.2.3",更新器却去 GitHub 抓另一个版本,用户看到的和装上的对不上
		url := updater.LatestReleaseURL()
		// 仅允许缓存过期后自动拉取或后台 5m ETag 刷新，不向免鉴权请求开放 force 旁路以防滥用上游限额
		res, ok := releaseCheckCache.fetch(c.Request.Context(), context.Background(), url, state, false)
		if !ok {
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": "request canceled", "current": Version})
			return
		}
		if !res.ok {
			c.JSON(http.StatusBadGateway, gin.H{
				"error":   res.errMsg,
				"current": Version,
			})
			return
		}

		latest := strings.TrimPrefix(res.release.TagName, "v")
		hasUpdate := Version != "dev" && semverGreater(latest, Version)

		c.JSON(http.StatusOK, gin.H{
			"current":     Version,
			"latest":      latest,
			"tag":         res.release.TagName,
			"name":        res.release.Name,
			"hasUpdate":   hasUpdate,
			"url":         res.release.HTMLURL,
			"publishedAt": res.release.PublishedAt,
			"body":        res.release.Body,
			"prerelease":  res.release.Prerelease,
			"checkedAt":   res.checkedAt,
			"lastError":   res.lastError,
			// 容器里自更新是停用的 —— 界面据此把"立即更新"换成"怎么拉新镜像",
			// 而不是给一个点了必然失败的按钮。
			"inContainer": updater.InContainer(),
			"updateHint":  containerHintIfNeeded(),
		})
	}
}

// containerHintIfNeeded 容器里返回更新指引,否则空串。
func containerHintIfNeeded() string {
	if updater.InContainer() {
		return updater.ContainerUpdateHint
	}
	return ""
}

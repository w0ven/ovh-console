package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/auth"
	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/notify"
	"github.com/ovh-buy/server/internal/storage"
)

func newSystemTestState(t *testing.T) *app.State {
	t.Helper()
	dir := t.TempDir()
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() {
		d.CleanupMaintState()
		_ = d.Close()
	})
	lg := logger.New(filepath.Join(dir, "logs.json"), nil)
	t.Cleanup(func() { lg.Close() })
	cfg := config.New(d)
	st := app.NewState(storage.Paths{DataDir: dir}, cfg, lg, d)
	return st
}

func TestMaybeNotifyNewReleaseBackoffSanitizeAndDedupe(t *testing.T) {
	st := newSystemTestState(t)
	origVersion := Version
	origBroadcast := updateNotifyBroadcastFn
	origAvail := updateNotifyAnyAvailableFn
	origNow := updateNotifyNowFn
	resetUpdateNotifyStateForTest()
	t.Cleanup(func() {
		Version = origVersion
		updateNotifyBroadcastFn = origBroadcast
		updateNotifyAnyAvailableFn = origAvail
		updateNotifyNowFn = origNow
		resetUpdateNotifyStateForTest()
	})

	fakeNow := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	updateNotifyNowFn = func() time.Time { return fakeNow }

	Version = "0.4.38"
	updateNotifyAnyAvailableFn = func(*app.State, bool) (bool, string) {
		return true, ""
	}

	calls := 0
	sendSuccess := false
	lastMsg := ""
	updateNotifyBroadcastFn = func(_ *app.State, msg string, _ map[string]interface{}) (int, []notify.Channel) {
		calls++
		lastMsg = msg
		if sendSuccess {
			return 1, nil
		}
		return 0, nil
	}

	res := updateCheckResult{
		ok: true,
		release: githubRelease{
			TagName: "v0.4.39",
			HTMLURL: "https://evil.example.com/phishing",
			Body:    "<b>New</b> features <script>alert(1)</script>",
		},
	}

	// 1. prerelease 跳过
	pre := res
	pre.release.Prerelease = true
	if maybeNotifyNewRelease(st, pre) || calls != 0 {
		t.Fatalf("prerelease should not notify, calls=%d", calls)
	}

	// 2. 首次发送失败（sent=0）→ 触发指数退避（5m 内重复调用不重发）
	if maybeNotifyNewRelease(st, res) {
		t.Fatalf("failed broadcast should return false")
	}
	if calls != 1 {
		t.Fatalf("expected 1 broadcast call, got %d", calls)
	}
	if strings.Contains(lastMsg, "evil.example.com") || strings.Contains(lastMsg, "<b>") || strings.Contains(lastMsg, "<script>") {
		t.Fatalf("untrusted URL or HTML tags leaked into notify msg: %q", lastMsg)
	}

	// 2 分钟后仍在退避窗口内，不重试
	fakeNow = fakeNow.Add(2 * time.Minute)
	if maybeNotifyNewRelease(st, res) || calls != 1 {
		t.Fatalf("should be suppressed during backoff window, calls=%d", calls)
	}

	// 超过 5 分钟退避后重试成功
	fakeNow = fakeNow.Add(4 * time.Minute)
	sendSuccess = true
	res.release.HTMLURL = "https://github.com/w0ven/ovh-console/releases/tag/v0.4.39"
	if !maybeNotifyNewRelease(st, res) || calls != 2 {
		t.Fatalf("expected retry success after backoff, calls=%d", calls)
	}
	if !strings.Contains(lastMsg, "https://github.com/w0ven/ovh-console/releases/tag/v0.4.39") {
		t.Fatalf("trusted official release URL should be kept: %q", lastMsg)
	}

	// 3. 同 tag 再次检查 → 严格去重
	if maybeNotifyNewRelease(st, res) || calls != 2 {
		t.Fatalf("duplicate tag should not notify again, calls=%d", calls)
	}

	// 4. 新版本号 v0.4.40 → 再次发送
	res2 := res
	res2.release.TagName = "v0.4.40"
	if !maybeNotifyNewRelease(st, res2) || calls != 3 {
		t.Fatalf("new tag v0.4.40 should notify once, calls=%d", calls)
	}
}

func TestSystemOpsAuthBoundaryRateLimitAndSingleUseToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st := newSystemTestState(t)
	st.Heartbeats.Register("queue_processor", 15, true)
	st.Heartbeats.BeatOK("queue_processor", 15)

	origTickets := backupTickets
	backupTickets = &backupTicketStore{
		tickets: map[string]backupDownloadTicket{},
		nowFn:   time.Now,
	}
	t.Cleanup(func() { backupTickets = origTickets })

	const masterKey = "super-master-secret-key"
	const deviceToken = "paired-device-token-123"

	r := gin.New()
	r.Use(auth.Middleware(auth.Config{
		APIKey:         masterKey,
		Enabled:        true,
		WhitelistPaths: auth.DefaultWhitelist(),
		DeviceTokenValid: func(tok string) (int64, bool) {
			if tok == deviceToken {
				return 42, true
			}
			return 0, false
		},
	}))
	sysOps := r.Group("/api/system")
	sysOps.Use(auth.RequireMasterKey())
	{
		sysOps.GET("/status", GetSystemStatus(st))
		sysOps.POST("/db/maintain", MaintainDatabase(st))
		sysOps.GET("/backups", ListSystemBackups(st))
		sysOps.POST("/backups", CreateSystemBackup(st))
		sysOps.POST("/backups/:id/download-token", IssueBackupDownloadToken(st))
		sysOps.POST("/backups/:id/download", DownloadSystemBackup(st))
	}

	// 0. App 配对设备令牌访问全部 6 个 /api/system/* 端点必须全部被 403 拒绝！
	for _, ep := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/system/status"},
		{http.MethodGet, "/api/system/backups"},
		{http.MethodPost, "/api/system/backups"},
		{http.MethodPost, "/api/system/db/maintain"},
		{http.MethodPost, "/api/system/backups/sniper-manual-20261122-120000-abcdef/download-token"},
		{http.MethodPost, "/api/system/backups/sniper-manual-20261122-120000-abcdef/download"},
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(ep.method, ep.path, bytes.NewBufferString(`{}`))
		req.Header.Set("Authorization", "Bearer "+deviceToken)
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("device token must get 403 on %s %s, got %d (%s)", ep.method, ep.path, w.Code, w.Body.String())
		}
	}

	// 1. 主密钥访问 /api/system/status 成功且不暴露绝对路径
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/system/status", nil)
	req.Header.Set("X-API-Key", masterKey)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status endpoint returned %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), st.Paths.DataDir) {
		t.Fatalf("status response must not expose filesystem path %q", st.Paths.DataDir)
	}

	// 2. 创建手动备份
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/system/backups", bytes.NewBufferString(`{"redact":false}`))
	req.Header.Set("X-API-Key", masterKey)
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create backup returned %d: %s", w.Code, w.Body.String())
	}
	var createResp struct {
		Backup db.BackupInfo `json:"backup"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &createResp); err != nil || createResp.Backup.ID == "" {
		t.Fatalf("decode create backup: %v (%s)", err, w.Body.String())
	}

	// 3. 批量签发 token 触发未消费上限（max=3），第 4 次签发必须 429 拒绝（彻底阻断预签发绕过）
	var issuedTokens []string
	for i := 0; i < 4; i++ {
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/system/backups/"+createResp.Backup.ID+"/download-token", bytes.NewBufferString(`{"redact":true}`))
		req.Header.Set("X-API-Key", masterKey)
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "10.0.0.1:12345"
		r.ServeHTTP(w, req)
		if i < 3 {
			if w.Code != http.StatusOK {
				t.Fatalf("issue #%d expected 200, got %d: %s", i, w.Code, w.Body.String())
			}
			var tr struct {
				Token string `json:"token"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &tr)
			issuedTokens = append(issuedTokens, tr.Token)
		} else if w.Code != http.StatusTooManyRequests {
			t.Fatalf("4th pending token must be rate-limited (429), got %d", w.Code)
		}
	}

	// 4. 换一个客户端 IP 尝试消费第 1 个 token → 必须 404 拒绝（且同时作废该 token）
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/system/backups/"+createResp.Backup.ID+"/download", bytes.NewBufferString(`{"token":"`+issuedTokens[0]+`"}`))
	req.Header.Set("X-API-Key", masterKey)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "10.0.0.2:54321"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("token used from different IP must fail with 404, got %d", w.Code)
	}

	// 5. 同 IP 使用第 2 个 token 下载成功且含安全头 + SQLite 魔数头
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/system/backups/"+createResp.Backup.ID+"/download", bytes.NewBufferString(`{"token":"`+issuedTokens[1]+`"}`))
	req.Header.Set("X-API-Key", masterKey)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "10.0.0.1:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("download returned %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected Cache-Control=no-store, got %q", got)
	}
	if !strings.HasPrefix(w.Body.String(), "SQLite format 3\x00") {
		t.Fatalf("downloaded payload is not valid sqlite header")
	}

	// 6. 复用同一 token 必须 404 拒绝
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/system/backups/"+createResp.Backup.ID+"/download", bytes.NewBufferString(`{"token":"`+issuedTokens[1]+`"}`))
	req.Header.Set("X-API-Key", masterKey)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "10.0.0.1:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("reused token must return 404, got %d", w.Code)
	}
}

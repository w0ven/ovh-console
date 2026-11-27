package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/catalog"
	"github.com/ovh-buy/server/internal/notify"
	"github.com/ovh-buy/server/internal/types"
	"github.com/ovh-buy/server/internal/updater"
)

func TestVersionIgnoreEndpointAndReleaseNotesSanitization(t *testing.T) {
	ResetUpdateCheckCache()
	resetUpdateNotifyStateForTest()
	t.Cleanup(func() {
		ResetUpdateCheckCache()
		resetUpdateNotifyStateForTest()
	})

	// 1. 仅含一行 Full Changelog compare 链接的空壳正文应返回空串，避免直接把裸链接当更新内容；
	//    带有真实更新条目时应剥离粗体/标题标记并移除冗余 Full Changelog 行。
	onlyCompare := "**Full Changelog**: https://github.com/w0ven/ovh-console/compare/v0.4.40...v0.4.41"
	if got := sanitizeReleaseBodyPlainText(onlyCompare, 400); got != "" {
		t.Fatalf("expected empty summary for compare-only body, got %q", got)
	}
	richBody := "## 更新内容\n\n- **抢购队列**：支持同配置合并探测与并发结账\n- **历史记录**：支持 6 列排序与多选筛选\n\n**Full Changelog**: https://github.com/w0ven/ovh-console/compare/v0.4.41...v0.4.42"
	gotRich := sanitizeReleaseBodyPlainText(richBody, 400)
	if !strings.Contains(gotRich, "抢购队列：支持同配置合并探测与并发结账") || strings.Contains(gotRich, "Full Changelog") || strings.Contains(gotRich, "**") {
		t.Fatalf("unexpected sanitized rich body: %q", gotRich)
	}

	// 2. check-update 与 POST /version/ignore 联动，且自动跳过已忽略版本的广播推送
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"tag_name":"v0.4.42",
			"name":"v0.4.42",
			"html_url":"https://github.com/` + updater.Repo + `/releases/tag/v0.4.42",
			"published_at":"2026-11-27T06:00:00Z",
			"body":"## 更新内容\n- 新增同配置合并抢购\n- 新增版本忽略按钮"
		}`))
	}))
	defer srv.Close()
	t.Setenv("OVH_UPDATE_API", srv.URL)

	oldVer := Version
	Version = "0.4.41"
	t.Cleanup(func() { Version = oldVer })

	st, _ := newWatchTestMonitor(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/version/check-update", CheckUpdate(st))
	r.POST("/api/version/ignore", IgnoreUpdate(st))

	// 初始：hasUpdate=true, ignored=false
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
	var resp1 map[string]any
	if w1.Code != http.StatusOK || json.Unmarshal(w1.Body.Bytes(), &resp1) != nil {
		t.Fatalf("check-update 1 failed: %d %s", w1.Code, w1.Body.String())
	}
	if resp1["hasUpdate"] != true || resp1["ignored"] != false {
		t.Fatalf("unexpected initial check-update: %+v", resp1)
	}

	// 忽略 v0.4.42
	wIgnore := httptest.NewRecorder()
	reqIgnore := httptest.NewRequest(http.MethodPost, "/api/version/ignore", strings.NewReader(`{"version":"v0.4.42"}`))
	reqIgnore.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(wIgnore, reqIgnore)
	if wIgnore.Code != http.StatusOK {
		t.Fatalf("ignore failed: %d %s", wIgnore.Code, wIgnore.Body.String())
	}

	// 忽略后：hasUpdate 仍为 true（弹窗内仍可看说明和手动更新），ignored=true
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
	var resp2 map[string]any
	if w2.Code != http.StatusOK || json.Unmarshal(w2.Body.Bytes(), &resp2) != nil {
		t.Fatalf("check-update 2 failed: %d %s", w2.Code, w2.Body.String())
	}
	if resp2["hasUpdate"] != true || resp2["ignored"] != true || resp2["ignoredVersion"] != "0.4.42" {
		t.Fatalf("unexpected ignored check-update: %+v", resp2)
	}

	// 已忽略该版本时，maybeNotifyNewRelease 不得发送通知
	origBroadcast := updateNotifyBroadcastFn
	origAnyAvail := updateNotifyAnyAvailableFn
	t.Cleanup(func() {
		updateNotifyBroadcastFn = origBroadcast
		updateNotifyAnyAvailableFn = origAnyAvail
	})
	broadcastCount := 0
	var capturedMarkup map[string]interface{}
	updateNotifyAnyAvailableFn = func(*app.State, bool) (bool, string) { return true, "" }
	updateNotifyBroadcastFn = func(_ *app.State, _ string, markup map[string]interface{}) (int, []notify.Channel) {
		broadcastCount++
		capturedMarkup = markup
		return 1, nil
	}
	res, ok := releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, st, false)
	if !ok || !res.ok {
		t.Fatalf("expected cached release ok")
	}
	if maybeNotifyNewRelease(st, res) || broadcastCount != 0 {
		t.Fatalf("ignored release must not broadcast notification, count=%d", broadcastCount)
	}

	// 取消忽略后：恢复广播且携带 Telegram 内联按钮（含忽略此版本且每个 callback_data <= 64 字节）
	wUnignore := httptest.NewRecorder()
	reqUnignore := httptest.NewRequest(http.MethodPost, "/api/version/ignore", strings.NewReader(`{"version":""}`))
	reqUnignore.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(wUnignore, reqUnignore)
	if wUnignore.Code != http.StatusOK {
		t.Fatalf("unignore failed: %d %s", wUnignore.Code, wUnignore.Body.String())
	}
	if !maybeNotifyNewRelease(st, res) || broadcastCount != 1 || capturedMarkup == nil {
		t.Fatalf("unignored release must broadcast with keyboard, count=%d markup=%v", broadcastCount, capturedMarkup)
	}
	// 校验长内容与短内容两种键盘的每颗按钮 callback_data 均不超过 Telegram 64 字节硬上限
	for _, kb := range []map[string]interface{}{
		buildUpdateNotifyKeyboard("v0.4.42", "https://github.com/"+updater.Repo+"/releases/tag/v0.4.42", false, false),
		buildUpdateNotifyKeyboard("v0.4.42", "https://github.com/"+updater.Repo+"/releases/tag/v0.4.42", true, true),
	} {
		rows, _ := kb["inline_keyboard"].([][]map[string]interface{})
		if len(rows) == 0 {
			t.Fatal("expected inline keyboard rows")
		}
		for _, row := range rows {
			for _, btn := range row {
				if cb, hasCB := btn["callback_data"].(string); hasCB {
					if len(cb) == 0 || len(cb) > 64 {
						t.Fatalf("callback_data %q length %d exceeds 64 bytes", cb, len(cb))
					}
				}
			}
		}
	}

	// 3. Telegram 回调 action="up"（忽略 / 取消忽略）端到端更新状态
	cfg := st.Config.Get()
	cfg.TgChatID = "1001"
	st.Config.Set(cfg)

	sendUpdateCB := func(updateID int, op, tag string) *updateCtx {
		cbData, _ := json.Marshal(map[string]string{"a": "up", "o": op, "t": tag})
		payload := map[string]interface{}{
			"update_id": float64(updateID),
			"callback_query": map[string]interface{}{
				"id":   "cb-1",
				"data": string(cbData),
				"from": map[string]interface{}{"id": float64(1001)},
				"message": map[string]interface{}{
					"message_id": float64(88),
					"chat":       map[string]interface{}{"id": float64(1001)},
				},
			},
		}
		return ProcessUpdate(st, nil, payload)
	}
	if u := sendUpdateCB(9001, "i", "v0.4.42"); u.status != http.StatusOK || getIgnoredVersion(st) != "0.4.42" {
		t.Fatalf("telegram ignore callback failed: status=%d ignored=%q", u.status, getIgnoredVersion(st))
	}
	if u := sendUpdateCB(9002, "u", "v0.4.42"); u.status != http.StatusOK || getIgnoredVersion(st) != "" {
		t.Fatalf("telegram unignore callback failed: status=%d ignored=%q", u.status, getIgnoredVersion(st))
	}
}

func TestUpdateQueueBundleRejectsOutOfCatalogDatacenter(t *testing.T) {
	catalog.ResetCachesForTesting()
	t.Cleanup(catalog.ResetCachesForTesting)
	previous := http.DefaultTransport
	http.DefaultTransport = queuePreorderTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"plans":[{"planCode":"27skle06-v1","configurations":[{"name":"dedicated_datacenter","values":["bhs","fra","gra","lon","rbx","sbg","waw"]}]}]}`
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = previous })

	state, _ := newWatchTestMonitor(t)
	state.Accounts = []types.OVHAccount{{ID: "ie", Endpoint: "ovh-eu", Zone: "IE-BUNDLE-DC"}}
	state.Queue = []types.QueueItem{{
		ID: "t1", AccountID: "ie", PlanCode: "27skle06-v1", Datacenter: "fra",
		Status: "running", RetryInterval: 30, Duration: "P1M", CreatedAt: "2026-11-27T00:00:00Z",
	}}
	if err := state.SaveQueue(); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.PUT("/queue/bundle", UpdateQueueBundle(state))

	// 1. 试图通过整单修改把美区机房 hil 加进 IE 账户订单 -> 400 拦截且原队列不变
	wBad := httptest.NewRecorder()
	reqBad := httptest.NewRequest(http.MethodPut, "/queue/bundle", bytes.NewBufferString(`{"ids":["t1"],"options":[],"retryInterval":30,"quantities":{"fra":1,"hil":1}}`))
	reqBad.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(wBad, reqBad)
	if wBad.Code != http.StatusBadRequest || !strings.Contains(wBad.Body.String(), "不在子公司") || len(state.Queue) != 1 {
		t.Fatalf("expected 400 for out-of-catalog bundle datacenter, got %d %s (len=%d)", wBad.Code, wBad.Body.String(), len(state.Queue))
	}

	// 2. 改为合法机房 fra + lon -> 200 成功扩充为 2 台
	wOK := httptest.NewRecorder()
	reqOK := httptest.NewRequest(http.MethodPut, "/queue/bundle", bytes.NewBufferString(`{"ids":["t1"],"options":[],"retryInterval":30,"quantities":{"fra":1,"lon":1}}`))
	reqOK.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(wOK, reqOK)
	if wOK.Code != http.StatusOK || len(state.Queue) != 2 {
		t.Fatalf("expected 200 for valid bundle datacenters, got %d %s (len=%d)", wOK.Code, wOK.Body.String(), len(state.Queue))
	}

	// 3. 空串机房或同义别名 0/2 混合:空串必须 400 拒绝(不能当成 gra),同义别名不得因 map 顺序丢失正数
	wEmpty := httptest.NewRecorder()
	reqEmpty := httptest.NewRequest(http.MethodPut, "/queue/bundle", bytes.NewBufferString(`{"ids":["t1"],"options":[],"retryInterval":30,"quantities":{"":2}}`))
	reqEmpty.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(wEmpty, reqEmpty)
	if wEmpty.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty datacenter key, got %d %s", wEmpty.Code, wEmpty.Body.String())
	}
}

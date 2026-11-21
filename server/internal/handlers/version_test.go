package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestCheckUpdateSingleflightAndCache(t *testing.T) {
	ResetUpdateCheckCache()
	t.Cleanup(ResetUpdateCheckCache)

	var upstreamCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		time.Sleep(40 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tag_name":"v0.4.38","name":"v0.4.38","html_url":"https://example.test/releases/v0.4.38","published_at":"2026-11-22T00:00:00Z","body":"ok","prerelease":false}`))
	}))
	defer srv.Close()

	t.Setenv("OVH_UPDATE_API", srv.URL)
	oldVer := Version
	Version = "0.4.37"
	t.Cleanup(func() { Version = oldVer })

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/version/check-update", CheckUpdate(nil))

	const concurrency = 100
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil)
			r.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errCh <- &httpStatusErr{want: http.StatusOK, got: w.Code}
				return
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				errCh <- err
				return
			}
			if body["latest"] != "0.4.38" || body["hasUpdate"] != true {
				errCh <- &httpStatusErr{want: 1, got: 0}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("expected 1 upstream call across %d concurrent requests, got %d", concurrency, got)
	}

	// TTL 内再次请求应直接命中缓存，不触发上游调用
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("cached request status = %d, want 200", w.Code)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("expected cache hit with 1 total upstream call, got %d", got)
	}
}

func TestCheckUpdateMapsUpstreamErrorsTo502AndCachesBriefly(t *testing.T) {
	ResetUpdateCheckCache()
	t.Cleanup(ResetUpdateCheckCache)

	var upstreamCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	t.Setenv("OVH_UPDATE_API", srv.URL)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/version/check-update", CheckUpdate(nil))

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
		if w.Code != http.StatusBadGateway {
			t.Fatalf("call %d: status = %d, want 502 (not upstream 404)", i+1, w.Code)
		}
		// 公开接口不得泄漏上游原始错误正文
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("invalid json response: %v", err)
		}
		if _, hasDetail := body["detail"]; hasDetail {
			t.Fatalf("public error response must not expose raw upstream detail: %v", body)
		}
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("expected failure response cached after 1 upstream call, got %d", got)
	}
}

func TestCheckUpdateRejectsEmptyTagAndRedirects(t *testing.T) {
	// 1. 合法 JSON "{}" 缺少 tag_name 必须判 502，不能当作成功缓存 15 分钟
	t.Run("missing_tag_name", func(t *testing.T) {
		ResetUpdateCheckCache()
		t.Cleanup(ResetUpdateCheckCache)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()

		t.Setenv("OVH_UPDATE_API", srv.URL)
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.GET("/api/version/check-update", CheckUpdate(nil))

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
		if w.Code != http.StatusBadGateway {
			t.Fatalf("empty JSON payload status = %d, want 502", w.Code)
		}
	})

	// 2. 上游 302 重定向不跟随，直接映射为 502
	t.Run("upstream_redirect", func(t *testing.T) {
		ResetUpdateCheckCache()
		t.Cleanup(ResetUpdateCheckCache)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://example.invalid/redirected", http.StatusFound)
		}))
		defer srv.Close()

		t.Setenv("OVH_UPDATE_API", srv.URL)
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.GET("/api/version/check-update", CheckUpdate(nil))

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/version/check-update", nil))
		if w.Code != http.StatusBadGateway {
			t.Fatalf("redirect upstream status = %d, want 502", w.Code)
		}
	})
}

func TestCheckUpdateETag304ReusesSnapshot(t *testing.T) {
	ResetUpdateCheckCache()
	t.Cleanup(ResetUpdateCheckCache)

	var calls atomic.Int32
	var sawIfNoneMatch atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.Header().Set("ETag", `"etag-v0438"`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tag_name":"v0.4.39","name":"v0.4.39","html_url":"https://example.test/releases/v0.4.39","published_at":"2026-11-22T00:00:00Z","body":"notes"}`))
			return
		}
		if r.Header.Get("If-None-Match") == `"etag-v0438"` {
			sawIfNoneMatch.Store(true)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	res1, ok := releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, nil, false)
	if !ok || !res1.ok || res1.release.TagName != "v0.4.39" {
		t.Fatalf("first fetch failed: ok=%v res=%+v", ok, res1)
	}
	// 后台强制刷新应带上 If-None-Match 并命中 304 复用快照
	res2, ok := releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, nil, true)
	if !ok || !res2.ok || res2.release.TagName != "v0.4.39" || !sawIfNoneMatch.Load() {
		t.Fatalf("304 ETag refresh failed: ok=%v sawETag=%v res=%+v", ok, sawIfNoneMatch.Load(), res2)
	}
}

func TestCheckUpdateETagClearedWhenNew200OmitsETagAndUpstreamCancelInFlight(t *testing.T) {
	t.Run("etag_bound_to_snapshot", func(t *testing.T) {
		ResetUpdateCheckCache()
		t.Cleanup(ResetUpdateCheckCache)

		var calls atomic.Int32
		var thirdSawETag atomic.Bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch calls.Add(1) {
			case 1:
				w.Header().Set("ETag", `"old-etag"`)
				_, _ = w.Write([]byte(`{"tag_name":"v0.4.38"}`))
			case 2:
				// 新 200 未携带 ETag，必须清空验证器，绝不沿用旧 ETag
				_, _ = w.Write([]byte(`{"tag_name":"v0.4.39"}`))
			default:
				if r.Header.Get("If-None-Match") != "" {
					thirdSawETag.Store(true)
				}
				// 未发 If-None-Match 时即使上游错误回 304 也不得当作成功复用
				w.WriteHeader(http.StatusNotModified)
			}
		}))
		defer srv.Close()

		_, _ = releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, nil, true)
		r2, _ := releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, nil, true)
		if r2.etag != "" || r2.release.TagName != "v0.4.39" {
			t.Fatalf("second 200 without ETag should clear etag, got %+v", r2)
		}
		// 非 force 过期刷新（清掉 entries 但保留 lastSuccess）验证第 3 次不带旧 ETag 且拒绝无条件 304
		releaseCheckCache.mu.Lock()
		delete(releaseCheckCache.entries, srv.URL)
		releaseCheckCache.mu.Unlock()
		r3, _ := releaseCheckCache.fetch(t.Context(), t.Context(), srv.URL, nil, false)
		if thirdSawETag.Load() {
			t.Fatal("stale ETag leaked after ETag-less 200 response")
		}
		if r3.ok {
			t.Fatalf("unsolicited 304 without sent If-None-Match must fail, got %+v", r3)
		}
	})

	t.Run("upstream_io_canceled_in_flight", func(t *testing.T) {
		ResetUpdateCheckCache()
		t.Cleanup(ResetUpdateCheckCache)

		started := make(chan struct{})
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = releaseCheckCache.fetch(ctx, ctx, srv.URL, nil, true)
		}()

		<-started
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("in-flight upstream update check did not cancel promptly when service ctx canceled")
		}
	})
}

type httpStatusErr struct {
	want int
	got  int
}

func (e *httpStatusErr) Error() string {
	return "unexpected response status or payload"
}

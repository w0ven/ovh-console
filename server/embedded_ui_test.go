package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/gin-gonic/gin"
)

func TestEmbeddedUICacheControlAndMissingAssets404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	distFS := fstest.MapFS{
		"index.html":            {Data: []byte("<!doctype html><html><body>SPA</body></html>")},
		"favicon.svg":           {Data: []byte("<svg></svg>")},
		"assets/app-a1b2c3.js":  {Data: []byte("console.log('ok');")},
		"assets/app-a1b2c3.css": {Data: []byte("body{color:red}")},
	}
	registerEmbeddedUI(r, distFS)

	// 1. 存在的 /assets/* 带内容哈希产物必须带长缓存头
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/assets/app-a1b2c3.js", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("existing asset status = %d, want 200", w.Code)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Fatalf("asset Cache-Control = %q, want immutable", cc)
	}

	// 2. 缺失的 /assets/* 必须返回 404 且 Cache-Control: no-store，不能回落 index.html
	for _, p := range []string{"/assets/", "/assets/server-control-OLDHASH.js", "/assets/no-ext-missing", "/missing-icon.png"} {
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, p, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("missing static path %q status = %d, want 404 (body=%q)", p, w.Code, w.Body.String())
		}
		if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("missing static path %q Cache-Control = %q, want no-store", p, cc)
		}
		if strings.Contains(w.Body.String(), "<html>") {
			t.Fatalf("missing static path %q must not fall back to SPA index.html", p)
		}
	}

	// 4. SPA 客户端路由返回 200 HTML 且 no-cache
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/settings", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("SPA route status = %d, want 200", w.Code)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("SPA index Cache-Control = %q, want no-cache", cc)
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("SPA Content-Type = %q, want text/html", w.Header().Get("Content-Type"))
	}

	// 5. 未命中的 /api/* 保持 JSON 404
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/does-not-exist", nil))
	if w.Code != http.StatusNotFound || !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("missing /api/* = %d %q, want 404 JSON", w.Code, w.Header().Get("Content-Type"))
	}
}

func TestGracefulShutdownStopsRegisteredBackgroundLoops(t *testing.T) {
	t.Cleanup(func() { registerBackgroundLoops(nil) })
	dir := t.TempDir()
	lg := newTestLogger(t, filepath.Join(dir, "app.log.json"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	loopExited := false
	go func() {
		defer close(done)
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond)
		loopExited = true
	}()
	registerBackgroundLoops(cancel, done)

	db := &closeSpy{}
	gracefulShutdown(nil, db, lg, nil, "terminated", time.Second)

	if !loopExited {
		t.Fatal("gracefulShutdown did not wait for registered background loop to exit")
	}
	if !db.closed {
		t.Fatal("db was not closed")
	}
}

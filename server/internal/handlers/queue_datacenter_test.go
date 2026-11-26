package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/ovh-buy/server/internal/catalog"
	"github.com/ovh-buy/server/internal/types"
)

func TestAddQueueRejectsOutOfCatalogDatacenterExceptPreorder(t *testing.T) {
	catalog.ResetCachesForTesting()
	t.Cleanup(catalog.ResetCachesForTesting)
	previous := http.DefaultTransport
	http.DefaultTransport = queuePreorderTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"plans":[{"planCode":"27skle06-v1","configurations":[{"name":"dedicated_datacenter","values":["bhs","fra","gra","lon","rbx","sbg","waw"]}]}]}`
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = previous })

	state, _ := newWatchTestMonitor(t)
	state.Accounts = []types.OVHAccount{{ID: "ie", Endpoint: "ovh-eu", Zone: "IE-QUEUE-DC"}}
	router := gin.New()
	router.POST("/queue", AddQueueItem(state))

	post := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/queue", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		return w
	}

	// 1. 非预售填美区机房 hil:入队闸门直接拦截并给出可选机房列表。
	w := post(`{"account_id":"ie","planCode":"27skle06-v1","datacenter":"hil"}`)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "不在子公司") || !strings.Contains(w.Body.String(), "fra") || len(state.Queue) != 0 {
		t.Fatalf("expected 400 for out-of-catalog datacenter, got %d %s", w.Code, w.Body.String())
	}

	// 2. 填本区目录内的机房 fra / lon:正常入队。
	w = post(`{"account_id":"ie","planCode":"27skle06-v1","datacenters":["fra","lon"]}`)
	if w.Code != http.StatusOK || len(state.Queue) != 2 {
		t.Fatalf("expected 200 for valid datacenters, got %d %s", w.Code, w.Body.String())
	}

	// 3. 预售任务:开售前目录机房列表可能尚未补充,保持放行不提前误杀。
	w = post(`{"account_id":"ie","planCode":"27skle06-v1","datacenter":"tor","preorder":true}`)
	if w.Code != http.StatusOK || len(state.Queue) != 3 {
		t.Fatalf("expected 200 for preorder task, got %d %s", w.Code, w.Body.String())
	}
}

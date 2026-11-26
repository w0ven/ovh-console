package catalog

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/types"
)

func TestClassifyDatacenterInOutUnknownAndFreshRefresh(t *testing.T) {
	resetCatalogCaches()
	original := fetchSubsidiaryCatalog
	t.Cleanup(func() {
		fetchSubsidiaryCatalog = original
		resetCatalogCaches()
	})

	st := testState(t)
	st.Accounts = []types.OVHAccount{{ID: "ie-acc", Endpoint: "ovh-eu", Zone: "IE"}}

	// 初始缓存:27skle06-v1 含 fra/lon/eu-west-par-a/ynm;noDCPlan 未声明机房列表。
	regionCache["IE"] = &subsidiaryCatalog{
		plans: map[string]planConfig{
			"27skle06-v1": {datacenters: []string{"bhs", "fra", "gra", "lon", "eu-west-par-a", "ynm"}},
			"noDCPlan":    {},
		},
		fetchedAt: time.Now().Add(-2 * time.Minute),
	}

	fetches := 0
	includeTor := false
	fetchSubsidiaryCatalog = func(_ *app.State, sub string) (*subsidiaryCatalog, error) {
		fetches++
		dcs := []string{"bhs", "fra", "gra", "lon", "eu-west-par-a", "ynm"}
		if includeTor {
			dcs = append(dcs, "ca-east-tor-a")
		}
		return &subsidiaryCatalog{
			plans: map[string]planConfig{
				"27skle06-v1": {datacenters: dcs},
				"noDCPlan":    {},
			},
			fetchedAt: time.Now(),
		}, nil
	}

	// 1. 命中 2 小时缓存的合法机房(含大小写与前端显示别名 mum/par-a):0 网络刷新。
	for _, dc := range []string{"fra", "LON", "mum", "par-a", "eu-west-par-a"} {
		verdict, dcs := ClassifyDatacenter(st, "ie-acc", "27skle06-v1", dc)
		if verdict != DatacenterVerdictInCatalog || len(dcs) != 6 {
			t.Fatalf("dc=%q: verdict=%v dcs=%v, want InCatalog", dc, verdict, dcs)
		}
	}
	if fetches != 0 {
		t.Fatalf("in-catalog hits must not trigger catalog refresh, got %d fetches", fetches)
	}

	// 2. 真正的跨区机房 hil:短 TTL 刷新核实成功且仍不在列表,确认为 OutOfCatalog。
	verdict, dcs := ClassifyDatacenter(st, "ie-acc", "27skle06-v1", "hil")
	if verdict != DatacenterVerdictOutOfCatalog || len(dcs) != 6 {
		t.Fatalf("hil: verdict=%v dcs=%v, want OutOfCatalog", verdict, dcs)
	}
	if fetches != 1 {
		t.Fatalf("out-of-catalog miss should refresh once via short TTL, got %d", fetches)
	}
	msg := FormatOutOfCatalogDatacenterMessage("IE", "27skle06-v1", "hil", dcs)
	if !strings.Contains(msg, "HIL") || !strings.Contains(msg, "IE") || !strings.Contains(msg, "fra") {
		t.Fatalf("unexpected diagnostic message: %s", msg)
	}

	// 3. OVH 刚在目录里给该型号扩充新机房 ca-east-tor-a,旧缓存(>15s)尚未包含:短 TTL 刷新后应认出 InCatalog,不误杀。
	regionCacheMu.Lock()
	regionCache["IE"].fetchedAt = time.Now().Add(-time.Minute)
	regionCacheMu.Unlock()
	includeTor = true
	verdict, _ = ClassifyDatacenter(st, "ie-acc", "27skle06-v1", "tor")
	if verdict != DatacenterVerdictInCatalog {
		t.Fatalf("newly added tor should resolve to InCatalog after short-TTL refresh, got %v", verdict)
	}

	// 4. R2 回归:型号不在目录 / 目录未声明机房枚举 / 空参数:
	// 即使旧缓存已超过 15s 短 TTL(但未满 2h),多次调用也绝不能触发 12MB 目录刷新,直接返回 Unknown。
	beforeFetches := fetches
	for i := 0; i < 3; i++ {
		regionCacheMu.Lock()
		regionCache["IE"].fetchedAt = time.Now().Add(-20 * time.Minute)
		regionCacheMu.Unlock()
		for _, tc := range []struct{ plan, dc string }{
			{"noDCPlan", "gra"},
			{"unknown-plan", "gra"},
			{"27skle06-v1", ""},
		} {
			if v, _ := ClassifyDatacenter(st, "ie-acc", tc.plan, tc.dc); v != DatacenterVerdictUnknown {
				t.Fatalf("iter=%d plan=%q dc=%q: got %v, want Unknown", i, tc.plan, tc.dc, v)
			}
		}
	}
	if fetches != beforeFetches {
		t.Fatalf("unlisted plan or empty datacenter list triggered %d unexpected catalog refreshes", fetches-beforeFetches)
	}

	// 5. R1 回归:旧缓存(90 分钟前)只含 fra/lon,此时查询新机房 waw 触发短 TTL 核实,
	// 但目录接口返回 503(loadSubsidiaryCatalogWithTTL 回退旧缓存且 err == nil):
	// 必须返回 Unknown,绝不能拿 90 分钟前的过期目录断言 OutOfCatalog(包括 30s 负缓存窗口内的二次调用)。
	resetCatalogCaches()
	regionCache["IE"] = &subsidiaryCatalog{
		plans: map[string]planConfig{
			"27skle06-v1": {datacenters: []string{"fra", "lon"}},
		},
		fetchedAt: time.Now().Add(-90 * time.Minute),
	}
	outageFetches := 0
	fetchSubsidiaryCatalog = func(*app.State, string) (*subsidiaryCatalog, error) {
		outageFetches++
		return nil, errors.New("HTTP 503 catalog unavailable")
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if v, _ := ClassifyDatacenter(st, "ie-acc", "27skle06-v1", "waw"); v != DatacenterVerdictUnknown {
			t.Fatalf("attempt %d during catalog outage with stale cache: got %v, want Unknown (never OutOfCatalog)", attempt, v)
		}
	}
	if outageFetches != 1 {
		t.Fatalf("expected 1 refresh attempt inside negative-cache window, got %d", outageFetches)
	}
	// 与此同时,旧缓存里已明确存在的 fra 仍应正常命中 InCatalog。
	if v, _ := ClassifyDatacenter(st, "ie-acc", "27skle06-v1", "fra"); v != DatacenterVerdictInCatalog {
		t.Fatalf("fra present in stale cache should still return InCatalog, got %v", v)
	}

	// 6. 目录完全不可用且无旧缓存:返回 Unknown。
	resetCatalogCaches()
	if v, _ := ClassifyDatacenter(st, "ie-acc", "27skle06-v1", "fra"); v != DatacenterVerdictUnknown {
		t.Fatalf("catalog outage without cache must return Unknown, got %v", v)
	}
}

package purchase

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/catalog"
	"github.com/ovh-buy/server/internal/types"
)

// 复现线上 27skle06-v1@fra / lon 结账瞬间被抢空场景:
// 机房明确在子公司目录里,checkout 返回 400 "<fqn> is not available in fra" 时:
//  1. 历史文案必须准确提示"已售罄",不得误写为"不在子公司目录"或"请求的参数不对";
//  2. 不计入 FailureCount(Attempted=false, WaitingForStock=true),连续多次擦肩而过也不会触发 MaxRetries 把任务判死;
//  3. 下一波真正有货时同一任务直接成功下单并覆盖历史为 success。
func TestInCatalogCheckoutSoldOutDoesNotBlameCatalogOrBurnRetries(t *testing.T) {
	catalog.ResetCachesForTesting()
	t.Cleanup(catalog.ResetCachesForTesting)
	state, _, checkouts, stock := hotFixture(t)
	stock.Store(true)

	origDefault := http.DefaultTransport
	http.DefaultTransport = preorderTransport(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/order/catalog/public/eco") {
			body := `{"locale":{"currencyCode":"EUR"},"plans":[{"planCode":"plan","configurations":[{"name":"dedicated_datacenter","values":["bhs","fra","gra","lon"]},{"name":"region","values":["europe"]}],"pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":5398000000}]}],"addons":[{"planCode":"ram-32g","pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":0}]}]}`
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		}
		return origDefault.RoundTrip(r)
	})
	t.Cleanup(func() { http.DefaultTransport = origDefault })
	state.Accounts[0].Zone = "DE"

	base, err := state.OVH.ClientFor("hot-eu")
	if err != nil {
		t.Fatal(err)
	}
	origAPITransport := base.Client.Transport
	var soldOut atomic.Bool
	soldOut.Store(true)
	base.Client.Transport = preorderTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/availabilities") {
			body := `[{"fqn":"plan.ram-32g","datacenters":[{"datacenter":"fra","availability":"1H-low"}]}]`
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		}
		if strings.HasSuffix(r.URL.Path, "/checkout") && soldOut.Load() {
			checkouts.Add(1)
			return &http.Response{
				StatusCode: 400,
				Header:     http.Header{"X-Ovh-Query-Id": []string{"EU.ext-2.soldout"}},
				Body:       io.NopCloser(strings.NewReader(`{"message":"plan.ram-32g is not available in fra"}`)),
				Request:    r,
			}, nil
		}
		return origAPITransport.RoundTrip(r)
	})

	item := types.QueueItem{
		ID:         "soldout-fra",
		AccountID:  "hot-eu",
		PlanCode:   "plan",
		Datacenter: "fra",
		Options:    []string{"ram-32g"},
		Status:     "running",
		MaxRetries: 2, // 若误计入 FailureCount,第 2 次就会被判 failed
	}
	state.Queue = []types.QueueItem{item}
	session := &cartSession{}
	defer session.discard()
	ctx := context.WithValue(context.Background(), cartSessionKey{}, session)

	for attempt := 1; attempt <= 3; attempt++ {
		sig := app.StockSignal{
			AccountID:  "hot-eu",
			PlanCode:   "plan",
			DetectedAt: time.Now(),
			Variants:   []map[string]interface{}{{"fqn": "plan.ram-32g", "datacenters": []interface{}{map[string]interface{}{"datacenter": "fra", "availability": "1H-low"}}}},
		}
		out := runQueueAttempt(context.WithValue(ctx, stockSignalKey{}, sig), state, item, PurchaseServer)
		if out.Fatal || out.Attempted || !out.WaitingForStock {
			t.Fatalf("attempt %d: unexpected outcome %+v", attempt, out)
		}
		if state.Queue[0].Status != "running" || state.Queue[0].FailureCount != 0 {
			t.Fatalf("attempt %d: sold-out race burned retry budget: status=%s failureCount=%d", attempt, state.Queue[0].Status, state.Queue[0].FailureCount)
		}
	}

	if len(state.History) != 1 || state.History[0].ErrorMessage == nil {
		t.Fatalf("expected 1 history entry, got %+v", state.History)
	}
	errText := *state.History[0].ErrorMessage
	if !strings.Contains(errText, "已售罄") || !strings.Contains(errText, "DE") || !strings.Contains(errText, "plan.ram-32g is not available in fra") {
		t.Fatalf("history missing sold-out explanation or original message: %s", errText)
	}
	if strings.Contains(errText, "不在子公司") || strings.Contains(errText, "请求的参数不对") {
		t.Fatalf("history still blames wrong catalog/parameters for in-catalog sold-out: %s", errText)
	}

	// 第 4 轮真正抢到库存:任务正常成交并把同一条历史升级为 success。
	soldOut.Store(false)
	out := runQueueAttempt(ctx, state, item, PurchaseServer)
	if !out.Success || state.Queue[0].Status != "completed" {
		t.Fatalf("expected order completion after restock, got out=%+v queue=%+v", out, state.Queue[0])
	}
	waitWarm(t, func() bool {
		rows, err := state.DB.ListHistory()
		return err == nil && len(rows) == 1 && rows[0].Status == "success" && rows[0].OrderStatus != ""
	})
}

// 真正的跨区/目录外机房(如在欧区子公司下填美区 hil):
// 无论在结账 400 阶段、加购建车阶段还是在库存列表缺失阶段,都应准确指出机房不在子公司目录并列明可选机房,且直接判 Fatal 停止无效重试。
func TestOutOfCatalogDatacenterStopsImmediatelyWithAllowedList(t *testing.T) {
	for _, tc := range []struct {
		name, zone string
		stage      string
	}{
		{"no-stock-feed", "NL", "stock"},
		{"cart-eco-rejected", "ES", "eco"},
		{"checkout-rejected", "IT", "checkout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog.ResetCachesForTesting()
			t.Cleanup(catalog.ResetCachesForTesting)
			state, _, checkouts, stock := hotFixture(t)
			stock.Store(true)

			origDefault := http.DefaultTransport
			http.DefaultTransport = preorderTransport(func(r *http.Request) (*http.Response, error) {
				if strings.Contains(r.URL.Path, "/order/catalog/public/eco") {
					body := `{"locale":{"currencyCode":"EUR"},"plans":[{"planCode":"plan","configurations":[{"name":"dedicated_datacenter","values":["fra","gra","lon"]},{"name":"region","values":["europe"]}],"pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":5398000000}]}],"addons":[{"planCode":"ram-32g","pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":0}]}]}`
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				}
				return origDefault.RoundTrip(r)
			})
			t.Cleanup(func() { http.DefaultTransport = origDefault })
			state.Accounts[0].Zone = tc.zone

			base, err := state.OVH.ClientFor("hot-eu")
			if err != nil {
				t.Fatal(err)
			}
			origAPITransport := base.Client.Transport
			base.Client.Transport = preorderTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/availabilities") {
					dc := "fra"
					if tc.stage != "stock" {
						dc = "hil"
					}
					body := `[{"fqn":"plan.ram-32g","datacenters":[{"datacenter":"` + dc + `","availability":"1H-low"}]}]`
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				}
				if tc.stage == "eco" && r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/eco") {
					return &http.Response{
						StatusCode: 400,
						Header:     http.Header{"X-Ovh-Query-Id": []string{"EU.ext-2.eco"}},
						Body:       io.NopCloser(strings.NewReader(`{"message":"plan is not available in hil"}`)),
						Request:    r,
					}, nil
				}
				if strings.HasSuffix(r.URL.Path, "/checkout") {
					checkouts.Add(1)
					return &http.Response{
						StatusCode: 400,
						Header:     http.Header{"X-Ovh-Query-Id": []string{"EU.ext-2.crossdc"}},
						Body:       io.NopCloser(strings.NewReader(`{"message":"plan.ram-32g is not available in hil"}`)),
						Request:    r,
					}, nil
				}
				return origAPITransport.RoundTrip(r)
			})

			item := types.QueueItem{
				ID:         "cross-hil-" + tc.name,
				AccountID:  "hot-eu",
				PlanCode:   "plan",
				Datacenter: "hil",
				Options:    []string{"ram-32g"},
				Status:     "running",
			}
			state.Queue = []types.QueueItem{item}
			out := runQueueAttempt(context.Background(), state, item, PurchaseServer)
			if !out.Fatal || state.Queue[0].Status != "failed" {
				t.Fatalf("expected immediate Fatal termination for cross-region datacenter, got out=%+v status=%s", out, state.Queue[0].Status)
			}
			if len(state.History) != 1 || state.History[0].ErrorMessage == nil {
				t.Fatalf("expected 1 failure history entry, got %+v", state.History)
			}
			msg := *state.History[0].ErrorMessage
			if !strings.Contains(msg, "不在子公司") || !strings.Contains(msg, tc.zone) || !strings.Contains(msg, "HIL") || !strings.Contains(msg, "fra, gra, lon") {
				t.Fatalf("expected out-of-catalog explanation with allowed datacenters, got: %s", msg)
			}
		})
	}
}

// R3 & S2 回归:
//  1. 无货预装阶段(!foundAvailable)若购物车校验偶发返回 Fatal(例如空 items),仍折叠为 WaitingForStock,绝不提前误杀任务;
//  2. 预售任务(Preorder=true)结账遇到 400 "is not available in <dc>" 时保持 WaitingForCatalog 且 Reason 中立,不误报"已售罄"或"不在目录"。
func TestStandbyPreparationAndPreorderKeepOriginalSafetySemantics(t *testing.T) {
	t.Run("standby-preparation-folds-cart-errors-to-waiting-stock", func(t *testing.T) {
		catalog.ResetCachesForTesting()
		t.Cleanup(catalog.ResetCachesForTesting)
		state, _, _, stock := hotFixture(t)
		stock.Store(false) // 无货预装阶段
		state.Accounts[0].Zone = "PT"

		origDefault := http.DefaultTransport
		http.DefaultTransport = preorderTransport(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/order/catalog/public/eco") {
				body := `{"locale":{"currencyCode":"EUR"},"plans":[{"planCode":"plan","configurations":[{"name":"dedicated_datacenter","values":["gra"]}],"pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":3998000000}]}],"addons":[{"planCode":"ram-32g","pricings":[{"mode":"default","intervalUnit":"month","interval":1,"price":0}]}]}`
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			}
			return origDefault.RoundTrip(r)
		})
		t.Cleanup(func() { http.DefaultTransport = origDefault })

		base, err := state.OVH.ClientFor("hot-eu")
		if err != nil {
			t.Fatal(err)
		}
		origAPI := base.Client.Transport
		base.Client.Transport = preorderTransport(func(r *http.Request) (*http.Response, error) {
			// 模拟 OVH 在无货预装核验购物车时偶发返回空 items(VerifyCart 返回 ErrCartPeriod -> prepareCart 返回 Fatal)
			if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/order/cart/cart-") && strings.Count(r.URL.Path, "/") == 3 {
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"cartId":"cart-1","items":[]}`)), Request: r}, nil
			}
			return origAPI.RoundTrip(r)
		})

		item := types.QueueItem{ID: "standby-safe", AccountID: "hot-eu", PlanCode: "plan", Datacenter: "gra", Options: []string{"ram-32g"}, Status: "running"}
		state.Queue = []types.QueueItem{item}
		session := &cartSession{}
		defer session.discard()
		out := runQueueAttempt(context.WithValue(context.Background(), cartSessionKey{}, session), state, item, PurchaseServer)
		if out.Fatal || !out.WaitingForStock || state.Queue[0].Status != "running" || len(state.History) != 0 {
			t.Fatalf("standby preparation killed waiting task or polluted history: out=%+v status=%s history=%+v", out, state.Queue[0].Status, state.History)
		}
	})

	var soldoutPreorderFixtureSeq atomic.Int32

	t.Run("preorder-checkout-400-stays-waiting-for-catalog-with-neutral-reason", func(t *testing.T) {
		catalog.ResetCachesForTesting()
		t.Cleanup(catalog.ResetCachesForTesting)
		idx := 2000 + int(soldoutPreorderFixtureSeq.Add(1))
		state, item, _, checkouts, _ := preorderCheckoutFixture(t, preorderCheckoutCase{checkoutFailure: 400}, idx)
		base, err := state.OVH.ClientFor(item.AccountID)
		if err != nil {
			t.Fatal(err)
		}
		origAPI := base.Client.Transport
		base.Client.Transport = preorderTransport(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/checkout") && r.Method == "POST" {
				checkouts.Add(1)
				return &http.Response{
					StatusCode: 400,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(`{"message":"27skle03-v1-mum is not available in ynm"}`)),
					Request:    r,
				}, nil
			}
			return origAPI.RoundTrip(r)
		})

		session := &cartSession{}
		defer session.discard()
		out := PurchaseServer(context.WithValue(context.Background(), cartSessionKey{}, session), state, &item)
		if !out.WaitingForCatalog || out.Fatal || out.Attempted || checkouts.Load() != 1 {
			t.Fatalf("unexpected preorder outcome: %+v checkouts=%d", out, checkouts.Load())
		}
		if strings.Contains(out.Reason, "等待下一波放货") || strings.Contains(out.Reason, "不在子公司") || strings.Contains(out.Reason, "请求的参数不对") {
			t.Fatalf("preorder waiting reason should stay neutral, got: %s", out.Reason)
		}
	})
}

package price

import (
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ovh-buy/server/internal/app"
	"github.com/ovh-buy/server/internal/catalog"
	"github.com/ovh-buy/server/internal/config"
	"github.com/ovh-buy/server/internal/db"
	"github.com/ovh-buy/server/internal/logger"
	"github.com/ovh-buy/server/internal/storage"
	"github.com/ovh-buy/server/internal/types"
	ovhsdk "github.com/ovh/go-ovh/ovh"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExplainPriceDatacenterUnavailableSeparatesSoldOutFromOutOfCatalog(t *testing.T) {
	catalog.ResetCachesForTesting()
	t.Cleanup(catalog.ResetCachesForTesting)
	dir := t.TempDir()
	database, err := db.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	lg := logger.New(filepath.Join(dir, "price.log.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(lg.Close)
	st := app.NewState(storage.Paths{DataDir: dir}, config.New(database), lg, database)
	st.Accounts = []types.OVHAccount{{ID: "ie", Endpoint: "ovh-eu", Zone: "IE-PRICE-TEST"}}

	orig := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"locale":{"currencyCode":"EUR"},"plans":[{"planCode":"27skle06-v1","configurations":[{"name":"dedicated_datacenter","values":["fra","gra","lon"]}]}]}`
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = orig })

	apiErr := &ovhsdk.APIError{Code: 400, Message: "27skle06-v1.ram-128g is not available in fra"}
	inCat := explainPriceDatacenterUnavailable(st, "ie", "IE-PRICE-TEST", "27skle06-v1", "fra", apiErr)
	if inCat.Success || inCat.Transient || !strings.Contains(inCat.Error, "已售罄") || strings.Contains(inCat.Error, "不在子公司") {
		t.Fatalf("unexpected in-catalog price result: %+v", inCat)
	}

	outCat := explainPriceDatacenterUnavailable(st, "ie", "IE-PRICE-TEST", "27skle06-v1", "hil",
		&ovhsdk.APIError{Code: 400, Message: "27skle06-v1.ram-128g is not available in hil"})
	if outCat.Success || outCat.Transient || !strings.Contains(outCat.Error, "不在子公司") || !strings.Contains(outCat.Error, "fra, gra, lon") {
		t.Fatalf("unexpected out-of-catalog price result: %+v", outCat)
	}

	unknown := explainPriceDatacenterUnavailable(st, "ie", "IE-PRICE-TEST", "unlisted-plan", "fra", apiErr)
	if unknown.Success || unknown.Transient || unknown.Error != "该配置在指定数据中心不可用" {
		t.Fatalf("unexpected unknown-catalog price result: %+v", unknown)
	}
}

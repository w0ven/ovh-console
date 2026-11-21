package handlers

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestPairURLSchemeDetection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { SetTrustedProxyChecker(nil) })

	makeCtx := func(remoteAddr string, tlsOn bool, xfProto string) *gin.Context {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := httptest.NewRequest(http.MethodPost, "http://ovh.example.com/api/app/pairing-codes", nil)
		req.Host = "ovh.example.com"
		req.RemoteAddr = remoteAddr
		if tlsOn {
			req.TLS = &tls.ConnectionState{}
		}
		if xfProto != "" {
			req.Header.Set("X-Forwarded-Proto", xfProto)
		}
		c.Request = req
		return c
	}

	// 1. 未配置可信反代时，即使伪造 X-Forwarded-Proto: https 也必须忽略，返回 http
	SetTrustedProxyChecker(nil)
	if got := pairURL(makeCtx("203.0.113.5:54321", false, "https"), "ABCD1234"); got != "http://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("untrusted peer with spoofed X-Forwarded-Proto should stay http, got %q", got)
	}

	// 2. 配置可信反代后：非可信直连仍忽略 X-Forwarded-Proto；可信反代则采信
	SetTrustedProxyChecker(func(ip string) bool { return ip == "172.18.0.1" })
	if got := pairURL(makeCtx("203.0.113.5:54321", false, "https"), "ABCD1234"); got != "http://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("non-matching peer with spoofed X-Forwarded-Proto should stay http, got %q", got)
	}
	if got := pairURL(makeCtx("172.18.0.1:41234", false, "https"), "ABCD1234"); got != "https://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("trusted reverse proxy https failed: %q", got)
	}
	if got := pairURL(makeCtx("172.18.0.1:41234", false, "https, http"), "ABCD1234"); got != "https://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("multi-hop X-Forwarded-Proto from trusted proxy failed: %q", got)
	}
	if got := pairURL(makeCtx("172.18.0.1:41234", false, ""), "ABCD1234"); got != "http://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("local plain http failed: %q", got)
	}
	// 3. TLS 直连始终返回 https
	if got := pairURL(makeCtx("203.0.113.5:54321", true, ""), "ABCD1234"); got != "https://ovh.example.com/api/app/pair#ABCD1234" {
		t.Fatalf("direct tls failed: %q", got)
	}
}

package auth

import "github.com/gin-gonic/gin"

// SecurityHeaders adds baseline response hardening headers across UI and API responses.
// Intentionally omits CSP (index.html uses an inline theme bootstrap script) and COOP
// (OfficialChat relies on the window handle returned by window.open).
func SecurityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Next()
	}
}

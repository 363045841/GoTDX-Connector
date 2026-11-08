// 本文件验证 CORS 中间件对公开页面访问本机（Private Network Access）的放行。
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/v1"
)

// TestPrivateNetworkAccessPreflight 验证 PNA 预检响应带允许头。
func TestPrivateNetworkAccessPreflight(t *testing.T) {
	router := NewRouter(v1.Deps{Status: func() client.Status { return client.Status{} }})
	request := httptest.NewRequest(http.MethodOptions, "/api/v1/market-data/bars", nil)
	request.Header.Set("Origin", "https://363045841.github.io")
	request.Header.Set("Access-Control-Request-Method", "POST")
	request.Header.Set("Access-Control-Request-Private-Network", "true")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if got := response.Header().Get("Access-Control-Allow-Private-Network"); got != "true" {
		t.Fatalf("Access-Control-Allow-Private-Network = %q, want true", got)
	}
}

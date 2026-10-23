// 本文件测试服务存活与就绪健康检查接口的状态响应。
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/v1"
)

// 验证存活探针不依赖上游状态且始终成功。
func TestLivenessAlwaysReturnsOK(t *testing.T) {
	router := NewRouter(v1.Deps{Status: func() client.Status { return client.Status{} }})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
}

// 验证任一行情域不可用时就绪探针返回服务不可用。
func TestReadinessReturnsServiceUnavailableWhenAnyDomainIsDown(t *testing.T) {
	status := client.Status{Ready: false, Domains: map[client.Domain]client.DomainStatus{
		client.DomainMain: {Ready: true},
		client.DomainEx:   {Ready: false, LastError: "unavailable"},
	}}
	router := NewRouter(v1.Deps{Status: func() client.Status { return status }})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
}

// 验证全部行情域就绪时就绪探针成功。
func TestReadinessReturnsOKWhenAllDomainsAreReady(t *testing.T) {
	status := client.Status{Ready: true, Domains: map[client.Domain]client.DomainStatus{
		client.DomainMain: {Ready: true},
		client.DomainEx:   {Ready: true},
		client.DomainMAC:  {Ready: true},
	}}
	router := NewRouter(v1.Deps{Status: func() client.Status { return status }})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
}

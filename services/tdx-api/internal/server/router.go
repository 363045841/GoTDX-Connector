// HTTP 服务装配：健康检查与 V1 路由；所有依赖由 main 构造后注入。
package server

import (
	"log"
	"net/http"
	"runtime/debug"
	"strings"

	"KlineChartQuantGo/services/tdx-api/internal/v1"
	"github.com/gin-gonic/gin"
)

// recoveryMiddleware 捕获处理链 panic，按 V1 契约返回错误并记录请求 ID。
func recoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[panic] request_id=%s %v\n%s", v1.RequestID(c), rec, debug.Stack())
				if strings.HasPrefix(c.Request.URL.Path, "/api/v1/") {
					v1.WriteInternalError(c)
					return
				}
				c.AbortWithStatusJSON(500, gin.H{"error": "internal server error"})
			}
		}()
		c.Next()
	}
}

// corsMiddleware 当前为本地开发放开跨域；收紧策略时在此处集中处理。
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-Request-Id")
		// Chrome 对公开页面访问本机的请求会先发 PNA 预检，缺少该头会被拦截。
		if c.GetHeader("Access-Control-Request-Private-Network") == "true" {
			c.Header("Access-Control-Allow-Private-Network", "true")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

// NewRouter 组装 HTTP 路由：请求 ID、panic 恢复、健康检查与 V1 行情协议。
func NewRouter(deps v1.Deps) *gin.Engine {
	r := gin.New()
	r.Use(v1.RequestIDMiddleware(), recoveryMiddleware(), corsMiddleware())
	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/health/ready", func(c *gin.Context) {
		current := deps.Status()
		code := http.StatusOK
		if !current.Ready {
			code = http.StatusServiceUnavailable
		}
		c.JSON(code, current)
	})

	v1.RegisterRoutes(r, deps)

	return r
}

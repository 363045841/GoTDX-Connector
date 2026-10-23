// tdx-api 进程入口：读取配置、装配资源、启动 HTTP 服务并统一关闭。
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/config"
	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/live"
	"KlineChartQuantGo/services/tdx-api/internal/server"
	"KlineChartQuantGo/services/tdx-api/internal/v1"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := config.Load()
	gin.SetMode(gin.ReleaseMode)

	manager := client.NewDefaultManager(client.Options{
		AutoSelect: cfg.AutoSelect,
		MainHosts:  cfg.MainHosts,
		ExHosts:    cfg.ExHosts,
		MACHosts:   cfg.MACHosts,
	})
	if err := manager.Start(); err != nil {
		log.Printf("gotdx started in degraded mode: %v", err)
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone, err := client.StartHeartbeat(
		heartbeatCtx, manager, client.DefaultHeartbeatInterval, client.DefaultHeartbeatFailureThreshold,
	)
	if err != nil {
		log.Fatalf("unable to start gotdx heartbeat: %v", err)
	}

	var (
		store       directory.Store
		storeCloser io.Closer
	)
	if opened, openErr := directory.NewSQLiteStore(cfg.SymbolDBPath); openErr != nil {
		log.Printf("symbol directory database disabled: %v", openErr)
	} else {
		store, storeCloser = opened, opened
	}
	cache := directory.NewCache(directory.GotdxLoader{Manager: manager}, store, 0)
	warmupDone := make(chan struct{})
	go func() {
		defer close(warmupDone)
		if err := cache.WarmUp(); err != nil {
			log.Printf("symbol directory: warmup failed: %v", err)
		}
	}()

	fetch := domain.NewFetcher(manager)
	hub := live.NewHub(live.Config{
		OpenInterval:     cfg.LiveInterval,
		MaxSubscriptions: cfg.LiveMaxStreams,
		MaxSeries:        cfg.LiveMaxSeries,
	})
	router := server.NewRouter(v1.Deps{
		Fetch:  fetch,
		Cache:  cache,
		Status: manager.Status,
		Hub:    hub,
		Probe: func() error {
			_, probeErr := client.ProbeMain(manager, func(c client.MainQuerier) (*proto.HeartBeatReply, error) {
				return c.GetServerHeartbeat()
			})
			return probeErr
		},
	})

	addr := fmt.Sprintf(":%s", cfg.Port)
	// 请求上下文派生自 baseContext：Shutdown 开始时取消，使长连接 SSE 立即退出。
	baseContext, cancelBase := context.WithCancel(context.Background())
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           router,
		BaseContext:       func(net.Listener) context.Context { return baseContext },
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	httpServer.RegisterOnShutdown(cancelBase)
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- httpServer.ListenAndServe() }()
	log.Printf("starting gotdx server on http://127.0.0.1%s", addr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-signals:
		log.Printf("received %s, shutting down", sig)
	case serveErr := <-serverErrors:
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("server error: %v", serveErr)
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- httpServer.Shutdown(shutdownCtx) }()

	hub.Close()
	stopHeartbeat()
	select {
	case <-heartbeatDone:
	case <-shutdownCtx.Done():
		log.Printf("heartbeat shutdown timed out: %v", shutdownCtx.Err())
	}
	if err := <-shutdownDone; err != nil {
		log.Printf("server shutdown failed: %v", err)
	}
	select {
	case <-warmupDone:
		if storeCloser != nil {
			if err := storeCloser.Close(); err != nil {
				log.Printf("symbol directory close failed: %v", err)
			}
		}
	case <-shutdownCtx.Done():
		log.Printf("symbol directory warmup still running; database left open")
	}
	if err := manager.CloseContext(shutdownCtx); err != nil {
		log.Printf("gotdx close failed: %v", err)
	}
}

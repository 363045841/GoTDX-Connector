package client

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/bensema/gotdx/proto"
)

const (
	DefaultHeartbeatInterval         = 30 * time.Second
	DefaultHeartbeatFailureThreshold = 3
)

type heartbeatMonitor struct {
	failureThreshold    int
	consecutiveFailures int
	heartbeat           func() error
	reprobe             func() error
}

func newHeartbeatMonitor(failureThreshold int, heartbeat, reprobe func() error) *heartbeatMonitor {
	return &heartbeatMonitor{
		failureThreshold: failureThreshold,
		heartbeat:        heartbeat,
		reprobe:          reprobe,
	}
}

func (monitor *heartbeatMonitor) check() {
	if err := monitor.heartbeat(); err == nil {
		monitor.consecutiveFailures = 0
		return
	} else {
		monitor.consecutiveFailures++
		log.Printf("[gotdx] heartbeat failed (%d/%d): %v", monitor.consecutiveFailures, monitor.failureThreshold, err)
	}

	if monitor.consecutiveFailures < monitor.failureThreshold {
		return
	}
	if err := monitor.reprobe(); err != nil {
		log.Printf("[gotdx] heartbeat re-probe failed: %v", err)
		return
	}
	monitor.consecutiveFailures = 0
}

func runHeartbeat(ctx context.Context, ticks <-chan time.Time, monitor *heartbeatMonitor, maintain func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			monitor.check()
			maintain()
		}
	}
}

// StartHeartbeat 启动 main 行情连接的定时保活，连续失败达到阈值后重新探测连接。
func StartHeartbeat(ctx context.Context, manager *Manager, interval time.Duration, failureThreshold int) (<-chan struct{}, error) {
	if interval <= 0 {
		return nil, errors.New("heartbeat interval must be positive")
	}
	if failureThreshold <= 0 {
		return nil, errors.New("heartbeat failure threshold must be positive")
	}

	var failedGeneration uint64
	monitor := newHeartbeatMonitor(failureThreshold, func() error {
		failedGeneration = manager.generation(DomainMain)
		_, err := ProbeMain(manager, func(c MainQuerier) (*proto.HeartBeatReply, error) {
			return c.GetServerHeartbeat()
		})
		return err
	}, func() error { return manager.reconnectGeneration(DomainMain, failedGeneration) })
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ticker.Stop()
		runHeartbeat(ctx, ticker.C, monitor, func() {
			manager.recoverUnready(DomainEx, DomainMAC)
		})
	}()
	return done, nil
}

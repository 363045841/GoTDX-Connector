// 本文件测试行情客户端心跳监控、重新探测和停止行为。
package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 验证心跳连续失败达到阈值后触发重新探测。
func TestHeartbeatMonitorReprobesAfterConsecutiveFailures(t *testing.T) {
	heartbeatErr := errors.New("heartbeat failed")
	reprobeCalls := 0
	monitor := newHeartbeatMonitor(3, func() error {
		return heartbeatErr
	}, func() error {
		reprobeCalls++
		return nil
	})

	monitor.check()
	monitor.check()
	if reprobeCalls != 0 {
		t.Fatalf("reprobe called before threshold: %d", reprobeCalls)
	}

	monitor.check()
	if reprobeCalls != 1 {
		t.Fatalf("reprobe calls = %d, want 1", reprobeCalls)
	}
}

// 验证成功心跳重置连续失败计数。
func TestHeartbeatMonitorSuccessResetsFailureCount(t *testing.T) {
	results := []error{errors.New("first"), errors.New("second"), nil, errors.New("third"), errors.New("fourth")}
	reprobeCalls := 0
	monitor := newHeartbeatMonitor(3, func() error {
		err := results[0]
		results = results[1:]
		return err
	}, func() error {
		reprobeCalls++
		return nil
	})

	for range 5 {
		monitor.check()
	}
	if reprobeCalls != 0 {
		t.Fatalf("reprobe calls = %d, want 0", reprobeCalls)
	}
}

// 验证重新探测失败后仍保留失败阈值状态。
func TestHeartbeatMonitorReprobeFailureKeepsThresholdReached(t *testing.T) {
	reprobeCalls := 0
	monitor := newHeartbeatMonitor(2, func() error {
		return errors.New("heartbeat failed")
	}, func() error {
		reprobeCalls++
		return errors.New("reprobe failed")
	})

	monitor.check()
	monitor.check()
	monitor.check()
	if reprobeCalls != 2 {
		t.Fatalf("reprobe calls = %d, want 2", reprobeCalls)
	}
}

// 验证重新探测成功后重置连续失败计数。
func TestHeartbeatMonitorReprobeSuccessResetsFailureCount(t *testing.T) {
	reprobeCalls := 0
	monitor := newHeartbeatMonitor(2, func() error {
		return errors.New("heartbeat failed")
	}, func() error {
		reprobeCalls++
		return nil
	})

	monitor.check()
	monitor.check()
	monitor.check()
	if reprobeCalls != 1 {
		t.Fatalf("reprobe calls = %d, want 1", reprobeCalls)
	}
}

// 验证心跳循环在上下文取消后停止。
func TestRunHeartbeatStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	done := make(chan struct{})
	monitor := newHeartbeatMonitor(3, func() error { return nil }, func() error { return nil })

	go func() {
		runHeartbeat(ctx, ticks, monitor, func() {})
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat runner did not stop after context cancellation")
	}
}

// 验证心跳启动拒绝无效的间隔和失败阈值配置。
func TestStartHeartbeatRejectsInvalidConfiguration(t *testing.T) {
	manager := NewDefaultManager(Options{})
	t.Cleanup(func() { _ = manager.Close() })
	ctx := context.Background()
	if _, err := StartHeartbeat(ctx, manager, 0, 3); err == nil {
		t.Fatal("StartHeartbeat accepted zero interval")
	}
	if _, err := StartHeartbeat(ctx, manager, time.Second, 0); err == nil {
		t.Fatal("StartHeartbeat accepted zero failure threshold")
	}
}

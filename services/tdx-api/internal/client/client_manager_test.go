// 本文件测试行情客户端管理器的并发、重连、重试和关闭行为。
package client

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bensema/gotdx"
	gotdxtypes "github.com/bensema/gotdx/types"
)

// testManager 创建可统计客户端构建次数的测试管理器。
func testManager(t *testing.T, domains ...Domain) (*Manager, map[Domain]*atomic.Int32) {
	t.Helper()
	builds := make(map[Domain]*atomic.Int32, len(domains))
	configs := make(map[Domain]domainConfig, len(domains))
	for _, domain := range domains {
		counter := &atomic.Int32{}
		builds[domain] = counter
		configs[domain] = domainConfig{
			build: func() *gotdx.Client {
				counter.Add(1)
				return gotdx.New()
			},
			connect: func(*gotdx.Client) error { return nil },
			close:   func(*gotdx.Client) error { return nil },
		}
	}
	manager := newManager(configs)
	if err := manager.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager, builds
}

// 验证解析 panic 释放租约、不触发重连，并允许后续请求和关闭完成。
func TestOperationPanicReleasesLeaseWithoutReconnect(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	done := make(chan error, 1)
	go func() {
		_, err := execute(manager, DomainMain, func(*gotdx.Client) (int, error) {
			panic("slice bounds out of range")
		})
		if !errors.Is(err, ErrMalformedReply) {
			done <- fmt.Errorf("panic error = %v, want ErrMalformedReply", err)
			return
		}
		// 坏帧是整包读完后解析失败，连接仍在帧边界，不应重建连接。
		if builds[DomainMain].Load() != 1 {
			done <- fmt.Errorf("builds = %d, want no reconnect on malformed reply", builds[DomainMain].Load())
			return
		}
		value, err := execute(manager, DomainMain, func(*gotdx.Client) (int, error) { return 42, nil })
		if err != nil || value != 42 {
			done <- fmt.Errorf("subsequent query = %d, %v", value, err)
			return
		}
		done <- manager.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("panic recovery, subsequent query or close blocked")
	}
}

// 验证重连等待正在执行的客户端租约释放。
func TestReconnectWaitsForActiveLease(t *testing.T) {
	manager, _ := testManager(t, DomainMain)
	started := make(chan struct{})
	release := make(chan struct{})
	requestDone := make(chan struct{})
	go func() {
		_, _ = execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
			close(started)
			<-release
			return struct{}{}, nil
		})
		close(requestDone)
	}()
	<-started

	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- manager.Reconnect(DomainMain) }()
	select {
	case err := <-reconnectDone:
		t.Fatalf("Reconnect completed while lease was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-requestDone
	select {
	case err := <-reconnectDone:
		if err != nil {
			t.Fatalf("Reconnect failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Reconnect did not complete after lease release")
	}
}

// 验证并发请求失败时仅重连一次客户端代际。
func TestConcurrentFailuresReconnectOneClientGeneration(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	initialGeneration := manager.generation(DomainMain)
	const workers = 8
	start := make(chan struct{})
	var failures atomic.Int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			_ = manager.reconnectGeneration(DomainMain, initialGeneration)
			failures.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if got := builds[DomainMain].Load(); got != 2 {
		t.Fatalf("client builds = %d, want initial + one replacement", got)
	}
	if failures.Load() != workers {
		t.Fatalf("workers completed = %d, want %d", failures.Load(), workers)
	}
}

// 验证并发冷启动请求只创建一个客户端。
func TestConcurrentColdRequestsCreateOneClient(t *testing.T) {
	var builds atomic.Int32
	manager := newManager(map[Domain]domainConfig{
		DomainMain: {
			build: func() *gotdx.Client {
				builds.Add(1)
				return gotdx.New()
			},
			connect: func(*gotdx.Client) error { return nil },
			close:   func(*gotdx.Client) error { return nil },
		},
	})
	t.Cleanup(func() { _ = manager.Close() })

	const workers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			_, _ = execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
				return struct{}{}, nil
			})
		}()
	}
	close(start)
	wg.Wait()
	if got := builds.Load(); got != 1 {
		t.Fatalf("client builds = %d, want 1", got)
	}
}

// 验证主站、扩展和MAC行情域独立重连。
func TestDomainsReconnectIndependently(t *testing.T) {
	manager, builds := testManager(t, DomainMain, DomainEx, DomainMAC)
	if err := manager.Reconnect(DomainEx); err != nil {
		t.Fatalf("Reconnect ex failed: %v", err)
	}
	if builds[DomainMain].Load() != 1 || builds[DomainEx].Load() != 2 || builds[DomainMAC].Load() != 1 {
		t.Fatalf("unexpected builds: main=%d ex=%d mac=%d", builds[DomainMain].Load(), builds[DomainEx].Load(), builds[DomainMAC].Load())
	}
}

// 验证可恢复请求失败仅重试一次。
func TestExecuteRetriesRecoverableFailureOnce(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	var calls int
	result, err := execute(manager, DomainMain, func(*gotdx.Client) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("connection is nil")
		}
		return "ok", nil
	})
	if err != nil || result != "ok" {
		t.Fatalf("execute result=%q err=%v", result, err)
	}
	if calls != 2 || builds[DomainMain].Load() != 2 {
		t.Fatalf("calls=%d builds=%d, want 2 and 2", calls, builds[DomainMain].Load())
	}
}

// 验证包装的 gotdx 断连错误被识别为可恢复错误。
func TestExecuteNormalizesWrappedGotdxDisconnectedError(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	var calls int
	_, err := execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
		calls++
		if calls == 1 {
			return struct{}{}, fmt.Errorf("query failed: %w", errors.New("connection is nil"))
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if calls != 2 || builds[DomainMain].Load() != 2 {
		t.Fatalf("calls=%d builds=%d, want 2 and 2", calls, builds[DomainMain].Load())
	}
}

// 验证协议或数据错误不触发客户端重试。
func TestExecuteDoesNotRetryProtocolDataFailure(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	var calls int
	_, err := execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
		calls++
		return struct{}{}, errors.New("invalid kline datetime")
	})
	if err == nil {
		t.Fatal("execute succeeded")
	}
	if calls != 1 || builds[DomainMain].Load() != 1 {
		t.Fatalf("calls=%d builds=%d, want 1 and 1", calls, builds[DomainMain].Load())
	}
}

// 验证 gotdx 协议帧异常会重建客户端，避免复用已失去帧边界的连接。
func TestExecuteRetriesBadDataAfterReconnect(t *testing.T) {
	manager, builds := testManager(t, DomainMain)
	var calls int
	result, err := execute(manager, DomainMain, func(*gotdx.Client) (string, error) {
		calls++
		if calls == 1 {
			return "", gotdxtypes.ErrBadData
		}
		return "ok", nil
	})
	if err != nil || result != "ok" {
		t.Fatalf("execute result=%q err=%v", result, err)
	}
	if calls != 2 || builds[DomainMain].Load() != 2 {
		t.Fatalf("calls=%d builds=%d, want 2 and 2", calls, builds[DomainMain].Load())
	}
}

// 验证关闭后的客户端管理器拒绝新请求。
func TestCloseRejectsNewRequests(t *testing.T) {
	manager, _ := testManager(t, DomainMain)
	if err := manager.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	_, err := execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("execute error = %v, want ErrManagerClosed", err)
	}
}

// 验证关闭等待活动客户端租约完成。
func TestCloseWaitsForActiveLease(t *testing.T) {
	manager, _ := testManager(t, DomainMain)
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
			close(started)
			<-release
			return struct{}{}, nil
		})
	}()
	<-started

	done := make(chan error, 1)
	go func() { done <- manager.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close completed while lease was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not complete after lease release")
	}
}

// 验证状态检查要求每个已配置行情域均就绪。
func TestStatusRequiresEveryConfiguredDomain(t *testing.T) {
	manager, _ := testManager(t, DomainMain, DomainEx, DomainMAC)
	if status := manager.Status(); !status.Ready {
		t.Fatalf("started manager is not ready: %+v", status)
	}
	manager.slots[DomainEx].mu.Lock()
	manager.slots[DomainEx].ready = false
	manager.slots[DomainEx].lastError = "ex unavailable"
	manager.slots[DomainEx].mu.Unlock()
	status := manager.Status()
	if status.Ready || status.Domains[DomainEx].LastError != "ex unavailable" {
		t.Fatalf("unexpected degraded status: %+v", status)
	}
}

// 验证重连失败保留旧就绪客户端并关闭替换客户端。
func TestFailedReconnectRetainsReadyClientAndClosesReplacement(t *testing.T) {
	var builds atomic.Int32
	var closes atomic.Int32
	manager := newManager(map[Domain]domainConfig{
		DomainMain: {
			build: func() *gotdx.Client {
				builds.Add(1)
				return gotdx.New()
			},
			connect: func(*gotdx.Client) error {
				if builds.Load() > 1 {
					return errors.New("replacement unavailable")
				}
				return nil
			},
			close: func(*gotdx.Client) error {
				closes.Add(1)
				return nil
			},
		},
	})
	if err := manager.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := manager.Reconnect(DomainMain); err == nil {
		t.Fatal("Reconnect succeeded")
	}
	status := manager.Status().Domains[DomainMain]
	if !status.Ready || status.LastRecoveryError != "replacement unavailable" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if closes.Load() != 1 {
		t.Fatalf("replacement closes = %d, want 1", closes.Load())
	}
	_, err := execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) { return struct{}{}, nil })
	if err != nil {
		t.Fatalf("retained client is unusable: %v", err)
	}
	_ = manager.Close()
}

// 验证请求失败且恢复失败后将客户端代际标记为未就绪。
func TestRequestFailureAndFailedRecoveryMarksGenerationUnready(t *testing.T) {
	var builds atomic.Int32
	manager := newManager(map[Domain]domainConfig{
		DomainMain: {
			build: func() *gotdx.Client { builds.Add(1); return gotdx.New() },
			connect: func(*gotdx.Client) error {
				if builds.Load() > 1 {
					return errors.New("replacement unavailable")
				}
				return nil
			},
			close: func(*gotdx.Client) error { return nil },
		},
	})
	if err := manager.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	_, err := execute(manager, DomainMain, func(*gotdx.Client) (struct{}, error) {
		return struct{}{}, ErrDisconnected
	})
	if err == nil {
		t.Fatal("execute succeeded")
	}
	status := manager.Status().Domains[DomainMain]
	if status.Ready || status.LastRecoveryError != "replacement unavailable" {
		t.Fatalf("unexpected status: %+v", status)
	}
	_ = manager.Close()
}

// 验证重连期间关闭会拒绝已连通的替换客户端。
func TestCloseDuringReconnectRejectsConnectedReplacement(t *testing.T) {
	connectStarted := make(chan struct{})
	releaseConnect := make(chan struct{})
	var closes atomic.Int32
	manager := newManager(map[Domain]domainConfig{
		DomainMain: {
			build: func() *gotdx.Client { return gotdx.New() },
			connect: func(*gotdx.Client) error {
				close(connectStarted)
				<-releaseConnect
				return nil
			},
			close: func(*gotdx.Client) error { closes.Add(1); return nil },
		},
	})
	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- manager.Reconnect(DomainMain) }()
	<-connectStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		manager.stateMu.RLock()
		closed := manager.closed
		manager.stateMu.RUnlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not mark manager closed")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseConnect)
	if err := <-reconnectDone; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Reconnect error = %v, want ErrManagerClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if closes.Load() != 1 {
		t.Fatalf("replacement closes = %d, want 1", closes.Load())
	}
}

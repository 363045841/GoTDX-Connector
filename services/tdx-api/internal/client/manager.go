package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bensema/gotdx"
	gotdxtypes "github.com/bensema/gotdx/types"
)

type Domain string

const (
	DomainMain Domain = "main"
	DomainEx   Domain = "ex"
	DomainMAC  Domain = "mac"
)

var ErrManagerClosed = errors.New("gotdx client manager is closed")
var ErrDisconnected = errors.New("gotdx connection is unavailable")
var ErrMalformedReply = errors.New("gotdx upstream reply is malformed")

type domainConfig struct {
	build   func() *gotdx.Client
	connect func(*gotdx.Client) error
	close   func(*gotdx.Client) error
}

// clientLease 一代客户端及其在途请求计数。
// 请求只在取客户端时持有 slot.mu，网络 I/O 不再持锁；重连换上新代后等待旧代在途归零再关闭。
type clientLease struct {
	client   *gotdx.Client
	inflight sync.WaitGroup
}

type clientSlot struct {
	mu                sync.RWMutex
	reconnectMu       sync.Mutex
	lease             *clientLease
	generation        uint64
	ready             bool
	lastError         string
	lastRecoveryError string
	lastSuccess       time.Time
	lastReconnect     time.Time
	config            domainConfig
}

type Manager struct {
	stateMu sync.RWMutex
	closed  bool
	slots   map[Domain]*clientSlot
}

type DomainStatus struct {
	Ready             bool      `json:"ready"`
	Generation        uint64    `json:"generation"`
	LastError         string    `json:"last_error,omitempty"`
	LastRecoveryError string    `json:"last_recovery_error,omitempty"`
	LastSuccess       time.Time `json:"last_success,omitempty"`
	LastReconnect     time.Time `json:"last_reconnect,omitempty"`
}

type Status struct {
	Ready   bool                    `json:"ready"`
	Domains map[Domain]DomainStatus `json:"domains"`
}

func newManager(configs map[Domain]domainConfig) *Manager {
	slots := make(map[Domain]*clientSlot, len(configs))
	for domain, config := range configs {
		slots[domain] = &clientSlot{config: config, generation: 1}
	}
	return &Manager{slots: slots}
}

func (manager *Manager) Start() error {
	var failures []error
	for _, domain := range []Domain{DomainMain, DomainEx, DomainMAC} {
		if _, ok := manager.slots[domain]; !ok {
			continue
		}
		if err := manager.reconnectGeneration(domain, manager.generation(domain)); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (manager *Manager) generation(domain Domain) uint64 {
	slot := manager.slots[domain]
	if slot == nil {
		return 0
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	return slot.generation
}

func (manager *Manager) Reconnect(domain Domain) error {
	return manager.reconnectGeneration(domain, 0)
}

func (manager *Manager) recoverUnready(domains ...Domain) {
	for _, domain := range domains {
		slot := manager.slots[domain]
		if slot == nil {
			continue
		}
		slot.mu.RLock()
		ready := slot.ready
		generation := slot.generation
		slot.mu.RUnlock()
		if !ready {
			_ = manager.reconnectGeneration(domain, generation)
		}
	}
}

func (manager *Manager) reconnectGeneration(domain Domain, failedGeneration uint64) error {
	slot := manager.slots[domain]
	if slot == nil {
		return fmt.Errorf("unknown gotdx domain %q", domain)
	}
	slot.reconnectMu.Lock()
	defer slot.reconnectMu.Unlock()

	manager.stateMu.RLock()
	closed := manager.closed
	manager.stateMu.RUnlock()
	if closed {
		return ErrManagerClosed
	}
	if failedGeneration != 0 && manager.generation(domain) != failedGeneration {
		return nil
	}

	log.Printf("upstream reconnect started: domain=%s generation=%d", domain, manager.generation(domain))
	replacement := slot.config.build()
	if replacement == nil {
		return manager.recordReconnectFailure(slot, domain, errors.New("no reachable hosts"))
	}
	if err := slot.config.connect(replacement); err != nil {
		_ = slot.config.close(replacement)
		return manager.recordReconnectFailure(slot, domain, err)
	}
	manager.stateMu.RLock()
	closed = manager.closed
	manager.stateMu.RUnlock()
	if closed {
		_ = slot.config.close(replacement)
		return ErrManagerClosed
	}

	slot.mu.Lock()
	if failedGeneration != 0 && slot.generation != failedGeneration {
		slot.mu.Unlock()
		_ = slot.config.close(replacement)
		return nil
	}
	previous := slot.lease
	slot.lease = &clientLease{client: replacement}
	slot.generation++
	slot.ready = true
	slot.lastError = ""
	slot.lastRecoveryError = ""
	slot.lastSuccess = time.Now()
	slot.lastReconnect = time.Now()
	slot.mu.Unlock()
	if previous != nil {
		previous.inflight.Wait()
		_ = slot.config.close(previous.client)
	}
	log.Printf("upstream reconnect succeeded: domain=%s generation=%d", domain, manager.generation(domain))
	return nil
}

func (manager *Manager) recordReconnectFailure(slot *clientSlot, domain Domain, err error) error {
	log.Printf("upstream reconnect failed: domain=%s error=%v", domain, err)
	slot.mu.Lock()
	if slot.lease == nil {
		slot.ready = false
		slot.lastError = err.Error()
	}
	slot.lastRecoveryError = err.Error()
	slot.lastReconnect = time.Now()
	slot.mu.Unlock()
	return fmt.Errorf("reconnect %s: %w", domain, err)
}

func execute[T any](manager *Manager, domain Domain, operation func(*gotdx.Client) (T, error)) (T, error) {
	result, generation, err := executeOnce(manager, domain, operation)
	err = normalizeTransportError(err)
	if err == nil || !isRecoverable(err) {
		return result, err
	}
	log.Printf("upstream request failed, recovering: domain=%s generation=%d error=%v", domain, generation, err)
	manager.recordOperationFailure(domain, generation, err)
	if reconnectErr := manager.reconnectGeneration(domain, generation); reconnectErr != nil {
		var zero T
		return zero, fmt.Errorf("%s request failed: %w; recovery failed: %v", domain, err, reconnectErr)
	}
	result, _, retryErr := executeOnce(manager, domain, operation)
	retryErr = normalizeTransportError(retryErr)
	if retryErr != nil && isRecoverable(retryErr) {
		manager.recordOperationFailure(domain, manager.generation(domain), retryErr)
	}
	return result, retryErr
}

func (manager *Manager) recordOperationFailure(domain Domain, generation uint64, err error) {
	slot := manager.slots[domain]
	if slot == nil {
		return
	}
	slot.mu.Lock()
	if slot.generation == generation {
		slot.ready = false
		slot.lastError = err.Error()
	}
	slot.mu.Unlock()
}

func executeOnce[T any](manager *Manager, domain Domain, operation func(*gotdx.Client) (T, error)) (T, uint64, error) {
	var zero T
	manager.stateMu.RLock()
	if manager.closed {
		manager.stateMu.RUnlock()
		return zero, 0, ErrManagerClosed
	}
	slot := manager.slots[domain]
	if slot == nil {
		manager.stateMu.RUnlock()
		return zero, 0, fmt.Errorf("unknown gotdx domain %q", domain)
	}
	slot.mu.RLock()
	manager.stateMu.RUnlock()
	lease := slot.lease
	generation := slot.generation
	if lease == nil {
		slot.mu.RUnlock()
		if err := manager.reconnectGeneration(domain, generation); err != nil {
			return zero, generation, err
		}
		return executeOnce(manager, domain, operation)
	}
	// 只把这一代客户端标记为在用，随后的网络 I/O 不持有 slot 锁。
	lease.inflight.Add(1)
	slot.mu.RUnlock()

	result, err := invokeOperation(lease.client, operation)
	lease.inflight.Done()

	if err == nil {
		slot.mu.Lock()
		if slot.generation == generation {
			slot.ready = true
			slot.lastError = ""
			slot.lastSuccess = time.Now()
		}
		slot.mu.Unlock()
	}
	return result, generation, err
}

// invokeOperation 将依赖库的解析 panic 转成协议错误，确保租约释放并让调用方拿到可诊断结果。
// 坏帧是整包读完后的解析失败，连接仍处于帧边界，因此不在此处重连。
func invokeOperation[T any](current *gotdx.Client, operation func(*gotdx.Client) (T, error)) (result T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero T
			result = zero
			err = fmt.Errorf("%w: %v", ErrMalformedReply, recovered)
		}
	}()
	return operation(current)
}

func isRecoverable(err error) bool {
	if err == nil {
		return false
	}
	// gotdx 在协议帧异常后无法保证连接仍处于帧边界，必须废弃连接并重连。
	if errors.Is(err, gotdxtypes.ErrBadData) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, ErrDisconnected)
}

func normalizeTransportError(err error) error {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if current.Error() == "connection is nil" {
			return fmt.Errorf("%w: %v", ErrDisconnected, err)
		}
	}
	return err
}

func (manager *Manager) Status() Status {
	status := Status{Ready: true, Domains: make(map[Domain]DomainStatus, len(manager.slots))}
	for domain, slot := range manager.slots {
		slot.mu.RLock()
		domainStatus := DomainStatus{
			Ready: slot.ready, Generation: slot.generation, LastError: slot.lastError,
			LastRecoveryError: slot.lastRecoveryError,
			LastSuccess:       slot.lastSuccess, LastReconnect: slot.lastReconnect,
		}
		slot.mu.RUnlock()
		status.Domains[domain] = domainStatus
		status.Ready = status.Ready && domainStatus.Ready
	}
	return status
}

func (manager *Manager) Close() error {
	return manager.CloseContext(context.Background())
}

func (manager *Manager) CloseContext(ctx context.Context) error {
	manager.stateMu.Lock()
	if manager.closed {
		manager.stateMu.Unlock()
		return nil
	}
	manager.closed = true
	manager.stateMu.Unlock()

	done := make(chan error, 1)
	go func() {
		var failures []error
		for domain, slot := range manager.slots {
			slot.reconnectMu.Lock()
			slot.mu.Lock()
			current := slot.lease
			slot.lease = nil
			slot.ready = false
			slot.mu.Unlock()
			if current != nil {
				current.inflight.Wait()
				if err := slot.config.close(current.client); err != nil {
					failures = append(failures, fmt.Errorf("close %s: %w", domain, err))
				}
			}
			slot.reconnectMu.Unlock()
		}
		done <- errors.Join(failures...)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

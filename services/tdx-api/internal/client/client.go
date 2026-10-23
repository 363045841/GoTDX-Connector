package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/bensema/gotdx"
	"github.com/bensema/gotdx/proto"
)

// Options 解析后的 gotdx 连接配置；环境变量只在 main 的 config 中读取。
type Options struct {
	AutoSelect bool
	MainHosts  []string
	ExHosts    []string
	MACHosts   []string
}

// NewDefaultManager 按给定配置创建三个行情域的管理器。
func NewDefaultManager(opts Options) *Manager {
	return newManager(map[Domain]domainConfig{
		DomainMain: domainConfigFor(DomainMain, opts),
		DomainEx:   domainConfigFor(DomainEx, opts),
		DomainMAC:  domainConfigFor(DomainMAC, opts),
	})
}

type MainQuerier interface {
	GetServerHeartbeat() (*proto.HeartBeatReply, error)
	StockAll(uint8) ([]proto.Security, error)
	StockCount(uint8) (uint16, error)
	StockList(uint8, uint32, uint32) ([]proto.Security, error)
	StockQuotesDetail([]uint8, []string) ([]proto.SecurityQuote, error)
	StockKLine(uint16, uint8, string, uint16, uint16, uint16, uint16) ([]proto.SecurityBar, error)
	StockTickChart(uint8, string, uint16, uint16) ([]proto.MinuteTimeData, error)
	StockHistoryTickChart(uint32, uint8, string) ([]proto.HistoryMinuteTimeData, error)
	StockHistoryFullTransaction(uint32, uint8, string) ([]proto.HistoryTransactionData, error)
	StockFullTransaction(uint8, string) ([]proto.TransactionData, error)
	StockIndexInfo(uint8, string) (*proto.GetIndexInfoReply, error)
	StockTransaction(uint8, string, uint16, uint16) ([]proto.TransactionData, error)
	StockHistoryTransaction(uint32, uint8, string, uint16, uint16) ([]proto.HistoryTransactionData, error)
	GetIndexBars(uint16, uint8, string, uint16, uint16) (*proto.GetIndexBarsReply, error)
}

type ExQuerier interface {
	ExCount() (uint32, error)
	ExList(uint32, uint16) ([]proto.ExListItem, error)
	ExQuote(uint8, string) (*proto.ExQuoteItem, error)
	ExQuotes([]uint8, []string) ([]proto.ExQuoteItem, error)
	ExKLine(uint8, string, uint16, uint32, uint16, uint16) ([]proto.ExKLineItem, error)
	ExTickChart(uint8, string, uint32) ([]proto.ExTickChartData, error)
	ExHistoryTransaction(uint32, uint8, string) ([]proto.ExHistoryTransactionItem, error)
	ExTable() (string, error)
}

// QueryMain 在主市场客户端上执行一次带恢复重试的只读请求。
func QueryMain[T any](manager *Manager, operation func(MainQuerier) (T, error)) (T, error) {
	return execute(manager, DomainMain, func(c *gotdx.Client) (T, error) { return operation(c) })
}

// ProbeMain 在主市场客户端上执行一次不重试的探测请求。
func ProbeMain[T any](manager *Manager, operation func(MainQuerier) (T, error)) (T, error) {
	result, _, err := executeOnce(manager, DomainMain, func(c *gotdx.Client) (T, error) { return operation(c) })
	return result, err
}

// QueryEx 在扩展行情客户端上执行一次带恢复重试的只读请求。
func QueryEx[T any](manager *Manager, operation func(ExQuerier) (T, error)) (T, error) {
	return execute(manager, DomainEx, func(c *gotdx.Client) (T, error) { return operation(c) })
}

// domainConfigFor 构造单个行情域的构建、连接与关闭动作。
func domainConfigFor(domain Domain, opts Options) domainConfig {
	return domainConfig{
		build:   func() *gotdx.Client { return buildDomainClient(domain, opts) },
		connect: func(c *gotdx.Client) error { return connectDomainClient(domain, c) },
		close:   func(c *gotdx.Client) error { return c.Disconnect() },
	}
}

// buildDomainClient 按域选择内置主机或显式配置的地址。
func buildDomainClient(domain Domain, opts Options) *gotdx.Client {
	var (
		label      string
		configured []string
		defaults   []string
	)
	switch domain {
	case DomainMain:
		label, configured, defaults = "main", opts.MainHosts, gotdx.MainHostAddresses()
	case DomainEx:
		label, configured, defaults = "ex", opts.ExHosts, gotdx.ExHostAddresses()
	case DomainMAC:
		label, configured, defaults = "mac", opts.MACHosts, gotdx.MACHostAddresses()
	default:
		return nil
	}
	hosts := resolveHosts(label, configured, defaults, 2*time.Second)
	if len(hosts) == 0 {
		log.Printf("[gotdx] no reachable %s hosts, using full default list", label)
		hosts = defaults
	}
	if len(hosts) == 0 {
		return nil
	}
	log.Printf("[gotdx] %s hosts (%d): %v", label, len(hosts), hosts)
	clientOpts := []gotdx.Option{gotdx.WithTimeoutSec(6)}
	switch domain {
	case DomainMain:
		clientOpts = append(clientOpts, gotdx.WithTCPAddress(hosts[0]), gotdx.WithTCPAddressPool(hosts[1:]...))
	case DomainEx:
		clientOpts = append(clientOpts, gotdx.WithExTCPAddress(hosts[0]), gotdx.WithExTCPAddressPool(hosts[1:]...))
	case DomainMAC:
		clientOpts = append(clientOpts, gotdx.WithMacTCPAddress(hosts[0]), gotdx.WithMacTCPAddressPool(hosts[1:]...))
	}
	if opts.AutoSelect {
		clientOpts = append(clientOpts, gotdx.WithAutoSelectFastest(true))
	}
	switch domain {
	case DomainEx:
		return gotdx.NewEx(clientOpts...)
	case DomainMAC:
		return gotdx.NewMAC(clientOpts...)
	default:
		return gotdx.New(clientOpts...)
	}
}

func connectDomainClient(domain Domain, c *gotdx.Client) error {
	if c == nil {
		return errors.New("client is nil")
	}
	switch domain {
	case DomainMain:
		_, err := c.Connect()
		return err
	case DomainEx:
		_, err := c.ConnectEx()
		return err
	case DomainMAC:
		return c.ConnectMAC()
	default:
		return fmt.Errorf("unknown gotdx domain %q", domain)
	}
}

// resolveHosts 优先使用显式配置；未配置时并发探测内置主机。
func resolveHosts(label string, configured, all []string, timeout time.Duration) []string {
	if len(configured) > 0 {
		log.Printf("[gotdx] %s: using configured hosts %v", label, configured)
		return configured
	}

	log.Printf("[gotdx] %s: probing %d hosts (timeout=%v)...", label, len(all), timeout)
	results := gotdx.ProbeAddresses(all, timeout)

	reachable := make([]string, 0, len(results))
	for _, r := range results {
		if r.Reachable {
			log.Printf("[gotdx] %s: %-21s reachable  latency=%v", label, r.Address, r.Latency)
			reachable = append(reachable, r.Address)
		} else {
			log.Printf("[gotdx] %s: %-21s unreachable", label, r.Address)
		}
	}
	log.Printf("[gotdx] %s: %d/%d hosts reachable", label, len(reachable), len(all))
	return reachable
}

// CloseContext 关闭管理器并等待在途请求结束；ctx 到期时返回 ctx.Err()。
func CloseContext(ctx context.Context, manager *Manager) error {
	return manager.CloseContext(ctx)
}

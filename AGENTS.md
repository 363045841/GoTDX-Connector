# KlineChartQuantGo — Agent Guide

单一 Go module（`KlineChartQuantGo`），只保留 tdx-api 一个服务；币安行情由独立的 `Binance-Connector` 仓库提供。

## Quick start

在仓库**根目录**执行：

```bash
# 通达信（默认 8080）
go run . tdx
# 或: go run ./services/tdx-api
```

根目录 `main.go` 是启动器，必须带服务名：`go run . tdx`。

## Services

| Service | Path | Port | Role |
|---|---|---|---|
| tdx-api | `./services/tdx-api` | `8080` | gotdx V1 market-data API（A股/指数/扩展行情） |

BaoStock 与 Binance 均在**独立仓库**，不纳入本 module。

## Key facts

- **One module**: root `go.mod` → `module KlineChartQuantGo`. No nested `go.mod` under services.
- **Go 1.27**, Gin framework, SQLite symbol directory persistence with startup warm-up, per-package tests, no CI.
- Import paths: `KlineChartQuantGo/services/tdx-api/internal/...`
- 依赖由 `main` 装配：`config.Load()` 读取环境变量，`client.NewDefaultManager` 创建连接管理器，`domain.NewFetcher` 绑定拉取函数，`server.NewRouter` 接收 `v1.Deps`。业务包不读环境变量、不使用包级单例。
- CORS 当前为本地开发放开；收紧时改 `internal/server/router.go`。
- Live bars: `GET /api/v1/market-data/sources/gotdx/stream` (SSE, KCQ `BarsLiveSource` frames); main-market A-shares/indices only.
- tdx-api 只有 V1 行情协议 + 健康检查，旧 `/api/stock|ex|mac|hosts` 接口已删除。

## Env vars

### tdx-api
| Var | Default | Note |
|---|---|---|
| `PORT` | `8080` | |
| `SYMBOL_DB_PATH` | `data/tdx-symbols.db` | Search directory snapshot; refreshed every 24 hours |
| `GOTDX_AUTO_SELECT` | — | `"1"` = auto-pick fastest host |
| `GOTDX_MAIN_HOSTS` | built-in | comma-separated |
| `GOTDX_EX_HOSTS` | built-in | comma-separated |
| `GOTDX_MAC_HOSTS` | built-in | comma-separated |
| `GOTDX_LIVE_POLL_INTERVAL` | `5s` | live-bars poll interval during CN trading hours (min 500ms) |
| `GOTDX_LIVE_MAX_STREAMS` | `256` | max concurrent live-bars SSE connections |
| `GOTDX_LIVE_MAX_SERIES` | `32` | max distinct (instrument, period, adjust) pollers |

## Structure
```
go.mod                 — module KlineChartQuantGo
main.go                — launcher: go run . tdx
services/
  tdx-api/
    main.go            — 组合根：配置、资源装配、HTTP 服务与统一关闭
    internal/config/     — 环境变量唯一读取点
    internal/client/     — gotdx 连接管理（探测、连接、代际租约、重连、心跳）
    internal/directory/  — 证券目录：加载/缓存/SQLite 持久化/搜索
    internal/domain/     — 领域层：K线分页、分时构建、昨收解析、kind 路由、Fetcher 装配
    internal/instrument/ — 品种路由词汇：kind、规范 ID、providerRef 与数值解析
    internal/live/       — 实时 K 线轮询中枢（每序列一个上游轮询器，差分后扇出）
    internal/v1/         — V1 行情协议：envelope、探测、搜索、bars/timeshare、stream SSE
    internal/server/     — HTTP 装配：请求 ID、恢复、健康检查 + V1 路由
```

## Conventions
- V1 错误响应统一 envelope：`{"error": {"code", "message"}, "requestId"}`；确定性错误码见 `internal/v1`。
- 上游错误细节只写日志，响应使用稳定文案；`X-Request-Id` 贯穿响应头与日志。
- K-line page size: `klinePageSize = 798` (`services/tdx-api/internal/domain/kline_range.go`).
- 前端只走 V1 协议：`KLineChartQuant` packages/core/src/data/provider/sources/gotdx.ts

## Comment Style

- 每个文件必须有头部注释，说明文件用途。
- 每个函数必须有注释，说明其职责、参数和返回值；简单函数可使用简短注释。
- 关键代码必须有注释，说明实现意图、业务规则或不直观的处理逻辑。
- 每个测试用例必须有中文注释，说明验证的行为和场景。
- 注释正文使用中文，技术术语保留英文。
- 注释必须简单明了，直接说明代码是什么或为什么这样实现，尽量使用一句话，避免冗长和重复代码本身的含义。

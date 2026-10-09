# KlineChartQuantGo — Agent Guide

单一 Go module（`KlineChartQuantGo`），两个可执行服务共用依赖，目录按服务拆分。

## Quick start

在仓库**根目录**执行：

```bash
# 通达信（默认 8080）
go run . tdx
# 或: go run ./services/tdx-api

# 币安（默认 8081）
go run . binance
# 或: go run ./services/binance-api
```

根目录 `main.go` 是启动器，必须带服务名：`go run . tdx` / `go run . binance`。

## Services

| Service | Path | Port | Role |
|---|---|---|---|
| tdx-api | `./services/tdx-api` | `8080` | gotdx V1 market-data API（A股/指数/扩展行情） |
| binance-api | `./services/binance-api` | `8081` | Binance orderbook + depth SSE |

BaoStock lives in a **separate** repo (`stockbao`, port 8000). Do not add it here.

## Key facts

- **One module**: root `go.mod` → `module KlineChartQuantGo`. No nested `go.mod` under services.
- **Go 1.27**, Gin framework, SQLite symbol directory persistence with startup warm-up, per-package tests, no CI.
- Import paths: `KlineChartQuantGo/services/tdx-api/internal/...` and `KlineChartQuantGo/services/binance-api/internal/...`
- tdx-api client singleton via `client.DefaultManager()` — probes gotdx hosts at startup.
- binance-api defaults proxy to `http://127.0.0.1:6666` if `HTTP_PROXY` unset.
- CORS wide-open on both services.
- Binance routes are `/api/binance/*` (not the old misspelled `/api/biance/*`).
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
| `GOTDX_LIVE_POLL_INTERVAL` | `2s` | live-bars poll interval during CN trading hours (min 500ms) |
| `GOTDX_LIVE_MAX_STREAMS` | `256` | max concurrent live-bars SSE connections |
| `GOTDX_LIVE_MAX_SERIES` | `32` | max distinct (instrument, period, adjust) pollers |

### binance-api
| Var | Default | Note |
|---|---|---|
| `PORT` | `8081` | |
| `SYMBOLS` | `btcusdt,ethusdt` | comma-separated |
| `HTTP_PROXY` / `HTTPS_PROXY` | `http://127.0.0.1:6666` | |

## Structure
```
go.mod                 — module KlineChartQuantGo
main.go                — launcher: go run . <tdx|binance>
services/
  tdx-api/
    main.go
    internal/client/     — gotdx singleton (probe, connect, reconnect, heartbeat)
    internal/directory/  — 证券目录：加载/缓存/SQLite 持久化/搜索
    internal/domain/     — 领域层：K线分页、分时构建、昨收解析、kind 路由
    internal/live/       — 实时 K 线轮询中枢（每序列一个上游轮询器，差分后扇出）
    internal/v1/         — V1 行情协议：envelope、探测、搜索、bars/timeshare、stream SSE
    internal/server/     — HTTP 装配：健康检查 + 目录缓存 + V1 路由
  binance-api/
    main.go
    internal/binance/  — WS client + DepthHub
    internal/handler/  — Gin router (orderbook + SSE)
```

## Conventions
- V1 错误响应统一 envelope：`{"error": {"code", "message"}, "requestId"}`；确定性错误码见 `internal/v1`。
- K-line page size: `klinePageSize = 798` (`services/tdx-api/internal/domain/kline_range.go`).
- 前端只走 V1 协议：`KLineChartQuant` packages/core/src/data/provider/sources/gotdx.ts

## Comment Style

- 每个文件必须有头部注释，说明文件用途。
- 每个函数必须有注释，说明其职责、参数和返回值；简单函数可使用简短注释。
- 关键代码必须有注释，说明实现意图、业务规则或不直观的处理逻辑。
- 每个测试用例必须有中文注释，说明验证的行为和场景。
- 注释正文使用中文，技术术语保留英文。
- 注释必须简单明了，直接说明代码是什么或为什么这样实现，尽量使用一句话，避免冗长和重复代码本身的含义。

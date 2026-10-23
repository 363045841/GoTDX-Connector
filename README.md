# GoTDX-Connecter

通达信行情代理 — 单一 Go module，为 [KLineChartQuant](https://github.com/363045841/KLineChartQuant) 提供本地后端。

| 服务 | 包路径 | 默认端口 | 说明 |
|---|---|---|---|
| **tdx-api** | `./services/tdx-api` | `8080` | 通达信 (gotdx) A 股/指数/扩展行情 V1 协议 |

> BaoStock、Binance 等其它数据源由独立仓库提供，不在本仓库。

## 快速开始

在仓库根目录执行（module: `KlineChartQuantGo`）：

```bash
# 通达信（默认 8080）
go run . tdx
# 或: go run ./services/tdx-api
```

或构建二进制：

```bash
go build -o tdx-api.exe ./services/tdx-api
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `PORT` | `8080` | HTTP 服务端口 |
| `SYMBOL_DB_PATH` | `data/tdx-symbols.db` | 证券目录 SQLite 快照；每 24 小时刷新 |
| `GOTDX_AUTO_SELECT` | — | 设为 `"1"` 自动选择最快服务器 |
| `GOTDX_MAIN_HOSTS` | 内置列表 | 覆盖主站探测地址（逗号分隔） |
| `GOTDX_EX_HOSTS` | 内置列表 | 覆盖行情探测地址（逗号分隔） |
| `GOTDX_MAC_HOSTS` | 内置列表 | 覆盖 MAC 探测地址（逗号分隔） |
| `GOTDX_LIVE_POLL_INTERVAL` | `5s` | 实时 K 线盘中轮询间隔（Go duration，最小 `500ms`）；休市 60s |
| `GOTDX_LIVE_MAX_STREAMS` | `256` | 实时 K 线 SSE 连接总上限 |
| `GOTDX_LIVE_MAX_SERIES` | `32` | 并发轮询的（品种, 周期, 复权）序列上限 |

## API 概览

所有接口返回 JSON，已全局开启 CORS。前端只消费 **V1 行情协议**（`/api/v1/market-data`），接口契约以 `KLineChartQuant` 前端 `api/types.ts` 为准。

| 方法 | 路由 | 说明 |
|---|---|---|
| GET | `/api/v1/market-data/sources/:sourceId/probe` | 数据源探测（状态 + 源级能力声明） |
| GET | `/api/v1/market-data/sources/gotdx/stream` | 实时 K 线 SSE（A 股/指数，见下） |
| POST | `/api/v1/market-data/instruments/search` | 品种目录搜索 |
| POST | `/api/v1/market-data/bars` | K 线（UTC 毫秒区间） |
| POST | `/api/v1/market-data/timeshare` | 分时（交易日 YYYY-MM-DD） |
| POST | `/api/v1/market-data/timeshare/range` | 多日分时（`endTradingDate` + `days`，上限 20 个交易日） |

### 实时 K 线 SSE

`GET /api/v1/market-data/sources/gotdx/stream?symbol=600519&period=5min&barAggregation=original`

- 响应 `text/event-stream`，每条消息是一行无名 `data: <json>`，帧格式与 KCQ `BarsLiveSource` 一致：
  `{type:'snapshot',symbol,period,bars}`（最近 2 根）→ `{type:'forming',symbol,period,bar}`（当前根变化时）→
  `{type:'closed',symbol,period,bar}`（收线终值，紧跟新一根的 forming）；`{type:'status',symbol,period,status,detail?}`
  取值 `live` / `market-closed` / `upstream-error` / `no-data`。`symbol`/`period` 原样回显请求值。
- `bar` 为 `{timestamp(UTC ms),open,high,low,close,volume?,turnover?}`，与 `/bars` 同一映射。
- `: ping` 心跳每 15 秒一次；首包带 `retry: 3000`。
- 品种解析：`instrumentId=gotdx:{stock|index}:{market}:{code}`（推荐）、`symbol=600519.SH`、`exchange=SH`，
  或仅凭代码在证券目录中唯一匹配；同码跨市场（如 `000001` 平安银行/上证指数）返回 400 并列出候选。
  指数必须与所属市场一致（上证 `000xxx` → market 1，深证 `399xxx` → market 0，北证 `899xxx` → market 2）。
- 可选 `adjustment=none|qfq|hfq`（默认 `none`，尾部 K 线 qfq 与 none 相同）；`barAggregation` 与 `/bars` 一样不改变结果。
- 同一（品种, 周期, 复权）所有连接共享一个上游轮询器：盘中（上海时间工作日 09:15–11:32、12:59–15:02）每 5 秒，
  休市 60 秒；09:35 后仍无当日 K 线视为节假日/停牌。只在数值变化时推送。
- 扩展行情（港美股、期货等）不支持，品种能力声明 `liveBars:false`。
- 反向代理需关闭缓冲并放宽读超时，例如 nginx：`proxy_buffering off; proxy_read_timeout 1h; proxy_http_version 1.1; proxy_set_header Connection "";`

确定性错误码（触发前端请求流转）：`UNSUPPORTED_CAPABILITY`、`INSTRUMENT_NOT_FOUND`；上游故障为 `UPSTREAM_UNAVAILABLE`（不流转）。
每个响应带 `X-Request-Id`（可由请求头传入），与服务端日志一一对应。

### 健康检查

| 方法 | 路由 | 说明 |
|---|---|---|
| GET | `/health/live` | 存活探针 |
| GET | `/health/ready` | 就绪探针（聚合各行情域状态） |

## 项目结构

```
├── go.mod                       # 唯一 module: KlineChartQuantGo
├── go.sum
├── main.go                      # 启动器: go run . tdx
├── README.md
├── AGENTS.md
└── services/
    └── tdx-api/
        ├── main.go              # 组合根：配置、资源装配、HTTP 服务与统一关闭
        └── internal/
            ├── config/          # 环境变量唯一读取点
            ├── client/          # gotdx 连接管理：探测/连接/代际租约/重连/心跳
            ├── directory/       # 证券目录：加载/缓存/SQLite 持久化/搜索
            ├── domain/          # 领域层：K 线分页、分时构建、昨收解析、Fetcher 装配
            ├── instrument/      # 品种路由：kind、规范 ID、providerRef、数值解析
            ├── live/            # 实时 K 线轮询中枢：按序列共享上游轮询、差分、扇出
            ├── v1/              # V1 行情协议：envelope、探测、搜索、bars/timeshare、stream SSE
            └── server/          # HTTP 装配：请求 ID、恢复、健康检查 + V1 路由
```

## 技术栈

- **Go 1.27** + Gin
- **gotdx** — 通达信协议 Go 实现
- **modernc.org/sqlite** — 证券目录本地快照

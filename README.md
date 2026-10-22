# GoTDX-Connecter

通达信（GOTDX）行情代理 — 单一 Go module，为 [KLineChartQuant](https://github.com/363045841/KLineChartQuant) 提供本地后端。

| 服务 | 包路径 | 默认端口 | 说明 |
|---|---|---|---|
| **tdx-api** | `./services/tdx-api` | `8080` | 通达信 (gotdx) A 股/指数/扩展行情 V1 协议 |

> 币安现货 K 线与逐笔成交已独立为 [Binance-Connector](https://github.com/363045841/Binance-Connector)。
> BaoStock 等其它数据源由独立仓库提供（如 `stockbao`，端口 `8000`），不在本仓库。

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

### tdx-api

| 变量 | 默认值 | 说明 |
|---|---|---|
| `PORT` | `8080` | HTTP 服务端口 |
| `SYMBOL_DB_PATH` | `data/tdx-symbols.db` | 证券目录 SQLite 快照；每 24 小时刷新 |
| `GOTDX_AUTO_SELECT` | — | 设为 `"1"` 自动选择最快服务器 |
| `GOTDX_MAIN_HOSTS` | 内置列表 | 覆盖主站探测地址（逗号分隔） |
| `GOTDX_EX_HOSTS` | 内置列表 | 覆盖行情探测地址（逗号分隔） |
| `GOTDX_MAC_HOSTS` | 内置列表 | 覆盖 MAC 探测地址（逗号分隔） |

## API 概览

所有接口返回 JSON，已全局开启 CORS。前端只消费 **V1 行情协议**（`/api/v1/market-data`），接口契约以 `KLineChartQuant` 前端 `api/types.ts` 为准。

### tdx-api — V1 行情协议

| 方法 | 路由 | 说明 |
|---|---|---|
| GET | `/api/v1/market-data/sources/:sourceId/probe` | 数据源探测（状态 + 源级能力声明） |
| POST | `/api/v1/market-data/instruments/search` | 品种目录搜索 |
| POST | `/api/v1/market-data/bars` | K 线（UTC 毫秒区间） |
| POST | `/api/v1/market-data/timeshare` | 分时（交易日 YYYY-MM-DD） |
| POST | `/api/v1/market-data/timeshare/range` | 多日分时（`endTradingDate` + `days`，上限 20 个交易日） |

确定性错误码（触发前端请求流转）：`UNSUPPORTED_CAPABILITY`、`INSTRUMENT_NOT_FOUND`；上游故障为 `UPSTREAM_UNAVAILABLE`（不流转）。

`bars` 能力声明包含 `maxBarsPerRequest`，与 `/bars` 的参数校验共用 `klinePageSize`（当前 798）。前端据此收敛单次请求根数，批量历史仍沿 cursor 分页获取。

### tdx-api — 健康检查

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
    └── tdx-api/                 # 通达信行情代理
        ├── main.go
        └── internal/
            ├── client/          # gotdx 客户端单例（连接/重连/心跳/状态）
            ├── directory/       # 证券目录：加载/缓存/SQLite 持久化/搜索
            ├── domain/          # 领域层：K 线分页、分时构建、昨收解析
            ├── v1/              # V1 行情协议：envelope、探测、搜索、bars/timeshare
            └── server/          # HTTP 装配：健康检查 + 目录缓存 + V1 路由
```

## 技术栈

- **Go 1.27** + Gin
- **gotdx** — 通达信协议 Go 实现（tdx-api）
- **modernc.org/sqlite** — 证券目录本地快照（纯内存代理，实时转发）

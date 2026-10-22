# v1 — V1 行情协议层

前端（KLineChartQuant）唯一数据接入层，负责协议封装与 DTO 映射。接口契约以 `KLineChartQuant` 前端 `api/types.ts` 为准。

## 职责

- **envelope 包装**：统一 `{ data, requestId }` 成功外壳与 `{ error, requestId }` 错误外壳
- **数据源探测**：主站心跳测延迟，输出在线/降级/离线 + 源级能力声明
- **品种搜索**：按关键字搜索证券目录并映射为 V1 品种描述
- **K 线 / 分时**：按 `providerRef.kind` 路由到对应领域查询，输出 V1 DTO
- **能力映射**：周期/复权/时区/资产类别的标准映射表与能力声明

## 依赖

`v1 → domain`（领域查询）、`v1 → client`（心跳探测）、`v1 → directory`（品种搜索）。**不依赖** `api`。

## 关键文件

| 文件 | 内容 |
|---|---|
| `routes.go` | `RegisterRoutes`、envelope、错误码、probe/search handler |
| `bars.go` | `POST /bars`：K 线序列 |
| `timeshare.go` | `POST /timeshare`：分时序列 |
| `mapping.go` | 周期/复权/时区映射、能力声明、`v1InstrumentDescriptor` |

## 路由

`/api/v1/market-data`

- `GET  /sources/:sourceId/probe` — 数据源探测
- `POST /instruments/search` — 品种目录搜索
- `POST /bars` — K 线（`limit` + 可选排他 `before` UTC 毫秒 cursor）
- `POST /timeshare` — 分时（交易日 YYYY-MM-DD）
- `POST /timeshare/range` — 多日分时（`endTradingDate` + `days` 个实际交易日，最大值由 `timeShareRange.maxTradingDays` 声明）

## 错误码

与前端 `MarketDataErrorCode` / `OpenAPI ApiError` 对齐：`INVALID_REQUEST`、`UNSUPPORTED_CAPABILITY`、`INSTRUMENT_NOT_FOUND`、`UPSTREAM_UNAVAILABLE`、`INVALID_RESPONSE`、`INTERNAL`。

## 设计要点

- 未知周期 / 不支持的复权返回 `UNSUPPORTED_CAPABILITY`，不静默回退日线
- `providerRef` 由前端在搜索结果中原样带回，后端据此路由 kind 与 market/category
- 能力声明区分源级（`probe`）与品种级（`search`），品种级按 kind 收敛复权范围
- 源级与品种级 `bars.maxBarsPerRequest` 声明单次 K 线请求上限（798），与 HTTP 参数校验共用常量；更大的视口历史目标须使用 `beforeTimestamp` 游标分批请求，不能直接当作单页 `limit`

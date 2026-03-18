# 涨跌家数统计实现说明（TDX 880005 口径）

本文档描述本项目“涨跌家数/涨跌停/总品种数”等统计数据的获取、缓存与对外 API 输出方式，目标是 **与通达信输入 880005 面板显示保持一致**。

## 1. 对外接口

### 1.1 API 路由

- 路由：`GET /api/market/overview`
- 入口：`app/api/endpoints.py`
- 数据来源：`MarketDataService.get_market_overview()`

### 1.2 返回字段（与涨跌统计相关）

接口返回 JSON 中与统计相关字段：

- `up`：上涨家数
- `down`：下跌家数
- `flat`：平盘停牌家数（TDX 880005 口径）
- `limit_up`：涨停家数
- `limit_down`：跌停家数
- `total`：总品种数（TDX 880005 口径）
- `stats_source`：统计来源标记（用于排查与展示）
- `time`：服务端计算/刷新时间（`HH:MM:SS`）

指数行情（上证/深证/创业板）在同一接口返回：`sh/sz/cy`，与涨跌统计逻辑无直接耦合。

## 2. 统计口径与数据源

### 2.1 主数据源：通达信 880005（TDX）

系统使用 TDX 行情接口直接查询“880005”这一条目，从而获得与通达信面板一致的统计值：

- Market：`1`（上海）
- Code：`880005`
- API：`pytdx.hq.TdxHq_API.get_security_quotes([(1, "880005")])`

字段映射规则如下（均为整数）：

- `up`（上涨家数）= `quote.price`
- `down`（下跌家数）= `quote.open`
- `total`（总品种数）= `quote.high`
- `flat`（平盘停牌）= `max(0, total - up - down)`
- `limit_up`（涨停家数）= `quote.bid_vol5`
- `limit_down`（跌停家数）= `quote.ask_vol5`

说明：
- 这里的 `open/high/price` 并非“价格含义”，而是通达信对 880005 这一特殊条目的字段复用（通达信客户端同样如此显示）。
- `flat` 采用差值推导是因为 880005 本身未单独给出“平盘停牌”字段，但通达信面板能展示该值，且满足 `总=涨+跌+平`。

对应实现：
- `app/services/market/market_data_service.py` 中的 `_fetch_tdx_880_counts_sync()`

### 2.2 辅助数据源：东方财富（EastMoney）

当 TDX 不可用时，会尝试调用东方财富接口抓取全市场统计作为兜底（可能与通达信口径存在差异）：

- `MarketDataService._fetch_eastmoney_counts()`

### 2.3 兜底数据源：数据库历史统计（DB_FALLBACK）

当 TDX 与 EastMoney 都不可用时，系统会回退到上一交易日的数据库统计：

- `MarketDataService.get_historical_market_stats(target_date)`

该来源只用于保证接口稳定可用，口径/时效不保证与通达信实时面板一致。

## 3. 缓存与持久化策略

### 3.1 运行时缓存（内存）

为减少请求量，系统对统计值做了短缓存：

- 统计缓存：`_eastmoney_count_cache` + `_last_count_time`，有效期 `_count_cache_duration`（默认 45 秒）
- `get_market_overview()` 本身也有 15 秒缓存（含指数行情）

### 3.2 收盘缓存（Redis/SQLite）

目的：非交易时段（收盘后、早盘前）也能快速返回“最近一个交易日”的统计值，避免外部数据源不稳定导致接口抖动。

写入时机：
- 当获取到统计值且判定已收盘，会写入收盘缓存。

写入位置：
- `MarketDataService._save_close_counts(trade_date_str, counts, source)`

存储介质：
- Redis（优先）：key `MARKET:CLOSE_COUNTS`，带过期时间（通常到下一交易日 08:00）
- SQLite（兜底）：表 `MarketCloseCounts`

允许写入的 `source`：
- `TDX_RULES`（对应通达信 880005 口径）
- `EASTMONEY`

读取位置：
- `MarketDataService._get_close_counts(trade_date_str)`

读取策略：
- 若收盘缓存存在但来源不是 `TDX_RULES`，系统会尝试实时拉取 TDX 880005 并在“可用且数值不同”时覆盖缓存，保证最终口径一致。

## 4. stats_source 约定

`stats_source` 用于 UI 展示与排查问题，常见取值：

- `TDX_880005`：实时来自 TDX 880005
- `CLOSE_CACHE_TDX_880005`：收盘缓存，且缓存来源为 TDX_RULES
- `CLOSE_CACHE_EASTMONEY`：收盘缓存，且缓存来源为 EASTMONEY
- `CLOSE_CACHE_REDIS` / `CLOSE_CACHE_SQLITE`：收盘缓存来自 Redis/SQLite（当无法识别来源时）
- `DB_FALLBACK`：回退到数据库历史统计

前端展示可能会对 `CLOSE_CACHE_` 前缀做简化显示。

## 5. 数据合理性校验（Plausibility）

系统会对统计结果进行“合理性”校验，以避免接口返回明显异常的数据：

- 函数：`MarketDataService._is_counts_plausible(counts)`
- 校验维度：
  - 各项非负
  - `total = up + down + flat` 在合理区间（大致 2500~7000）
  - `limit_up/limit_down` 不超过 `total`

只有通过校验的数据才会被作为最终统计来源/写入收盘缓存。

## 6. 常见问题排查

### 6.1 stats_source 显示 DB_FALLBACK

说明实时源失败，检查：

- TDX 是否可连通（网络/服务器 IP 可用性）
- Redis 是否可用（不影响实时，但影响收盘缓存读取）

### 6.2 flat 与通达信差异

本实现 flat 通过 `total - up - down` 推导，若出现差异通常意味着：

- 读取到的不是 `TDX_880005` 或 `CLOSE_CACHE_TDX_880005`
- 某一来源返回的 `total/up/down` 口径不一致（如 EastMoney/DB）

优先确认 `stats_source`，再检查 `_fetch_tdx_880_counts_sync()` 是否成功执行。


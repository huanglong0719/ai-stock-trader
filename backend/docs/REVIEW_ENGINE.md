# 复盘引擎：细节与逻辑说明

本文档描述系统“复盘（Review）”能力的完整数据流、触发方式、核心算法、AI 生成逻辑与返回结构，便于排查问题与二次开发。

---

## 1. 复盘是什么

复盘的目标是把“市场状态 + 连板梯队 + 成交额/题材机会 + 账户持仓”整合成：

- **市场情绪快照**：上涨/下跌/涨停/跌停/成交额、连板高度、市场温度、主线题材
- **文字总结**：主线（main_theme）+ 当日情绪与次日预期（summary）
- **交易建议**：
  - **target_plans**：次日（或当日）可执行的买入候选
  - **holding_plans**：对当前持仓的“继续持有/卖出”建议

复盘结果会落库到 `MarketSentiment` 表，计划建议会落库到 `TradingPlan` 表。

---

## 2. 触发入口（API 与定时任务）

### 2.1 手动触发：/api/trading/review/daily

实现见 [trading_endpoints.py](../app/api/trading_endpoints.py)（`/review/daily`）。

请求体（简化）：

```json
{
  "review_date": "2026-01-23",
  "watchlist": ["600000.SH", "000001.SZ"],
  "async_mode": true
}
```

行为：
- `async_mode=true`（默认）：**先写入“生成中”占位记录**，立刻返回占位结果，然后后台异步生成完整复盘。
- `async_mode=false`：同步执行完整复盘，直到生成最终结果后返回（耗时取决于数据抓取与 AI 调用）。

日期选择逻辑（简化）：
- 未传 `review_date`：默认取“最近一个有数据的交易日”
- 若当前时间位于 11:00-15:00 且未显式指定日期：倾向执行“午间复盘”逻辑（详见 3.2）

### 2.2 流式触发与进度：/api/trading/review/stream

实现见 [trading_endpoints.py](../app/api/trading_endpoints.py)（`/review/stream`）。

这是 Server-Sent Events（SSE）接口，用于在前端实时显示：
- `placeholder`：占位快照（立即返回）
- `log`：后台执行日志（持续输出）
- `final`：最终复盘结果（生成完成后输出一次并结束）
- `ping`：保活（无业务含义）

示例：

```bash
curl -N "http://127.0.0.1:8000/api/trading/review/stream?watchlist=600000.SH,000001.SZ"
```

前端接入逻辑见：
- [App.jsx](../../frontend/src/App.jsx)（`EventSource('/api/trading/review/stream?...')`）
- [ReviewModal.jsx](../../frontend/src/components/ReviewModal.jsx)（loading 展示占位数据与日志）

### 2.3 定时触发：午间自动复盘

调度器见 [scheduler.py](../app/services/scheduler.py)，其中 `noon_review` 定时任务会触发午间复盘逻辑（11:40）。

---

## 3. 复盘类型与核心流程

### 3.1 每日收盘复盘（perform_daily_review）

实现见 [review_service.py](../app/services/review_service.py)（`perform_daily_review`）。

流程（按执行顺序）：

1) 写入/更新“生成中”占位（`MarketSentiment.main_theme="生成中"`）

2) 获取全市场快照（涨跌家数、涨跌停、成交额等）
- 来源：`data_provider.get_market_snapshot(review_date)`

3) 连板天梯与情绪温度
- 连板梯队：`_analyze_limit_ladder(db, review_date)`
- 温度计算：`_calculate_market_temperature(stats, ladder_info)`

4) 机会构建：成交额 Top 与梯队联动机会
- 成交额 Top：`_build_turnover_top(db, top_n=80, trade_date=review_date)`
- 趋势过滤：`_passes_trend_filter(db, ts_code, review_date)`（见 4.1）
- 梯队机会：`_build_ladder_opportunities(db, ladder_info, turnover_top)`

5) 全网资讯（宏观/板块）
- `search_service.search_market_news()`

6) AI 生成市场总结（主线 + 总结）
- `_generate_ai_market_summary(review_date, stats, ladder, temperature, news, selector_tracking=...)`
- 输出要求为 JSON：`{"main_theme": "...", "summary": "..."}`（失败会走文本兜底）

7) 账户上下文（资金 + 持仓 + 今日计划）
- `_get_account_context(db)`（为后续 AI 计划生成提供约束与持仓感知）

8) 扫描次日机会并生成交易计划
- 候选池来源（按优先级）：
  - 用户传入 `watchlist`（最高优先级）
  - `stock_selector.scan_evening_opportunities()` 的龙头候选
  - 梯队联动候选 `ladder_opps`
  - 成交额 Top（取前 60）
- 候选去重与排序：按 priority、成交额、代码排序
- 对候选做趋势过滤（通过才进入 AI 分析队列）
- **AI 计划生成（并发）**：最多分析 `max_ai_analyze=15` 个，得分阈值 `min_confidence=70`，最终最多保留 `max_plans=8`
- 持久化：满足阈值的计划会通过 `trading_service.create_plan(..., source="system")` 写入 `TradingPlan`（计划日期为下一交易日）

9) 对持仓生成管理建议（并发）
- 对每个持仓调用 `_generate_ai_plan(..., strategy="持仓管理")`
- 输出动作转换为 `HOLD/SELL`
- 持久化到 `TradingPlan`（`ai_decision` 写入）

10) 回写最终 MarketSentiment 并返回结果
- main_theme/summary/温度/最高连板高度 等落库

### 3.2 午间复盘（perform_noon_review）

实现见 [review_service.py](../app/services/review_service.py)（`perform_noon_review`）。

午间复盘关注“盘中情绪与热点变化”，大体流程类似，但有差异：
- 连板梯队使用盘中实时涨停名单辅助：`get_realtime_limit_up_codes()`
- 候选池由午间机会扫描：`stock_selector.scan_noon_opportunities()`
- 计划日期为当日（`plan_date=date.today()`），适配盘中执行

---

## 4. 关键算法与过滤规则

### 4.1 趋势过滤（_passes_trend_filter）

实现见 [review_service.py](../app/services/review_service.py)（`_passes_trend_filter`）。

目的：避免把“弱趋势/过度乖离”的标的塞进候选池，降低 AI 误判率与计划噪声。

规则（简化）：
- 至少有 20 个交易日收盘价数据
- 满足 `MA5 > MA10 > MA20` 且 `收盘价 > MA20`
- 乖离约束：`收盘价 / MA20 <= 1.15`

### 4.2 候选池合并与排序

每日复盘候选池合并见 [review_service.py](../app/services/review_service.py)（`perform_daily_review` 内候选池合并部分）。

要点：
- 使用 `ts_code` 去重
- 引入 `priority`（自选股优先 > 梯队联动 > 龙头候选 > 成交额筛选）
- 同一标的若多来源命中：优先级取更高，成交额取更大

---

## 5. AI 生成逻辑（市场总结与交易计划）

### 5.1 市场总结（_generate_ai_market_summary）

实现见 [review_service.py](../app/services/review_service.py)（`_generate_ai_market_summary`）。

输入包括：
- 市场快照（上涨/下跌/涨停/成交额等）
- 连板天梯结构（ladder）
- 温度值（temperature）
- 市场资讯（news，截取前 1500 字符用于提示词）
- 近 7 日选股池跟踪（selector_tracking）

输出要求：
- 仅输出 JSON（否则会走解析兜底）
- main_theme 用于 UI 主标题
- summary 用于详细复盘内容

### 5.2 交易计划（_generate_ai_plan）

实现见 [review_service.py](../app/services/review_service.py)（`_generate_ai_plan`）。

统一逻辑：
- 获取多周期上下文：`chat_service.get_ai_trading_context(ts_code)`
- 获取实时价格：`data_provider.get_realtime_quote(ts_code)`
- 注入账户上下文（资金/持仓/可用数量等）
- 调用 `ai_service.analyze_realtime_trade_signal_v3(...)`

输出转换：
- 买入计划：仅当 `action == BUY` 才返回计划
- 持仓管理：将 `CANCEL` 映射为 `SELL`，其余映射为 `HOLD`

---

## 6. 数据落库与回读

### 6.1 MarketSentiment（复盘快照）

模型见 [stock_models.py](../app/models/stock_models.py)（`MarketSentiment`、`TradingPlan`）。

复盘流程中会：
- 开始时写入 “生成中” 占位（便于前端立刻显示并避免 404）
- 结束时写入最终 main_theme/summary/温度等结果

### 6.2 TradingPlan（计划建议）

模型见 [stock_models.py](../app/models/stock_models.py)（`TradingPlan`）。

复盘生成的计划会写入：
- 次日买入候选（target_plans）：`buy_price_limit/stop_loss_price/take_profit_price/position_pct/score`
- 持仓建议（holding_plans）：`ai_decision=SELL/HOLD`，策略名映射为“持仓卖出/持仓持有”

### 6.3 获取复盘结果：/api/trading/review/latest

实现见 [trading_endpoints.py](../app/api/trading_endpoints.py)（`/review/latest`）。

读取策略：
- 未传日期：按 `MarketSentiment.updated_at desc, id desc` 取最新记录
- 再通过 `review_service.get_review_result(date)` 把 ladder/turnover_top/plan 列表补齐后返回

---

## 7. 返回结构（SentimentResponse）

接口返回字段由 [trading_endpoints.py](../app/api/trading_endpoints.py) 中的 `SentimentResponse` 定义。

常用字段说明：
- `date`：复盘日期（YYYY-MM-DD）
- `up_count/down_count/limit_up_count/limit_down_count/total_volume`：市场快照
- `market_temperature`：温度值（由 `_calculate_market_temperature` 计算）
- `highest_plate`：最高连板高度
- `ladder/turnover_top/ladder_opportunities`：结构化机会数据
- `main_theme/summary`：AI 总结
- `target_plan/target_plans`：买入候选（结构见 5.2 输出）
- `holding_plans`：持仓建议
- `created_at`：落库时间（用于区分占位与最终结果）

---

## 8. 常见问题与排查

### 8.1 前端显示“生成中”一直不结束

优先排查：
- SSE 流是否持续输出 log（`/review/stream`）
- 后端日志是否出现 AI 调用报错/超时
- `MarketSentiment` 是否被更新（`updated_at` 是否刷新）

### 8.2 复盘很慢

主要耗时来自：
- 全网资讯抓取/整理
- 多只候选股并发 AI 决策（默认最多 15 只）

可调参方向：
- 降低 `max_ai_analyze` / `max_plans`
- 优化候选池过滤，减少进入 AI 的数量

### 8.3 AI 上下文过长导致失败

系统已引入“分段压缩 + 融合摘要”机制（不做简单截断），用于降低单次上下文体量并保留关键信息。

相关配置见 [config.py](../app/core/config.py)：
- `AI_CONTEXT_SUMMARY_ENABLED`
- `AI_CONTEXT_SUMMARY_TRIGGER_CHARS`
- `AI_CONTEXT_SUMMARY_CHUNK_CHARS`
- `AI_CONTEXT_SUMMARY_MAX_MERGE_CHARS`

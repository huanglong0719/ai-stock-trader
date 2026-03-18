# 通达信 (TDX) 分钟数据服务接口文档

## 1. 概述
本项目集成了通达信 (TDX) 接口，用于获取 **5分钟** 和 **30分钟** 的分钟 K 线数据，支持前复权 (QFQ)。

通达信盘后分钟数据通常需要 **16:00 之后** 才能稳定获取，因此本系统默认把“分钟线自动增量同步”放在盘后执行（可配置）。

## 2. 数据架构

### 2.1 数据流向
1.  **接入层**: `TdxDataService` 通过 `pytdx` 直连通达信行情服务器 (TCP) 拉取分钟 K 线。
2.  **缓存层**: 最新分钟数据写入 Redis：`MARKET:MIN:{freq}:{ts_code}`（List<JSON>，默认保留最新 800 条），用于盘中快速展示。
3.  **持久层**: 盘后将近 30 天分钟数据增量 upsert 写入 SQLite 表 `minute_bars`（用于稳定与快速查询）。
4.  **对外服务**: `/api/market/kline/{symbol}?freq=5min|30min` 优先返回 **本地 minute_bars + Redis 实时拼接**，仅在本地没有数据时回退 TDX。

### 2.2 存储方案
*   **Redis Key**: `MARKET:MIN:5min:000001.SZ` (List<JSON>)
*   **Database**: 
    *   表名: `minute_bars`
    *   唯一键: `(ts_code, trade_time, freq)`
    *   字段: `ts_code`, `trade_time`, `freq`, `open`, `high`, `low`, `close`, `vol`, `amount`, `adj_factor`

## 3. API 接口

### 3.1 获取 K 线数据
*   **Endpoint**: `GET /api/market/kline/{symbol}`
*   **Parameters**:
    *   `freq`: 频率，支持 `5min`, `30min` (新增), `D`, `W`, `M`
    *   `start`/`end`: 可选日期范围（分钟线默认返回近约 1 个月数据点，避免一次返回过大）
    *   `limit`: 可选返回条数（分钟线会自动限制上限以保证响应稳定）
*   **Example**:
    ```http
    GET /api/market/kline/000001.SZ?freq=5min&limit=100
    ```
*   **Response**:
    ```json
    [
      {
        "time": "2023-10-27 14:55:00",
        "open": 10.5,
        "close": 10.52,
        "high": 10.53,
        "low": 10.49,
        "volume": 50000,
        "adj_factor": 1.0,
        "ma5": 10.51,
        "macd": 0.02
      },
      ...
    ]
    ```

### 3.2 实时行情状态
*   **Endpoint**: `GET /api/market/overview`
    *   包含大盘指数及实时涨跌统计。

### 3.3 手动触发分钟数据下载（后台任务）
*   **Endpoint**: `POST /api/sync/minute/download`
*   **Body**:
    *   `pool`: `shsz`（沪深全市场）或 `active`（持仓+计划）
    *   `days`: 下载近 N 天（建议 30）
    *   `freqs`: `["5min","30min"]`
    *   `limit`: `shsz` 池限制下载股票数量（建议先 200 小规模验证）

## 4. 自动化任务 (Scheduler)

| 任务 ID | 执行时间 | 描述 |
| :--- | :--- | :--- |
| `post_close_minute_sync` | 工作日 16:10（可配置） | 盘后分钟数据增量同步（写入 minute_bars，并更新 Redis 最新分钟数据） |
| `daily_sync` | 工作日 17:30 | 日线同步 + 指标增量 + 复盘 |

## 5. 异常处理
*   **连接失败**: 自动轮询备用服务器 IP 列表。
*   **数据缺失**: 分钟 K 线优先返回本地 minute_bars；若本地无数据则回退 TDX，失败时返回空列表（前端显示“暂无行情数据”）。
*   **盘后时点**: 通达信盘后数据可能 16:00 前不完整，建议把增量同步放在 16:00 之后执行。
*   **复权异常**: 如缺失复权因子，默认使用 1.0。

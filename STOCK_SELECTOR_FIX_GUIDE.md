# 选股功能修复指南

## 问题诊断

选股功能点击后立即提示完成的根本原因是：**StockIndicator 表为空**

### 问题链路
1. 用户点击"选股" → 调用 `stock_selector.py` 的 `select_stocks()` 方法
2. `select_stocks()` → 调用 `_filter_candidates()` 进行初选
3. `_filter_candidates()` → 调用 `_get_indicators_batch()` 获取预计算指标
4. 由于 `StockIndicator` 表为空，所有股票都被过滤掉
5. 初选结果为空 → 直接返回空列表 → 提示"完成"

## 解决方案

### 步骤 1: 验证数据库状态

运行检查脚本确认问题：

```bash
python check_indicators.py
```

预期输出应该显示：
- StockIndicator 表记录数: 0
- Stock 表记录数: > 0
- DailyBar 表记录数: > 0

### 步骤 2: 运行指标计算

执行以下命令计算所有股票的技术指标：

```bash
cd backend
python trigger_indicator_calc.py
```

**注意事项：**
- 这个过程可能需要 5-15 分钟（取决于股票数量）
- 脚本会显示进度信息
- 计算完成后会显示 "✅ 指标计算完成！"

### 步骤 3: 验证修复结果

再次运行检查脚本：

```bash
python check_indicators.py
```

现在应该看到 StockIndicator 表有数据了。

### 步骤 4: 测试选股功能

重新启动应用并测试选股功能：

```bash
# 如果应用正在运行，先停止
# 然后重新启动
start.bat
```

现在点击"选股"应该能正常工作了。

## 技术细节

### 指标计算内容

`indicator_service.py` 会为每只股票计算以下指标：

**日线指标：**
- MA5, MA10, MA20, MA60（移动平均线）
- VOL_MA5（成交量均线）
- MACD_DIFF, MACD_DEA（MACD 指标）
- is_daily_bullish（日线多头信号）
- is_trend_recovering（趋势恢复信号）

**周线指标：**
- weekly_ma20（周线 MA20）
- weekly_ma20_slope（周线 MA20 斜率）
- is_weekly_bullish（周线多头信号）

**月线指标：**
- monthly_ma20（月线 MA20）
- is_monthly_bullish（月线多头信号，包含复杂的启动柱逻辑）

### 选股逻辑

选股器使用这些预计算指标进行多维度筛选：

1. **基础过滤**：排除 ST、科创板、北交所、估值过高的股票
2. **强势回调 (Pullback) 专项逻辑 (2026-01-12)**:
   - **强势背景**: 最近 20 日涨幅 > 12% 或存在涨停板历史。
   - **回调形态**: 2%-30% 的适度回撤，回踩 MA20 支撑线。
   - **成交量特征**: 回调期缩量，显示抛压枯竭。
3. **技术面通用筛选**:
   - 月线趋势必须向上（is_monthly_bullish）
   - 周线趋势必须向上（is_weekly_bullish）
   - 周线斜率不能太差（weekly_ma20_slope > -5）
   - 日线趋势向上或正在恢复
3. **资金流排序**：按资金净流入比率排序
4. **实时行情校验**：股价不能大幅偏离 MA5
5. **AI 深度分析**：对初选股票进行 AI 分析和评分

## 后续优化建议

### 1. 定时任务

建议设置定时任务每天自动计算指标：

```python
# 可以在 backend/app/tasks/ 目录下创建定时任务
# 或使用 Windows 任务计划程序每天收盘后运行
```

### 2. 增量更新

当前是全量计算，可以优化为增量更新：
- 只更新有新数据的股票
- 只更新最新交易日的指标

### 3. 错误处理优化

在 `stock_selector.py` 中增加更友好的错误提示：

```python
if indicators_map is None or len(indicators_map) == 0:
    selector_logger.log("警告：指标数据为空，请先运行指标计算", level="ERROR")
    return []
```

## 常见问题

**Q: 为什么指标表会是空的？**
A: 可能是首次部署，或者数据库被重置了。指标需要手动触发计算。

**Q: 多久需要重新计算一次指标？**
A: 建议每天收盘后计算一次，确保数据是最新的。

**Q: 计算指标会影响性能吗？**
A: 首次计算较慢（5-15分钟），但之后选股会非常快，因为都是查询预计算的结果。

**Q: 如果计算失败怎么办？**
A: 检查日志输出，通常是因为：
- 数据库连接问题
- DailyBar 表数据不足（需要至少 60 天数据）
- 内存不足（可以调整批次大小）

import sys
import os
from datetime import datetime, timedelta

# Adjust path
current_dir = os.getcwd()
backend_dir = os.path.join(current_dir, 'backend')
sys.path.append(backend_dir)

from app.services.market.market_data_service import MarketDataService
from app.services.market.stock_data_service import stock_data_service

# Mock stock_data_service.aggregate_period_stats
def mock_aggregate_stats(ts_code, period_start, curr_date):
    print(f"[Mock] Aggregating from {period_start} to {curr_date}")
    # 假设本周前两天 (1.5, 1.6) 的数据：
    # 1.5 Open: 9.02, High: 9.15, Low: 8.96, Vol: 1000
    # 1.6 Open: 9.12, High: 9.20, Low: 9.05, Vol: 1200
    # 聚合: Open: 9.02, MaxHigh: 9.20, MinLow: 8.96, SumVol: 2200
    return {
        "sum_vol": 2200,
        "max_high": 9.20,
        "min_low": 8.96,
        "open": 9.02 # 正确的周一开盘价
    }

stock_data_service.aggregate_period_stats = mock_aggregate_stats

def test_merge():
    service = MarketDataService()
    
    # 1. 模拟历史周线 (截止到上周五 2026-01-02)
    kline = [
        {"time": "2026-01-02", "open": 8.5, "close": 8.8, "high": 8.9, "low": 8.4, "volume": 5000}
    ]
    
    # 2. 模拟今天的实时数据 (2026-01-07 周三)
    # 开盘 9.10, 最高 9.29, 最低 9.03, 收盘 9.17
    quote = {
        "symbol": "002658.SZ",
        "time": "2026-01-07 15:00:00",
        "open": 9.10,
        "high": 9.29,
        "low": 9.03,
        "price": 9.17,
        "vol": 192000,
        "pct_chg": 0.77
    }
    
    print("【测试前】 最后一根Bar:", kline[-1])
    print("【实时数据】:", quote)
    
    # 3. 执行合并
    result = service.merge_realtime_to_kline(kline, quote, freq='W')
    
    # 4. 验证结果
    last_bar = result[-1]
    print("【测试后】 最后一根Bar:", last_bar)
    
    expected_open = 9.02
    if last_bar['open'] == expected_open:
        print(f"✅ 测试通过! Open价正确: {last_bar['open']}")
    else:
        print(f"❌ 测试失败! Open价错误: {last_bar['open']} (预期: {expected_open})")
        print(f"   (如果是 9.10，说明使用了实时数据的Open，覆盖了周线Open)")

if __name__ == "__main__":
    test_merge()

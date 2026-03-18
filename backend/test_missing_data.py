import asyncio
import logging
import sys
from datetime import datetime
from app.services.market.market_data_service import market_data_service
from app.services.market.stock_data_service import stock_data_service

# 配置日志到控制台
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def test_missing_data():
    ts_code = "300308.SZ" # 中际旭创
    
    print(f"Testing get_kline for {ts_code}...")
    
    # 1. 检查数据库中最新的交易日
    last_trade_date = await market_data_service.get_last_trade_date(include_today=True)
    print(f"System last_trade_date: {last_trade_date}")
    
    # 2. 检查本地数据库中该股票的最新数据
    local_kline = await asyncio.to_thread(stock_data_service.get_local_kline, ts_code, 'D', limit=5)
    if local_kline:
        print(f"Local DB last bar: {local_kline[-1]['time']}")
    else:
        print("Local DB empty for this stock")
        
    # 3. 调用 get_kline 看看实际返回
    # 模拟 UI 请求
    kline = await market_data_service.get_kline(ts_code, freq='D', limit=800, is_ui_request=True, local_only=False)
    
    if kline:
        print(f"Returned {len(kline)} bars")
        print("Last 5 bars:")
        for bar in kline[-5:]:
            print(f"{bar['time']} O:{bar['open']} C:{bar['close']} V:{bar['volume']}")
    else:
        print("get_kline returned empty")

if __name__ == "__main__":
    asyncio.run(test_missing_data())

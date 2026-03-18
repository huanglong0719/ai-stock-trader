
import asyncio
import logging
import pandas as pd
from datetime import datetime
import os
import sys

# 设置路径
sys.path.insert(0, os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import market_data_service
from app.services.market.tushare_client import tushare_client

logging.basicConfig(level=logging.INFO)

async def test_minute_kline():
    ts_code = '000001.SZ'
    print(f"\n=== Testing 5min K-line for {ts_code} ===")
    
    try:
        # 获取 5min K线
        kline = await market_data_service.get_kline(ts_code, freq='5min', limit=20)
        
        if not kline:
            print("❌ No kline data returned!")
            return
            
        df = pd.DataFrame(kline)
        print(f"Total bars: {len(df)}")
        print(f"Latest 5 bars:")
        print(df[['time', 'open', 'high', 'low', 'close', 'volume']].tail(5))
        
        last_time = df.iloc[-1]['time']
        print(f"\nLast bar time: {last_time}")
        print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查是否包含今天的数据
        today_str = datetime.now().strftime('%Y-%m-%d')
        if today_str in last_time:
            print("✅ Contains today's data.")
        else:
            print("❌ Missing today's data.")
            
    finally:
        # 强制关闭资源
        await tushare_client.close()

if __name__ == "__main__":
    asyncio.run(test_minute_kline())

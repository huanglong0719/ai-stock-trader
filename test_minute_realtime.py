import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import market_data_service

async def test_realtime_minute_data():
    ts_code = "000001.SZ" # 平安银行
    freq = "5min"
    
    print(f"Testing {freq} kline for {ts_code}...")
    print(f"Current time: {datetime.now()}")
    
    # 获取 5 分钟线
    res = await market_data_service.get_kline(ts_code, freq=freq, limit=50)
    
    if not res:
        print("Error: No data returned!")
        return
        
    print(f"Total bars returned: {len(res)}")
    last_bar = res[-1]
    print(f"Last bar: {last_bar}")
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    if last_bar['time'].startswith(today_str):
        print(f"SUCCESS: Found today's data! Last bar time: {last_bar['time']}")
    else:
        print(f"FAILURE: No data for today. Last bar time: {last_bar['time']}")

if __name__ == "__main__":
    asyncio.run(test_realtime_minute_data())

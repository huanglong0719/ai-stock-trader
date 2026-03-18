import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import MarketDataService

async def test_kline_flow():
    service = MarketDataService()
    ts_code = "000001.SZ"
    
    # 模拟盘中调用 get_kline
    # 假设本地数据只到 2026-01-09
    # 今天是 2026-01-12
    print(f"Testing get_kline for {ts_code} with is_ui_request=True...")
    kline = await service.get_kline(ts_code, freq='D', is_ui_request=True)
    
    if kline:
        print(f"Total bars: {len(kline)}")
        print(f"Last bar: {kline[-1]}")
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        last_date = kline[-1]['time'].split(' ')[0]
        
        if last_date == today_str:
            print("SUCCESS: Today's K-line is present.")
        else:
            print(f"FAILURE: Today's K-line is MISSING. Last date is {last_date}, expected {today_str}")
    else:
        print("FAILURE: No kline data returned.")

if __name__ == "__main__":
    asyncio.run(test_kline_flow())

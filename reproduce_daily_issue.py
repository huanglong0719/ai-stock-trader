
import asyncio
import sys
import os
from datetime import datetime, time

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import MarketDataService

async def reproduce_issue():
    service = MarketDataService()
    
    # 1. 模拟日线场景
    print("\n--- Testing Daily Kline Merge ---")
    kline = [
        {"time": "2026-01-30", "open": 10.0, "high": 11.0, "low": 9.9, "close": 10.5, "volume": 1000}
    ]
    
    # Case A: Quote time is exactly 15:00:00 (Should merge)
    quote_a = {
        "time": "2026-02-02 15:00:00", 
        "price": 10.8, "open": 10.5, "high": 10.9, "low": 10.4, "vol": 2000, "amount": 20000
    }
    
    res_a = await service.merge_realtime_to_kline(list(kline), quote_a, freq='D')
    print(f"Case A (15:00:00): Last bar time: {res_a[-1]['time']}")
    if len(res_a) == 2 and res_a[-1]['time'].startswith('2026-02-02'):
        print("SUCCESS: Merged 15:00:00 quote")
    else:
        print("FAILURE: Failed to merge 15:00:00 quote")

    # Case B: Quote time is 15:01:00 (Likely fails currently)
    quote_b = {
        "time": "2026-02-02 15:01:00", 
        "price": 10.8, "open": 10.5, "high": 10.9, "low": 10.4, "vol": 2000, "amount": 20000
    }
    
    res_b = await service.merge_realtime_to_kline(list(kline), quote_b, freq='D')
    print(f"Case B (15:01:00): Last bar time: {res_b[-1]['time']}")
    if len(res_b) == 2 and res_b[-1]['time'].startswith('2026-02-02'):
        print("SUCCESS: Merged 15:01:00 quote")
    else:
        print("FAILURE: Failed to merge 15:01:00 quote")

    # Case C: Quote time is 15:30:00 (Likely fails currently)
    quote_c = {
        "time": "2026-02-02 15:30:00", 
        "price": 10.8, "open": 10.5, "high": 10.9, "low": 10.4, "vol": 2000, "amount": 20000
    }
    
    res_c = await service.merge_realtime_to_kline(list(kline), quote_c, freq='D')
    print(f"Case C (15:30:00): Last bar time: {res_c[-1]['time']}")
    if len(res_c) == 2 and res_c[-1]['time'].startswith('2026-02-02'):
        print("SUCCESS: Merged 15:30:00 quote")
    else:
        print("FAILURE: Failed to merge 15:30:00 quote")

if __name__ == "__main__":
    asyncio.run(reproduce_issue())

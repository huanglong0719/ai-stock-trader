import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import MarketDataService

async def debug_kline():
    service = MarketDataService()
    
    # Mock kline (yesterday's data)
    kline = [
        {
            "time": "2026-01-09",
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "volume": 1000000.0,
            "pct_chg": 2.0,
            "adj_factor": 1.0
        }
    ]
    
    # Mock quote (today's realtime data)
    quote = {
        "symbol": "000001.SZ",
        "price": 10.5,
        "open": 10.1,
        "high": 10.6,
        "low": 10.0,
        "vol": 500000.0,
        "pct_chg": 2.94,
        "time": "2026-01-12 10:30:00"
    }
    
    print("Initial kline:", kline)
    print("Quote:", quote)
    
    # Simulate merge
    merged = await service.merge_realtime_to_kline(list(kline), quote, freq='D')
    
    print("\nMerged kline (D):", merged)
    
    # Check time format
    if merged:
        print(f"Last bar time: {merged[-1]['time']}")

if __name__ == "__main__":
    asyncio.run(debug_kline())

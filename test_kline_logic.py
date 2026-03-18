import sys
import os
import asyncio
from datetime import datetime

# Add backend directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from app.services.data_provider import data_provider

async def test_get_kline_002009():
    print("Testing get_kline for 002009.SZ...")
    try:
        # Simulate the request made by UI
        # UI likely requests with freq='D', adj='qfq' (default)
        kline = await data_provider.get_kline("002009.SZ", freq="D", limit=300, is_ui_request=True)
        
        if not kline:
            print("No kline data returned!")
            return

        print(f"Returned {len(kline)} bars.")
        
        # Find the bar for 2025-12-03
        target_bar = None
        for bar in kline:
            if bar['time'].startswith('2025-12-03'):
                target_bar = bar
                break
        
        if target_bar:
            print(f"Bar 2025-12-03: {target_bar}")
        else:
            print("Bar 2025-12-03 not found in returned kline.")
            
        # Also print last bar
        print(f"Last Bar: {kline[-1]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_get_kline_002009())

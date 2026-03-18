
import asyncio
import sys
import os
from datetime import datetime, time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.data_provider import data_provider
from app.services.market.market_utils import is_trading_time

async def check():
    print("=== Diagnostic Check ===")
    
    # 1. Check Trade Day for Monday Feb 2nd
    date_str = "20260202"
    print(f"Checking trade day for {date_str} (Monday)...")
    try:
        res = await data_provider.check_trade_day(date_str)
        print(f"Result: {res}")
    except Exception as e:
        print(f"Error: {e}")

    # 2. Check Trade Day for Tuesday Feb 3rd
    date_str = "20260203"
    print(f"Checking trade day for {date_str} (Tuesday)...")
    try:
        res = await data_provider.check_trade_day(date_str)
        print(f"Result: {res}")
    except Exception as e:
        print(f"Error: {e}")

    # 3. Check Trading Time logic (mocking time)
    print("\nChecking is_trading_time logic...")
    # Mock datetime.now() is hard, but we can check the function logic if we could pass time.
    # market_utils.is_trading_time() uses datetime.now().
    # Let's just print current result.
    print(f"Current time: {datetime.now()}")
    print(f"is_trading_time(): {is_trading_time()}")

    # 4. Check Scheduler Job Configuration (if possible without starting)
    # This is harder, skipping.

if __name__ == "__main__":
    asyncio.run(check())

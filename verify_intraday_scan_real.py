import asyncio
import sys
import os
import datetime
from unittest.mock import MagicMock

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

# Monkey-patch trading_service to avoid writing to DB
from app.services.trading_service import trading_service
original_create_plan = trading_service.create_plan

async def mock_create_plan(**kwargs):
    print(f"\n[Real-Data Test] Would create plan: {kwargs['ts_code']} {kwargs['strategy_name']} {kwargs['action'] if 'action' in kwargs else 'BUY'} Price: {kwargs.get('buy_price')} Score: {kwargs.get('score')}")
    # Call original if you want to verify DB write, but user might not want spam.
    # User said "full loop", so maybe we should let it write?
    # But let's print it clearly first.
    return True

trading_service.create_plan = mock_create_plan

async def verify_intraday_scan_real():
    print(f">>> Starting Real-Data Intraday Scan Verification at {datetime.datetime.now()} <<<")
    
    # Import services
    from app.services.monitor_service import monitor_service
    from app.services.review_service import review_service
    from app.services.stock_selector import stock_selector
    from app.services.data_provider import data_provider
    
    # Check if we can fetch real data
    print(">>> Checking Market Data Connection <<<")
    try:
        # Test with a known active stock
        quotes = await data_provider.get_realtime_quotes(['000001.SZ'])
        if quotes and '000001.SZ' in quotes:
            q = quotes['000001.SZ']
            print(f"[Data] 000001.SZ: Price={q.get('price')}, Time={q.get('time')}, Source={q.get('source', 'unknown')}")
        else:
            print("[Data] Failed to fetch 000001.SZ quote!")
            return
    except Exception as e:
        print(f"[Data] Error fetching quote: {e}")
        return

    # Run the scan
    print("\n>>> Executing Intraday Scan (Real Data + AI) <<<")
    # We call review_service.perform_intraday_scan directly to await it and see logs
    # monitor_service wraps it with logging/timeout
    
    try:
        # Note: This will trigger AI calls which might take time/cost
        await review_service.perform_intraday_scan()
        print("\n>>> Scan Completed <<<")
    except Exception as e:
        print(f"\n!!! Scan Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(verify_intraday_scan_real())
    except KeyboardInterrupt:
        print("\nTest Interrupted")
    except Exception as e:
        print(f"\nTest Error: {e}")

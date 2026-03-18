
import sys
import os
import asyncio
import time
import pandas as pd
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.tdx_data_service import tdx_service
from app.services.data_sync import data_sync_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_minute_download():
    ts_code = "000001.SZ"
    freq = "5min"
    
    print(f"Testing download for {ts_code} {freq}...")
    start_time = time.time()
    
    # 1. Test single fetch via service
    df = tdx_service.fetch_minute_bars(ts_code, freq, count=800, start=0)
    print(f"Single fetch result: {len(df)} rows")
    if not df.empty:
        print(f"Time range: {df['trade_time'].min()} -> {df['trade_time'].max()}")
        print(df.tail())
    
    # 2. Test data_sync logic (mocking the worker)
    print("\nTesting data_sync logic...")
    try:
        # We use a short range to be quick
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=5)).strftime("%Y%m%d")
        
        count = await asyncio.to_thread(
            data_sync_service.download_minute_data, 
            ts_code, start_date, end_date, freq, force_network=True
        )
        print(f"Data sync saved {count} records.")
    except Exception as e:
        print(f"Data sync failed: {e}")
        import traceback
        traceback.print_exc()

    print(f"Total time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    asyncio.run(test_minute_download())

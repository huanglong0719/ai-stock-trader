
import asyncio
import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.tdx_vipdoc_service import TdxVipdocService
from app.core.config import settings

def test_aggregation():
    # Mock some 5min data
    # 09:35, 09:40, 09:45, 09:50, 09:55, 10:00 -> Should aggregate to 10:00
    data = {
        "trade_time": [
            "2026-02-02 09:35:00",
            "2026-02-02 09:40:00", 
            "2026-02-02 09:45:00",
            "2026-02-02 09:50:00",
            "2026-02-02 09:55:00",
            "2026-02-02 10:00:00",
            "2026-02-02 10:05:00" # Should be in 10:30 bucket
        ],
        "open": [10, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6],
        "high": [10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7],
        "low": [9.9, 10.0, 10.1, 10.2, 10.3, 10.4, 10.5],
        "close": [10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7],
        "vol": [100, 100, 100, 100, 100, 100, 50],
        "amount": [1000, 1000, 1000, 1000, 1000, 1000, 500]
    }
    df_5m = pd.DataFrame(data)
    
    print("Original 5min Data:")
    print(df_5m)
    
    agg = TdxVipdocService.aggregate_30min_from_5min(df_5m)
    print("\nAggregated 30min Data:")
    print(agg)
    
    # Check 10:00 bucket
    bucket_1000 = agg[agg['trade_time'] == pd.Timestamp("2026-02-02 10:00:00")]
    if not bucket_1000.empty:
        vol = bucket_1000.iloc[0]['vol']
        print(f"\n10:00 Bucket Vol: {vol}")
        if vol == 600:
            print("SUCCESS: Volume aggregation correct (600)")
        else:
            print(f"FAILURE: Volume aggregation incorrect, expected 600, got {vol}")
    else:
        print("FAILURE: 10:00 Bucket missing")

    # Check 10:30 bucket
    bucket_1030 = agg[agg['trade_time'] == pd.Timestamp("2026-02-02 10:30:00")]
    if not bucket_1030.empty:
        vol = bucket_1030.iloc[0]['vol']
        print(f"10:30 Bucket Vol: {vol}")
        if vol == 50:
            print("SUCCESS: Volume aggregation correct (50)")
        else:
            print(f"FAILURE: Volume aggregation incorrect, expected 50, got {vol}")
    else:
        print("FAILURE: 10:30 Bucket missing")

if __name__ == "__main__":
    test_aggregation()

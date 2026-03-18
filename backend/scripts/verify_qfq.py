import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.data_provider import data_provider
import pandas as pd

def verify_qfq():
    ts_code = '001311.SZ'
    print(f"Verifying QFQ data for {ts_code}...")
    
    # Get Daily Kline
    kline = data_provider.get_kline(ts_code, freq='D', start_date='20230101')
    if not kline:
        print("No kline data returned.")
        return

    df = pd.DataFrame(kline)
    print(f"Loaded {len(df)} records.")
    
    # Find the peak price
    max_close = df['close'].max()
    max_high = df['high'].max()
    
    peak_row = df.loc[df['high'].idxmax()]
    
    print(f"Max High (QFQ): {max_high}")
    print(f"Max Close (QFQ): {max_close}")
    print(f"Peak Date: {peak_row['time']}")
    
    if max_high > 100:
        print("❌ Verification Failed: Price still too high (around 100+).")
    elif 40 < max_high < 50:
        print("✅ Verification Passed: Price is in reasonable range (40-50).")
    else:
        print(f"⚠️ Verification Uncertain: Price is {max_high}")

if __name__ == "__main__":
    verify_qfq()

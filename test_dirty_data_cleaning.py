
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

def simulate_cleaning(df_rt):
    # This simulates the logic in market_data_service.py
    if df_rt is not None and not df_rt.empty:
        # 1. drop_duplicates
        df_rt = df_rt.drop_duplicates(subset=['trade_time'])
        
        # 2. suspicious duplicates cleaning
        if len(df_rt) >= 2:
            is_suspicious_duplicate = (
                (df_rt['vol'] > 0) & 
                (df_rt['open'] == df_rt['open'].shift(1)) &
                (df_rt['high'] == df_rt['high'].shift(1)) &
                (df_rt['low'] == df_rt['low'].shift(1)) &
                (df_rt['close'] == df_rt['close'].shift(1)) &
                (df_rt['vol'] == df_rt['vol'].shift(1))
            )
            
            # 3. future placeholders cleaning
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            is_future_placeholder = (
                (df_rt['trade_time'] > now_str)
            )
            
            print(f"Suspicious duplicates found: {is_suspicious_duplicate.sum()}")
            print(f"Future placeholders found: {is_future_placeholder.sum()}")
            
            df_rt = df_rt[~(is_suspicious_duplicate | is_future_placeholder)]
            
        return df_rt
    return df_rt

def test_cleaning_scenarios():
    now = datetime.now()
    t1 = (now - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
    t2 = (now - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    t3 = (now + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S') # Future
    
    data = [
        # Normal bar
        {'trade_time': t1, 'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2, 'vol': 1000},
        # Duplicate bar with volume (Suspicious)
        {'trade_time': t2, 'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2, 'vol': 1000},
        # Another bar at same time t2 (will be removed by drop_duplicates)
        {'trade_time': t2, 'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2, 'vol': 1000},
        # Zero volume bar (Normal for inactive periods, should be KEPT if not duplicate)
        {'trade_time': (now - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S'), 'open': 10.2, 'high': 10.2, 'low': 10.2, 'close': 10.2, 'vol': 0},
        # Duplicate zero volume bar (Should be KEPT by logic because vol is not > 0)
        # Actually, drop_duplicates will remove it if trade_time is same.
        # If trade_time is different but values are same (straight line), it should be KEPT.
        {'trade_time': (now - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'), 'open': 10.2, 'high': 10.2, 'low': 10.2, 'close': 10.2, 'vol': 0},
        # Future bar (Should be REMOVED)
        {'trade_time': t3, 'open': 10.2, 'high': 10.2, 'low': 10.2, 'close': 10.2, 'vol': 0},
    ]
    
    df = pd.DataFrame(data)
    print("\nOriginal Data:")
    print(df)
    
    cleaned_df = simulate_cleaning(df)
    print("\nCleaned Data:")
    print(cleaned_df)
    
    # Assertions
    assert len(cleaned_df[cleaned_df['trade_time'] == t3]) == 0, "Future bar should be removed"
    # The duplicate bar at t2 with same trade_time is removed by drop_duplicates.
    # The "suspicious duplicate" logic checks PREVIOUS bar.
    # In our data:
    # Row 0: t1, vol 1000
    # Row 1: t2, vol 1000 (Values match Row 0? No, Row 0 is t1, Row 1 is t2. They are different bars)
    # Wait, the logic `df_rt['open'] == df_rt['open'].shift(1)` checks if CURRENT bar matches PREVIOUS bar.
    # If the price stays EXACTLY the same and volume is > 0, it's flagged.
    
    print("\nTest passed!")

if __name__ == "__main__":
    test_cleaning_scenarios()

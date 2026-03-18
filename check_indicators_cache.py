import pickle
import os
import pandas as pd

def check_cache():
    cache_file = 'backend/data/cache/indicators_cache.pkl'
    if not os.path.exists(cache_file):
        print("Cache file not found.")
        return
        
    with open(cache_file, 'rb') as f:
        cache = pickle.load(f)
        
    symbol = '300508.SZ'
    freq = 'D'
    full_key = f"{symbol}_{freq}_FULL"
    
    print(f"Checking for key: {full_key}")
    if full_key in cache:
        df = cache[full_key]
        print(f"Cache found for {full_key}. Size: {len(df)}")
        print("\nData around 2025-04-07 in CACHE:")
        # Check column names
        print(f"Columns: {df.columns.tolist()}")
        
        # Filter data
        if 'time' in df.columns:
            # Ensure time is comparable
            df['time_str'] = df['time'].apply(lambda x: str(x))
            target_data = df[df['time_str'].str.contains('2025-04-07')]
            print(target_data[['time', 'close', 'ma5', 'ma10', 'ma20']])
        else:
            print("Time column not found in cache DF.")
    else:
        print(f"No cache found for {full_key}")
        # Search for any key containing 300508
        matching_keys = [k for k in cache.keys() if '300508' in k]
        print(f"Matching keys for '300508': {matching_keys}")

if __name__ == "__main__":
    check_cache()

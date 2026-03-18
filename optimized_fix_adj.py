import sqlite3
import pandas as pd
import time
import os

db_path = 'd:/木偶说/backend/aitrader.db'

def optimized_fix():
    print("Starting optimized adj_factor fix...")
    start_time = time.time()
    
    conn = sqlite3.connect(db_path)
    
    # 1. Load valid factors
    print("Loading valid factors into memory...")
    # We select only necessary columns to minimize memory usage
    df = pd.read_sql_query("SELECT ts_code, trade_date, adj_factor FROM daily_bars WHERE adj_factor IS NOT NULL", conn)
    print(f"Loaded {len(df)} rows. ({time.time() - start_time:.1f}s)")
    
    if df.empty:
        print("No valid factors found!")
        conn.close()
        return

    # 2. Find latest factor for each stock
    print("Calculating latest factors...")
    # Sort by date to ensure we get the latest
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    latest_factors = df.sort_values('trade_date').groupby('ts_code')['adj_factor'].last()
    
    factor_map = latest_factors.to_dict()
    print(f"Found factors for {len(factor_map)} stocks.")
    
    # 3. Find stocks that need update
    print("Identifying stocks with missing factors...")
    missing_df = pd.read_sql_query("SELECT DISTINCT ts_code FROM daily_bars WHERE adj_factor IS NULL", conn)
    stocks_to_fix = missing_df['ts_code'].tolist()
    print(f"Found {len(stocks_to_fix)} stocks needing update.")
    
    # 4. Prepare updates
    updates = []
    for ts_code in stocks_to_fix:
        if ts_code in factor_map:
            updates.append((factor_map[ts_code], ts_code))
        else:
            # If no history at all, default to 1.0? Or leave it?
            # Let's default to 1.0 to prevent calculation errors
            updates.append((1.0, ts_code))
            
    print(f"Prepared {len(updates)} updates.")
    
    # 5. Execute Batch Update
    print("Executing batch update...")
    update_start = time.time()
    
    cursor = conn.cursor()
    batch_size = 1000
    
    # Use executemany in batches
    # Query: Update all rows for this stock that have NULL factor
    sql = "UPDATE daily_bars SET adj_factor = ? WHERE ts_code = ? AND adj_factor IS NULL"
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        cursor.executemany(sql, batch)
        conn.commit()
        if (i + batch_size) % 1000 == 0:
             print(f"Updated {i + batch_size} stocks...")

    conn.commit()
    conn.close()
    print(f"Fix completed in {time.time() - start_time:.1f}s.")

if __name__ == "__main__":
    optimized_fix()

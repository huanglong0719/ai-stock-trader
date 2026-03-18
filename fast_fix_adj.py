import sqlite3
import time

db_path = 'd:/木偶说/backend/aitrader.db'

def fast_fix_all_stocks():
    print("Starting fast global adj_factor fix...")
    conn = sqlite3.connect(db_path)
    # Increase cache size for speed
    conn.execute("PRAGMA cache_size = -64000") # 64MB
    cursor = conn.cursor()
    
    start_time = time.time()
    
    # 1. Fetch latest factors for all stocks in one go
    print("Fetching latest factors map...")
    query = """
    SELECT t1.ts_code, t1.adj_factor 
    FROM daily_bars t1 
    JOIN (
        SELECT ts_code, MAX(trade_date) as max_date 
        FROM daily_bars 
        WHERE adj_factor IS NOT NULL 
        GROUP BY ts_code
    ) t2 ON t1.ts_code = t2.ts_code AND t1.trade_date = t2.max_date
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    factor_map = {r[0]: r[1] for r in rows}
    print(f"Fetched {len(factor_map)} latest factors. ({time.time() - start_time:.1f}s)")
    
    # 2. Get list of stocks that need update
    print("Identifying stocks to update...")
    cursor.execute("SELECT DISTINCT ts_code FROM daily_bars WHERE adj_factor IS NULL")
    affected_stocks = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(affected_stocks)} affected stocks.")
    
    # 3. Batch Update
    update_start = time.time()
    count = 0
    
    # Prepare batch update
    # Using executemany might be tricky with different values, so we loop but use one transaction
    
    for i, ts_code in enumerate(affected_stocks):
        factor = factor_map.get(ts_code, 1.0) # Default to 1.0 if no history found
        
        cursor.execute("UPDATE daily_bars SET adj_factor = ? WHERE ts_code = ? AND adj_factor IS NULL", (factor, ts_code))
        count += 1
        
        if count % 1000 == 0:
            print(f"Queued {count} updates...")
            
    print("Committing transaction...")
    conn.commit()
    
    end_time = time.time()
    print(f"Done. Updated {count} stocks in {end_time - update_start:.1f}s.")
    conn.close()

if __name__ == "__main__":
    fast_fix_all_stocks()

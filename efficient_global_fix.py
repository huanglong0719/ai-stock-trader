import sqlite3
import time

db_path = 'd:/木偶说/backend/aitrader.db'

def global_fix():
    print("Starting efficient global adj_factor fix...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Get all ts_codes that have NULL adj_factors
    cursor.execute("SELECT DISTINCT ts_code FROM daily_bars WHERE adj_factor IS NULL")
    stocks = [r[0] for r in cursor.fetchall()]
    total = len(stocks)
    print(f"Found {total} stocks needing fix.")
    
    start_time = time.time()
    for i, ts_code in enumerate(stocks):
        sql = """
        UPDATE daily_bars 
        SET adj_factor = (
            SELECT adj_factor 
            FROM daily_bars AS d2 
            WHERE d2.ts_code = daily_bars.ts_code 
              AND d2.adj_factor IS NOT NULL 
              AND d2.trade_date < daily_bars.trade_date 
            ORDER BY d2.trade_date DESC 
            LIMIT 1
        ) 
        WHERE ts_code = ? AND adj_factor IS NULL
        """
        cursor.execute(sql, (ts_code,))
        
        # If still NULL (meaning no preceding record), try following record
        if cursor.rowcount == 0:
            sql_back = """
            UPDATE daily_bars 
            SET adj_factor = (
                SELECT adj_factor 
                FROM daily_bars AS d2 
                WHERE d2.ts_code = daily_bars.ts_code 
                  AND d2.adj_factor IS NOT NULL 
                  AND d2.trade_date > daily_bars.trade_date 
                ORDER BY d2.trade_date ASC 
                LIMIT 1
            ) 
            WHERE ts_code = ? AND adj_factor IS NULL
            """
            cursor.execute(sql_back, (ts_code,))
            
        if i % 100 == 0:
            conn.commit()
            print(f"Processed {i}/{total} stocks... ({time.time() - start_time:.1f}s)")
            
    conn.commit()
    conn.close()
    print(f"Global fix completed in {time.time() - start_time:.1f}s.")

if __name__ == "__main__":
    global_fix()

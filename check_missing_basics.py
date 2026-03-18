
import sqlite3
import os

def check_missing_basics():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    target_date = '2026-01-09'
    
    print(f"Checking missing daily_basics for {target_date}...")
    
    # Stocks that have Daily K-line but missing Daily Basic
    cursor.execute("""
        SELECT ts_code FROM daily_bars 
        WHERE trade_date = ? 
        AND ts_code NOT IN (SELECT ts_code FROM daily_basics WHERE trade_date = ?)
    """, (target_date, target_date))
    missing = cursor.fetchall()
    print(f"Stocks with K-line but missing Daily Basic: {len(missing)}")
    for m in missing:
        print(f"  {m[0]}")
        
    conn.close()

if __name__ == "__main__":
    check_missing_basics()

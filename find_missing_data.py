import sqlite3
from datetime import datetime

def find_missing_data():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    target_date = '2026-01-09'
    
    # List tables to verify names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Tables in DB: {tables}")
    
    stock_table = 'stocks' if 'stocks' in tables else 'stock_info'
    print(f"Using stock table: {stock_table}")
    
    # 1. 查找缺失 K 线的股票
    cursor.execute(f"""
        SELECT ts_code, name FROM {stock_table} 
        WHERE ts_code NOT IN (SELECT ts_code FROM daily_bars WHERE trade_date = ?)
    """, (target_date,))
    missing_bars = cursor.fetchall()
    
    # 2. 查找缺失 Daily Basics 的股票
    cursor.execute(f"""
        SELECT ts_code, name FROM {stock_table} 
        WHERE ts_code NOT IN (SELECT ts_code FROM daily_basics WHERE trade_date = ?)
    """, (target_date,))
    missing_basics = cursor.fetchall()
    
    conn.close()
    
    print(f"--- Missing Data for {target_date} ---")
    print(f"Missing Daily Bars: {len(missing_bars)}")
    for code, name in missing_bars[:10]:
        print(f"  {code} ({name})")
    
    print(f"\nMissing Daily Basics: {len(missing_basics)}")
    for code, name in missing_basics[:10]:
        print(f"  {code} ({name})")

if __name__ == "__main__":
    find_missing_data()

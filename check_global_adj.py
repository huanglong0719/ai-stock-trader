import sqlite3
import pandas as pd

db_path = 'd:/木偶说/backend/aitrader.db'

def check_global_nulls():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Checking global NULL adj_factor count...")
    cursor.execute("SELECT COUNT(*) FROM daily_bars WHERE adj_factor IS NULL")
    count = cursor.fetchone()[0]
    print(f"Total rows with NULL adj_factor: {count}")
    
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_bars WHERE adj_factor IS NULL")
    stock_count = cursor.fetchone()[0]
    print(f"Affected stocks: {stock_count}")
    
    conn.close()

if __name__ == "__main__":
    check_global_nulls()

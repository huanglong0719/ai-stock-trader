
import sqlite3
import os

def check_columns():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    tables = ['daily_bars', 'daily_basics', 'industry_data', 'stock_indicators']
    for table in tables:
        print(f"\nColumns in {table}:")
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
            
        print(f"\nSample records from {table}:")
        cursor.execute(f"SELECT * FROM {table} LIMIT 1")
        sample = cursor.fetchone()
        print(f"  {sample}")

    conn.close()

if __name__ == "__main__":
    check_columns()

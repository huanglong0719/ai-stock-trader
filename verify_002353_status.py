import sqlite3
import pandas as pd

db_path = 'd:/木偶说/backend/aitrader.db'

def verify_stock():
    conn = sqlite3.connect(db_path)
    
    print("--- 002353.SZ Factor Check ---")
    # Check if any NULLs remain
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM daily_bars WHERE ts_code = '002353.SZ' AND adj_factor IS NULL")
    null_count = cursor.fetchone()[0]
    print(f"Remaining NULL factors: {null_count}")
    
    # Check values
    df = pd.read_sql_query("SELECT trade_date, close, adj_factor FROM daily_bars WHERE ts_code = '002353.SZ' ORDER BY trade_date DESC LIMIT 5", conn)
    print("\n--- Recent Data ---")
    print(df)
    
    print("\n--- Historical Data (2024-01) ---")
    df_hist = pd.read_sql_query("SELECT trade_date, close, adj_factor FROM daily_bars WHERE ts_code = '002353.SZ' AND trade_date LIKE '2024-01%' ORDER BY trade_date LIMIT 5", conn)
    print(df_hist)
    
    conn.close()

if __name__ == "__main__":
    verify_stock()

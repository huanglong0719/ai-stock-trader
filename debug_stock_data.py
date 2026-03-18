import sqlite3
import pandas as pd
import os

db_path = 'd:/木偶说/backend/aitrader.db'

def check_stock_data():
    if not os.path.exists(db_path):
        print("DB not found")
        return

    conn = sqlite3.connect(db_path)
    
    # 1. Check recent daily bars for 002353.SZ
    print("--- Recent Daily Bars for 002353.SZ ---")
    query = "SELECT * FROM daily_bars WHERE ts_code = '002353.SZ' ORDER BY trade_date DESC LIMIT 10"
    df = pd.read_sql_query(query, conn)
    print(df)
    
    # 2. Check if there are any outliers in the history
    print("\n--- Outliers Check (Price > 50) ---")
    query_outlier = "SELECT * FROM daily_bars WHERE ts_code = '002353.SZ' AND close > 50 ORDER BY trade_date DESC LIMIT 10"
    df_outlier = pd.read_sql_query(query_outlier, conn)
    print(df_outlier)

    # 3. Check for prices < 10 recently
    print("\n--- Low Price Check (Price < 10) ---")
    query_low = "SELECT * FROM daily_bars WHERE ts_code = '002353.SZ' AND close < 10 ORDER BY trade_date DESC LIMIT 10"
    df_low = pd.read_sql_query(query_low, conn)
    print(df_low)
    
    conn.close()

if __name__ == "__main__":
    check_stock_data()

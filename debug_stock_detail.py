import sqlite3
import pandas as pd
import os

db_path = 'd:/木偶说/backend/aitrader.db'

def inspect_data():
    conn = sqlite3.connect(db_path)
    
    # Select specific columns to confirm the hypothesis
    query = "SELECT trade_date, close, adj_factor FROM daily_bars WHERE ts_code = '002353.SZ' ORDER BY trade_date DESC LIMIT 20"
    df = pd.read_sql_query(query, conn)
    
    print("--- Detailed Data for 002353.SZ ---")
    print(df)
    
    conn.close()

if __name__ == "__main__":
    inspect_data()

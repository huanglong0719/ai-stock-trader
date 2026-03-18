import sqlite3
import pandas as pd

def check_indicator_raw():
    conn = sqlite3.connect('backend/aitrader.db')
    
    print("Direct query from stock_indicators for 300508.SZ on 2025-04-07:")
    query = "SELECT trade_date, ma5, ma10, ma20, adj_factor FROM stock_indicators WHERE ts_code='300508.SZ' AND trade_date='2025-04-07'"
    df = pd.read_sql_query(query, conn)
    print(df)
    
    # Check column name for adj_factor in stock_indicators
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(stock_indicators)")
    cols = [c[1] for c in cur.fetchall()]
    print(f"StockIndicator columns: {cols}")
    
    conn.close()

if __name__ == "__main__":
    check_indicator_raw()

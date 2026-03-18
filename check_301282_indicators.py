import sqlite3
import pandas as pd

def check_indicators():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    # Check indicators for 301282.SZ around April 2025
    query = """
    SELECT trade_date, ma5, ma10, ma20, ma60 
    FROM stock_indicators 
    WHERE ts_code = '301282.SZ' 
    AND trade_date >= '2025-03-01' 
    AND trade_date <= '2025-05-31' 
    ORDER BY trade_date ASC
    """
    df = pd.read_sql_query(query, conn)
    print("--- Stock Indicators for 301282.SZ ---")
    print(df.to_string())
    
    # Also check the raw prices to see if there's a jump
    query_prices = """
    SELECT trade_date, open, close, high, low, pct_chg, adj_factor
    FROM daily_bars
    WHERE ts_code = '301282.SZ'
    AND trade_date >= '2025-03-25'
    AND trade_date <= '2025-04-15'
    ORDER BY trade_date ASC
    """
    df_prices = pd.read_sql_query(query_prices, conn)
    print("\n--- Raw Prices for 301282.SZ ---")
    print(df_prices.to_string())
    
    conn.close()

if __name__ == "__main__":
    check_indicators()

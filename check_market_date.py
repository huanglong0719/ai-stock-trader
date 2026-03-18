import sqlite3
import pandas as pd

def check_market_on_date():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    date = '2025-04-07'
    query = f"""
    SELECT ts_code, pct_chg, close, open, adj_factor
    FROM daily_bars 
    WHERE trade_date = '{date}'
    ORDER BY pct_chg ASC
    LIMIT 20
    """
    df = pd.read_sql_query(query, conn)
    print(f"--- Top Decliners on {date} ---")
    print(df.to_string())
    
    query_gainers = f"""
    SELECT ts_code, pct_chg, close, open, adj_factor
    FROM daily_bars 
    WHERE trade_date = '{date}'
    ORDER BY pct_chg DESC
    LIMIT 20
    """
    df_gainers = pd.read_sql_query(query_gainers, conn)
    print(f"\n--- Top Gainers on {date} ---")
    print(df_gainers.to_string())
    
    conn.close()

if __name__ == "__main__":
    check_market_on_date()

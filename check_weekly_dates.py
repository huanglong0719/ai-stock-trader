import sqlite3
import pandas as pd

def check_weekly_bars():
    conn = sqlite3.connect('backend/aitrader.db')
    query = """
    SELECT ts_code, trade_date
    FROM weekly_bars 
    WHERE ts_code = '002245.SZ' 
    ORDER BY trade_date DESC 
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print("WeeklyBar dates:")
    print(df)
    
    query_ind = """
    SELECT ts_code, trade_date, weekly_ma5
    FROM stock_indicators
    WHERE ts_code = '002245.SZ' AND trade_date IN (SELECT trade_date FROM weekly_bars WHERE ts_code = '002245.SZ')
    ORDER BY trade_date DESC
    LIMIT 10
    """
    df_ind = pd.read_sql_query(query_ind, conn)
    print("\nStockIndicator records matching WeeklyBar dates:")
    print(df_ind)
    
    conn.close()

if __name__ == "__main__":
    check_weekly_bars()

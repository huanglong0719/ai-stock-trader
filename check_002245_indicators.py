import sqlite3
import pandas as pd

def check_indicators():
    conn = sqlite3.connect('backend/aitrader.db')
    query = """
    SELECT ts_code, trade_date, ma5, weekly_ma5, monthly_ma5, vol_ma5, weekly_vol_ma5, monthly_vol_ma5
    FROM stock_indicators 
    WHERE ts_code = '002245.SZ' 
    ORDER BY trade_date DESC 
    LIMIT 5
    """
    df = pd.read_sql_query(query, conn)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)
    conn.close()

if __name__ == "__main__":
    check_indicators()

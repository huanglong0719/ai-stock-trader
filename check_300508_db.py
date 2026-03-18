import sqlite3
import pandas as pd

def check_indicators():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT trade_date, ma5, ma10, ma20 
    FROM stock_indicators 
    WHERE ts_code = '300508.SZ' 
    AND trade_date BETWEEN '20250401' AND '20250415'
    ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn)
    print("Stored Indicators in DB:")
    print(df)
    
    # Also check daily bars to see if indicators match
    query_bars = """
    SELECT trade_date, close, adj_factor
    FROM daily_bars
    WHERE ts_code = '300508.SZ'
    AND trade_date BETWEEN '20250301' AND '20250415'
    ORDER BY trade_date
    """
    df_bars = pd.read_sql_query(query_bars, conn)
    
    # Calculate expected MA20 (unadjusted)
    df_bars['ma20_calc'] = df_bars['close'].rolling(window=20).mean()
    print("\nCalculated MA20 (Unadjusted):")
    print(df_bars[df_bars['trade_date'] >= '20250401'][['trade_date', 'close', 'ma20_calc']])
    
    conn.close()

if __name__ == "__main__":
    check_indicators()

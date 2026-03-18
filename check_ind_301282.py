
import sqlite3
import pandas as pd

def check_indicators():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT trade_date, ma5, ma10, ma20, ma60, adj_factor
    FROM stock_indicators 
    WHERE ts_code = '301282.SZ' 
    AND trade_date BETWEEN '2025-03-25' AND '2025-04-15'
    ORDER BY trade_date ASC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("--- Stored Indicators for 301282.SZ ---")
    print(df.to_string(index=False))

if __name__ == "__main__":
    check_indicators()

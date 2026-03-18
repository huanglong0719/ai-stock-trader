import sqlite3
import pandas as pd

def check_index():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT trade_date, close, pct_chg
    FROM daily_bars 
    WHERE ts_code = '000001.SH' 
    AND trade_date >= '2025-03-25' AND trade_date <= '2025-04-15' 
    ORDER BY trade_date ASC
    """
    df = pd.read_sql_query(query, conn)
    print("--- Shanghai Index Around April 2025 ---")
    print(df.to_string())
    conn.close()

if __name__ == "__main__":
    check_index()

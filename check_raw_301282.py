
import sqlite3
import pandas as pd

def check_raw_data():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT trade_date, open, high, low, close, adj_factor, pct_chg
    FROM daily_bars 
    WHERE ts_code = '301282.SZ' 
    AND trade_date BETWEEN '2025-04-01' AND '2025-04-10'
    ORDER BY trade_date ASC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("--- Raw Daily Data for 301282.SZ ---")
    print(df.to_string(index=False))

if __name__ == "__main__":
    check_raw_data()

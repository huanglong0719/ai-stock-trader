import sqlite3

def check_301282_details():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    print("--- Detailed Daily Bars for 301282.SZ (April 2025) ---")
    query = """
    SELECT trade_date, open, high, low, close, vol, pct_chg 
    FROM daily_bars 
    WHERE ts_code = '301282.SZ' 
    AND trade_date >= '2025-04-01' 
    AND trade_date <= '2025-04-15' 
    ORDER BY trade_date ASC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()

if __name__ == "__main__":
    check_301282_details()

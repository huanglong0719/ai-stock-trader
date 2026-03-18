import sqlite3

def compare_data():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    print(f"{'Date':<12} | {'Close':<8} | {'MA20':<8} | {'Source'}")
    print("-" * 45)
    
    # Get all dates from both tables for the range
    query = """
    SELECT COALESCE(b.trade_date, i.trade_date) as t_date, b.close, i.ma20
    FROM daily_bars b
    LEFT JOIN stock_indicators i ON b.ts_code = i.ts_code AND b.trade_date = i.trade_date
    WHERE b.ts_code = '301282.SZ' AND b.trade_date >= '2025-03-25' AND b.trade_date <= '2025-04-25'
    UNION
    SELECT COALESCE(b.trade_date, i.trade_date) as t_date, b.close, i.ma20
    FROM stock_indicators i
    LEFT JOIN daily_bars b ON b.ts_code = i.ts_code AND b.trade_date = i.trade_date
    WHERE i.ts_code = '301282.SZ' AND i.trade_date >= '2025-03-25' AND i.trade_date <= '2025-04-25'
    ORDER BY t_date ASC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        t_date, close, ma20 = row
        close_str = f"{close:.2f}" if close else "MISSING"
        ma20_str = f"{ma20:.2f}" if ma20 else "MISSING"
        print(f"{t_date:<12} | {close_str:<8} | {ma20_str:<8}")
        
    conn.close()

if __name__ == "__main__":
    compare_data()

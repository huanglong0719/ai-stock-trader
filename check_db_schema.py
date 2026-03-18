import sqlite3

def check_schema():
    conn = sqlite3.connect('backend/aitrader.db')
    cur = conn.cursor()
    
    print("stock_indicators columns:")
    cur.execute("PRAGMA table_info(stock_indicators)")
    for col in cur.fetchall():
        print(col)
        
    print("\ndaily_bars columns:")
    cur.execute("PRAGMA table_info(daily_bars)")
    for col in cur.fetchall():
        print(col)
        
    conn.close()

if __name__ == "__main__":
    check_schema()

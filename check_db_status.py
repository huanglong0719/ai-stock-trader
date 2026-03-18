
import sqlite3
import os

db_path = "backend/data/stock_data.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT count(*) FROM weekly_bars")
    weekly_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT count(*) FROM monthly_bars")
    monthly_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT count(*) FROM stock_indicator WHERE weekly_macd IS NOT NULL")
    weekly_macd_count = cursor.fetchone()[0]
    
    print(f"Weekly bars: {weekly_count}")
    print(f"Monthly bars: {monthly_count}")
    print(f"Stock indicators with weekly MACD: {weekly_macd_count}")
    
    conn.close()
else:
    print(f"DB not found at {db_path}")

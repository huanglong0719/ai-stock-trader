import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(stock_indicators)")
rows = cursor.fetchall()
for row in rows:
    print(row)
conn.close()

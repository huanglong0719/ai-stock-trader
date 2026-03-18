import sqlite3
conn = sqlite3.connect('backend/aitrader.db')
cursor = conn.cursor()
cursor.execute('PRAGMA table_info(stock_indicators)')
cols = cursor.fetchall()
for col in cols:
    print(col[1])
conn.close()

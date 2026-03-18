import sqlite3
db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='daily_bars'")
indexes = cursor.fetchall()
for idx in indexes:
    print(idx[0])
conn.close()

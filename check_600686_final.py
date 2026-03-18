import sqlite3
conn = sqlite3.connect('backend/aitrader.db')
cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM stock_indicators WHERE ts_code='600686.SH' AND macd IS NULL")
print(f"Missing MACD records for 600686.SH: {cursor.fetchone()[0]}")
conn.close()

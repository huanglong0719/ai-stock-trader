import sqlite3
import os

db_path = 'backend/aitrader.db'
if not os.path.exists(db_path):
    print(f"Database {db_path} not found.")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check columns in stock_indicators
cursor.execute("PRAGMA table_info(stock_indicators)")
columns = [row[1] for row in cursor.fetchall()]
print(f"Current columns in stock_indicators: {columns}")

# Add vol_ma10 if missing
if 'vol_ma10' not in columns:
    print("Adding vol_ma10 column to stock_indicators...")
    try:
        cursor.execute("ALTER TABLE stock_indicators ADD COLUMN vol_ma10 FLOAT")
        conn.commit()
        print("vol_ma10 column added successfully.")
    except Exception as e:
        print(f"Error adding vol_ma10: {e}")
else:
    print("vol_ma10 column already exists.")

conn.close()

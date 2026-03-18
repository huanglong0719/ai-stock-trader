import sqlite3
import os

db_path = 'backend/aitrader.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM positions WHERE vol <= 0")
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Cleaned up {deleted} empty positions.")
else:
    print(f"Database not found at {db_path}")

import sqlite3
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
if not os.path.exists(db_path):
    # 如果在backend目录下运行
    db_path = os.path.join(os.getcwd(), 'aitrader.db')
print(f"Connecting to database at: {db_path}")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 检查列是否存在
    cursor.execute("PRAGMA table_info(trading_plans)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'frozen_amount' not in columns:
        print("Adding column frozen_amount to trading_plans...")
        cursor.execute("ALTER TABLE trading_plans ADD COLUMN frozen_amount FLOAT DEFAULT 0.0")
    
    if 'frozen_vol' not in columns:
        print("Adding column frozen_vol to trading_plans...")
        cursor.execute("ALTER TABLE trading_plans ADD COLUMN frozen_vol INTEGER DEFAULT 0")
        
    conn.commit()
    print("Database migration successful.")
    conn.close()
except Exception as e:
    print(f"Migration failed: {e}")

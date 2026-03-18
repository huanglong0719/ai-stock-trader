import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    # Query ALL reports for 300139 to see history
    table_name = 'ai_analysis_reports'
    cursor.execute(f"SELECT response_json, created_at FROM {table_name} WHERE request_json LIKE '%300139%' OR response_json LIKE '%300139%' ORDER BY created_at DESC LIMIT 5")
    rows = cursor.fetchall()
    print(f"--- AI Reports for 300139 ---")
    for row in rows:
        print(f"Created at: {row[1]}")
        print(row[0])
        print("-" * 20)

    # Check trading_plans schema
    cursor.execute("PRAGMA table_info(trading_plans)")
    columns = cursor.fetchall()
    print(f"\nColumns in trading_plans: {[col[1] for col in columns]}")

    # Query trading_plans for 300139
    cursor.execute("SELECT * FROM trading_plans WHERE symbol LIKE '%300139%' ORDER BY created_at DESC LIMIT 5")
    rows = cursor.fetchall()
    print(f"\n--- Trading Plans for 300139 ---")
    for row in rows:
        print(f"Plan: {dict(zip([col[1] for col in columns], row))}")
        print("-" * 20)

except Exception as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()

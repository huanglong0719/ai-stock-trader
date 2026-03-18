import sqlite3

conn = sqlite3.connect('backend/stock_data.db')
cursor = conn.cursor()

try:
    cursor.execute("SELECT ts_code, trade_date FROM stock_indicators WHERE ts_code = 'IND_橡胶' ORDER BY trade_date DESC LIMIT 5")
    rows = cursor.fetchall()
    print("\nIND_橡胶 indicators in DB:", flush=True)
    for row in rows:
        print(f"Code: {row[0]}, Date: {row[1]}", flush=True)
        
    cursor.execute("SELECT ts_code, trade_date FROM stock_indicators WHERE ts_code LIKE 'IND_%' AND trade_date = '2026-01-09'")
    rows = cursor.fetchall()
    print(f"\nTotal industry indicators for 2026-01-09 in DB: {len(rows)}", flush=True)
    for row in rows[:10]:
        print(f"Code: {row[0]}, Date: {row[1]}", flush=True)

    cursor.execute("SELECT count(*) FROM industry_data WHERE trade_date = '2026-01-09'")
    print(f"\nTotal industry_data records for 2026-01-09: {cursor.fetchone()[0]}", flush=True)

    cursor.execute("SELECT industry, trade_date FROM industry_data WHERE industry = '橡胶' AND trade_date = '2026-01-09'")
    rows = cursor.fetchall()
    print("\n'橡胶' industry_data in DB:", flush=True)
    for row in rows:
        print(f"Industry: {row[0]}, Date: {row[1]}", flush=True)

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()

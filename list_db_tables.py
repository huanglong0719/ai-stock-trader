import sqlite3

def list_tables():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables in aitrader.db:")
    for table in tables:
        print(f" - {table[0]}")
    conn.close()

if __name__ == "__main__":
    list_tables()

import sqlite3
import os

def check_db(path):
    print(f"Checking {path}...")
    if not os.path.exists(path):
        print("Path does not exist.")
        return
    try:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables: {[t[0] for t in tables]}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

check_db(r'd:\木偶说\backend\data\stock_data.db')
check_db(r'd:\木偶说\stock_data.db')

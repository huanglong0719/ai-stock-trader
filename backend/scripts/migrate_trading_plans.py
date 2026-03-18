import sqlite3
import os

db_path = os.path.join("backend", "aitrader.db")

def migrate():
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        print("Adding 'order_type' and 'limit_price' columns to 'trading_plans' table...")
        
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(trading_plans)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'order_type' not in columns:
            cursor.execute("ALTER TABLE trading_plans ADD COLUMN order_type TEXT DEFAULT 'MARKET'")
            print("Added column 'order_type'")
        else:
            print("Column 'order_type' already exists")

        if 'limit_price' not in columns:
            cursor.execute("ALTER TABLE trading_plans ADD COLUMN limit_price REAL DEFAULT 0.0")
            print("Added column 'limit_price'")
        else:
            print("Column 'limit_price' already exists")

        conn.commit()
        print("Migration completed successfully.")
    except Exception as e:
        print(f"Error during migration: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

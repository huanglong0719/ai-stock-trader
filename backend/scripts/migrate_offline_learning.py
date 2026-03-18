import sqlite3
import os

db_path = 'd:/木偶说/backend/aitrader.db'

def migrate():
    if not os.path.exists(db_path):
        print("Database not found!")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 1. Handle PatternLibrary / PatternCase
        # We found both exist and are empty (or at least pattern_cases exists).
        # We want pattern_cases to be the instances table (old schema)
        # And pattern_library to be the rules table (new schema)
        
        # Check if pattern_cases exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pattern_cases'")
        if cursor.fetchone():
            print("Table pattern_cases exists.")
        else:
            # If not, try to rename pattern_library to pattern_cases
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pattern_library'")
            if cursor.fetchone():
                print("Renaming pattern_library to pattern_cases...")
                cursor.execute("ALTER TABLE pattern_library RENAME TO pattern_cases")
            else:
                print("Neither pattern_cases nor pattern_library found (or just created).")

        # Now we want to create the NEW pattern_library
        # First drop it if it exists (it might be the old one if rename failed or duplicate)
        # BUT only if it has the WRONG schema. 
        # Since we know from inspection it has the OLD schema (same as pattern_cases), we drop it.
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pattern_library'")
        if cursor.fetchone():
            print("Dropping old pattern_library (incorrect schema)...")
            cursor.execute("DROP TABLE pattern_library")

        # Create new PatternLibrary table
        print("Creating new pattern_library table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pattern_library (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR,
                features_json TEXT,
                success_rate FLOAT,
                sample_count INTEGER,
                created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
                updated_at DATETIME
            )
        """)
        
        # 3. Add real_pnl_pct to trading_plans
        print("Adding real_pnl_pct to trading_plans...")
        try:
            cursor.execute("ALTER TABLE trading_plans ADD COLUMN real_pnl_pct FLOAT")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e):
                print("Column real_pnl_pct already exists.")
            else:
                print(f"Error adding column: {e}")

        # 4. Add max_drawdown to strategy_stats
        print("Adding max_drawdown to strategy_stats...")
        try:
            cursor.execute("ALTER TABLE strategy_stats ADD COLUMN max_drawdown FLOAT DEFAULT 0.0")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e):
                print("Column max_drawdown already exists.")
            else:
                print(f"Error adding column: {e}")

        conn.commit()
        print("Migration completed successfully.")

    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

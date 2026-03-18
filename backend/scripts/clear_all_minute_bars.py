import sys
import os
import time
from sqlalchemy import text

# Add backend directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from app.db.session import SessionLocal

def clear_all_minute_data():
    db = SessionLocal()
    try:
        print("Starting to clear ALL data from minute_bars table...")
        
        # Check total count
        total = db.execute(text("SELECT count(*) FROM minute_bars")).scalar()
        print(f"Total rows to delete: {total}")
        
        if total == 0:
            print("Table is already empty.")
            return

        # Try TRUNCATE first (fastest for MySQL/PostgreSQL)
        try:
            db.execute(text("TRUNCATE TABLE minute_bars"))
            db.commit()
            print("Successfully truncated table.")
            return
        except Exception as e:
            print(f"TRUNCATE failed (expected if SQLite): {e}")
            db.rollback()
        
        # Fallback to batched DELETE
        deleted_total = 0
        batch_size = 10000
        while True:
            # Delete in batches to avoid locking/timeout
            # Using raw SQL with limit (syntax varies by DB, but LIMIT is common in MySQL/SQLite)
            # Note: SQLite default build might not support LIMIT in DELETE, but let's try standard approach first.
            # A more compatible way for SQLite is: DELETE FROM minute_bars WHERE id IN (SELECT id FROM minute_bars LIMIT 10000)
            
            # Check DB type indirectly or just use the IN clause method which is safer
            subquery = f"SELECT id FROM minute_bars LIMIT {batch_size}"
            stmt = text(f"DELETE FROM minute_bars WHERE id IN ({subquery})")
            
            result = db.execute(stmt)
            count = int(getattr(result, "rowcount", 0) or 0)
            db.commit()
            
            if count == 0:
                break
                
            deleted_total += count
            print(f"Deleted {deleted_total} / {total} rows...")
            time.sleep(0.1) # Brief pause to let DB breathe
            
        print("Successfully cleared all data.")
            
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    clear_all_minute_data()

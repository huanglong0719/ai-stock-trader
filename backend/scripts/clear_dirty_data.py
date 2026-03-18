import sys
import os
from sqlalchemy import text

# Add backend directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from app.db.session import SessionLocal

def clear_data():
    db = SessionLocal()
    try:
        ts_code = '002353.SZ'
        print(f"Checking data for {ts_code}...")
        
        # Check count before
        count = db.execute(text(f"SELECT count(*) FROM minute_bars WHERE ts_code = '{ts_code}'")).scalar()
        count_val = int(count or 0)
        print(f"Found {count_val} rows for {ts_code} before deletion")
        
        if count_val > 0:
            print(f"Deleting data for {ts_code}...")
            db.execute(text(f"DELETE FROM minute_bars WHERE ts_code = '{ts_code}'"))
            db.commit()
            print(f"Successfully deleted data for {ts_code}")
        else:
            print("No data found to delete.")
            
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    clear_data()

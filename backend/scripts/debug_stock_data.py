import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import desc

def check_data():
    db = SessionLocal()
    try:
        ts_code = '001311.SZ'
        print(f"Checking data for {ts_code}...")
        
        # Get latest 10 records
        bars = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(desc(DailyBar.trade_date)).limit(10).all()
        
        if not bars:
            print("No data found.")
            return

        print(f"{'Date':<15} {'Open':<10} {'Close':<10} {'Adj Factor'}")
        print("-" * 50)
        for bar in bars:
            print(f"{str(bar.trade_date):<15} {bar.open:<10} {bar.close:<10} {bar.adj_factor}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_data()

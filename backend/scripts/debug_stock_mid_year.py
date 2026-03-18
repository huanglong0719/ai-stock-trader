import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import desc

def check_mid_year_data():
    db = SessionLocal()
    try:
        ts_code = '001311.SZ'
        print(f"Checking data for {ts_code} around 2025-06...")
        
        # Get data from 2025-05-01 to 2025-08-01
        bars = db.query(DailyBar).filter(
            DailyBar.ts_code == ts_code,
            DailyBar.trade_date >= '2025-05-01',
            DailyBar.trade_date <= '2025-08-01'
        ).order_by(DailyBar.trade_date.asc()).all()
        
        if not bars:
            print("No data found in this range.")
            return

        print(f"{'Date':<15} {'Open':<10} {'Close':<10} {'PreClose':<10} {'Adj Factor'}")
        print("-" * 60)
        for bar in bars:
            print(f"{str(bar.trade_date):<15} {bar.open:<10} {bar.close:<10} {bar.pre_close:<10} {bar.adj_factor}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_mid_year_data()

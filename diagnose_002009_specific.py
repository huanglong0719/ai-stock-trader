import sys
import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Add backend directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar

def diagnose_specific_date(ts_code, date_str):
    print(f"Diagnosing {ts_code} around {date_str}...")
    db = SessionLocal()
    try:
        # Check records around the date
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        start_date = target_date - timedelta(days=10)
        end_date = target_date + timedelta(days=10)
        
        records = db.query(DailyBar).filter(
            DailyBar.ts_code == ts_code,
            DailyBar.trade_date >= start_date,
            DailyBar.trade_date <= end_date
        ).order_by(DailyBar.trade_date).all()
        
        for r in records:
            print(f"{r.trade_date}: Open={r.open}, Close={r.close}, High={r.high}, Low={r.low}, Adj={r.adj_factor}")
            
    finally:
        db.close()

if __name__ == "__main__":
    diagnose_specific_date("002009.SZ", "2025-12-03")

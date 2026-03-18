
import sys
import os
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.models.stock_models import MarketCloseCounts
from app.db.session import SessionLocal
from datetime import date

def check_db_counts():
    db = SessionLocal()
    try:
        target_date = "2026-01-26"
        records = db.query(MarketCloseCounts).filter(MarketCloseCounts.date == target_date).all()
        print(f"Found {len(records)} records for {target_date}")
        for r in records:
            print(f"Date: {r.date}, Up: {r.up}, Down: {r.down}, Amount: {r.amount}, Source: {r.source}")
    finally:
        db.close()

if __name__ == "__main__":
    check_db_counts()

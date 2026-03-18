
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar, Stock
from sqlalchemy import func, desc

def diagnose():
    db = SessionLocal()
    try:
        # 1. Total Stocks
        total_stocks = db.query(Stock).count()
        print(f"Total Stocks in DB: {total_stocks}")

        # 2. Latest Date in DailyBar
        last_bar = db.query(DailyBar).order_by(desc(DailyBar.trade_date)).first()
        if not last_bar:
            print("No DailyBar data found.")
            return
        
        last_date = last_bar.trade_date
        print(f"Latest DailyBar Date: {last_date}")

        # 3. Count bars for the latest date
        latest_count = db.query(DailyBar).filter(DailyBar.trade_date == last_date).count()
        print(f"DailyBar count for {last_date}: {latest_count}")

        # 4. Coverage
        coverage = (latest_count / total_stocks * 100) if total_stocks > 0 else 0
        print(f"Coverage: {coverage:.2f}%")

        # 5. List some of the stocks that HAVE data
        existing_bars = db.query(DailyBar).filter(DailyBar.trade_date == last_date).limit(10).all()
        print(f"Sample stocks with data for {last_date}:")
        for bar in existing_bars:
            print(f" - {bar.ts_code}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    diagnose()

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar, DailyBar
from sqlalchemy import desc

def check_data(ts_code='000001.SZ'):
    db = SessionLocal()
    try:
        print(f"Checking data for {ts_code}...")
        
        # 1. Check DailyBar (Latest)
        daily = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(desc(DailyBar.trade_date)).first()
        if daily:
            print(f"Latest DailyBar: {daily.trade_date}, Close: {daily.close}, Adj: {daily.adj_factor}")
        else:
            print("No DailyBar found.")
            
        # 2. Check MinuteBar (Latest 5)
        minutes = db.query(MinuteBar).filter(
            MinuteBar.ts_code == ts_code,
            MinuteBar.freq == '1min'
        ).order_by(desc(MinuteBar.trade_time)).limit(5).all()
        
        if minutes:
            print(f"Found {len(minutes)} minute bars.")
            for m in minutes:
                print(f"Time: {m.trade_time}, Close: {m.close}, Adj: {m.adj_factor}")
        else:
            print("No MinuteBar found.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_data(sys.argv[1])
    else:
        # Try to find a stock that has minute data
        db = SessionLocal()
        code = db.query(MinuteBar.ts_code).first()
        db.close()
        if code:
            check_data(code[0])
        else:
            print("No minute data in DB.")

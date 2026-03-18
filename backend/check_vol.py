
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import func
from datetime import datetime

def check_volume():
    db = SessionLocal()
    ts_code = '002353.SZ'
    period_start = '20251201'
    curr_dt = datetime.now()
    
    total_vol = db.query(func.sum(DailyBar.vol)).filter(
        DailyBar.ts_code == ts_code,
        DailyBar.trade_date >= datetime.strptime(period_start, '%Y%m%d').date(),
        DailyBar.trade_date <= curr_dt.date()
    ).scalar() or 0
    
    print(f"Total volume for {ts_code} in Dec: {total_vol}")
    
    # Check latest daily bars
    bars = db.query(DailyBar).filter(
        DailyBar.ts_code == ts_code,
        DailyBar.trade_date >= datetime.strptime(period_start, '%Y%m%d').date()
    ).order_by(DailyBar.trade_date.desc()).all()
    
    for bar in bars:
        print(f"Date: {bar.trade_date}, Vol: {bar.vol}")
        
    db.close()

if __name__ == '__main__':
    check_volume()

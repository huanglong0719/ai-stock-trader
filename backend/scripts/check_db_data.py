from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import func
from datetime import date

def check_db():
    db = SessionLocal()
    try:
        max_date = db.query(func.max(DailyBar.trade_date)).scalar()
        print(f"Latest trade date in DB: {max_date}")
        
        codes = ['601136.SH', '601011.SH', '000001.SZ', '600000.SH']
        for c in codes:
            count = db.query(DailyBar).filter(DailyBar.ts_code == c).count()
            latest = db.query(DailyBar).filter(DailyBar.ts_code == c).order_by(DailyBar.trade_date.desc()).first()
            latest_date = latest.trade_date if latest else "None"
            print(f"{c}: total count={count}, latest date={latest_date}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_db()

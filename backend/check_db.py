import asyncio
import logging
import sys
from datetime import datetime
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import desc

# 配置日志到控制台
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def check_db_data():
    ts_code = "300308.SZ" # 中际旭创
    print(f"Checking DB data for {ts_code}...")
    
    db = SessionLocal()
    try:
        bars = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(desc(DailyBar.trade_date)).limit(10).all()
        
        print(f"Found {len(bars)} bars in DB:")
        for bar in bars:
            print(f"Date: {bar.trade_date} O:{bar.open} C:{bar.close} H:{bar.high} L:{bar.low} Vol:{bar.vol} Adj:{bar.adj_factor}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_db_data()

import sys
import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Add backend directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar

def diagnose_stock(ts_code):
    print(f"Diagnosing {ts_code}...")
    db = SessionLocal()
    try:
        # 1. Check daily bars count and range
        count = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).count()
        print(f"Total DailyBars: {count}")
        
        if count == 0:
            print("No data found!")
            return

        # 2. Check adj_factor stats
        # Get min, max, count of nulls, count of 1.0
        sql = text(f"""
            SELECT 
                MIN(trade_date) as min_date,
                MAX(trade_date) as max_date,
                MIN(adj_factor) as min_adj,
                MAX(adj_factor) as max_adj,
                COUNT(CASE WHEN adj_factor IS NULL THEN 1 END) as null_adj,
                COUNT(CASE WHEN adj_factor = 1.0 THEN 1 END) as one_adj,
                COUNT(CASE WHEN adj_factor = 0 THEN 1 END) as zero_adj
            FROM daily_bars
            WHERE ts_code = '{ts_code}'
        """)
        result = db.execute(sql).fetchone()
        print(f"Date Range: {result.min_date} - {result.max_date}")
        print(f"Adj Factor: Min={result.min_adj}, Max={result.max_adj}")
        print(f"Null Adjs: {result.null_adj}")
        print(f"1.0 Adjs: {result.one_adj}")
        print(f"0 Adjs: {result.zero_adj}")

        # 3. Check recent records to see if adj_factor is applied
        print("\nRecent 10 records:")
        recent = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(DailyBar.trade_date.desc()).limit(10).all()
        for r in recent:
            print(f"{r.trade_date}: Close={r.close}, Adj={r.adj_factor}")

        # 4. Check historical records around a potential split (if any) or just some random ones
        print("\nOldest 5 records:")
        oldest = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(DailyBar.trade_date.asc()).limit(5).all()
        for r in oldest:
            print(f"{r.trade_date}: Close={r.close}, Adj={r.adj_factor}")
            
    finally:
        db.close()

if __name__ == "__main__":
    diagnose_stock("002009.SZ")

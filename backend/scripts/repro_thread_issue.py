import asyncio
from datetime import date
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import func

async def test_thread_query():
    db = SessionLocal()
    asof_date = date(2026, 1, 29)
    ts_code = '601136.SH'
    
    def run_query(d, code, adate):
        print(f"Running query in thread for {code} asof {adate}")
        rows = (
            d.query(DailyBar.trade_date, DailyBar.close)
            .filter(DailyBar.ts_code == code)
            .filter(DailyBar.trade_date <= adate)
            .order_by(DailyBar.trade_date.desc())
            .limit(30)
            .all()
        )
        return rows

    try:
        # Test in main thread
        rows_main = run_query(db, ts_code, asof_date)
        print(f"Main thread: found {len(rows_main)} rows")
        
        # Test in to_thread
        rows_thread = await asyncio.to_thread(run_query, db, ts_code, asof_date)
        print(f"To_thread: found {len(rows_thread)} rows")
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_thread_query())

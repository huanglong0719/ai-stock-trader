
import asyncio
from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar, DailyBar

async def check_units():
    db = SessionLocal()
    try:
        # Check MinuteBar
        mb = db.query(MinuteBar).limit(1).first()
        if mb:
            print(f"MinuteBar Amount: {mb.amount} for {mb.ts_code} at {mb.trade_time}")
        
        # Check DailyBar
        db_bar = db.query(DailyBar).limit(1).first()
        if db_bar:
            print(f"DailyBar Amount: {db_bar.amount} for {db_bar.ts_code} at {db_bar.trade_date}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_units())

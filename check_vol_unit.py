
import asyncio
from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar

async def check_vol_unit():
    db = SessionLocal()
    try:
        bar = db.query(MinuteBar).order_by(MinuteBar.trade_time.desc()).first()
        if bar:
            print(f"MinuteBar {bar.ts_code} at {bar.trade_time}: Vol={bar.vol}, Amount={bar.amount}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_vol_unit())

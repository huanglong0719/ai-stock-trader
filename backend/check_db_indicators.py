
import asyncio
from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator
from sqlalchemy import func

async def check_db():
    db = SessionLocal()
    try:
        # 1. 检查总数
        total = db.query(StockIndicator).count()
        print(f"Total indicators in DB: {total}")
        
        # 2. 检查日期格式和最新日期
        latest = db.query(func.max(StockIndicator.trade_date)).scalar()
        print(f"Latest trade_date in DB: {latest} (Type: {type(latest)})")
        
        # 3. 检查特定日期的数量
        target_date_str = "20260109"
        count_str = db.query(StockIndicator).filter(StockIndicator.trade_date == target_date_str).count()
        print(f"Count for {target_date_str}: {count_str}")
        
        target_date_hyphen = "2026-01-09"
        count_hyphen = db.query(StockIndicator).filter(StockIndicator.trade_date == target_date_hyphen).count()
        print(f"Count for {target_date_hyphen}: {count_hyphen}")

        # 4. 打印几条样本
        samples = db.query(StockIndicator).limit(3).all()
        for s in samples:
            print(f"Sample: ts_code={s.ts_code}, trade_date={s.trade_date}")

    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_db())


import asyncio
from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, TradeRecord

async def inspect_plans():
    db = SessionLocal()
    try:
        print("=== Inspecting Plans for 2026-01-08 ===")
        plans = db.query(TradingPlan).filter(TradingPlan.date == '2026-01-08').all()
        for p in plans:
            print(f"ID: {p.id} | {p.ts_code} | Strategy: {p.strategy_name} | Frozen: {p.frozen_amount} | Executed: {p.executed} | BuyPriceLimit: {p.buy_price_limit}")
            
        print("\n=== Inspecting All Executed Plans with Frozen Amount > 0 ===")
        plans = db.query(TradingPlan).filter(TradingPlan.executed == True, TradingPlan.frozen_amount > 0).all()
        for p in plans:
            print(f"ID: {p.id} | {p.ts_code} | Frozen: {p.frozen_amount} | Entry: {p.entry_price}")
            
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(inspect_plans())


import asyncio
from app.db.session import SessionLocal
from app.models.stock_models import TradeRecord, Account, TradingPlan, Position
from sqlalchemy import func

async def detail_audit():
    db = SessionLocal()
    try:
        print("=== Account Status ===")
        account = db.query(Account).first()
        if account:
            print(f"Available Cash: {account.available_cash:,.2f}")
            print(f"Frozen Cash: {account.frozen_cash:,.2f}")
            print(f"Market Value: {account.market_value:,.2f}")
            print(f"Total Assets: {account.total_assets:,.2f}")
        else:
            print("No account found!")

        print("\n=== Trade Records ===")
        trades = db.query(TradeRecord).order_by(TradeRecord.trade_time.asc()).all()
        calc_cash = 1000000.0
        for t in trades:
            fee = t.fee or 0
            if t.trade_type == 'BUY':
                cost = t.amount + fee
                calc_cash -= cost
                print(f"{t.trade_time} | BUY  | {t.ts_code} | {t.vol:>6} @ {t.price:>7.2f} | Amount: {t.amount:>10.2f} | Fee: {fee:>7.2f} | Cash: {calc_cash:,.2f}")
            else:
                income = t.amount
                calc_cash += income
                print(f"{t.trade_time} | SELL | {t.ts_code} | {t.vol:>6} @ {t.price:>7.2f} | Amount: {t.amount:>10.2f} | Fee: {fee:>7.2f} | Cash: {calc_cash:,.2f}")

        print(f"\nFinal Calculated Cash (Trades only): {calc_cash:,.2f}")

        print("\n=== Active Frozen Funds in Plans ===")
        plans = db.query(TradingPlan).filter(TradingPlan.frozen_amount > 0).all()
        total_frozen_in_plans = 0
        for p in plans:
            print(f"Plan ID: {p.id} | {p.ts_code} | Frozen: {p.frozen_amount:,.2f} | Vol: {p.frozen_vol}")
            total_frozen_in_plans += p.frozen_amount
        print(f"Total Frozen in Plans: {total_frozen_in_plans:,.2f}")

        print("\n=== Positions ===")
        positions = db.query(Position).all()
        total_mv = 0
        for pos in positions:
            mv = pos.vol * pos.current_price
            total_mv += mv
            print(f"{pos.ts_code} | {pos.name} | Vol: {pos.vol:>6} | Avg: {pos.avg_price:>7.2f} | Curr: {pos.current_price:>7.2f} | MV: {mv:,.2f}")
        print(f"Total Market Value: {total_mv:,.2f}")

        print("\n=== Summary Discrepancy ===")
        print(f"Calculated Available Cash (Calculated - Total Frozen): {calc_cash - total_frozen_in_plans:,.2f}")
        print(f"Actual Account Available Cash: {account.available_cash:,.2f}")
        print(f"Difference: {account.available_cash - (calc_cash - total_frozen_in_plans):,.2f}")

    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(detail_audit())

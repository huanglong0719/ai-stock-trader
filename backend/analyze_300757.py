import sys
import os
# Append the directory containing 'app' package to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import SessionLocal
from app.models.stock_models import Position, TradingPlan, TradeRecord
from datetime import date, datetime

def check_300757():
    db = SessionLocal()
    try:
        ts_code = '300757.SZ'
        today = date.today()
        
        # 1. Check Position
        pos = db.query(Position).filter(Position.ts_code == ts_code).first()
        print(f"--- Position Info for {ts_code} ---")
        if pos:
            print(f"Volume: {pos.vol}")
            print(f"Available Volume: {pos.available_vol}")
            print(f"Average Price: {pos.avg_price}")
            print(f"Current Price: {pos.current_price}")
            print(f"Profit: {pos.float_pnl}")
            print(f"Profit Rate: {pos.pnl_pct}%")
        else:
            print("No position found.")

        # 2. Check Plan
        plan = db.query(TradingPlan).filter(
            TradingPlan.ts_code == ts_code,
            TradingPlan.date == today
        ).first()
        
        print(f"\n--- Plan Info for {ts_code} ({today}) ---")
        if plan:
            print(f"Strategy: {plan.strategy_name}")
            print(f"Status: {'Executed' if plan.executed else 'Not Executed'}")
            print(f"Reason: {plan.reason}")
            print(f"Stop Loss: {plan.stop_loss_price}")
            print(f"Take Profit: {plan.take_profit_price}")
            print(f"Review Content: {plan.review_content}")
        else:
            print("No plan found for today.")
            
        # 3. Check Trade Records
        records = db.query(TradeRecord).filter(
            TradeRecord.ts_code == ts_code,
            TradeRecord.trade_time >= datetime.combine(today, datetime.min.time())
        ).all()
        
        print(f"\n--- Trade Records for {ts_code} Today ---")
        if records:
            for r in records:
                print(f"Time: {r.trade_time}, Type: {r.trade_type}, Price: {r.price}, Vol: {r.vol}")
        else:
            print("No trade records found for today.")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    check_300757()

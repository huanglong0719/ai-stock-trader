import os
import sys

# 设置项目根目录
root_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(root_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, TradeRecord

def check_price_violation():
    db = SessionLocal()
    try:
        # 查找所有已执行的买入计划
        plans = db.query(TradingPlan).filter(
            TradingPlan.executed == True,
            TradingPlan.buy_price_limit > 0
        ).all()
        
        violation_count = 0
        print(f"{'TS Code':<12} {'Date':<12} {'Limit Price':<15} {'Exec Price':<15} {'Diff':<10}")
        print("-" * 65)
        
        for plan in plans:
            trade = db.query(TradeRecord).filter(TradeRecord.plan_id == plan.id).first()
            if not trade:
                continue
                
            if trade.price > plan.buy_price_limit + 0.001: # 允许极小的浮点误差
                print(f"{plan.ts_code:<12} {str(plan.date):<12} {plan.buy_price_limit:<15.2f} {trade.price:<15.2f} {trade.price - plan.buy_price_limit:<10.2f}")
                violation_count += 1
        
        if violation_count > 0:
            print(f"\nFound {violation_count} price violations.")
        else:
            print("\nNo price violations found in historical data.")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_price_violation()

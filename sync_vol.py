import os
import sys

# 设置项目根目录
root_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(root_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, TradeRecord

def sync_historical_vol():
    db = SessionLocal()
    try:
        # 查找已执行但数量不一致或没有 frozen_vol 的计划
        plans = db.query(TradingPlan).filter(TradingPlan.executed == True).all()
        
        updated_count = 0
        print(f"{'TS Code':<12} {'Date':<12} {'Old Frozen':<12} {'New Frozen (Exec)':<15}")
        print("-" * 60)
        
        for plan in plans:
            # 查找关联的成交记录
            trade = db.query(TradeRecord).filter(TradeRecord.plan_id == plan.id).first()
            if not trade:
                continue
                
            if plan.frozen_vol != trade.vol:
                old_vol = plan.frozen_vol
                plan.frozen_vol = trade.vol
                # 同时确保 frozen_amount 也是同步的（按成交价+手续费估算，或者直接用 trade.amount）
                plan.frozen_amount = trade.amount + (trade.fee or 0)
                
                print(f"{plan.ts_code:<12} {str(plan.date):<12} {str(old_vol):<12} {plan.frozen_vol:<15}")
                updated_count += 1
        
        if updated_count > 0:
            db.commit()
            print(f"\nSuccessfully updated {updated_count} plans.")
        else:
            print("\nNo mismatches found to update.")
            
    finally:
        db.close()

if __name__ == "__main__":
    sync_historical_vol()

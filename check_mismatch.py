import os
import sys
from datetime import date

# 设置项目根目录
root_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(root_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, TradeRecord

def check_quantity_mismatch():
    db = SessionLocal()
    try:
        stocks = ['300508.SZ', '002945.SZ', '002131.SZ', '600050.SH', '600485.SH']
        print(f"{'Stock':<12} {'Plan Vol':<10} {'Exec Vol':<10} {'Diff':<10} {'Plan Price':<10} {'Exec Price':<10}")
        print("-" * 70)
        
        for ts_code in stocks:
            # 获取最近的交易计划
            plan = db.query(TradingPlan).filter(
                TradingPlan.ts_code == ts_code,
                TradingPlan.executed == True
            ).order_by(TradingPlan.date.desc()).first()
            
            if not plan:
                continue
                
            # 获取对应的成交记录
            # 优先使用 plan_id 关联
            trade = db.query(TradeRecord).filter(TradeRecord.plan_id == plan.id).first()
            
            if not trade:
                # 如果没关联 plan_id，尝试按代码和日期找
                from sqlalchemy import cast, Date
                trade = db.query(TradeRecord).filter(
                    TradeRecord.ts_code == ts_code,
                    cast(TradeRecord.trade_time, Date) == plan.date
                ).first()
            
            if not trade:
                continue
            
            # 计算计划应该有的委托数量 (基于 position_pct)
            # 假设初始总资产 1,000,000 进行估算，或者从账户获取
            from app.models.stock_models import Account
            account = db.query(Account).first()
            total_assets = account.total_assets if account else 1000000
            
            # 理论委托数量 (基于计划限价)
            ref_price = plan.buy_price_limit or plan.limit_price or trade.price
            calc_vol = int(total_assets * plan.position_pct / ref_price / 100) * 100
            
            # 实际成交数量
            exec_vol = trade.vol
            
            # 计划中记录的冻结数量 (如果当时有记的话)
            frozen_vol = plan.frozen_vol or 0
            
            print(f"{ts_code:<12} {frozen_vol or calc_vol:<10} {exec_vol:<10} {(frozen_vol or calc_vol) - exec_vol:<10} {ref_price:<10.2f} {trade.price:<10.2f}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_quantity_mismatch()

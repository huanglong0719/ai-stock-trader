import asyncio
import sys
import os
from datetime import date, timedelta

# Add backend path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyPerformance, Account

def check_history():
    print("=== 检查最近 5 天的资金曲线数据 ===")
    db = SessionLocal()
    try:
        today = date.today()
        start_date = today - timedelta(days=5)
        
        records = db.query(DailyPerformance).filter(
            DailyPerformance.date >= start_date
        ).order_by(DailyPerformance.date.asc()).all()
        
        for r in records:
            print(f"[{r.date}] 总资产: {r.total_assets:.2f}, 可用: {r.available_cash:.2f}, 市值: {r.market_value:.2f}, 当日盈亏: {r.daily_pnl:.2f}")
            
        # 获取当前 Account
        acc = db.query(Account).first()
        if acc:
            print(f"\n当前 Account 表状态: 总资产 {acc.total_assets:.2f}")
        else:
            print("\n当前 Account 表状态: 无记录")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_history()

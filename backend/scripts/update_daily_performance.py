import asyncio
import sys
import os
from datetime import date

# Add backend path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyPerformance, Account

def update_performance():
    print("=== 修正今日资金曲线记录 ===")
    db = SessionLocal()
    try:
        today = date.today()
        # 1. 获取最新修正后的账户数据
        account = db.query(Account).first()
        if not account:
            print("错误：找不到账户数据")
            return
            
        print(f"当前账户真实状态: 总资产 {account.total_assets:.2f}, 可用 {account.available_cash:.2f}")

        # 2. 获取今日记录
        perf = db.query(DailyPerformance).filter(DailyPerformance.date == today).first()
        if not perf:
            print(f"未找到 {today} 的曲线记录，可能尚未生成。尝试生成...")
            perf = DailyPerformance()
            perf.date = today
            db.add(perf)
        
        # 3. 获取昨日记录以计算盈亏
        yesterday_perf = db.query(DailyPerformance).filter(DailyPerformance.date < today).order_by(DailyPerformance.date.desc()).first()
        last_total_assets = yesterday_perf.total_assets if yesterday_perf else 1000000.0
        
        print(f"昨日 ({yesterday_perf.date if yesterday_perf else 'Initial'}) 总资产: {last_total_assets:.2f}")

        # 4. 更新今日数据
        perf.total_assets = account.total_assets
        perf.available_cash = account.available_cash
        perf.frozen_cash = account.frozen_cash
        perf.market_value = account.market_value
        
        # 重新计算盈亏
        perf.daily_pnl = perf.total_assets - last_total_assets
        perf.daily_pnl_pct = (perf.daily_pnl / last_total_assets * 100) if last_total_assets > 0 else 0
        
        perf.total_pnl = account.total_pnl
        perf.total_pnl_pct = account.total_pnl_pct
        
        db.commit()
        print(f"✅ 已修正 {today} 记录:")
        print(f"  修正后总资产: {perf.total_assets:.2f}")
        print(f"  修正后当日盈亏: {perf.daily_pnl:.2f} ({perf.daily_pnl_pct:.2f}%)")

    except Exception as e:
        print(f"❌ 更新失败: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    update_performance()

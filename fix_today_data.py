
import asyncio
from app.services.data_sync import data_sync_service
from app.services.indicator_service import indicator_service
from app.db.session import SessionLocal
from app.models.stock_models import DailyBasic, StockIndicator
from datetime import datetime

async def fix_data():
    trade_date = "20260112"
    print(f"--- 修复 {trade_date} 数据 ---")
    
    # 1. 尝试同步 DailyBasic
    print("步骤 1: 正在同步 DailyBasic...")
    count = await data_sync_service.sync_daily_basic(trade_date)
    print(f"DailyBasic 同步完成，新增记录: {count}")
    
    # 2. 检查同步结果
    db = SessionLocal()
    try:
        t_date = datetime.strptime(trade_date, '%Y%m%d').date()
        basic_count = db.query(DailyBasic).filter(DailyBasic.trade_date == t_date).count()
        print(f"数据库中 DailyBasic 记录数: {basic_count}")
        
        if basic_count > 0:
            # 3. 触发指标计算
            print("步骤 2: 正在计算所有指标 (增量模式)...")
            await indicator_service.calculate_all_indicators(trade_date)
            
            ind_count = db.query(StockIndicator).filter(StockIndicator.trade_date == t_date).count()
            print(f"数据库中 StockIndicator 记录数: {ind_count}")
        else:
            print("错误: DailyBasic 依然为空，请检查 Tushare API 是否有数据或权限。")
            
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(fix_data())

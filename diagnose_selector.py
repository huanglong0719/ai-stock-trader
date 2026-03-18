
import asyncio
import pandas as pd
from datetime import datetime
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar, StockIndicator, DailyBasic
from app.services.data_provider import data_provider
from app.services.indicator_service import indicator_service

async def diagnose():
    print("--- 选股诊断工具 ---")
    
    # 1. 获取最新交易日
    trade_date = await data_provider.get_last_trade_date()
    print(f"当前最新交易日: {trade_date}")
    
    db = SessionLocal()
    try:
        t_date = datetime.strptime(trade_date, '%Y%m%d').date()
        
        # 2. 检查 DailyBar
        bar_count = db.query(DailyBar).filter(DailyBar.trade_date == t_date).count()
        print(f"DailyBar 记录数 ({trade_date}): {bar_count}")
        
        # 3. 检查 DailyBasic
        basic_count = db.query(DailyBasic).filter(DailyBasic.trade_date == t_date).count()
        print(f"DailyBasic 记录数 ({trade_date}): {basic_count}")
        
        # 4. 检查 StockIndicator
        ind_count = db.query(StockIndicator).filter(StockIndicator.trade_date == t_date).count()
        print(f"StockIndicator 记录数 ({trade_date}): {ind_count}")
        
        if ind_count == 0 and bar_count > 0:
            print("警告: 存在 K 线数据但缺少技术指标数据！")
            print("尝试为前 5 只股票手动触发计算...")
            
            # 获取前 5 个代码
            codes = [b.ts_code for b in db.query(DailyBar.ts_code).filter(DailyBar.trade_date == t_date).limit(5).all()]
            if codes:
                print(f"计算代码: {codes}")
                await indicator_service.calculate_for_codes(codes, trade_date=trade_date)
                
                # 再次检查
                new_count = db.query(StockIndicator).filter(StockIndicator.trade_date == t_date).count()
                print(f"计算后 StockIndicator 记录数: {new_count}")
        elif bar_count == 0:
            print("错误: 缺少今日 K 线数据，同步任务可能未运行或运行失败。")
            
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(diagnose())

import asyncio
import pandas as pd
from sqlalchemy import text
from datetime import datetime
import sys
import os

# 将 backend 目录添加到 sys.path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.db.session import SessionLocal, engine
from app.services.indicator_service import IndicatorService
from app.models.stock_models import StockIndicator

async def batch_recalc_indicators():
    service = IndicatorService()
    db = SessionLocal()
    
    try:
        # 1. 获取所有股票列表
        result = db.execute(text("SELECT ts_code, symbol, name FROM stocks"))
        stocks = [dict(zip(result.keys(), row)) for row in result]
        total = len(stocks)
        print(f"找到 {total} 只股票，准备重新计算指标 (含周线和月线)...")

        batch_size = 30 # 减小批次以降低内存压力
        for i in range(0, total, batch_size):
            batch = stocks[i:i+batch_size]
            print(f"正在处理第 {i+1}-{min(i+batch_size, total)} 只股票...")
            
            tasks = []
            for stock in batch:
                ts_code = stock['ts_code']
                # 一次调用即可获得日、周、月所有指标
                tasks.append(service._calculate_single_stock(ts_code, "", force_no_cache=True, return_full_history=True))
            
            # 并发执行一批
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 展平结果并保存
            all_records = []
            for res in results_list:
                if isinstance(res, list):
                    all_records.extend(res)
                elif isinstance(res, Exception):
                    print(f"计算出错: {res}")

            if all_records:
                # 使用 SQL 直接保存以提高效率
                save_db = SessionLocal()
                try:
                    from sqlalchemy.dialects.sqlite import insert
                    for record in all_records:
                        # 移除不属于数据库列的字段
                        # StockIndicator 并没有 freq 列，所以不需要在 on_conflict 中使用 freq
                        stmt = insert(StockIndicator).values(**record)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['ts_code', 'trade_date'],
                            set_={k: v for k, v in record.items() if k not in ['ts_code', 'trade_date']}
                        )
                        save_db.execute(stmt)
                    save_db.commit()
                except Exception as e:
                    save_db.rollback()
                    print(f"保存批次失败: {e}")
                finally:
                    save_db.close()
            
            print(f"进度: {min(i+batch_size, total)}/{total}")

    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(batch_recalc_indicators())


import asyncio
import sys
import os

# 将 backend 目录添加到 PYTHONPATH
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicator_service import IndicatorService
from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator

async def force_recalc(ts_code):
    service = IndicatorService()
    print(f"正在为 {ts_code} 重新计算全量指标...")
    
    # 获取全量历史指标
    results = await service._calculate_single_stock(ts_code, "", force_no_cache=True, return_full_history=True)
    
    if not results:
        print("计算失败")
        return
        
    print(f"计算完成，共 {len(results)} 条记录。正在保存到数据库...")
    
    db = SessionLocal()
    try:
        from sqlalchemy.dialects.sqlite import insert
        
        # 使用 upsert 逻辑
        for i, data in enumerate(results):
            stmt = insert(StockIndicator).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['ts_code', 'trade_date'],
                set_={k: v for k, v in data.items() if k not in ['ts_code', 'trade_date']}
            )
            db.execute(stmt)
            if i % 100 == 0:
                db.commit()
        db.commit()
        print("保存成功")
    except Exception as e:
        db.rollback()
        print(f"保存失败: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ts_code = sys.argv[1]
    else:
        ts_code = '000001.SZ'
    asyncio.run(force_recalc(ts_code))

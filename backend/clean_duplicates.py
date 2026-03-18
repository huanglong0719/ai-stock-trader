import asyncio
import logging
import sys
from datetime import datetime, timedelta
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import desc, func

# 配置日志到控制台
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_duplicate_bars():
    """
    清理数据库中重复的日线数据 (OHLCV 完全一致的相邻记录)
    """
    logger.info("Starting duplicate bar cleaning...")
    
    db = SessionLocal()
    try:
        # 1. 获取所有股票代码
        ts_codes = [r[0] for r in db.query(DailyBar.ts_code).distinct().all()]
        logger.info(f"Checking {len(ts_codes)} stocks...")
        
        total_deleted = 0
        
        for i, ts_code in enumerate(ts_codes):
            # 获取最近 30 天的数据
            bars = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(DailyBar.trade_date.asc()).all()
            
            if len(bars) < 2:
                continue
                
            to_delete = []
            
            for j in range(1, len(bars)):
                prev = bars[j-1]
                curr = bars[j]
                
                # 检查是否完全一致
                # 注意：浮点数比较需要 epsilon，但这里我们找的是完全复制的脏数据
                # 所以直接相等比较即可，或者容忍极小误差
                if (curr.open == prev.open and 
                    curr.close == prev.close and 
                    curr.high == prev.high and 
                    curr.low == prev.low and 
                    curr.vol == prev.vol and
                    curr.amount == prev.amount):
                    
                    # 再次确认日期不是同一天 (理论上不应该有同一天的两条记录，因为有唯一索引)
                    if curr.trade_date > prev.trade_date:
                        # 这是一个重复的记录，删除较晚的那个
                        to_delete.append(curr.id)
                        
            if to_delete:
                db.query(DailyBar).filter(DailyBar.id.in_(to_delete)).delete(synchronize_session=False)
                total_deleted += len(to_delete)
                logger.info(f"Cleaned {len(to_delete)} duplicate bars for {ts_code}")
            
            if (i + 1) % 100 == 0:
                db.commit()
                logger.info(f"Processed {i + 1} stocks...")
                
        db.commit()
        logger.info(f"Cleanup complete. Total deleted: {total_deleted}")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    clean_duplicate_bars()

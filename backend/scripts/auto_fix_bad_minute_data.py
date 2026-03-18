
import sys
import os
import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import func, desc

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar, DailyBar
from app.services.data_sync import data_sync_service
from app.core.redis import redis_client

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def auto_fix_bad_minute_data():
    """
    自动检测并修复异常的分钟线数据
    逻辑：对比最新分钟线价格与最新日线价格，如果偏差超过 30%，则视为数据异常，执行重置。
    """
    db = SessionLocal()
    try:
        logger.info("开始扫描分钟线数据异常...")
        
        # 1. 获取所有有分钟数据的股票代码 (使用 distinct 可能会慢，改用 DailyBar 活跃股或直接遍历)
        # 为了效率，我们只检查最近有更新的 DailyBar 的股票
        latest_date = db.query(func.max(DailyBar.trade_date)).scalar()
        if not latest_date:
            logger.warning("没有日线数据，无法对比")
            return

        logger.info(f"基准日线日期: {latest_date}")
        
        # 获取活跃股票列表 (假设最近有日线更新的都是活跃股)
        active_stocks = db.query(DailyBar.ts_code, DailyBar.close).filter(
            DailyBar.trade_date == latest_date
        ).all()
        
        total_stocks = len(active_stocks)
        logger.info(f"扫描范围: {total_stocks} 只活跃股票")
        
        bad_stocks = []
        
        for idx, (ts_code, daily_close) in enumerate(active_stocks):
            if idx % 100 == 0:
                logger.info(f"已扫描 {idx}/{total_stocks}...")
                
            # 获取该股票最新的 30min 线
            last_min = db.query(MinuteBar).filter(
                MinuteBar.ts_code == ts_code,
                MinuteBar.freq == '30min'
            ).order_by(MinuteBar.trade_time.desc()).first()
            
            if not last_min:
                continue
                
            # 检查时间是否太旧 (超过 5 天没更新，可能停牌或数据断更，暂不处理)
            # if (datetime.now() - last_min.trade_time).days > 5:
            #     continue
            
            minute_close = last_min.close
            
            if daily_close <= 0:
                continue
                
            # 计算偏差
            diff_pct = abs(minute_close - daily_close) / daily_close
            
            # 阈值设为 30% (应对 1.9 vs 60 这种巨大差异)
            # 注意：如果是涨跌停，偏差也就 10%-20%，30% 肯定是数据错误
            if diff_pct > 0.3:
                logger.warning(f"发现异常: {ts_code} 日线={daily_close}, 分钟线={minute_close}, 偏差={diff_pct:.2%}, 时间={last_min.trade_time}")
                bad_stocks.append(ts_code)
        
        logger.info(f"扫描结束，共发现 {len(bad_stocks)} 只异常股票: {bad_stocks}")
        
        if not bad_stocks:
            return

        # 2. 执行自动修复
        for ts_code in bad_stocks:
            logger.info(f"正在修复 {ts_code} ...")
            try:
                # 清理 DB
                db.query(MinuteBar).filter(MinuteBar.ts_code == ts_code).delete()
                db.commit()
                
                # 清理 Redis
                if redis_client:
                    keys = [
                        f"MARKET:MIN:5min:{ts_code}",
                        f"MARKET:MIN:30min:{ts_code}",
                        f"MARKET:MIN:1min:{ts_code}"
                    ]
                    for k in keys:
                        redis_client.delete(k)
                
                # 重新下载 (异步转同步调用)
                end_str = datetime.now().strftime("%Y%m%d")
                await asyncio.to_thread(data_sync_service.download_minute_data, ts_code, None, end_str, freq='30min')
                await asyncio.to_thread(data_sync_service.download_minute_data, ts_code, None, end_str, freq='5min')
                
                logger.info(f"修复完成: {ts_code}")
                
            except Exception as e:
                logger.error(f"修复失败 {ts_code}: {e}")
                db.rollback()
                
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(auto_fix_bad_minute_data())

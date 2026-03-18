import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stock_models import MonthlyBar, WeeklyBar, DailyBar, Stock
import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_single_stock_bars(ts_code):
    logger.info(f"Fixing Weekly/Monthly bars for {ts_code}...")
    db = SessionLocal()
    try:
        # 1. 尝试直接从 daily_bar 关联更新
        # 这里的关键是：必须确保 weekly_bar.trade_date 对应的 daily_bar 是存在的
        # 如果 weekly_bar.trade_date 是周五，但该周五休市，或者数据缺失，关联就会失败，导致 adj_factor 依然是 NULL
        
        # 更强力的修复策略：使用子查询找到<=周线日期的最近一个有值的复权因子
        # SQLite 支持关联更新，但语法比较特殊。
        # 稳妥起见，我们使用 Python 逻辑进行分批更新，或者优化 SQL。
        
        # 优化 SQL：更新那些 adj_factor 为空的记录
        # 查找逻辑：对于每个 adj_factor 为空的 weekly_bar，找到其 trade_date 对应的 daily_bar
        # 如果找不到（比如周线日期是周五，但周五没交易），则找该周线日期之前最近的一个交易日
        
        # 简单起见，先再次执行精确匹配更新，看看能否解决大部分
        db.execute(text(f"""
            UPDATE monthly_bars 
            SET adj_factor = (
                SELECT adj_factor 
                FROM daily_bars 
                WHERE daily_bars.ts_code = '{ts_code}' 
                AND daily_bars.trade_date = monthly_bars.trade_date
            )
            WHERE ts_code = '{ts_code}' AND (adj_factor IS NULL OR adj_factor = 1.0)
        """))
        
        db.execute(text(f"""
            UPDATE weekly_bars 
            SET adj_factor = (
                SELECT adj_factor 
                FROM daily_bars 
                WHERE daily_bars.ts_code = '{ts_code}' 
                AND daily_bars.trade_date = weekly_bars.trade_date
            )
            WHERE ts_code = '{ts_code}' AND (adj_factor IS NULL OR adj_factor = 1.0)
        """))
        
        db.commit()
        
        # 2. 检查是否仍有 NULL 值，如果有，采用更暴力的填充：使用最近的有效值填充
        # 获取所有仍为 NULL 的记录
        null_weekly = db.execute(text(f"SELECT count(*) FROM weekly_bars WHERE ts_code='{ts_code}' AND adj_factor IS NULL")).scalar()
        null_weekly_val = int(null_weekly or 0)
        if null_weekly_val > 0:
            logger.warning(f"Still {null_weekly} NULL adj_factors in weekly_bars for {ts_code}. Applying fallback fix...")
            
            # Fallback: 遍历所有 NULL 记录，用 Python 逻辑找到最近的 adj_factor
            # 获取该股票所有 DailyBar 的 (date, adj) 字典
            daily_adjs = db.query(DailyBar.trade_date, DailyBar.adj_factor).filter(DailyBar.ts_code == ts_code).order_by(DailyBar.trade_date).all()
            # 转为 DataFrame 以便快速查找
            import pandas as pd
            df_adj = pd.DataFrame(daily_adjs, columns=['date', 'adj'])
            df_adj['date'] = pd.to_datetime(df_adj['date'])
            df_adj.set_index('date', inplace=True)
            
            # 获取需要修复的 WeeklyBar
            target_weeks = db.query(WeeklyBar).filter(WeeklyBar.ts_code == ts_code, WeeklyBar.adj_factor == None).all()
            for wb in target_weeks:
                t_date = pd.to_datetime(wb.trade_date)
                # 找 <= t_date 的最近一条
                # 使用 asof 查找
                idx = df_adj.index.searchsorted(t_date, side='right') - 1
                if idx >= 0:
                    setattr(wb, "adj_factor", float(df_adj.iloc[idx]['adj']))
            
            # Monthly 同理
            target_months = db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code, MonthlyBar.adj_factor == None).all()
            for mb in target_months:
                t_date = pd.to_datetime(mb.trade_date)
                idx = df_adj.index.searchsorted(t_date, side='right') - 1
                if idx >= 0:
                    setattr(mb, "adj_factor", float(df_adj.iloc[idx]['adj']))
            
            db.commit()
            logger.info("Fallback fix complete.")

        logger.info(f"Fix complete for {ts_code}")
    except Exception as e:
        logger.error(f"Error fixing {ts_code}: {e}")
        db.rollback()
    finally:
        db.close()

def fix_weekly_monthly_adj_factors():
    logger.info("Starting fix for Weekly/MonthlyBar adj_factors...")
    db = SessionLocal()
    
    try:
        # Get all stocks from Stock table (Much faster)
        ts_codes = [r[0] for r in db.query(Stock.ts_code).all()]
        logger.info(f"Found {len(ts_codes)} stocks to process.")
        
        # 批量执行修复
        for i, ts_code in enumerate(ts_codes):
            if i % 100 == 0:
                logger.info(f"Processed {i}/{len(ts_codes)} stocks...")
            
            # 复用单股修复逻辑
            fix_single_stock_bars(ts_code)
            
        logger.info("Bulk update complete.")
            
    except Exception as e:
        logger.error(f"Error fixing bars: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    # Test single stock first
    # fix_single_stock_bars('000050.SZ')
    fix_weekly_monthly_adj_factors()


import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import text
import logging

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.db.session import SessionLocal, engine
from app.models.stock_models import DailyBar, Stock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rebuild_bars_batch():
    db = SessionLocal()
    try:
        # 1. 获取所有股票代码
        ts_codes = [r[0] for r in db.query(Stock.ts_code).all()]
        total_stocks = len(ts_codes)
        logger.info(f"Found {total_stocks} stocks to process.")

        # 分批处理股票，每批 200 个
        batch_size = 200
        for i in range(0, total_stocks, batch_size):
            batch_codes = ts_codes[i : i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_stocks-1)//batch_size + 1} ({len(batch_codes)} stocks)...")
            
            processed_in_batch = 0
            query = f"""
                SELECT ts_code, trade_date, open, high, low, close, vol, amount, adj_factor 
                FROM daily_bars 
                WHERE ts_code IN ({','.join([f"'{c}'" for c in batch_codes])})
                ORDER BY trade_date ASC
            """
            df_daily = pd.read_sql(query, engine)
            
            if df_daily.empty:
                continue
                
            df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
            
            # --- 处理周线和月线 ---
            # 重置索引以便按 ts_code 分组
            df_daily.reset_index(drop=True, inplace=True)
            
            weekly_data = []
            monthly_data = []
            
            for ts_code, group in df_daily.groupby('ts_code'):
                try:
                    # 确保按日期排序
                    group = group.sort_values('trade_date')
                    
                    # --- 周线聚合 (显式按年-周分组) ---
                    # isocalendar() 返回 (year, week, weekday)
                    group['year_week'] = group['trade_date'].apply(lambda x: f"{x.isocalendar()[0]}-{x.isocalendar()[1]:02d}")
                    
                    w_grouped = group.groupby('year_week').agg({
                        'trade_date': 'last', # 周线的日期取这周最后一个交易日
                        'open': 'first',      # 这周第一个交易日的开盘价
                        'high': 'max',        # 这周最高价
                        'low': 'min',         # 这周最低价
                        'close': 'last',      # 这周最后一个交易日的收盘价
                        'vol': 'sum',         # 这周成交量总和
                        'amount': 'sum',      # 这周成交额总和
                        'adj_factor': 'last'  # 这周最后一个交易日的复权因子
                    })
                    
                    for _, row in w_grouped.iterrows():
                        weekly_data.append({
                            "ts_code": ts_code,
                            "trade_date": row['trade_date'].date(),
                            "open": float(row['open']),
                            "high": float(row['high']),
                            "low": float(row['low']),
                            "close": float(row['close']),
                            "vol": float(row['vol']),
                            "amount": float(row['amount']),
                            "adj_factor": float(row['adj_factor']),
                            "updated_at": datetime.now()
                        })
                    
                    # --- 月线聚合 (显式按年-月分组) ---
                    group['year_month'] = group['trade_date'].apply(lambda x: f"{x.year}-{x.month:02d}")
                    
                    m_grouped = group.groupby('year_month').agg({
                        'trade_date': 'last', # 月线的日期取这月最后一个交易日
                        'open': 'first',      # 这月第一个交易日的开盘价
                        'high': 'max',        # 这月最高价
                        'low': 'min',         # 这月最低价
                        'close': 'last',      # 这月最后一个交易日的收盘价
                        'vol': 'sum',         # 这月成交量总和
                        'amount': 'sum',      # 这月成交额总和
                        'adj_factor': 'last'  # 这月最后一个交易日的复权因子
                    })
                    
                    for _, row in m_grouped.iterrows():
                        monthly_data.append({
                            "ts_code": ts_code,
                            "trade_date": row['trade_date'].date(),
                            "open": float(row['open']),
                            "high": float(row['high']),
                            "low": float(row['low']),
                            "close": float(row['close']),
                            "vol": float(row['vol']),
                            "amount": float(row['amount']),
                            "adj_factor": float(row['adj_factor']),
                            "updated_at": datetime.now()
                        })
                        
                    # 每 10 个股票提交一次，增加反馈
                    processed_in_batch += 1
                    if processed_in_batch % 10 == 0:
                        logger.info(f"  Progress in batch: {processed_in_batch}/{len(batch_codes)} stocks processed.")
                except Exception as e:
                    logger.error(f"Error processing {ts_code}: {e}")
                    continue
            
            # 批量写入
            if weekly_data:
                sql_w = text("""
                    INSERT OR REPLACE INTO weekly_bars 
                    (ts_code, trade_date, open, high, low, close, vol, amount, adj_factor, updated_at)
                    VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :vol, :amount, :adj_factor, :updated_at)
                """)
                db.execute(sql_w, weekly_data)
            
            if monthly_data:
                sql_m = text("""
                    INSERT OR REPLACE INTO monthly_bars 
                    (ts_code, trade_date, open, high, low, close, vol, amount, adj_factor, updated_at)
                    VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :vol, :amount, :adj_factor, :updated_at)
                """)
                db.execute(sql_m, monthly_data)
            
            db.commit()
            # 恢复索引状态以便下一轮 groupby
            df_daily.reset_index(drop=True, inplace=True)

        logger.info("Rebuild complete!")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    rebuild_bars_batch()


import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import logging

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicators.technical_indicators import technical_indicators

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def prep_for_calc(df):
    """准备数据用于指标计算"""
    if df.empty: return []
    res = []
    for _, row in df.iterrows():
        res.append({
            'time': str(row['trade_date']),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'vol': float(row['vol']),
            'amount': float(row['amount']),
            'adj_factor': float(row['adj_factor'])
        })
    return res

def process_stock(conn, ts_code):
    """处理单个股票：合并周月线 -> 计算指标 -> 保存结果"""
    try:
        # 1. 读取日线数据
        query = f"SELECT * FROM daily_bars WHERE ts_code = '{ts_code}' ORDER BY trade_date ASC"
        df_daily = pd.read_sql(query, conn)
        
        if df_daily.empty:
            return False
            
        df_daily['trade_date_dt'] = pd.to_datetime(df_daily['trade_date'])
        
        # --- 2. 聚合周线和月线 ---
        # 周线聚合
        df_daily['year_week'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.isocalendar()[0]}-{x.isocalendar()[1]:02d}")
        w_grouped = df_daily.groupby('year_week').agg({
            'trade_date': 'last', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum', 'amount': 'sum', 'adj_factor': 'last'
        }).reset_index()
        
        # 月线聚合
        df_daily['year_month'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.year}-{x.month:02d}")
        m_grouped = df_daily.groupby('year_month').agg({
            'trade_date': 'last', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum', 'amount': 'sum', 'adj_factor': 'last'
        }).reset_index()
        
        # --- 3. 计算指标 ---
        df_d = technical_indicators._calculate_indicators(pd.DataFrame(prep_for_calc(df_daily)))
        df_w = technical_indicators._calculate_indicators(pd.DataFrame(prep_for_calc(w_grouped)))
        df_m = technical_indicators._calculate_indicators(pd.DataFrame(prep_for_calc(m_grouped)))
        
        if df_d.empty: return True
            
        # --- 4. 合并指标并保存 ---
        weekly_map = {row['time']: row for row in df_w.to_dict('records')} if not df_w.empty else {}
        monthly_map = {row['time']: row for row in df_m.to_dict('records')} if not df_m.empty else {}
        
        indicator_records = []
        latest_adj = float(df_daily['adj_factor'].iloc[-1])
        
        for _, row in df_d.iterrows():
            d_date = row['time']
            w_dates = sorted([d for d in weekly_map.keys() if d >= d_date])
            w_row = weekly_map[w_dates[0]] if w_dates else {}
            m_dates = sorted([d for d in monthly_map.keys() if d >= d_date])
            m_row = monthly_map[m_dates[0]] if m_dates else {}
            
            day_adj_rows = df_daily[df_daily['trade_date'] == d_date]
            current_day_adj = float(day_adj_rows['adj_factor'].iloc[0]) if not day_adj_rows.empty else latest_adj
            unadj_ratio = latest_adj / current_day_adj if current_day_adj != 0 else 1.0
            
            def unadjust(val):
                if val is None or pd.isna(val): return None
                return float(val * unadj_ratio)

            indicator_records.append((
                ts_code, d_date, 
                unadjust(row.get('ma5')), unadjust(row.get('ma10')), unadjust(row.get('ma20')), unadjust(row.get('ma60')),
                float(row.get('vol_ma5', 0)) if not pd.isna(row.get('vol_ma5')) else None,
                unadjust(row.get('macd_diff')), unadjust(row.get('macd_dea')),
                unadjust(w_row.get('ma20')), 0.0,
                int(w_row.get('ma5', 0) > w_row.get('ma10', 0) > w_row.get('ma20', 0)) if 'ma5' in w_row else 0,
                unadjust(m_row.get('ma20')),
                int(m_row.get('ma5', 0) > m_row.get('ma10', 0) > m_row.get('ma20', 0)) if 'ma5' in m_row else 0,
                int(row.get('ma5', 0) > row.get('ma10', 0) > row.get('ma20', 0)),
                0, current_day_adj, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                unadjust(row.get('macd')), float(row.get('vol_ma10', 0)) if not pd.isna(row.get('vol_ma10')) else None,
                unadjust(w_row.get('ma5')), unadjust(w_row.get('ma10')), unadjust(w_row.get('ma60')),
                float(w_row.get('vol_ma5', 0)) if not pd.isna(w_row.get('vol_ma5')) else None,
                float(w_row.get('vol_ma10', 0)) if not pd.isna(w_row.get('vol_ma10')) else None,
                unadjust(w_row.get('macd')), unadjust(w_row.get('macd_dea')), unadjust(w_row.get('macd_diff')),
                unadjust(m_row.get('ma5')), unadjust(m_row.get('ma10')), unadjust(m_row.get('ma60')),
                float(m_row.get('vol_ma5', 0)) if not pd.isna(m_row.get('vol_ma5')) else None,
                float(m_row.get('vol_ma10', 0)) if not pd.isna(m_row.get('vol_ma10')) else None,
                unadjust(m_row.get('macd')), unadjust(m_row.get('macd_dea')), unadjust(m_row.get('macd_diff'))
            ))

        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_indicators WHERE ts_code = ?", (ts_code,))
        cursor.executemany("""
            INSERT INTO stock_indicators (
                ts_code, trade_date, ma5, ma10, ma20, ma60, vol_ma5, macd_diff, macd_dea, 
                weekly_ma20, weekly_ma20_slope, is_weekly_bullish, monthly_ma20, is_monthly_bullish, 
                is_daily_bullish, is_trend_recovering, adj_factor, updated_at, macd, vol_ma10, 
                weekly_ma5, weekly_ma10, weekly_ma60, weekly_vol_ma5, weekly_vol_ma10, 
                weekly_macd, weekly_macd_dea, weekly_macd_diff, monthly_ma5, monthly_ma10, 
                monthly_ma60, monthly_vol_ma5, monthly_vol_ma10, monthly_macd, monthly_macd_dea, monthly_macd_diff
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, indicator_records)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error processing {ts_code}: {e}")
        return False

def main():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    
    cursor = conn.cursor()
    cursor.execute("SELECT ts_code FROM stocks")
    ts_codes = [r[0] for r in cursor.fetchall()]
    
    total = len(ts_codes)
    logger.info(f"Starting rebuild for {total} stocks (Pure Mode)...")
    
    success_count = 0
    for i, ts_code in enumerate(ts_codes):
        if process_stock(conn, ts_code):
            success_count += 1

        if (i + 1) % 10 == 0:
            print(f"Progress: {i+1}/{total} (Success: {success_count})", end='\r')
            
    conn.close()
    logger.info(f"\nRebuild finished: {success_count}/{total} successful.")

if __name__ == "__main__":
    main()


import tushare as ts
import sqlite3
import os
from datetime import datetime, timedelta
import pandas as pd

# 配置
TUSHARE_TOKEN = '46af14e3cedaaefa40f1658929ccf3d2bf05d07aa83e9bb22742e923'
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

db_path = os.path.join('backend', 'aitrader.db')

def sync_specific_stocks(codes, target_date_str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"--- 尝试同步 {len(codes)} 只缺失股票的 {target_date_str} 数据 ---")
    
    for ts_code in codes:
        try:
            print(f"正在同步 {ts_code}...")
            # 1. 获取日线 K 线
            df = pro.daily(ts_code=ts_code, start_date=target_date_str, end_date=target_date_str)
            if df.empty:
                print(f"  {ts_code} 在 Tushare 也没有 {target_date_str} 的 K 线数据")
            else:
                # 获取复权因子
                adj_df = pro.adj_factor(ts_code=ts_code, start_date=target_date_str, end_date=target_date_str)
                adj_factor = adj_df.iloc[0]['adj_factor'] if not adj_df.empty else 1.0
                
                row = df.iloc[0]
                trade_date_dt = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                
                # 插入 daily_bars
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_bars 
                    (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount, adj_factor, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts_code, trade_date_dt, row['open'], row['high'], row['low'], row['close'], 
                    row['pre_close'], row['change'], row['pct_chg'], row['vol'], row['amount'], 
                    adj_factor, datetime.now()
                ))
                print(f"  {ts_code} K 线同步成功")

            # 2. 获取每日指标 (daily_basic)
            db_df = pro.daily_basic(ts_code=ts_code, trade_date=target_date_str)
            if not db_df.empty:
                row = db_df.iloc[0]
                trade_date_dt = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_basics 
                    (ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts_code, trade_date_dt, row['close'], row['turnover_rate'], row['turnover_rate_f'], 
                    row['volume_ratio'], row['pe'], row['pe_ttm'], row['pb'], row['ps'], row['ps_ttm'], 
                    row['dv_ratio'], row['dv_ttm'], row['total_share'], row['float_share'], row['free_share'], 
                    row['total_mv'], row['circ_mv'], datetime.now()
                ))
                print(f"  {ts_code} 基础数据同步成功")
                
            conn.commit()
        except Exception as e:
            print(f"  同步 {ts_code} 失败: {e}")
            
    conn.close()

if __name__ == "__main__":
    missing_codes = ['920680.BJ', '600200.SH']
    sync_specific_stocks(missing_codes, '20260109')

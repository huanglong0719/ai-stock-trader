
import sqlite3
import pandas as pd

def check_301282_indicators():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    print("--- 301282.SZ daily_bars vs stock_indicators in 2025-04 ---")
    
    # 获取 daily_bars
    query_daily = """
    SELECT trade_date, open, high, low, close, adj_factor 
    FROM daily_bars 
    WHERE ts_code = '301282.SZ' 
    AND trade_date BETWEEN '2025-03-01' AND '2025-05-31'
    ORDER BY trade_date ASC
    """
    df_daily = pd.read_sql_query(query_daily, conn)
    
    # 获取 stock_indicators
    query_indicators = """
    SELECT trade_date, ma5, ma10, ma20, ma60, adj_factor as ind_adj
    FROM stock_indicators 
    WHERE ts_code = '301282.SZ' 
    AND trade_date BETWEEN '2025-03-01' AND '2025-05-31'
    ORDER BY trade_date ASC
    """
    df_ind = pd.read_sql_query(query_indicators, conn)
    
    conn.close()
    
    if df_daily.empty or df_ind.empty:
        print("No data found.")
        return

    # 合并对比
    df_merged = pd.merge(df_daily, df_ind, on='trade_date', how='inner')
    
    # 假设最新的 adj_factor 是 1.0387 (从之前的调试中得知)
    latest_adj = 1.0387
    
    print(f"Using latest_adj: {latest_adj}")
    print(df_merged[['trade_date', 'close', 'adj_factor', 'ma20', 'ind_adj']].head(20))
    
    # 检查 ma20 是否和 QFQ 后的收盘价匹配
    # QFQ_close = close * (adj_factor / latest_adj)
    df_merged['qfq_close'] = df_merged['close'] * (df_merged['adj_factor'] / latest_adj)
    
    # 计算 QFQ 后的 MA20 (简单滑动平均)
    # 注意：这里的 MA20 是在数据库里存着的，我们要看看它是不是基于某个特定的 adj_factor 计算的
    print("\n--- Checking MA20 alignment ---")
    for i in range(len(df_merged)):
        row = df_merged.iloc[i]
        qfq_close = row['qfq_close']
        ma20 = row['ma20']
        diff = abs(qfq_close - ma20)
        # print(f"Date: {row['trade_date']}, QFQ_Close: {qfq_close:.3f}, MA20: {ma20:.3f}, Diff: {diff:.3f}")

    # 找出 MA20 突变的地方
    df_merged['ma20_diff'] = df_merged['ma20'].diff()
    print("\n--- MA20 Diffs (to find jumps) ---")
    print(df_merged[df_merged['ma20_diff'].abs() > 0.5][['trade_date', 'ma20', 'ma20_diff']])

if __name__ == "__main__":
    check_301282_indicators()

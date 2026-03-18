
import pandas as pd
import numpy as np
import sqlite3
from app.services.indicators.technical_indicators import technical_indicators

def test_300508_calc():
    db_path = 'backend/aitrader.db'
    conn = sqlite3.connect(db_path)
    
    # 模拟增量更新：获取 4-7 之前的数据
    # 假设我们只拿到了很少的数据
    query = """
    SELECT trade_date as time, open, high, low, close, vol, adj_factor 
    FROM daily_bars 
    WHERE ts_code = '300508.SZ' AND trade_date <= '2025-04-07' 
    ORDER BY trade_date DESC 
    LIMIT 15
    """
    df = pd.read_sql_query(query, conn)
    df = df.sort_values('time')
    
    print(f"参与计算的数据条数: {len(df)}")
    
    # 执行计算
    res = technical_indicators.calculate(df.to_dict('records'))
    
    print("\n--- 计算结果 (2025-04-07) ---")
    last_row = res.iloc[-1]
    print(f"日期: {last_row['time']}")
    print(f"收盘价: {last_row['close']}")
    print(f"MA5: {last_row['ma5']}")
    print(f"MA10: {last_row['ma10']}")
    print(f"MA20: {last_row['ma20']}  <-- 重点看这里")
    
    conn.close()

if __name__ == "__main__":
    test_300508_calc()

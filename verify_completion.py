import sqlite3
from datetime import datetime

def verify():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    # 1. 获取最新交易日
    cursor.execute("SELECT MAX(trade_date) FROM daily_bars")
    latest_trade_date = cursor.fetchone()[0]
    print(f"最新交易日: {latest_trade_date}")
    
    # 2. 统计总股票数
    cursor.execute("SELECT COUNT(*) FROM stocks")
    total_stocks = cursor.fetchone()[0]
    print(f"数据库中总股票数: {total_stocks}")
    
    # 3. 统计已计算指标的股票数 (针对最新交易日)
    cursor.execute("SELECT COUNT(*) FROM stock_indicators WHERE trade_date = ?", (latest_trade_date,))
    calculated_stocks = cursor.fetchone()[0]
    print(f"已计算最新指标的股票数: {calculated_stocks}")
    
    # 4. 统计指数和板块指标
    cursor.execute("SELECT ts_code FROM stock_indicators WHERE trade_date = ?", (latest_trade_date,))
    all_codes = [r[0] for r in cursor.fetchall()]
    
    indices = [c for c in all_codes if any(c.endswith(s) for s in ['.SH', '.SZ', '.BJ']) and (c.startswith('000') or c.startswith('399') or c.startswith('899'))]
    industries = [c for c in all_codes if c.startswith('IND_')]
    
    print(f"已计算指数指标数: {len(indices)}")
    print(f"已计算行业板块指标数: {len(industries)}")
    
    # 5. 计算完成率
    completion_rate = (calculated_stocks / total_stocks * 100) if total_stocks > 0 else 0
    print(f"\n股票指标完成率: {completion_rate:.2f}%")
    
    if completion_rate < 90:
        print("\n[提示] 指标计算尚未完全覆盖所有股票，建议执行增量计算。")
    else:
        print("\n[提示] 大部分股票指标已计算完成。")
        
    conn.close()

if __name__ == "__main__":
    verify()

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar, WeeklyBar, MonthlyBar
from datetime import datetime, date

def check_stock_data(ts_code):
    db = SessionLocal()
    print(f"Checking data for {ts_code}...")
    
    # 1. 检查月线
    print("\n--- Monthly Bars (Last 12) ---")
    m_bars = db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code).order_by(MonthlyBar.trade_date.desc()).limit(12).all()
    for b in m_bars:
        print(f"Date: {b.trade_date}, Open: {b.open}, High: {b.high}, Low: {b.low}, Close: {b.close}, Vol: {b.vol}, Adj: {b.adj_factor}")

    # 2. 检查最近 5 个月内的日线最高价
    print("\n--- Daily Bars (Recent 5 Months High: 2025-08-01 to 2026-01-08) ---")
    start_date = date(2025, 8, 1)
    end_date = date(2026, 1, 8)
    high_bar = db.query(DailyBar).filter(
        DailyBar.ts_code == ts_code,
        DailyBar.trade_date >= start_date,
        DailyBar.trade_date <= end_date
    ).order_by(DailyBar.high.desc()).first()
    
    if high_bar:
        print(f"Highest Daily Bar in range: Date: {high_bar.trade_date}, High: {high_bar.high}, Adj: {high_bar.adj_factor}")
    else:
        print("No daily bars found in range.")

    db.close()

if __name__ == "__main__":
    check_stock_data("600699.SH")

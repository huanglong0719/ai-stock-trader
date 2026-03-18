import asyncio
import pandas as pd
from app.services.stock_selector import stock_selector
from app.services.data_provider import data_provider
from app.services.logger import selector_logger

async def test_filter():
    trade_date = "20260109"
    print(f"Testing filter for {trade_date}")
    
    # 1. Get basic data
    df_basic = await data_provider.get_daily_basic(trade_date=trade_date)
    if df_basic.empty:
        print("Basic data is empty")
        return
    
    print(f"Basic data size: {len(df_basic)}")
    
    # 2. Filter ST, 688, BJ
    df = df_basic[
        ~df_basic['ts_code'].str.startswith('688') & 
        ~df_basic['ts_code'].str.startswith('8') & 
        ~df_basic['ts_code'].str.startswith('4') &
        ~df_basic['ts_code'].str.endswith('.BJ')
    ].copy()
    
    stock_list = await data_provider.get_stock_basic()
    name_map = {s['ts_code']: s['name'] for s in stock_list}
    df['name'] = df['ts_code'].map(name_map)
    df = df[~df['name'].str.contains('ST', na=False)]
    df = df[~df['name'].str.contains('退', na=False)]
    
    mv_col = 'circ_mv' if 'circ_mv' in df.columns else 'total_mv'
    mask = (df['pe'] > 0) & (df['pe'] < 80) & (df[mv_col] > 200000)
    df = df[mask]
    
    print(f"After basic filter: {len(df)}")
    
    # 3. Indicator filter
    ts_codes = df['ts_code'].tolist()
    indicators_map = await stock_selector._get_indicators_batch(ts_codes, trade_date=trade_date)
    print(f"Indicators found: {len(indicators_map)}")
    
    if len(indicators_map) == 0:
        # Check if there are ANY indicators for this date
        from app.db.session import SessionLocal
        from app.models.stock_models import StockIndicator
        db = SessionLocal()
        count = db.query(StockIndicator).filter(StockIndicator.trade_date == trade_date).count()
        print(f"Total indicators in DB for {trade_date}: {count}")
        db.close()
        return

    valid_codes = []
    stats = {
        "no_ind": 0,
        "monthly_fail": 0,
        "weekly_fail": 0,
        "slope_fail": 0,
        "daily_fail": 0
    }
    
    for code in ts_codes:
        ind = indicators_map.get(code)
        if not ind:
            stats["no_ind"] += 1
            continue
        
        if not ind.is_monthly_bullish:
            stats["monthly_fail"] += 1
            continue
        if not ind.is_weekly_bullish:
            stats["weekly_fail"] += 1
            continue
        if ind.weekly_ma20_slope < -5:
            stats["slope_fail"] += 1
            continue
        if not (ind.is_daily_bullish or ind.is_trend_recovering):
            stats["daily_fail"] += 1
            continue
        
        valid_codes.append(code)
    
    print(f"Stats: {stats}")
    print(f"Valid codes: {len(valid_codes)}")

if __name__ == "__main__":
    asyncio.run(test_filter())

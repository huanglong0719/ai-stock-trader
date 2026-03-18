import asyncio
print("DIAGNOSIS SCRIPT STARTED")
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import text

from app.db.session import SessionLocal
from app.models.stock_models import Stock, DailyBar, WeeklyBar, MonthlyBar
from app.services.data_provider import data_provider
from app.services.indicator_service import indicator_service
from app.services.market.tushare_client import tushare_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Diagnosis Logic ---

def _normalize_date(value: str) -> str:
    if not value:
        return ""
    return str(value).split(" ")[0].replace("-", "").replace("/", "")

def _gap_threshold(freq: str) -> int:
    if freq == "W":
        return 10
    if freq == "M":
        return 40
    return 3

async def _check_symbol(symbol: str, latest_trade_date: str, freq: str = "D", limit: int = 200) -> Dict:
    try:
        # Use is_ui_request=True to check what the UI sees (cache + merge)
        kline = await data_provider.get_kline(symbol, freq=freq, limit=limit, is_ui_request=True)
    except Exception as e:
        return {"symbol": symbol, "freq": freq, "error": str(e)}

    if not kline:
        return {"symbol": symbol, "freq": freq, "issue": "empty"}

    last = kline[-1]
    last_time = str(last.get("time") or "")
    last_date = _normalize_date(last_time)
    today = datetime.now().strftime("%Y%m%d")

    issues: List[str] = []
    
    # 1. Future date check
    if last_date > today:
        issues.append("future_last")

    # 2. Lag check
    if latest_trade_date and last_date < latest_trade_date:
        try:
            gap_days = (datetime.strptime(latest_trade_date, "%Y%m%d") - datetime.strptime(last_date, "%Y%m%d")).days
            if gap_days > _gap_threshold(freq):
                issues.append(f"behind_latest_trade_date:{gap_days}")
        except:
            pass

    # 3. Price Jump / Flat Line Check
    prev_close_val: Optional[float] = None
    for bar in kline:
        close_v = bar.get("close")
        if close_v is None or float(close_v) <= 0:
            issues.append("nonpositive_close")
            break
        
        curr_close = float(close_v)
        
        if prev_close_val and prev_close_val > 0:
            # Threshold: 50% jump for Daily/Weekly/Monthly is suspicious (unless extreme volatility)
            # 20% for intraday?
            threshold = 0.5 
            if abs(curr_close / prev_close_val - 1.0) > threshold:
                issues.append(f"price_jump:{prev_close_val}->{curr_close}")
                break
        prev_close_val = curr_close

    # 4. Adj Factor Check
    last_adj = last.get("adj_factor")
    if last_adj is None or float(last_adj) <= 0:
        issues.append("adj_factor_missing")

    if issues:
        return {
            "symbol": symbol,
            "freq": freq,
            "last_time": last_time,
            "latest_trade_date": latest_trade_date,
            "issues": issues
        }

    return {}

async def _run_checks(symbols: List[str], latest_trade_date: str) -> List[Dict]:
    sem = asyncio.Semaphore(10) # Limit concurrency
    results: List[Dict] = []

    async def _run_one(symbol: str):
        async with sem:
            # Check Daily, Weekly, Monthly
            # If Daily is bad, Weekly/Monthly likely bad too.
            # But sometimes Daily is fine (re-fetched) but Weekly/Monthly is stale.
            res_d = await _check_symbol(symbol, latest_trade_date, freq="D", limit=200)
            if res_d:
                results.append(res_d)
                return # Stop if Daily is bad

            res_w = await _check_symbol(symbol, latest_trade_date, freq="W", limit=120)
            if res_w:
                results.append(res_w)
                return

            res_m = await _check_symbol(symbol, latest_trade_date, freq="M", limit=120)
            if res_m:
                results.append(res_m)

    tasks = [_run_one(s) for s in symbols]
    await asyncio.gather(*tasks)
    return results

# --- Fix Logic ---

async def fix_stock(ts_code: str):
    logger.info(f"Fixing stock {ts_code}...")
    
    # 1. Force re-fetch Daily Bars from Tushare (update DB)
    # Using tushare_client directly or data_sync_service?
    # data_sync_service._sync_daily_bars is for specific date.
    # We need range fetch.
    # Use tushare_client.daily
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        # [Fix] Increase range to 5 years to cover historical bad data (e.g. 2022-2023)
        start_date = (datetime.now() - timedelta(days=1825)).strftime('%Y%m%d')
        
        logger.info(f"Fetching daily data for {ts_code} from {start_date} to {end_date}...")
        
        # Fetch Daily
        # tushare_client.query(api_name, fields='', **kwargs) -> params
        df = await asyncio.to_thread(
            tushare_client.query, 
            'daily', 
            params={
                'start_date': start_date, 
                'end_date': end_date, 
                'ts_code': ts_code
            }
        )
        if df is not None and not df.empty:
            # Fetch Adj Factor
            adj = await asyncio.to_thread(
                tushare_client.query, 
                'adj_factor', 
                params={
                    'start_date': start_date, 
                    'end_date': end_date, 
                    'ts_code': ts_code
                }
            )
            if adj is not None and not adj.empty:
                df = pd.merge(df, adj[['trade_date', 'adj_factor']], on='trade_date', how='left')
                df['adj_factor'] = df['adj_factor'].fillna(1.0)
            else:
                df['adj_factor'] = 1.0

            # Update DB
            db = SessionLocal()
            try:
                # Delete existing range? Or Upsert?
                # Upsert is safer.
                # But for speed, we can delete and insert if we are sure.
                # Let's use Upsert logic (slow but safe).
                for _, row in df.iterrows():
                    d_str = row['trade_date']
                    d_date = datetime.strptime(d_str, '%Y%m%d').date()
                    
                    bar = db.query(DailyBar).filter(DailyBar.ts_code == ts_code, DailyBar.trade_date == d_date).first()
                    if not bar:
                        bar = DailyBar(ts_code=ts_code, trade_date=d_date)
                        db.add(bar)
                    
                    bar.open = row['open']
                    bar.close = row['close']
                    bar.high = row['high']
                    bar.low = row['low']
                    bar.vol = row['vol']
                    bar.amount = row['amount']
                    bar.adj_factor = row['adj_factor']
                
                db.commit()
            finally:
                db.close()
    except Exception as e:
        logger.error(f"Error fetching/updating daily for {ts_code}: {e}")

    # 2. Rebuild Weekly/Monthly
    from app.services.data_sync import data_sync_service
    try:
        await asyncio.to_thread(data_sync_service.reconstruct_weekly_monthly, ts_code)
    except Exception as e:
        logger.error(f"Error rebuilding W/M for {ts_code}: {e}")

    # 3. Recalculate Indicators
    try:
        await indicator_service.calculate_for_codes([ts_code], force_full=True)
    except Exception as e:
        logger.error(f"Error recalc indicators for {ts_code}: {e}")


async def main():
    logger.info("Starting Batch Diagnosis...")
    

    # 0. Priority Fix for 001285.SZ if requested
    target_stocks = ["300418.SZ"]
    
    if target_stocks:
        logger.info(f"Targeting specific stocks: {target_stocks}")
        for s in target_stocks:
            await fix_stock(s)
        # If target stocks are specified, only check those stocks
        stocks = target_stocks
    else:
        # Get all stocks
        db = SessionLocal()
        try:
            stocks = [r[0] for r in db.query(Stock.ts_code).all()]
        finally:
            db.close()
    
    logger.info(f"Total stocks to check: {len(stocks)}")
    
    latest_trade_date = await data_provider.get_last_trade_date(include_today=True)
    
    # Run Checks
    bad_stocks = await _run_checks(stocks, latest_trade_date)
    
    logger.info(f"Found {len(bad_stocks)} bad stocks.")
    
    for b in bad_stocks:
        logger.info(f"Bad: {b}")
        
    # Fix
    if bad_stocks:
        logger.info("Starting Auto-Fix...")
        for b in bad_stocks:
            await fix_stock(b['symbol'])
        logger.info("Auto-Fix Complete.")
    else:
        logger.info("No anomalies found.")

if __name__ == "__main__":
    asyncio.run(main())

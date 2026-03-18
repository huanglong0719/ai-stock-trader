import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from app.db.session import SessionLocal
from app.models.stock_models import Stock, DailyBar, WeeklyBar, MonthlyBar
from app.services.indicator_service import indicator_service
from app.services.data_provider import data_provider
from app.core.config import settings

import pandas as pd
import tushare as ts

AUTO_FIX = True
AUTO_FIX_FREQS = {"W", "M"}
AUTO_FIX_ISSUES = {"price_jump", "adj_factor_missing"}
FIX_DAILY = True
TARGET_SYMBOLS = ["002009.SZ"]
RUN_BATCH_CHECKS = False


def _date_to_str(d) -> str:
    if hasattr(d, "strftime"):
        return d.strftime("%Y%m%d")
    return str(d)


def _repair_daily_from_tushare(ts_code: str, start_date: str, end_date: str) -> int:
    pro = ts.pro_api(settings.TUSHARE_TOKEN)
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return 0
    adj_df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if adj_df is None or adj_df.empty:
        df["adj_factor"] = 1.0
    else:
        adj_df = adj_df[["ts_code", "trade_date", "adj_factor"]]
        df = pd.merge(df, adj_df, on=["ts_code", "trade_date"], how="left")
        df["adj_factor"] = df["adj_factor"].fillna(1.0)

    df = df.sort_values("trade_date", ascending=False)
    db = SessionLocal()
    try:
        updated = 0
        for _, row in df.iterrows():
            trade_date = datetime.strptime(str(row["trade_date"]), "%Y%m%d").date()
            existing = db.query(DailyBar).filter(DailyBar.ts_code == ts_code, DailyBar.trade_date == trade_date).first()
            if existing:
                existing.open = row["open"]
                existing.high = row["high"]
                existing.low = row["low"]
                existing.close = row["close"]
                existing.pre_close = row["pre_close"]
                existing.change = row["change"]
                existing.pct_chg = row["pct_chg"]
                existing.vol = row["vol"]
                existing.amount = row["amount"]
                existing.adj_factor = float(row["adj_factor"] or 1.0)
            else:
                bar = DailyBar(
                    ts_code=ts_code,
                    trade_date=trade_date,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    pre_close=row["pre_close"],
                    change=row["change"],
                    pct_chg=row["pct_chg"],
                    vol=row["vol"],
                    amount=row["amount"],
                )
                setattr(bar, "adj_factor", float(row["adj_factor"] or 1.0))
                db.add(bar)
            updated += 1
        db.commit()
        return updated
    finally:
        db.close()


def _rebuild_weekly_monthly(ts_code: str, start_date: str, end_date: str) -> int:
    db = SessionLocal()
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
        rows = (
            db.query(DailyBar)
            .filter(DailyBar.ts_code == ts_code, DailyBar.trade_date >= start_dt, DailyBar.trade_date <= end_dt)
            .order_by(DailyBar.trade_date.asc())
            .all()
        )
        if not rows:
            return 0

        df = pd.DataFrame(
            [
                {
                    "trade_date": r.trade_date,
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                    "vol": r.vol,
                    "amount": r.amount,
                    "adj_factor": r.adj_factor,
                }
                for r in rows
            ]
        )
        df["trade_date_dt"] = pd.to_datetime(df["trade_date"])

        df["year_week"] = df["trade_date_dt"].dt.to_period("W-FRI")
        w_grouped = df.groupby("year_week").agg(
            {
                "trade_date": "last",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "vol": "sum",
                "amount": "sum",
                "adj_factor": "last",
            }
        )

        df["year_month"] = df["trade_date_dt"].dt.to_period("M")
        m_grouped = df.groupby("year_month").agg(
            {
                "trade_date": "last",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "vol": "sum",
                "amount": "sum",
                "adj_factor": "last",
            }
        )

        count = 0
        for _, row in w_grouped.iterrows():
            w_trade_date = row["trade_date"]
            weekly_existing = db.query(WeeklyBar).filter(WeeklyBar.ts_code == ts_code, WeeklyBar.trade_date == w_trade_date).first()
            if weekly_existing:
                weekly_existing.open = float(row["open"])
                weekly_existing.high = float(row["high"])
                weekly_existing.low = float(row["low"])
                weekly_existing.close = float(row["close"])
                weekly_existing.vol = float(row["vol"])
                weekly_existing.amount = float(row["amount"])
                weekly_existing.adj_factor = float(row["adj_factor"] or 1.0)
            else:
                db.add(
                    WeeklyBar(
                        ts_code=ts_code,
                        trade_date=w_trade_date,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        vol=float(row["vol"]),
                        amount=float(row["amount"]),
                        adj_factor=float(row["adj_factor"] or 1.0),
                    )
                )
            count += 1

        for _, row in m_grouped.iterrows():
            m_trade_date = row["trade_date"]
            monthly_existing = db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code, MonthlyBar.trade_date == m_trade_date).first()
            if monthly_existing:
                monthly_existing.open = float(row["open"])
                monthly_existing.high = float(row["high"])
                monthly_existing.low = float(row["low"])
                monthly_existing.close = float(row["close"])
                monthly_existing.vol = float(row["vol"])
                monthly_existing.amount = float(row["amount"])
                monthly_existing.adj_factor = float(row["adj_factor"] or 1.0)
            else:
                db.add(
                    MonthlyBar(
                        ts_code=ts_code,
                        trade_date=m_trade_date,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        vol=float(row["vol"]),
                        amount=float(row["amount"]),
                        adj_factor=float(row["adj_factor"] or 1.0),
                    )
                )
            count += 1

        db.commit()
        return count
    finally:
        db.close()


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
    prev_time = ""
    seen_times = set()
    prev_close_val: Optional[float] = None
    for bar in kline:
        bar_time = str(bar.get("time") or "")
        bar_date = _normalize_date(bar_time)
        if bar_date and bar_date > today:
            issues.append("future_time")
            break
        if prev_time and bar_time and bar_time < prev_time:
            issues.append("time_not_sorted")
            break
        if bar_time and bar_time in seen_times:
            issues.append("time_duplicated")
            break
        if bar_time:
            seen_times.add(bar_time)
        prev_time = bar_time

        close_v = bar.get("close")
        high_v = bar.get("high")
        low_v = bar.get("low")
        if close_v is None or float(close_v) <= 0:
            issues.append("nonpositive_close")
            break
        if high_v is not None and low_v is not None and float(high_v) < float(low_v):
            issues.append("high_lt_low")
            break
        try:
            curr_close = float(close_v)
        except Exception:
            curr_close = None
        if curr_close is not None and prev_close_val and prev_close_val > 0:
            threshold = 0.5 if freq in {"D", "W", "M"} else 0.2
            if abs(curr_close / prev_close_val - 1.0) > threshold:
                issues.append("price_jump")
                break
        if curr_close is not None:
            prev_close_val = curr_close
    if last_date and last_date > today:
        issues.append("future_last")
    if latest_trade_date and last_date and last_date < latest_trade_date:
        gap_days = 0
        try:
            gap_days = (datetime.strptime(latest_trade_date, "%Y%m%d") - datetime.strptime(last_date, "%Y%m%d")).days
        except Exception:
            gap_days = 0
        if gap_days > _gap_threshold(freq):
            issues.append(f"behind_latest_trade_date:{gap_days}")

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


async def _run_checks(symbols: List[str], latest_trade_date: str, include_minutes: bool = False) -> List[Dict]:
    sem = asyncio.Semaphore(8)
    results: List[Dict] = []

    async def _run_one(symbol: str, freq: str, limit: int):
        async with sem:
            res = await _check_symbol(symbol, latest_trade_date, freq=freq, limit=limit)
            if res:
                results.append(res)

    tasks = []
    for s in symbols:
        tasks.append(asyncio.create_task(_run_one(s, "D", 200)))
        tasks.append(asyncio.create_task(_run_one(s, "W", 120)))
        tasks.append(asyncio.create_task(_run_one(s, "M", 120)))
        if include_minutes:
            tasks.append(asyncio.create_task(_run_one(s, "5min", 96)))
            tasks.append(asyncio.create_task(_run_one(s, "30min", 48)))

    if tasks:
        await asyncio.gather(*tasks)
    return results


async def main():
    latest_trade_date = await data_provider.get_last_trade_date(include_today=True)
    db = SessionLocal()
    try:
        rows = db.query(Stock.ts_code).limit(300).all()
    finally:
        db.close()

    symbols = [r[0] for r in rows]
    if "002009.SZ" not in symbols:
        symbols.append("002009.SZ")
    print(f"latest_trade_date={latest_trade_date}, sample={len(symbols)}")

    if FIX_DAILY and TARGET_SYMBOLS:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=1825)).strftime("%Y%m%d")
        for s in TARGET_SYMBOLS:
            fixed = _repair_daily_from_tushare(s, start_date, end_date)
            print(f"daily_fixed={s},{fixed}")
            rebuilt = _rebuild_weekly_monthly(s, start_date, end_date)
            print(f"weekly_monthly_rebuilt={s},{rebuilt}")
            await indicator_service.calculate_for_codes([s], trade_date=end_date, force_full=True)
            d_res = await _check_symbol(s, latest_trade_date, freq="D", limit=2000)
            w_res = await _check_symbol(s, latest_trade_date, freq="W", limit=400)
            m_res = await _check_symbol(s, latest_trade_date, freq="M", limit=400)
            if d_res:
                print(d_res)
            if w_res:
                print(w_res)
            if m_res:
                print(m_res)

    bad: List[Dict] = []
    if RUN_BATCH_CHECKS:
        bad = await _run_checks(symbols, latest_trade_date)
        minute_symbols = symbols[:40]
        minute_bad = await _run_checks(minute_symbols, latest_trade_date, include_minutes=True)
        bad.extend([r for r in minute_bad if r and r.get("freq") in {"5min", "30min"}])
        print(f"bad_count={len(bad)}")
        for r in bad[:50]:
            print(r)

    if AUTO_FIX and RUN_BATCH_CHECKS:
        fix_symbols = {
            r["symbol"]
            for r in bad
            if r.get("freq") in AUTO_FIX_FREQS and AUTO_FIX_ISSUES.intersection(set(r.get("issues", [])))
        }
        if fix_symbols:
            from scripts.fix_weekly_monthly_adj import fix_single_stock_bars

            print(f"auto_fix_symbols={len(fix_symbols)}")
            for s in sorted(fix_symbols):
                fix_single_stock_bars(s)

            fixed_checks = await _run_checks(sorted(fix_symbols), latest_trade_date)
            fixed_bad = [r for r in fixed_checks if r]
            print(f"auto_fix_remaining={len(fixed_bad)}")
            for r in fixed_bad[:50]:
                print(r)


if __name__ == "__main__":
    asyncio.run(main())

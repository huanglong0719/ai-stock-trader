import sys
import calendar
from pathlib import Path
from datetime import datetime, timedelta

from sqlalchemy import func

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from app.services.data_provider import data_provider


def week_range(end_date: str):
    d = datetime.strptime(end_date, "%Y-%m-%d")
    start = (d - timedelta(days=d.weekday())).date()
    end = (start + timedelta(days=6))
    return start, end


def month_range(end_date: str):
    d = datetime.strptime(end_date, "%Y-%m-%d")
    start = datetime(d.year, d.month, 1).date()
    end = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1]).date()
    return start, end


def sum_daily_vol(db, ts_code: str, start_date, end_date) -> float:
    vol = (
        db.query(func.sum(DailyBar.vol))
        .filter(
            DailyBar.ts_code == ts_code,
            DailyBar.trade_date >= start_date,
            DailyBar.trade_date <= end_date,
        )
        .scalar()
        or 0
    )
    return float(vol)


async def verify(ts_code: str, tail: int = 3):
    db = SessionLocal()
    try:
        weekly = await data_provider.get_kline(ts_code, "W")
        monthly = await data_provider.get_kline(ts_code, "M")
        weekly = weekly or []
        monthly = monthly or []

        print(f"== {ts_code}")
        w_items = weekly[-tail:] if len(weekly) > tail else weekly
        if len(w_items) >= 2:
            for item in w_items[:-1]:
                start, end = week_range(item["time"])
                summed = sum_daily_vol(db, ts_code, start, end)
                api = float(item.get("volume") or 0)
                print(f"W {item['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f}")

            last = w_items[-1]
            start, end = week_range(last["time"])
            summed = sum_daily_vol(db, ts_code, start, end)
            api = float(last.get("volume") or 0)
            print(f"W {last['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f} (latest, may include realtime)")
        else:
            for item in w_items:
                start, end = week_range(item["time"])
                summed = sum_daily_vol(db, ts_code, start, end)
                api = float(item.get("volume") or 0)
                print(f"W {item['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f}")

        m_items = monthly[-tail:] if len(monthly) > tail else monthly
        if len(m_items) >= 2:
            for item in m_items[:-1]:
                start, end = month_range(item["time"])
                summed = sum_daily_vol(db, ts_code, start, end)
                api = float(item.get("volume") or 0)
                print(f"M {item['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f}")

            last = m_items[-1]
            start, end = month_range(last["time"])
            summed = sum_daily_vol(db, ts_code, start, end)
            api = float(last.get("volume") or 0)
            print(f"M {last['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f} (latest, may include realtime)")
        else:
            for item in m_items:
                start, end = month_range(item["time"])
                summed = sum_daily_vol(db, ts_code, start, end)
                api = float(item.get("volume") or 0)
                print(f"M {item['time']} api={api:.2f} sum={summed:.2f} diff={api - summed:.2f}")
    finally:
        db.close()


async def main():
    codes = ["002353.SZ", "000001.SH", "399001.SZ"]
    for code in codes:
        await verify(code)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

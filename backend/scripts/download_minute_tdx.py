import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from app.services.data_sync import data_sync_service
from app.db.session import SessionLocal
from app.models.stock_models import Stock, Position, TradingPlan
from sqlalchemy import desc


def _parse_codes(codes_str: str | None) -> list[str]:
    if not codes_str:
        return []
    parts = [p.strip() for p in codes_str.replace(";", ",").replace(" ", ",").split(",")]
    parts = [p for p in parts if p]
    res = []
    for p in parts:
        if "." in p:
            res.append(p.upper())
        elif len(p) == 6 and p.isdigit():
            if p.startswith("6"):
                res.append(f"{p}.SH")
            elif p.startswith("8") or p.startswith("4"):
                res.append(f"{p}.BJ")
            else:
                res.append(f"{p}.SZ")
    return sorted(list(dict.fromkeys(res)))


def _load_active_pool(limit: int = 200) -> list[str]:
    db = SessionLocal()
    try:
        codes = set()
        for r in db.query(Position.ts_code).filter(Position.vol > 0).limit(limit).all():
            codes.add(r[0])
        for r in db.query(TradingPlan.ts_code).filter(TradingPlan.executed == False).order_by(desc(TradingPlan.id)).limit(limit).all():
            codes.add(r[0])
        return sorted(list(codes))
    finally:
        db.close()


def _load_all_stocks(limit: int = 200) -> list[str]:
    db = SessionLocal()
    try:
        rows = db.query(Stock.ts_code).order_by(Stock.ts_code.asc()).limit(limit).all()
        return [r[0] for r in rows]
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--pool", type=str, default="active", choices=["active", "all", "custom", "shsz"])
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--freqs", type=str, default="5min,30min")
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    freqs = [f.strip().lower() for f in args.freqs.split(",") if f.strip()]
    freqs = ["5min" if f in ["5", "5m", "5min"] else "30min" if f in ["30", "30m", "30min"] else f for f in freqs]
    freqs = [f for f in freqs if f in ["5min", "30min"]]
    if not freqs:
        raise SystemExit("freqs must include 5min and/or 30min")

    if args.pool == "custom":
        codes = _parse_codes(args.codes)
    elif args.pool == "shsz":
        db = SessionLocal()
        try:
            rows = db.query(Stock.ts_code).filter(
                (Stock.ts_code.like("%.SZ")) | (Stock.ts_code.like("%.SH"))
            ).order_by(Stock.ts_code.asc()).limit(max(1, args.limit)).all()
            codes = [r[0] for r in rows]
        finally:
            db.close()
    elif args.pool == "all":
        codes = _load_all_stocks(limit=max(1, args.limit))
    else:
        codes = _load_active_pool(limit=max(1, args.limit))

    if not codes:
        raise SystemExit("no codes to download")

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=max(1, int(args.days)))).strftime("%Y%m%d")

    total_ok = 0
    for i, code in enumerate(codes, start=1):
        for f in freqs:
            try:
                n = data_sync_service.download_minute_data(code, start_date, end_date, freq=f)
                total_ok += 1
                print(f"[{i}/{len(codes)}] {code} {f}: upserted {n}")
            except Exception as e:
                print(f"[{i}/{len(codes)}] {code} {f}: failed: {e}")

    print(f"done. ok_tasks={total_ok}, codes={len(codes)}, freqs={freqs}, range={start_date}-{end_date}")


if __name__ == "__main__":
    main()


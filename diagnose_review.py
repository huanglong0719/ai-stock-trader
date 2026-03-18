
import asyncio
import sys
import os
from datetime import date, datetime, timedelta
from typing import Any, cast

# No special characters to avoid encoding issues
_stdout = cast(Any, sys.stdout)
if hasattr(_stdout, "reconfigure"):
    _stdout.reconfigure(encoding="utf-8")

# Add backend to path
sys.path.append(os.path.abspath('backend'))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar, MarketSentiment, TradingPlan, Position
from app.models.system_models import SystemJobLog

async def check_status():
    db = SessionLocal()
    today = date.today()
    tomorrow = today + timedelta(days=1)
    
    output = []
    output.append("="*50)
    output.append(f"DIAGNOSTIC REPORT - {datetime.now()}")
    output.append(f"Today: {today}, Tomorrow: {tomorrow}")
    output.append("="*50)

    # 1. Check Job Logs
    output.append("\n[1. Recent System Job Logs]")
    recent_jobs = db.query(SystemJobLog).filter(
        SystemJobLog.start_time >= datetime.combine(today, datetime.min.time())
    ).order_by(SystemJobLog.start_time.desc()).all()
    
    if recent_jobs:
        for job in recent_jobs:
            output.append(f"Job: {job.job_name:20} | Status: {job.status:10} | Start: {job.start_time.strftime('%H:%M:%S')} | Msg: {job.message or ''}")
    else:
        output.append("No job logs found for today.")

    # 2. Check Daily Bars
    output.append("\n[2. Daily Bar Check]")
    bar_count = db.query(DailyBar).filter(DailyBar.trade_date == today).count()
    output.append(f"Today's bar count: {bar_count}")
    
    # 3. Check Sentiment
    output.append("\n[3. Market Sentiment Check]")
    sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == today).first()
    if sentiment:
        output.append(f"Found sentiment for today!")
        output.append(f"Temperature: {sentiment.market_temperature}")
        summary_len = len(sentiment.summary) if sentiment.summary else 0
        output.append(f"Summary Length: {summary_len} characters")
        output.append("-" * 20)
        output.append("FULL SUMMARY:")
        output.append(sentiment.summary or "")
        output.append("-" * 20)
    else:
        output.append(f"No sentiment record found for {today}")

    # 4. Check Trading Plans for tomorrow
    output.append("\n[4. Trading Plans for tomorrow]")
    tomorrow_plans = db.query(TradingPlan).filter(TradingPlan.date == tomorrow).all()
    output.append(f"Plans for tomorrow ({tomorrow}): {len(tomorrow_plans)}")
    for p in tomorrow_plans:
        output.append(f"  - {p.ts_code}: {p.strategy_name} (Reason: {p.reason[:100]}...)")

    # 5. Check Today's Intraday Plans
    output.append("\n[5. Today's Intraday Plans]")
    today_plans = db.query(TradingPlan).filter(TradingPlan.date == today).all()
    output.append(f"Plans for today ({today}): {len(today_plans)}")
    for p in today_plans:
        output.append(f"  - {p.ts_code}: {p.strategy_name}")

    # 6. Check Positions
    output.append("\n[6. Active Positions Check]")
    positions = db.query(Position).filter(Position.vol > 0).all()
    output.append(f"Active positions: {len(positions)}")
    for pos in positions:
        output.append(f"  - {pos.ts_code}: {pos.vol} shares, price {pos.avg_price}")

    db.close()
    
    # Final write to file
    with open("diag_output.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    
    print(f"Diagnostic complete. Output written to diag_output.txt")

if __name__ == "__main__":
    asyncio.run(check_status())

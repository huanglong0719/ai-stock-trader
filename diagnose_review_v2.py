import os
import sys
from datetime import datetime, date
import logging

# Set UTF-8 encoding for stdout
if sys.platform == 'win32':
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Adjust path to include 'backend'
current_dir = os.getcwd()
backend_dir = os.path.join(current_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment, TradingPlan
from app.models.system_models import SystemJobLog
from sqlalchemy import desc

def diagnose():
    db = SessionLocal()
    output = []
    try:
        output.append(f"--- Diagnostic Report ({datetime.now()}) ---")
        
        # 1. Check Job Logs for 'daily_sync'
        output.append("\n[1. Recent Daily Sync Job Logs]")
        logs = db.query(SystemJobLog).filter(SystemJobLog.job_name == 'daily_sync').order_by(desc(SystemJobLog.start_time)).limit(5).all()
        if not logs:
            output.append("No logs found for 'daily_sync'.")
        for log in logs:
            output.append(f"ID: {log.id} | Start: {log.start_time} | End: {log.end_time} | Status: {log.status} | Msg: {log.message}")

        # 2. Check MarketSentiment
        output.append("\n[2. Recent Market Sentiment]")
        sentiments = db.query(MarketSentiment).order_by(desc(MarketSentiment.date)).limit(3).all()
        for s in sentiments:
                    # 使用 updated_at 或根据实际模型字段调整
                    updated_time = getattr(s, 'updated_at', 'N/A')
                    output.append(f"Date: {s.date} | Updated: {updated_time} | Temp: {s.market_temperature} | Summary: {s.summary[:100]}...")

        # 3. Check TradingPlans for today/tomorrow
        output.append("\n[3. Recent Trading Plans]")
        plans = db.query(TradingPlan).order_by(desc(TradingPlan.date)).limit(5).all()
        for p in plans:
            output.append(f"Date: {p.date} | Code: {p.ts_code} | Strategy: {p.strategy_name} | Created: {p.created_at}")

        # 4. Check Scheduler Status (if possible)
        # This is harder to check from a script, but we can check if the process is running.
        
        with open("diag_output_v2.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(output))
        
        print("Diagnostic report written to diag_output_v2.txt")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    diagnose()

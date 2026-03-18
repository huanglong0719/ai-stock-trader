
import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Dict

# Add backend directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'backend')))

from app.db.session import SessionLocal
from app.models.system_models import SystemJobLog

async def check_db_logs():
    db = SessionLocal()
    try:
        today_start = datetime(2026, 2, 3, 9, 0, 0)
        today_end = datetime(2026, 2, 3, 15, 0, 0)
        
        print(f"Checking SystemJobLog for {today_start} to {today_end}...")
        
        logs = db.query(SystemJobLog).filter(
            SystemJobLog.start_time >= today_start,
            SystemJobLog.start_time <= today_end
        ).order_by(SystemJobLog.start_time.asc()).all()
        
        if not logs:
            print("No jobs found in DB for this period.")
        else:
            print(f"Found {len(logs)} jobs.")
            job_counts: Dict[str, int] = {}
            for log in logs:
                job_counts[log.job_name] = job_counts.get(log.job_name, 0) + 1
                # Print details for key jobs
                if log.job_name in ["trade_monitor", "intraday_scan", "position_settlement"]:
                    print(f"[{log.start_time}] {log.job_name}: {log.status} ({log.message})")
            
            print("\nJob Counts:")
            for name, count in job_counts.items():
                print(f"{name}: {count}")
                
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_db_logs())

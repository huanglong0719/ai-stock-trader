import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.system_models import SystemJobLog, SystemHeartbeat

def check_db():
    print(f"Checking DB at {datetime.now()}")
    db = SessionLocal()
    try:
        # Check Heartbeats
        print("\n=== Heartbeats ===")
        hbs = db.query(SystemHeartbeat).all()
        for hb in hbs:
            print(f"Component: {hb.component}, Last Beat: {hb.last_beat}, Status: {hb.status}")

        # Check Latest Job Logs
        print("\n=== Latest 10 Job Logs ===")
        logs = db.query(SystemJobLog).order_by(SystemJobLog.id.desc()).limit(10).all()
        for log in logs:
            print(f"ID: {log.id}, Job: {log.job_name}, Start: {log.start_time}, Status: {log.status}")

    finally:
        db.close()

if __name__ == "__main__":
    check_db()

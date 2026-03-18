import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan
from datetime import date

def fix_300757():
    db = SessionLocal()
    try:
        ts_code = '300757.SZ'
        today = date.today()
        
        plan = db.query(TradingPlan).filter(
            TradingPlan.ts_code == ts_code,
            TradingPlan.date == today
        ).first()
        
        if plan:
            print(f"Found Plan: {plan.id}, Executed: {plan.executed}")
            if plan.executed:
                print("Resetting plan status to Not Executed...")
                plan.executed = False
                plan.review_content = (plan.review_content or "") + " [System Reset: Position remains, resuming monitor]"
                db.commit()
                print("Plan updated successfully.")
            else:
                print("Plan is already Not Executed.")
        else:
            print("No plan found.")
            
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    fix_300757()

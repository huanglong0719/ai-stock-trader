import sys
import os
from datetime import date
from sqlalchemy import text, bindparam

# Add the backend directory to sys.path
sys.path.append(os.getcwd())

from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan

def cleanup_tracking_plans():
    db = SessionLocal()
    try:
        print("\n=== Cleaning up Tracking Plans based on AI Analysis ===")
        
        # 1. Cancel finished/executed plans that are still marked as TRACKING
        # User said: "已执行/持仓类股票（剔除跟踪，已完成操作）"
        print("Cancelling executed/finished plans...")
        db.execute(text("""
            UPDATE trading_plans 
            SET track_status = 'FINISHED', review_content = '已执行/持仓，自动结束监控'
            WHERE executed = 1 AND track_status = 'TRACKING'
        """))
        
        # 2. Cancel specific stocks identified by AI as "剔除跟踪"
        # List from user input:
        # 000547.SZ, 600089.SH, 000035.SZ, 002842.SZ, 300139.SZ, 300157.SZ, 300191.SZ, 
        # 000988.SZ, 002151.SZ, 601872.SZ, 300846.SZ, 300476.SZ, 300394.SZ, 300017.SZ, 
        # 002131.SZ, 600111.SH, 002463.SZ, 002938.SZ, 002009.SZ, 301575.SZ, 300724.SZ, 
        # 600714.SH, 603667.SH, 600498.SH, 600549.SH, 301396.SZ, 002155.SZ, 300308.SZ, 
        # 300502.SZ, 600410.SH, 603993.SH, 000792.SZ, 000338.SZ, 601899.SH, 300418.SZ, 
        # 002202.SZ, 002195.SZ, 002506.SZ, 600589.SH, 300465.SZ
        
        # Explicitly keep: 300316.SZ
        
        stocks_to_remove = [
            "000547.SZ", "600089.SH", "000035.SZ", "002842.SZ", "300139.SZ", "300157.SZ", "300191.SZ", 
            "000988.SZ", "002151.SZ", "601872.SZ", "300846.SZ", "300476.SZ", "300394.SZ", "300017.SZ", 
            "002131.SZ", "600111.SH", "002463.SZ", "002938.SZ", "002009.SZ", "301575.SZ", "300724.SZ", 
            "600714.SH", "603667.SH", "600498.SH", "600549.SH", "301396.SZ", "002155.SZ", "300308.SZ", 
            "300502.SZ", "600410.SH", "603993.SH", "000792.SZ", "000338.SZ", "601899.SH", "300418.SZ", 
            "002202.SZ", "002195.SZ", "002506.SZ", "600589.SH", "300465.SZ"
        ]
        
        print(f"Removing {len(stocks_to_remove)} stocks from tracking...")
        
        # Use executemany or IN clause
        if stocks_to_remove:
            stmt = text("""
                UPDATE trading_plans
                SET track_status = 'CANCELLED', review_content = 'AI复核剔除：结构不符或数据缺失'
                WHERE ts_code IN :codes AND executed = 0 AND track_status = 'TRACKING'
            """).bindparams(bindparam("codes", expanding=True))
            db.execute(stmt, {"codes": stocks_to_remove})

        db.commit()
        print("Cleanup completed.")
        
        # Verify
        remaining = db.query(TradingPlan).filter(
            TradingPlan.executed == False,
            TradingPlan.track_status == 'TRACKING',
            TradingPlan.date == date.today()
        ).all()
        
        print(f"\nRemaining Tracking Plans ({len(remaining)}):")
        for p in remaining:
            print(f"- {p.ts_code} {p.strategy_name}")

    except Exception as e:
        print(f"Error cleaning up plans: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    cleanup_tracking_plans()


from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, TradeRecord, Position
from datetime import date, datetime

def check_hualin():
    db = SessionLocal()
    today = date.today()
    ts_code = "002945.SZ"
    
    print(f"--- Checking {ts_code} for {today} ---")
    
    plans = db.query(TradingPlan).filter(
        TradingPlan.ts_code == ts_code,
        TradingPlan.date == today
    ).all()
    
    print(f"Plans found: {len(plans)}")
    for p in plans:
        print(f"Plan ID: {p.id}, Time: {p.created_at}, Executed: {p.executed}, Strategy: {p.strategy_name}")
        
    records = db.query(TradeRecord).filter(
        TradeRecord.ts_code == ts_code,
        TradeRecord.trade_time >= datetime.combine(today, datetime.min.time())
    ).all()
    
    print(f"\nRecords found: {len(records)}")
    for r in records:
        print(f"Record ID: {r.id}, Time: {r.trade_time}, Type: {r.trade_type}, Plan ID: {r.plan_id}")
        
    pos = db.query(Position).filter(Position.ts_code == ts_code).first()
    if pos:
        print(f"\nPosition: Vol={pos.vol}, Available={pos.available_vol}, AvgPrice={pos.avg_price}")
    else:
        print("\nNo position found.")
        
    db.close()

if __name__ == "__main__":
    check_hualin()

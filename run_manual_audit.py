import asyncio
from app.services.audit_service import audit_service
from app.db.session import SessionLocal
import json

async def run_audit():
    print("Starting manual audit...")
    report = await audit_service.run_daily_audit()
    
    db = SessionLocal()
    from app.models.stock_models import AuditDetail, TradeRecord, Account
    
    # --- New Cash Check ---
    print("\nVerifying Cash Balance against Trade History...")
    trades = db.query(TradeRecord).order_by(TradeRecord.trade_time.asc()).all()
    account = db.query(Account).first()
    
    initial_cash = 1000000.0
    current_cash = initial_cash
    for t in trades:
        if t.trade_type == 'BUY':
            current_cash -= (t.amount + (t.fee or 0))
        elif t.trade_type == 'SELL':
            current_cash += (t.amount - (t.fee or 0))
    
    print(f"Initial Cash: {initial_cash}")
    print(f"Calculated Cash from Trades: {current_cash:.2f}")
    print(f"Account Record Cash: {account.available_cash:.2f}")
    print(f"Discrepancy: {account.available_cash - current_cash:.2f}")
    
    details = db.query(AuditDetail).filter(AuditDetail.report_id == report.id).all()
    
    print(f"\nAudit Report Status: {report.status}")
    print(f"Summary: {report.summary}")
    
    if details:
        print("\nFound Issues:")
        for d in details:
            print(f"- [{d.ts_code}] {d.diff_type}: {d.description}")
            print(f"  Expected: {d.expected_value}, Actual: {d.actual_value}")
    else:
        print("\nNo issues found by AuditService.")
        
    db.close()

if __name__ == "__main__":
    asyncio.run(run_audit())

"""
检查数据库表数据
"""
from app.db.session import SessionLocal
from app.models.stock_models import Stock, DailyBar, StockIndicator
from sqlalchemy import func

db = SessionLocal()
try:
    print("=" * 60)
    print("检查数据库表")
    print("=" * 60)
    
    # 1. Stock 表
    stock_count = db.query(func.count(Stock.ts_code)).scalar()
    print(f"\n1. Stock 表: {stock_count} 条记录")
    if stock_count > 0:
        samples = db.query(Stock).limit(3).all()
        for s in samples:
            print(f"   {s.ts_code}: {s.name}")
    
    # 2. DailyBar 表
    bar_count = db.query(func.count(DailyBar.ts_code)).scalar()
    print(f"\n2. DailyBar 表: {bar_count} 条记录")
    if bar_count > 0:
        from sqlalchemy import desc
        latest_date = db.query(DailyBar.trade_date).order_by(desc(DailyBar.trade_date)).first()
        print(f"   最新日期: {latest_date[0] if latest_date else 'N/A'}")
    
    # 3. StockIndicator 表
    ind_count = db.query(func.count(StockIndicator.ts_code)).scalar()
    print(f"\n3. StockIndicator 表: {ind_count} 条记录")
    
    print("\n" + "=" * 60)
    
    if stock_count == 0:
        print("⚠️ Stock 表为空！需要先同步股票基本信息")
        print("解决方案: 调用 /api/sync/stocks 接口")
    
finally:
    db.close()

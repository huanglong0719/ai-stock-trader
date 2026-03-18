from app.models.stock_models import DailyBasic
from app.db.session import SessionLocal
from datetime import datetime

db = SessionLocal()

# 检查 DailyBasic 的数据
count_27 = db.query(DailyBasic).filter(DailyBasic.trade_date == datetime(2026, 1, 27).date()).count()
count_28 = db.query(DailyBasic).filter(DailyBasic.trade_date == datetime(2026, 1, 28).date()).count()

print(f'DailyBasic 在 2026-01-27 的记录数: {count_27}')
print(f'DailyBasic 在 2026-01-28 的记录数: {count_28}')

# 检查最新的几个交易日
latest_dates = db.query(DailyBasic.trade_date).distinct().order_by(DailyBasic.trade_date.desc()).limit(5).all()
print(f'\n最新的5个交易日:')
for (date,) in latest_dates:
    count = db.query(DailyBasic).filter(DailyBasic.trade_date == date).count()
    print(f'  {date}: {count} 条记录')

db.close()

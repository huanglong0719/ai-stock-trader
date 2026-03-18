from app.models.stock_models import DailyBar, DailyBasic
from app.db.session import SessionLocal

db = SessionLocal()

# 检查 DailyBar
latest_bar = db.query(DailyBar).order_by(DailyBar.trade_date.desc()).first()
print(f'DailyBar 最新交易日: {latest_bar.trade_date if latest_bar else "无数据"}')

# 检查 DailyBasic
latest_basic = db.query(DailyBasic).order_by(DailyBasic.trade_date.desc()).first()
print(f'DailyBasic 最新交易日: {latest_basic.trade_date if latest_basic else "无数据"}')

db.close()

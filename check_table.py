from app.db.session import engine
from sqlalchemy import text
with engine.connect() as conn:
    res = conn.execute(text('PRAGMA table_info(market_close_counts)'))
    for row in res:
        print(row)

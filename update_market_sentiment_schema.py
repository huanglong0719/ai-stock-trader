
from app.db.session import engine, SessionLocal
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_schema():
    columns_to_add = [
        ("ladder_json", "TEXT"),
        ("turnover_top_json", "TEXT"),
        ("ladder_opportunities_json", "TEXT")
    ]
    
    with engine.connect() as conn:
        for col_name, col_type in columns_to_add:
            try:
                # Check if column exists
                # SQLite doesn't support 'IF NOT EXISTS' in ALTER TABLE directly for columns in older versions
                # but we can try and catch the error or query table info
                conn.execute(text(f"ALTER TABLE market_sentiments ADD COLUMN {col_name} {col_type}"))
                logger.info(f"Added column {col_name} to market_sentiments")
            except Exception as e:
                if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                    logger.info(f"Column {col_name} already exists, skipping")
                else:
                    logger.error(f"Error adding column {col_name}: {e}")
        
        conn.commit()

if __name__ == "__main__":
    update_schema()

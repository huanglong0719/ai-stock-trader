import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from app.core.config import settings
import tushare as ts
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_adj_factors(ts_code):
    logger.info(f"Fixing adj_factors for {ts_code}...")
    pro = ts.pro_api(settings.TUSHARE_TOKEN)
    db = SessionLocal()
    
    try:
        # 1. Get all adj_factors from Tushare
        df = pro.adj_factor(ts_code=ts_code)
        if df is None or df.empty:
            logger.warning(f"No adj_factor data found for {ts_code}")
            return

        # Convert to dict for faster lookup: date_str -> factor
        adj_map = {row['trade_date']: float(row['adj_factor']) for _, row in df.iterrows()}
        
        # 2. Get all daily bars from DB
        bars = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).all()
        if not bars:
            logger.warning(f"No daily bars found in DB for {ts_code}")
            return
            
        updated_count = 0
        for bar in bars:
            trade_date = bar.trade_date or datetime.now().date()
            d_str = trade_date.strftime('%Y%m%d')
            if d_str in adj_map:
                new_factor = adj_map[d_str]
                # Check if update is needed
                # Use a small epsilon for float comparison
                current_factor = float(bar.adj_factor or 0.0)
                if abs(current_factor - new_factor) > 1e-6:
                    setattr(bar, "adj_factor", new_factor)
                    updated_count += 1
        
        if updated_count > 0:
            db.commit()
            logger.info(f"Updated {updated_count} records for {ts_code}")
        else:
            logger.info(f"No records needed update for {ts_code}")
            
    except Exception as e:
        logger.error(f"Error fixing {ts_code}: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    # Fix all stocks
    logger.info("Starting full adj_factor fix for ALL stocks...")
    db = SessionLocal()
    try:
        stocks = db.query(DailyBar.ts_code).distinct().all()
        ts_codes = [s[0] for s in stocks]
        logger.info(f"Found {len(ts_codes)} stocks to check.")
        
        for i, ts_code in enumerate(ts_codes):
            if i % 100 == 0:
                logger.info(f"Processed {i}/{len(ts_codes)} stocks...")
            fix_adj_factors(ts_code)
            
        logger.info("Full fix complete.")
    except Exception as e:
        logger.error(f"Global error: {e}")
    finally:
        db.close()

import sys
import os
import logging
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.data_sync import data_sync_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sync_recent_basics(days=30):
    """Sync DailyBasic for the last N days"""
    logger.info(f"Starting DailyBasic sync for last {days} days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    current = start_date
    while current <= end_date:
        date_str = current.strftime('%Y%m%d')
        # Check if it's a weekday (simple check, API will handle holidays)
        if current.weekday() < 5: 
            logger.info(f"Syncing basics for {date_str}...")
            try:
                data_sync_service.sync_daily_basic(date_str)
            except Exception as e:
                logger.error(f"Error syncing {date_str}: {e}")
            # Avoid hitting API limits too hard
            time.sleep(0.5)
        current += timedelta(days=1)
        
    logger.info("DailyBasic sync complete.")

if __name__ == "__main__":
    # Default to 30 days, can be changed
    sync_recent_basics(30)

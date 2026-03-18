import sys
import os
import io
import logging
from datetime import datetime

# Force UTF-8 encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.trading_service import trading_service

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_trade_check():
    """Manually trigger trade check"""
    logger.info(f"Starting manual trade check at {datetime.now()}...")
    
    # 1. Check if there are any plans for today
    plans = await trading_service.get_todays_plans()
    pending = [p for p in plans if not p.executed]
    
    logger.info(f"Found {len(plans)} plans for today, {len(pending)} pending.")
    
    if not pending:
        logger.info("No pending plans to check.")
    else:
        # 2. Execute check (this will call AI and might execute trades)
        try:
            await trading_service.check_and_execute_plans()
            logger.info("Trade check finished.")
        except Exception as e:
            logger.error(f"Trade check failed: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_trade_check())

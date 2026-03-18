import sys
import os
from datetime import datetime, timedelta

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.data_provider import data_provider
from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar

def check_minute_data(ts_code):
    print(f"\nChecking minute data for {ts_code}...")
    
    # 1. Trigger realtime quote fetch (which should save minute data)
    print("Fetching realtime quote...")
    import asyncio
    quote = asyncio.run(data_provider.get_realtime_quote(ts_code))
    if not quote:
        print("Failed to get quote!")
        return

    print(f"Quote Time: {quote['time']}")
    print(f"Quote Price: {quote['price']}")

    # 2. Check MinuteBar in DB
    db = SessionLocal()
    try:
        # Calculate expected bar time (recent)
        # Assuming the quote time is 'YYYY-MM-DD HH:MM:SS'
        quote_dt = datetime.strptime(quote['time'], "%Y-%m-%d %H:%M:%S")
        
        # Check 1min bar
        print("\nQuerying 1min bar in DB...")
        # Since _save_minute_data adjusts time to the nearest minute/bar end, 
        # we check for bars within the last hour to be safe, filtering by updated_at or trade_time
        
        # Let's look for the specific bar corresponding to the quote time
        # The logic in _save_minute_data for 1min is:
        # remainder = dt.minute % 1 => 0
        # if remainder == 0 and dt.second == 0: bar_dt = dt
        # else: bar_dt = dt + 1min (seconds=0)
        
        # We'll just search for the latest minute bar for this stock
        last_bar = db.query(MinuteBar).filter(
            MinuteBar.ts_code == ts_code,
            MinuteBar.freq == '1min'
        ).order_by(MinuteBar.trade_time.desc()).first()

        if last_bar:
            print(f"Found MinuteBar: {last_bar.trade_time} | Close: {last_bar.close} | Vol: {last_bar.vol}")
            
            # Verify approximate match (time should be close to quote time)
            # Note: quote time might be slightly behind system time if market is closed or data is cached/local
            print(f"Match status: {'SUCCESS' if str(last_bar.close) == str(quote['price']) else 'MISMATCH (Normal if data aggregated)'}")
        else:
            print("No MinuteBar found!")

    except Exception as e:
        print(f"Error checking DB: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # Test with a common stock
    check_minute_data('000001.SH') # Index
    check_minute_data('002353.SZ') # Stock

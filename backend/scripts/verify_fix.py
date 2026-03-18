import sys
import os
import asyncio

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.services.data_provider import data_provider

def verify_snapshot():
    print("Verifying Market Snapshot Fix...")
    try:
        snapshot = data_provider.get_market_snapshot()
        print("Snapshot Result:")
        print(f"  Up Count: {snapshot['up_count']}")
        print(f"  Down Count: {snapshot['down_count']}")
        print(f"  Limit Up: {snapshot['limit_up_count']}")
        print(f"  Limit Down: {snapshot['limit_down_count']}")
        
        if snapshot['up_count'] > 0:
            print("SUCCESS: Real-time data fetched successfully.")
        else:
            print("WARNING: Counts are still 0 (Market closed or API failed?)")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_snapshot()

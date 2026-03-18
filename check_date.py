import asyncio
import os
import sys

# Add the backend directory to sys.path to import from app
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'backend')))

from app.services.data_provider import data_provider

async def check():
    print("Checking last trade date...")
    date = await data_provider.get_last_trade_date()
    print(f"Last trade date: {date}")

if __name__ == "__main__":
    asyncio.run(check())

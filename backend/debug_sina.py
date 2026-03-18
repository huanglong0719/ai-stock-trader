
import asyncio
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.sina_data_service import SinaDataService

async def test_sina():
    service = SinaDataService()
    # Codes from the screenshot
    codes = ["300781.SZ", "000025.SZ", "000026.SZ", "000028.SZ", "000001.SH"]
    
    print(f"Fetching quotes for {codes} from Sina...")
    try:
        quotes = await service.fetch_quotes(codes)
        print(f"Result count: {len(quotes)}")
        for code, q in quotes.items():
            print(f"--- {code} ---")
            print(f"Name: {q.get('name')}")
            print(f"Price: {q.get('price')}")
            print(f"PreClose: {q.get('pre_close')}")
            print(f"PctChg: {q.get('pct_chg')}")
            print(f"Source: {q.get('source')}")
            print(f"Raw Vol: {q.get('vol')}")
            print(f"Amount: {q.get('amount')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_sina())

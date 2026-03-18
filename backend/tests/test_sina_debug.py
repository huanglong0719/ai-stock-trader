
import asyncio
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

async def test_sina():
    from app.services.market.sina_data_service import SinaDataService
    service = SinaDataService()
    
    codes = ["300781.SZ", "000025.SZ", "000001.SZ"]
    print(f"Fetching {codes} from Sina...")
    
    results = await service.fetch_quotes(codes)
    
    print(f"Results: {len(results)}")
    for code, data in results.items():
        print(f"{code}: {data}")

if __name__ == "__main__":
    asyncio.run(test_sina())

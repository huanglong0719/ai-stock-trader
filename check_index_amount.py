import asyncio
from app.services.tdx_data_service import tdx_service

async def check():
    if not tdx_service.connect():
        print("Failed to connect")
        return
    
    # Fetch SH index (000001) and SZ index (399001)
    # SH is market 1, SZ is market 0
    quotes = tdx_service.fetch_realtime_quotes(["000001.SH", "399001.SZ"])
    for q in quotes:
        print(f"Code: {q.get('code')}, Amount: {q.get('amount')}")

if __name__ == "__main__":
    asyncio.run(check())

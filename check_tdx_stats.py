
import asyncio
import sys
import os

# Add the project root to sys.path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.market.market_data_service import MarketDataService

async def test_tdx_stats():
    try:
        service = MarketDataService()
        print("Testing TDX 880005 stats...", flush=True)
        
        from app.services.tdx_data_service import tdx_service
        if tdx_service.connect():
            with tdx_service._api_lock:
                q = tdx_service.api.get_security_quotes([(1, "880005")])
                if q:
                    print(f"Raw 880005 Data: {q[0]}")
        
        # Use a timeout for the thread execution
        loop = asyncio.get_running_loop()
        stats = await asyncio.wait_for(
            loop.run_in_executor(None, service._fetch_tdx_880_counts_sync),
            timeout=15.0
        )
        print(f"Parsed Stats: {stats}", flush=True)
        if stats:
            if len(stats) == 6:
                up, down, limit_up, limit_down, flat, amount = stats
            else:
                up, down, limit_up, limit_down, flat = stats
                amount = 0.0
            total = up + down + flat
            print(f"Total: {total}", flush=True)
            print(f"Plausible: {service._is_counts_plausible(stats[:5])}", flush=True)
            print(f"Amount: {amount}亿", flush=True)
        else:
            print("No stats returned from TDX.", flush=True)
    except asyncio.TimeoutError:
        print("Timeout while fetching TDX stats.", flush=True)
    except Exception as e:
        print(f"Error occurred: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_tdx_stats())

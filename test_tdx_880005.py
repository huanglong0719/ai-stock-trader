
import asyncio
from app.services.tdx_data_service import tdx_service
from app.utils import get_logger

logger = get_logger(__name__)

async def test_880005():
    print("Connecting to TDX...")
    if not tdx_service.connect():
        print("Failed to connect to TDX")
        return
    print("Connected successfully.")

    try:
        print("Fetching quotes for 880005...")
        with tdx_service._api_lock:
            q = tdx_service.api.get_security_quotes([(1, "880005"), (1, "000001"), (0, "399001")]) or []
        
        # 将结果写入文件，防止终端显示问题
        with open("tdx_output.txt", "w", encoding="utf-8") as f:
            f.write(f"Received {len(q)} quotes.\n")
            if q:
                for r in q:
                    f.write(f"\nQuote for {r.get('code')}:\n")
                    for k, v in r.items():
                        f.write(f"  {k}: {v}\n")
        print("Results saved to tdx_output.txt")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("Starting test...")
    try:
        asyncio.run(test_880005())
    except Exception as e:
        print(f"Main error: {e}")
    print("Test finished.")


import sys
import os
import time

# Add the project root to sys.path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.tdx_data_service import tdx_service

def test_simple_tdx():
    print("Testing TDX connection...", flush=True)
    start = time.time()
    if tdx_service.connect():
        print(f"Connected in {time.time() - start:.2f}s", flush=True)
        try:
            with tdx_service._api_lock:
                print("Fetching 880005 quote...", flush=True)
                q = tdx_service.api.get_security_quotes([(1, "880005")])
                print(f"Quote: {q}", flush=True)
        except Exception as e:
            print(f"Error fetching quote: {e}", flush=True)
    else:
        print("Failed to connect to TDX", flush=True)

if __name__ == "__main__":
    test_simple_tdx()

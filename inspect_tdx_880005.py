
import os
import sys
import asyncio
import json

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.tdx_data_service import tdx_service

async def inspect_880005():
    try:
        if tdx_service.connect():
            api = tdx_service.api
            quotes = api.get_security_quotes([(1, '880005')])
            print(f"Raw 880005 quotes: {json.dumps(quotes, indent=2, ensure_ascii=False)}")
            
            # Also check 880001 (Market overview?)
            quotes_1 = api.get_security_quotes([(1, '880001')])
            print(f"Raw 880001 quotes: {json.dumps(quotes_1, indent=2, ensure_ascii=False)}")
            
            # Check 000001 (SH Index)
            quotes_sh = api.get_security_quotes([(1, '000001')])
            print(f"Raw 000001.SH quotes: {json.dumps(quotes_sh, indent=2, ensure_ascii=False)}")
        else:
            print("Failed to connect to TDX")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("Starting inspection...")
    asyncio.run(inspect_880005())
    print("Done.")

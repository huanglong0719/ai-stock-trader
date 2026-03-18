import asyncio
import os
import sys
import httpx
import math
from typing import Dict, Union

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

async def test_bse_fs():
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    # m:0+t:81+s:2048 is BSE
    base_params: Dict[str, Union[str, int]] = {
        "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:81+s:2048", 
        "fields": "f12,f14" 
    }
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=dict(base_params, pn=1, pz=100))
        data = r.json()
        total = data.get('data', {}).get('total', 0)
        print(f"BSE Total (m:0+t:81+s:2048): {total}")
        
        # Also test SH/SZ combined
        sh_sz_fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
        r = await client.get(url, params=dict(base_params, fs=sh_sz_fs, pn=1, pz=100))
        total_sh_sz = r.json().get('data', {}).get('total', 0)
        print(f"SH/SZ Total: {total_sh_sz}")
        
        print(f"Grand Total (SH/SZ + BSE): {total_sh_sz + total}")

if __name__ == "__main__":
    asyncio.run(test_bse_fs())

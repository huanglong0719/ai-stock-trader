import asyncio
import os
import sys
import httpx
import math
from typing import Dict, Union

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

async def analyze_sh_sz():
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    base_params: Dict[str, Union[str, int]] = {
        "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", 
        "fields": "f12,f14" 
    }
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=dict(base_params, pn=1, pz=100))
        data = r.json()
        total = data.get('data', {}).get('total', 0)
        all_data = data.get('data', {}).get('diff', [])
        
        pages = math.ceil(total / 100)
        tasks = [client.get(url, params=dict(base_params, pn=p, pz=100)) for p in range(2, pages + 1)]
        responses = await asyncio.gather(*tasks)
        for resp in responses:
            all_data.extend(resp.json().get('data', {}).get('diff', []))
            
    prefix_counts: Dict[str, int] = {}
    for item in all_data:
        code = item.get('f12', '')
        if not code: continue
        prefix = code[:3] # Use 3 digits
        prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
        
    print(f"--- SH/SZ Analysis (Total: {len(all_data)}) ---")
    for p, count in sorted(prefix_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"Prefix {p}: {count}")

if __name__ == "__main__":
    asyncio.run(analyze_sh_sz())

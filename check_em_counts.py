import asyncio
import httpx
import math
from typing import Dict, Union

async def check_counts():
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    base_params: Dict[str, Union[str, int]] = {
        "pn": 1, "pz": 1, "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", 
        "fields": "f3,f12,f14" 
    }
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=base_params)
        data = r.json()
        total = data.get('data', {}).get('total', 0)
        print(f"Total stocks according to Eastmoney: {total}")
        
        page_size = 100
        total_pages = math.ceil(total / page_size)
        
        all_pcts = []
        for page in range(1, total_pages + 1):
            p = base_params.copy()
            p['pn'] = page
            p['pz'] = page_size
            resp = await client.get(url, params=p)
            d = resp.json()
            items = d.get('data', {}).get('diff', [])
            print(f"Page {page}: received {len(items)} items")
            for item in items:
                val = item.get('f3')
                if val is not None and val != "-":
                    all_pcts.append(float(val))
        
        up = sum(1 for p in all_pcts if p > 0)
        down = sum(1 for p in all_pcts if p < 0)
        flat = sum(1 for p in all_pcts if p == 0)
        
        print(f"Stats: Total fetched: {len(all_pcts)}")
        print(f"Up: {up}")
        print(f"Down: {down}")
        print(f"Flat: {flat}")
        
        if len(all_pcts) > 0:
             print(f"First 10 pcts: {all_pcts[:10]}")

if __name__ == "__main__":
    asyncio.run(check_counts())

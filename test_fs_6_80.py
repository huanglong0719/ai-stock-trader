import asyncio
import httpx
from typing import Dict, Union

async def test_fs_6_80():
    for fs in ["m:0+t:6", "m:0+t:80"]:
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params: Dict[str, Union[str, int]] = {
            "po": 1, "np": 1, "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f14", "pn": 1, "pz": 10
        }
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params=params)
            data = r.json()
            total = data.get('data', {}).get('total', 0)
            print(f"Total items in {fs}: {total}", flush=True)
            # diffs = data.get('data', {}).get('diff', [])
            # for item in diffs[:3]:
            #     print(f"  {item.get('f12')}: {item.get('f14')}")

if __name__ == "__main__":
    asyncio.run(test_fs_6_80())

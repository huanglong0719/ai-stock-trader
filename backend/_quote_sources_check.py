import os
import sys
import json
import asyncio
from typing import Any, Dict, List

sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.market.market_data_service import market_data_service


async def _run(codes: List[str]) -> Dict[str, Any]:
    tdx_ok = True
    sina_ok = True
    tdx_data: Dict[str, Dict] = {}
    sina_data: Dict[str, Dict] = {}
    tdx_error = ""
    sina_error = ""
    try:
        tdx_data = await market_data_service._fetch_tdx_quotes(codes, timeout=3.0)
        if not tdx_data:
            tdx_ok = False
    except Exception as e:
        tdx_ok = False
        tdx_error = str(e)

    try:
        sina_data = await market_data_service._fetch_sina_quotes(codes)
        if not sina_data:
            sina_ok = False
    except Exception as e:
        sina_ok = False
        sina_error = str(e)

    return {
        "tdx": {
            "ok": tdx_ok,
            "count": len(tdx_data),
            "error": tdx_error,
        },
        "sina": {
            "ok": sina_ok,
            "count": len(sina_data),
            "error": sina_error,
        },
        "sample": {
            "tdx": {k: v.get("price", 0) for k, v in list(tdx_data.items())[:3]},
            "sina": {k: v.get("price", 0) for k, v in list(sina_data.items())[:3]},
        }
    }


def main():
    codes_env = os.getenv("QUOTE_CODES")
    if codes_env:
        codes = [c.strip() for c in codes_env.split(",") if c.strip()]
    else:
        codes = ["000001.SH", "399001.SZ", "600000.SH"]
    result = asyncio.run(_run(codes))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    path = os.path.join(os.getcwd(), "backend", "_quote_sources_check_last.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

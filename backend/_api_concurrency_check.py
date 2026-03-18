import os
import sys
import json
import time
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

sys.path.append(os.path.join(os.getcwd(), "backend"))


@dataclass
class Stat:
    name: str
    count: int = 0
    ok: int = 0
    fail: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0

    def add(self, ok: bool, ms: float) -> None:
        self.count += 1
        self.ok += 1 if ok else 0
        self.fail += 1 if not ok else 0
        self.total_ms += ms
        if ms > self.max_ms:
            self.max_ms = ms

    def summary(self) -> Dict[str, Any]:
        avg = (self.total_ms / self.count) if self.count else 0.0
        return {
            "count": self.count,
            "ok": self.ok,
            "fail": self.fail,
            "avg_ms": round(avg, 2),
            "max_ms": round(self.max_ms, 2),
        }


async def _fetch_symbol(client: httpx.AsyncClient) -> str:
    try:
        resp = await client.get("/api/market/stocks")
        if resp.status_code != 200:
            return "000001.SH"
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    code = item.get("ts_code") or item.get("code") or ""
                    if isinstance(code, str) and (code.endswith(".SH") or code.endswith(".SZ")):
                        return code
    except Exception:
        return "000001.SH"
    return "000001.SH"


async def _request(client: httpx.AsyncClient, method: str, url: str, name: str, payload: Optional[dict] = None) -> tuple[str, bool, float]:
    start = time.perf_counter()
    try:
        if method == "GET":
            resp = await client.get(url)
        else:
            resp = await client.post(url, json=payload)
        ok = resp.status_code == 200
    except Exception:
        ok = False
    ms = (time.perf_counter() - start) * 1000
    return name, ok, ms


async def run_concurrency(base_url: Optional[str], concurrency: int, rounds: int, timeout: float) -> Dict[str, Any]:
    stats: Dict[str, Stat] = {}
    if base_url:
        client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
    else:
        from app.main import app
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://testserver", timeout=timeout)

    async with client:
        symbol = await _fetch_symbol(client)
        tasks: List[asyncio.Task] = []
        sem = asyncio.Semaphore(concurrency)

        async def schedule(method: str, url: str, name: str, payload: Optional[dict] = None):
            async with sem:
                return await _request(client, method, url, name, payload)

        for _ in range(rounds):
            tasks.extend([
                asyncio.create_task(schedule("GET", "/", "root")),
                asyncio.create_task(schedule("GET", "/api/market/overview", "market_overview")),
                asyncio.create_task(schedule("GET", "/api/market/quote/" + symbol, "quote")),
                asyncio.create_task(schedule("POST", "/api/market/quotes", "quotes", [symbol])),
                asyncio.create_task(schedule("GET", f"/api/market/kline/{symbol}?freq=D&limit=120", "kline_D")),
                asyncio.create_task(schedule("GET", f"/api/market/kline/{symbol}?freq=W&limit=60", "kline_W")),
                asyncio.create_task(schedule("GET", f"/api/market/kline/{symbol}?freq=M&limit=36", "kline_M")),
                asyncio.create_task(schedule("GET", "/api/trading/account", "trading_account")),
                asyncio.create_task(schedule("GET", "/api/trading/positions", "trading_positions")),
            ])

        for coro in asyncio.as_completed(tasks):
            name, ok, ms = await coro
            stat = stats.get(name)
            if not stat:
                stat = Stat(name=name)
                stats[name] = stat
            stat.add(ok, ms)

    return {k: v.summary() for k, v in stats.items()}


def main():
    base_url = os.getenv("API_BASE_URL")
    concurrency = int(os.getenv("API_CONCURRENCY", "8"))
    rounds = int(os.getenv("API_ROUNDS", "6"))
    timeout = float(os.getenv("API_TIMEOUT", "12"))

    result = asyncio.run(run_concurrency(base_url, concurrency, rounds, timeout))
    output = {"ok": True, "results": result}
    print(json.dumps(output, ensure_ascii=False, indent=2))
    path = os.path.join(os.getcwd(), "backend", "_api_concurrency_check_last.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

import httpx
import asyncio
import re
from typing import List, Dict, Optional, Any
from app.services.logger import logger

class SinaDataService:
    def __init__(self):
        self.headers = {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.timeout = httpx.Timeout(connect=2.0, read=3.5, write=3.5, pool=2.0)

    def _normalize_code(self, code: str) -> str:
        """
        Convert standard ts_code (e.g. 000001.SZ) to Sina format (e.g. sz000001)
        """
        if not code:
            return ""
        
        # Handle BJ stocks (Sina uses bj prefix)
        if code.endswith(".BJ"):
             return f"bj{code.split('.')[0]}"
             
        if code.endswith(".SH"):
            return f"sh{code.split('.')[0]}"
        if code.endswith(".SZ"):
            return f"sz{code.split('.')[0]}"
            
        # Fallback for raw codes
        if code.startswith("6"):
            return f"sh{code}"
        if code.startswith(("0", "3")):
            return f"sz{code}"
        if code.startswith(("4", "8")):
            return f"bj{code}"
            
        return code

    async def fetch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch real-time quotes from Sina
        """
        if not codes:
            return {}

        sina_codes = [self._normalize_code(c) for c in codes]
        # Sina supports ~800 codes per request, but let's limit to 100 for safety
        chunk_size = 100
        results = {}

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for i in range(0, len(sina_codes), chunk_size):
                chunk = sina_codes[i:i + chunk_size]
                query = ",".join(chunk)
                url = f"http://hq.sinajs.cn/list={query}"
                
                try:
                    for attempt in range(2):
                        try:
                            resp = await client.get(url, headers=self.headers)
                            if resp.status_code == 200:
                                text = resp.text
                                parsed = self._parse_sina_response(text, codes[i:i + chunk_size])
                                if parsed:
                                    results.update(parsed)
                                    break
                        except Exception as e:
                            if attempt == 0:
                                await asyncio.sleep(0.15)
                            else:
                                raise e
                except Exception as e:
                    logger.warning(f"Sina quote fetch failed for chunk {i}: {e}")
                    continue

        return results

    def _parse_sina_response(self, text: str, original_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        results = {}
        lines = text.split('\n')
        
        # Create a map from sina code to original code
        # Since response order matches request order usually, but let's be safe
        # Sina response format: var hq_str_sz000001="Payh...";
        
        for line in lines:
            if not line.strip():
                continue
                
            match = re.search(r'var hq_str_([a-z]{2}\d{6})="(.*)";', line)
            if not match:
                continue
                
            sina_code = match.group(1)
            content = match.group(2)
            
            # Find original code
            ts_code = None
            # Reverse lookup logic (simplified)
            market = sina_code[:2].upper()
            code = sina_code[2:]
            if market == "SH": ts_code = f"{code}.SH"
            elif market == "SZ": ts_code = f"{code}.SZ"
            elif market == "BJ": ts_code = f"{code}.BJ"
            
            if not ts_code: 
                continue

            parts = content.split(',')
            if len(parts) < 30:
                continue
                
            try:
                def _to_float(v):
                    try:
                        return float(v)
                    except Exception:
                        return 0.0

                # Sina format:
                # 0: name, 1: open, 2: pre_close, 3: price, 4: high, 5: low, 6: bid, 7: ask, 8: vol, 9: amount
                # 30: date, 31: time
                
                price = _to_float(parts[3])
                pre_close = _to_float(parts[2])
                pct_chg = 0.0
                if pre_close > 0:
                    pct_chg = round((price / pre_close - 1) * 100, 2)

                bid_ask = {
                    "b1_p": _to_float(parts[11]),
                    "b1_v": _to_float(parts[10]) / 100,
                    "b2_p": _to_float(parts[13]),
                    "b2_v": _to_float(parts[12]) / 100,
                    "b3_p": _to_float(parts[15]),
                    "b3_v": _to_float(parts[14]) / 100,
                    "b4_p": _to_float(parts[17]),
                    "b4_v": _to_float(parts[16]) / 100,
                    "b5_p": _to_float(parts[19]),
                    "b5_v": _to_float(parts[18]) / 100,
                    "s1_p": _to_float(parts[21]),
                    "s1_v": _to_float(parts[20]) / 100,
                    "s2_p": _to_float(parts[23]),
                    "s2_v": _to_float(parts[22]) / 100,
                    "s3_p": _to_float(parts[25]),
                    "s3_v": _to_float(parts[24]) / 100,
                    "s4_p": _to_float(parts[27]),
                    "s4_v": _to_float(parts[26]) / 100,
                    "s5_p": _to_float(parts[29]),
                    "s5_v": _to_float(parts[28]) / 100,
                }

                quote = {
                    "ts_code": ts_code,
                    "name": parts[0],
                    "open": _to_float(parts[1]),
                    "pre_close": pre_close,
                    "price": price,
                    "high": _to_float(parts[4]),
                    "low": _to_float(parts[5]),
                    "vol": float(parts[8]) / 100, # Sina vol is in shares, we usually use hand (100 shares) or match TDX
                    "amount": _to_float(parts[9]),
                    "pct_chg": pct_chg,
                    "date": parts[30],
                    "time": parts[31],
                    "source": "sina",
                    "bid_ask": bid_ask,
                }
                
                # TDX uses 'vol' as hands (usually), let's verify standard
                # Our system standard: vol in hands? 
                # Checking market_data_service._calculate_turnover_rate:
                # quote['vol'] * 100 / (float_share * 10000) -> vol is hands?
                # Sina returns shares. So / 100 is correct for hands.
                
                results[ts_code] = quote
            except Exception:
                continue
                
        return results

sina_data_service = SinaDataService()

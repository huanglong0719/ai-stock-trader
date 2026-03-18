import sys
import os
import asyncio
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.chat_service import chat_service
from app.services.market.market_data_service import market_data_service


def _fmt_price(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "N/A"


async def verify(symbol: str, message: str, call_ai: bool):
    data = await chat_service.process_user_message(message, dry_run=True)
    context_str = data.get("context", "")

    quote = await market_data_service.get_realtime_quote(symbol) or {}
    expected_rt = (
        f"开盘 {_fmt_price(quote.get('open'))}, "
        f"最高 {_fmt_price(quote.get('high'))}, "
        f"最低 {_fmt_price(quote.get('low'))}, "
        f"昨收 {_fmt_price(quote.get('pre_close'))}, "
        f"现价 {_fmt_price(quote.get('price'))}"
    )

    rt_line = ""
    for line in context_str.splitlines():
        if symbol in line and line.strip().startswith("●"):
            rt_line = line.strip()
            break

    kline_counts = {}
    for key in ["日线", "周线", "30分钟", "5分钟"]:
        m = re.search(rf"{key} K线数据 \\(最近\\s*(\\d+)\\s*根\\)", context_str)
        if m:
            kline_counts[key] = int(m.group(1))
        else:
            kline_counts[key] = 0

    def _extract_quote_numbers(line: str):
        fields = {}
        for k in ["开盘", "最高", "最低", "昨收", "现价"]:
            m = re.search(rf"{k} ([0-9.]+)", line)
            if m:
                fields[k] = float(m.group(1))
        return fields

    expected_nums = _extract_quote_numbers(expected_rt.replace(",", ""))
    context_nums = _extract_quote_numbers(rt_line)
    quote_match = "PASS"
    for k in expected_nums:
        ev = expected_nums.get(k)
        cv = context_nums.get(k)
        if cv is None or abs(ev - cv) > 0.02:
            quote_match = "FAIL"
            break

    summary_path = os.path.join(os.path.dirname(__file__), "verify_ai_trader_chat_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Symbol={symbol}\n")
        f.write(f"Realtime_Expected={expected_rt}\n")
        f.write(f"Realtime_Context={rt_line}\n")
        f.write(f"Realtime_Match={quote_match}\n")
        for k, v in kline_counts.items():
            f.write(f"{k}_Context_Count={v}\n")

    print(f"Realtime Expected: {expected_rt}")
    print(f"Realtime Context:  {rt_line}")
    print(f"Realtime Match:    {quote_match}")
    print("Kline Context Counts:", kline_counts)
    print(f"Summary saved: {summary_path}")

    context_path = os.path.join(os.path.dirname(__file__), "verify_ai_trader_chat_context.txt")
    with open(context_path, "w", encoding="utf-8") as f:
        f.write(context_str)

    if call_ai:
        response = await chat_service.process_user_message(message)
        resp_path = os.path.join(os.path.dirname(__file__), "verify_ai_trader_chat_response.txt")
        with open(resp_path, "w", encoding="utf-8") as f:
            f.write(str(response))
        print(f"AI Response saved: {resp_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="600871.SH")
    parser.add_argument("--message", default="请核对600871.SH的实时行情与多周期K线")
    parser.add_argument("--call-ai", action="store_true")
    args = parser.parse_args()
    asyncio.run(verify(args.symbol, args.message, args.call_ai))

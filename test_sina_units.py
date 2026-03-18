
import asyncio
import sys
import os

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.market.tushare_client import tushare_client

def test_sina_units():
    codes = ['000001.SH', '399001.SZ', '600519.SH']
    print(f"Testing Sina units for: {codes}")
    quotes = tushare_client.get_realtime_quotes(codes)
    for code, q in quotes.items():
        print(f"Code: {code}")
        print(f"  Name: {q['name']}")
        print(f"  Price: {q['price']}")
        print(f"  Amount (processed): {q['amount']}")
        # Raw amount from Sina would be fields[9]
        # In tushare_client.py, amount = float(fields[9]) / 1000
        print(f"  Raw Amount (estimated): {q['amount'] * 1000}")

if __name__ == "__main__":
    test_sina_units()

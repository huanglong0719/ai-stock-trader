
import requests
import json

url = "http://localhost:8000/api/market/kline/600686.SH?freq=D&limit=5"
response = requests.get(url)
data = response.json()

if data:
    last_item = data[-1]
    print(f"Time: {last_item.get('time')}")
    print(f"MACD: {last_item.get('macd')}")
    print(f"MACD DEA: {last_item.get('macd_dea')}")
    print(f"MACD DIFF: {last_item.get('macd_diff')}")
    print(f"Full item keys: {list(last_item.keys())}")
else:
    print("No data received")

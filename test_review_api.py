
import requests
import json
from datetime import date

def test_latest_review():
    url = "http://127.0.0.1:8000/api/trading/review/latest"
    try:
        response = requests.get(url)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print("Latest Review Data:")
            print(f"Date: {data.get('date')}")
            print(f"Summary: {data.get('summary')[:100]}...")
            print(f"Target Plan (Singular): {data.get('target_plan')}")
            print(f"Target Plans (List): {len(data.get('target_plans', []))} plans found")
            for i, p in enumerate(data.get('target_plans', [])):
                print(f"  Plan {i+1}: {p.get('ts_code')} - {p.get('strategy')}")
            print(f"Holding Plans: {len(data.get('holding_plans', []))} plans found")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_latest_review()

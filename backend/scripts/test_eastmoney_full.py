import requests
import json

def check_eastmoney_data():
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 20, "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", 
        "fields": "f3,f14" # f3: pct_chg, f14: name
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        items = data['data']['diff']
        print("Sample Data:")
        for item in items[:5]:
            print(f"{item['f14']}: {item['f3']}%")
            
        # Try fetching ALL (pz=6000)
        print("\nFetching ALL stocks...")
        params['pz'] = 6000
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        all_items = data['data']['diff']
        print(f"Total items fetched: {len(all_items)}")
        
        up = sum(1 for x in all_items if x['f3'] > 0)
        down = sum(1 for x in all_items if x['f3'] < 0)
        limit_up = sum(1 for x in all_items if x['f3'] >= 9.8) # Rough estimate
        
        print(f"Calculated: Up {up}, Down {down}, Limit Up (approx >9.8%) {limit_up}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_eastmoney_data()

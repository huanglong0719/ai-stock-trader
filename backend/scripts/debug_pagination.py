import requests

def debug_eastmoney_pagination():
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    base_params = {
        "pn": 1, "pz": 500, "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", 
        "fields": "f3"
    }
    
    print("Requesting pz=500...")
    r = requests.get(url, params=base_params)
    data = r.json()
    items = data.get('data', {}).get('diff', [])
    total = data.get('data', {}).get('total', 0)
    print(f"Total reported: {total}")
    print(f"Items returned: {len(items)}")
    
    if len(items) < 500:
        print(f"WARNING: Server capped page size at {len(items)}!")
    
    # Check page 2
    base_params['pn'] = 2
    r = requests.get(url, params=base_params)
    items2 = r.json().get('data', {}).get('diff', [])
    print(f"Page 2 items returned: {len(items2)}")

if __name__ == "__main__":
    debug_eastmoney_pagination()

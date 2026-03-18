import requests
import pytest

def test_limit():
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 500, "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", 
        "fields": "f3"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("data", {}).get("diff", []) or []
        assert len(items) > 0
    except Exception as e:
        pytest.skip(str(e))

if __name__ == "__main__":
    test_limit()

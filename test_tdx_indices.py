from pytdx.hq import TdxHq_API
import socket

def test_880005():
    api = TdxHq_API()
    # Using the one that worked
    ip = '180.153.18.170'
    port = 7709
    
    print(f"Connecting to {ip}:{port}...")
    try:
        socket.setdefaulttimeout(2)
        if api.connect(ip, port):
            print("Connected!")
            # Get indices with all fields
            indices = api.get_security_quotes([(1, '000001'), (0, '399001'), (0, '399006')])
            for idx in indices:
                print(f"Index {idx.get('code')} raw: {idx}")
                price = float(idx.get('price', 0))
                pre_close = float(idx.get('last_close', 0))
                if pre_close > 0:
                    pct = (price / pre_close - 1) * 100
                    print(f"  Calculated Pct: {pct:.2f}%")
            api.disconnect()
        else:
            print("Connect failed")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_880005()

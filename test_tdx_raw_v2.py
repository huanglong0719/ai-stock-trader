from pytdx.hq import TdxHq_API
import socket

def test_880005():
    api = TdxHq_API()
    ips = [
        ('119.147.212.81', 7709),
        ('123.125.108.23', 7709),
        ('180.153.18.170', 7709),
        ('114.80.149.19', 7709),
        ('115.238.56.198', 7709),
    ]
    
    for ip, port in ips:
        print(f"Connecting to {ip}:{port}...")
        try:
            socket.setdefaulttimeout(2)
            if api.connect(ip, port):
                print("Connected!")
                # Get 880005 quote
                quotes = api.get_security_quotes([(1, "880005")])
                if quotes:
                    print(f"880005 Data: {quotes[0]}")
                    # Also get indices to compare
                    indices = api.get_security_quotes([(1, '000001'), (0, '399001'), (0, '399006')])
                    for idx in indices:
                        print(f"Index {idx.get('code')}: {idx.get('name')} Price={idx.get('price')} Pct={idx.get('reversed_bytes0')}")
                api.disconnect()
                return
            else:
                print("Connect failed")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    test_880005()

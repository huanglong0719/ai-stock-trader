
from pytdx.hq import TdxHq_API
import json

def test_880005():
    api = TdxHq_API()
    # 常用服务器
    ips = [
        ('119.147.212.81', 7709),
        ('123.125.108.23', 7709),
        ('180.153.18.171', 7709)
    ]
    
    for ip, port in ips:
        print(f"\nConnecting to {ip}:{port}...")
        if api.connect(ip, port):
            try:
                # 880005 是全市场统计指数
                # Market 1 (SH), Code '880005'
                quotes = api.get_security_quotes([(1, '880005')])
                if quotes:
                    r = quotes[0]
                    print(f"Raw data for 880005 from {ip}:")
                    # 打印所有非零字段，帮助识别
                    for k, v in r.items():
                        if v != 0 and v is not None:
                            print(f"  {k}: {v}")
                    
                    # 打印一些可能是 0 的关键字段
                    for k in ['bid_vol5', 'ask_vol5', 'bid_vol1', 'ask_vol1']:
                        if r.get(k) == 0:
                            print(f"  {k}: 0 (Checked)")
                else:
                    print("Empty response for 880005")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                api.disconnect()
        else:
            print("Connect failed")

if __name__ == "__main__":
    test_880005()

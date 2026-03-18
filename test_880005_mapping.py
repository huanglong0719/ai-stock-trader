
import asyncio
from pytdx.hq import TdxHq_API
from app.services.logger import logger

async def test_880005():
    api = TdxHq_API()
    # 尝试几个常用的服务器
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
                    for k, v in r.items():
                        print(f"  {k}: {v}")
                    
                    # 验证猜测的映射
                    # price: 涨, last_close: 跌, open: 平, high: 总, bid_vol5: 涨停, ask_vol5: 跌停
                    up = r.get('price')
                    down = r.get('last_close')
                    flat = r.get('open')
                    total = r.get('high')
                    limit_up = r.get('bid_vol5')
                    limit_down = r.get('ask_vol5')
                    amount = r.get('amount')
                    
                    print(f"\nInterpreted stats:")
                    print(f"  Up: {up}")
                    print(f"  Down: {down}")
                    print(f"  Flat: {flat}")
                    print(f"  Total: {total}")
                    print(f"  Limit Up: {limit_up}")
                    print(f"  Limit Down: {limit_down}")
                    print(f"  Amount: {amount/100000000:.2f}亿" if amount else "Amount: N/A")
                    
                    if total and up and down:
                        print(f"  Verification: Up + Down + Flat = {up + down + flat}, Total = {total}")
                else:
                    print("Empty response for 880005")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                api.disconnect()
        else:
            print("Connect failed")

if __name__ == "__main__":
    asyncio.run(test_880005())

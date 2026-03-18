
import os
import sys

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.market.tushare_client import tushare_client

def test():
    try:
        q = tushare_client.get_realtime_quotes(['000001.SH'])
        if q and '000001.SH' in q:
            amt = q['000001.SH'].get('amount')
            print(f'Tushare index amount: {amt}')
        else:
            print('No result')
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    test()

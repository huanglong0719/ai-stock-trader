import asyncio
from app.services.market.market_data_service import MarketDataService
import pandas as pd

async def test():
    m_service = MarketDataService()
    ts_code = '301282.SZ'
    print(f"Fetching K-line for {ts_code}...")
    kline = await m_service.get_kline(ts_code, limit=100, include_indicators=True)
    
    df = pd.DataFrame(kline)
    target = df[df['time'] == '2025-04-17']
    if not target.empty:
        row = target.iloc[0]
        print(f"Date: {row['time']}")
        print(f"Close: {row['close']}")
        print(f"MA5: {row['ma5']}")
        print(f"Adj Factor (DailyBar): {row.get('adj_factor')}")
        print(f"Ind Adj Factor (Stored): {row.get('ind_adj_factor')}")
        
        # Calculate what MA5 should be from the surrounding prices in this same response
        idx = target.index[0]
        if idx >= 4:
            subset = df.iloc[idx-4:idx+1]
            calc_ma5 = subset['close'].mean()
            print(f"Calculated MA5 from prices in response: {calc_ma5}")
            print(f"Difference: {row['ma5'] - calc_ma5}")

if __name__ == "__main__":
    asyncio.run(test())


import pandas as pd
import numpy as np

# Simulate daily data
dates = pd.date_range(start='2026-01-01', end='2026-01-12', freq='B')
df_d = pd.DataFrame({
    'trade_date_dt': dates,
    'close': np.random.randn(len(dates)) + 10
})

# Simulate weekly data (aggregated from daily)
df_w = df_d.set_index('trade_date_dt').resample('W-FRI').agg({
    'close': 'last'
}).reset_index()
df_w['weekly_indicator'] = df_w['close'] * 2

print("Daily Data (df_d):")
print(df_d.tail(3))
print("\nWeekly Data (df_w):")
print(df_w)

# Merge
df_final = pd.merge_asof(df_d.sort_values('trade_date_dt'), 
                         df_w.sort_values('trade_date_dt'), 
                         on='trade_date_dt', 
                         direction='backward')

print("\nMerged Result (df_final):")
print(df_final.tail(3))

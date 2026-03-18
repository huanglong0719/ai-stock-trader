import requests
import json

response = requests.post('http://127.0.0.1:8000/api/analysis/kline', json={'symbol': '000792.SZ'}, timeout=120)
result = response.json()
print('Score:', result.get('score'))
report = result.get('full_report', '')
lines = report.split('\n') if report else []
print('First 3 lines:')
for i, line in enumerate(lines[:3]):
    print(f'{i+1}: {line}')

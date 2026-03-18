"""
触发股票基本信息同步
"""
import requests

BASE_URL = "http://localhost:8000"

print("=" * 60)
print("触发股票基本信息同步")
print("=" * 60)

try:
    response = requests.post(f"{BASE_URL}/api/sync/stocks", timeout=60)
    print(f"\n状态码: {response.status_code}")
    print(f"响应: {response.json()}")
    
    if response.status_code == 200:
        print("\n✅ 同步任务已启动，请等待后台任务完成...")
        print("可以通过查看后端日志监控进度")
    else:
        print(f"\n❌ 同步失败")
except Exception as e:
    print(f"\n❌ 请求异常: {e}")

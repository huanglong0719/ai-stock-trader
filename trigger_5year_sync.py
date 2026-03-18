import requests
import json
import time

def trigger_sync():
    url = "http://127.0.0.1:8000/api/sync/backfill"
    # 5年 = 365 * 5 = 1825 天
    payload = {
        "days": 1825,
        "date": None
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print(f"正在请求后端 API: {url}")
        print(f"请求参数: 更新最近 {payload['days']} 天的数据")
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 200:
            print("成功触发数据更新任务！")
            print("后端返回:", response.json())
            print("数据将在后台自动下载和存入数据库，请耐心等待。")
            print("你可以通过查看后端控制台日志来监控进度。")
        else:
            print(f"请求失败，状态码: {response.status_code}")
            print("错误信息:", response.text)
            
    except Exception as e:
        print(f"发生错误: {e}")
        print("请确保后端服务已启动 (http://127.0.0.1:8000)")

if __name__ == "__main__":
    trigger_sync()

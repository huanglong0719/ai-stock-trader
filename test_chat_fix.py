"""
测试聊天历史接口修复
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_chat_history():
    """测试聊天历史接口"""
    print("测试 GET /api/chat/history...")
    try:
        response = requests.get(f"{BASE_URL}/api/chat/history", timeout=5)
        print(f"状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ 成功获取聊天历史，共 {len(data)} 条消息")
            if data:
                print(f"最新消息: {data[-1]}")
            return True
        else:
            print(f"❌ 请求失败: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

def test_chat_send():
    """测试发送消息接口"""
    print("\n测试 POST /api/chat/send...")
    try:
        response = requests.post(
            f"{BASE_URL}/api/chat/send",
            json={"content": "你好，请介绍一下你自己"},
            timeout=30
        )
        print(f"状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ AI 回复: {data.get('content', '')[:100]}...")
            return True
        else:
            print(f"❌ 请求失败: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("聊天接口测试")
    print("=" * 60)
    
    # 测试历史记录接口
    history_ok = test_chat_history()
    
    # 测试发送消息接口
    send_ok = test_chat_send()
    
    # 再次测试历史记录，验证新消息是否保存
    if send_ok:
        print("\n再次测试历史记录接口...")
        test_chat_history()
    
    print("\n" + "=" * 60)
    if history_ok and send_ok:
        print("✅ 所有测试通过")
    else:
        print("⚠️ 部分测试失败")
    print("=" * 60)

import requests
import sys

BASE_URL = "http://127.0.0.1:8000/api"

def test_account_chat():
    print("Testing AI Account Awareness...")
    
    # 1. Ask about holdings
    question = "我的持仓情况如何？"
    print(f"User: {question}")
    
    try:
        response = requests.post(f"{BASE_URL}/chat/send", json={"content": question})
        if response.status_code == 200:
            data = response.json()
            answer = data.get("content", "")
            print(f"AI: {answer}")
            
            # Check keywords
            if "持仓" in answer or "空仓" in answer or "资产" in answer:
                print("\nSUCCESS: AI response contains account context.")
            else:
                print("\nWARNING: AI response might not be using account context.")
        else:
            print(f"FAILURE: API returned {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_account_chat()

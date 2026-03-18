import httpx
import time

def test_deepseek_connectivity():
    url = "https://api.deepseek.com/chat/completions"
    print(f"正在测试连接到: {url} ...")
    
    start_time = time.time()
    try:
        # 仅发送一个极简的 HEAD 或 GET 请求测试基础连接
        # 注意：DeepSeek 接口通常只接受 POST，所以这里我们预期得到 405 或 401 都是“连接成功”的标志
        with httpx.Client(timeout=10.0) as client:
            response = client.get("https://api.deepseek.com/")
            print(f"连接成功! 状态码: {response.status_code}")
    except Exception as e:
        print(f"连接失败: {type(e).__name__}: {str(e)}")
    
    print(f"耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    test_deepseek_connectivity()

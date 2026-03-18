import sys
import os
import asyncio

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.services.ai_service import ai_service

async def consult_ai_needs():
    print("正在咨询 AI 交易员的需求...")
    
    prompt = """
    # Role: 激进型 AI 基金经理 (AI Fund Manager)
    
    # Goal
    你的终极目标是：**以最快的速度把资金账户做到收益最大化**。
    我们要不仅仅是稳健，更要追求资金的极致利用效率和超额收益。
    
    # Context
    我是系统的架构师。目前的系统已经为你提供了：
    1. 基础行情 (价格、成交量)
    2. 账户持仓与资金数据
    3. 简单的盘中市场快照 (涨跌家数、情绪定性)
    
    # Question
    为了让你能更好地达成“收益最大化”的目标，**你需要系统再为你提供哪些具体的数据、信息或系统能力？**
    
    请按重要性列出清单 (Top 5)，并简要说明每一项数据如何帮助你捕捉暴利机会或规避大跌。
    
    例如：
    - 是否需要实时 Level-2 资金流？
    - 是否需要更快的龙虎榜数据？
    - 是否需要盘中突发新闻推送？
    - 是否需要对打板、翘板等特定模式的即时扫描能力？
    
    请用专业的交易员视角回答。
    """
    
    try:
        # 使用 MiMo 或 DeepSeek
        client = ai_service.mimo_client if ai_service.mimo_client else ai_service.ds_client
        model = "deepseek-chat" # Default
        if ai_service.mimo_client:
            from app.core.config import settings
            model = settings.MIMO_MODEL
            
        response = await ai_service._call_ai_api(
            client, 
            model, 
            prompt, 
            system_prompt="你是一个渴望高收益的顶级游资操盘手。"
        )
        
        print("\n" + "="*20 + " AI 交易员的回复 " + "="*20)
        print(response)
        print("="*60)
        
    except Exception as e:
        print(f"Error consulting AI: {e}")

if __name__ == "__main__":
    asyncio.run(consult_ai_needs())

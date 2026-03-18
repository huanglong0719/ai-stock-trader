import sys
import os
import asyncio

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.services.ai_service import ai_service
from app.services.chat_service import ChatService

async def discuss_implementation_with_ai():
    print("正在准备与 AI 交易员的务实沟通...")
    
    chat_service = ChatService()
    
    # 1. 获取最真实的账户上下文
    # 注意：这里直接调用 chat_service 的内部方法获取实时数据
    try:
        real_account_context = await chat_service._get_account_context()
    except Exception as e:
        real_account_context = "（获取账户信息失败，假设为初始资金 100万，空仓）"
        print(f"Warning: Could not fetch real context: {e}")

    # 2. 构建“现实主义”的 Prompt
    prompt = f"""
    # Role: 系统架构师 (System Architect)
    # User: AI 交易员 (You)
    
    # Dialogue
    你好，AI。我看到了你之前列出的 Top 5 需求（Level-2、毫秒级数据等）。
    但作为架构师，我必须把你拉回现实。**我们目前的资源有限，必须基于实际情况进行优化。**
    
    【我们的客观现实 (Constraints)】
    1. **数据源**：目前主要依赖 Tushare 和公开 API，数据延迟在 3-5秒左右，**没有 Level-2**，也没有毫秒级逐笔成交。
    2. **资金规模**：中小资金，船小好调头。
    3. **系统能力**：Python 后端，计算能力尚可，但无法处理海量高频数据。
    
    【我们当前的实际账户状况 (Real Context)】
    {real_account_context}
    
    # Your Task (Critical Analysis)
    请看着上面【当前的持仓】和【盈亏】，结合我们的【系统限制】：
    
    1. **诊断当前持仓风险**：基于现有数据能力（K线、成交量、基本面），你觉得我们目前持仓最大的隐患是什么？
    2. **提出一个立即能做的改进**：不要再说 Level-2 了。在现有条件下，为了保护上述持仓或捕捉机会，你最希望我**马上**为你开发的一个**低成本、高性价比**的功能是什么？
       (例如：增加一个针对当前持仓股的“5分钟急跌预警”？还是增加一个“同板块个股联动监控”？)
    
    请具体到逻辑层面，告诉我怎么做才能帮你把现在的账户管好。
    """
    
    print("-" * 50)
    print("发送给 AI 的真实情境：")
    print(real_account_context)
    print("-" * 50)
    
    try:
        # 优先使用 DeepSeek (逻辑推理能力更强)
        client = ai_service.mimo_client if ai_service.mimo_client else ai_service.ds_client
        model = "deepseek-chat"
        if ai_service.mimo_client:
             from app.core.config import settings
             model = settings.MIMO_MODEL
            
        print(f"正在等待 AI ({model}) 的分析与反馈...")
        response = await ai_service._call_ai_api(
            client, 
            model, 
            prompt, 
            system_prompt="你是一个务实、精明且专注于解决问题的实盘交易员。"
        )
        
        print("\n" + "="*20 + " AI 交易员的务实反馈 " + "="*20)
        print(response)
        print("="*60)
        
    except Exception as e:
        print(f"Error discussing with AI: {e}")

if __name__ == "__main__":
    asyncio.run(discuss_implementation_with_ai())

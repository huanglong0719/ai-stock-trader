
import sys
import os
import asyncio
import pandas as pd
import logging
import argparse

# 将 backend 目录添加到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.market.market_data_service import market_data_service
from app.services.ai.prompt_builder import prompt_builder

# 配置日志以便观察输出
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_context_data(ts_code: str):
    
    print(f"=== Verifying AI Context Data for {ts_code} ===")
    
    # 1. 模拟获取 AI 上下文数据 (这会触发 market_data_service 的真实逻辑)
    print("Fetching data from market_data_service...")
    context_data = await market_data_service.get_ai_context_data(ts_code)
    
    # 2. 检查原始 DataFrame 的长度 (Service 层获取到的)
    df_d = pd.DataFrame(context_data.get('kline_d') or [])
    df_w = pd.DataFrame(context_data.get('weekly_k') or [])
    df_m = pd.DataFrame(context_data.get('monthly_k') or [])
    df_30m = pd.DataFrame(context_data.get('kline_30m') or [])
    df_5m = pd.DataFrame(context_data.get('kline_5m') or [])
    
    print("\n[Service Layer Fetched Data Count]")
    print(f"Daily (D): {len(df_d) if df_d is not None else 0} (Expected >= 30)")
    print(f"Weekly (W): {len(df_w) if df_w is not None else 0} (Expected >= 20)")
    print(f"Monthly (M): {len(df_m) if df_m is not None else 0} (Expected >= 15)")
    print(f"30min: {len(df_30m) if df_30m is not None else 0} (Expected >= 16)")
    print(f"5min: {len(df_5m) if df_5m is not None else 0} (Expected >= 48)")
    
    # 3. 模拟构建 Prompt (这会触发 prompt_builder 的切片逻辑)
    print("\nBuilding Prompt...")
    prompt = await prompt_builder.generate_analysis_prompt(
        symbol=ts_code,
        df=df_d,
        basic_info=None,
        realtime_quote=context_data.get('quote'),
        df_w=df_w,
        df_m=df_m,
        df_30m=df_30m,
        df_5m=df_5m,
        sector_info=None
    )
    
    # 打印部分 prompt 以便人工检查
    print("\n[Prompt Snapshot]")
    print(prompt[:500] + "..." + prompt[-500:])

    # 4. 验证 Prompt 中的数据行数 (最终注入给 AI 的)
    # 我们通过简单的字符串计数来估算，因为 format_kline_raw_data 会生成 CSV 格式
    
    def count_csv_lines(section_name, prompt_text):
        try:
            # 找到对应板块
            start_idx = prompt_text.find(f"【{section_name}")
            if start_idx == -1: return 0
            
            # 找到下一个板块的开始 (通常是换行后接【)
            next_section_idx = prompt_text.find("\n【", start_idx + 1)
            if next_section_idx == -1: next_section_idx = len(prompt_text)
            
            section_content = prompt_text[start_idx:next_section_idx]
            # 统计行数，减去标题行和表头
            lines = section_content.strip().split('\n')
            # 过滤掉空行
            data_lines = [l for l in lines if ',' in l and not l.startswith('ts_code') and not l.startswith('Dt,')]
            return len(data_lines)
        except Exception:
            return 0

    print("\n[Prompt Injected Data Count]")
    
    # 注意：prompt_builder 中有些是用 _format_kline_raw_data (CSV)，有些是用 _get_historical_context (文字描述)
    # 但我们在最新的修改中，日线、周线、月线、5分钟线都使用了 CSV 格式或包含了 CSV
    # 30分钟线使用了文字描述，可能包含 CSV 也可能不包含，需检查代码
    
    # 日线: _format_kline_raw_data (limit=30)
    count_d = count_csv_lines("日线", prompt)
    print(f"Daily Injected: {count_d} (Target: 30)")
    
    # 周线: _format_kline_raw_data (limit=20)
    count_w = count_csv_lines("周线", prompt)
    print(f"Weekly Injected: {count_w} (Target: 20)")
    
    # 月线: _format_kline_raw_data (limit=15)
    count_m = count_csv_lines("月线", prompt)
    print(f"Monthly Injected: {count_m} (Target: 15)")
    
    # 5分钟线: _format_kline_raw_data (limit=48)
    count_5m = count_csv_lines("5分钟线", prompt)
    print(f"5min Injected: {count_5m} (Target: 48)")
    
    count_30m = count_csv_lines("30分钟线", prompt)
    print(f"30min Injected: {count_30m} (Target: 16)")

    # 验证静态描述文本
    print(f"\n[Static Description Check]")
    if "- 日K线：最近30根K线数据" in prompt:
        print("PASS: Daily description matches (30)")
    else:
        print("FAIL: Daily description mismatch")
        
    if "- 5分钟K线：最近48根K线数据" in prompt:
        print("PASS: 5min description matches (48)")
    else:
        print("FAIL: 5min description mismatch")

    def fmt_price(v):
        try:
            return f"{float(v):.2f}"
        except Exception:
            return "N/A"

    def fmt_pct(v):
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "N/A"

    quote = context_data.get("quote") or {}
    expected_rt = (
        f"开盘:{fmt_price(quote.get('open'))} "
        f"最高:{fmt_price(quote.get('high'))} "
        f"最低:{fmt_price(quote.get('low'))} "
        f"现价:{fmt_price(quote.get('price'))} "
        f"昨收:{fmt_price(quote.get('pre_close'))} "
        f"涨跌幅:{fmt_pct(quote.get('pct_chg'))} "
        f"量比:{fmt_price(quote.get('vol_ratio'))} "
        f"换手:{fmt_pct(quote.get('turnover_rate'))}"
    )

    rt_line = ""
    for line in prompt.splitlines():
        if line.strip().startswith("- 实时行情:"):
            rt_line = line.strip().split(":", 1)[1].strip()
            break

    rt_match = "PASS" if rt_line == expected_rt else "FAIL"
    print(f"\n[Realtime Quote Match]\nExpected: {expected_rt}\nPrompt:   {rt_line}\nResult:   {rt_match}")

    summary_path = os.path.join(os.path.dirname(__file__), "verify_ai_context_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Daily_Fetched={len(df_d)}\n")
        f.write(f"Weekly_Fetched={len(df_w)}\n")
        f.write(f"Monthly_Fetched={len(df_m)}\n")
        f.write(f"Min30_Fetched={len(df_30m)}\n")
        f.write(f"Min5_Fetched={len(df_5m)}\n")
        f.write(f"Daily_Injected={count_d}\n")
        f.write(f"Weekly_Injected={count_w}\n")
        f.write(f"Monthly_Injected={count_m}\n")
        f.write(f"Min30_Injected={count_30m}\n")
        f.write(f"Min5_Injected={count_5m}\n")
        f.write(f"Realtime_Expected={expected_rt}\n")
        f.write(f"Realtime_Prompt={rt_line}\n")
        f.write(f"Realtime_Match={rt_match}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="600714.SH")
    args = parser.parse_args()
    asyncio.run(verify_context_data(args.symbol))

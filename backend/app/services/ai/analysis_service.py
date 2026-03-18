import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any, Union

from app.core.config import settings
from app.services.logger import logger
from app.services.search_service import search_service
from app.services.ai.ai_client import ai_client
from app.services.ai.prompt_builder import prompt_builder
from app.services.indicators.technical_indicators import technical_indicators
from app.db.session import SessionLocal
from app.services.evolution_service import evolution_service
from app.models.stock_models import ReflectionMemory, PolicyConfig, MarketSentiment
from sqlalchemy import desc, or_
from app.services.learning_service import learning_service

class AnalysisService:
    def __init__(self):
        # 市场状态缓存 (30分钟刷新)
        self._market_status_cache: Dict[str, Any] = {
            "timestamp": 0.0,
            "content": "",
            "data": None
        }
        # [新增] 全局分值一致性缓存: ts_code -> (score, timestamp)
        self._score_consistency_cache = {}
        self._realtime_decision_cache = {}
        # [新增] 全局 AI 并发控制信号量 (防止线程池耗尽和 API 限制)
        self.ai_semaphore = asyncio.Semaphore(5)

    def _normalize_auto_reason(self, reason: str) -> str:
        if not reason:
            return reason
        cleaned = reason.replace("仅供参考", "")
        return cleaned.replace("建议", "决定")

    def _get_ai_candidates(self, preferred_provider: Optional[str] = None):
        out = []
        all_potential = []
        if ai_client.mimo_client:
            all_potential.append((ai_client.mimo_client, settings.MIMO_MODEL, "Xiaomi MiMo"))
        if ai_client.ds_client:
            all_potential.append((ai_client.ds_client, "deepseek-chat", "DeepSeek"))
        if getattr(ai_client, "nim_client", None):
            all_potential.append((ai_client.nim_client, settings.NVIDIA_NIM_MODEL, "NVIDIA NIM"))

        if preferred_provider:
            match = next((c for c in all_potential if c[2] == preferred_provider), None)
            if match:
                out.append(match)
                for c in all_potential:
                    if c[2] != preferred_provider:
                        out.append(c)
        
        if not out:
            out = all_potential
        return out

    async def _call_ai_best_effort(self, prompt: str, system_prompt: str | None = None, preferred_provider: Optional[str] = None, api_key: Optional[str] = None):
        candidates = self._get_ai_candidates(preferred_provider=preferred_provider)
        if not candidates:
            raise Exception("No AI client initialized")
        last_err = None
        async with self.ai_semaphore:
            for client, model, label in candidates:
                try:
                    content = await asyncio.to_thread(ai_client.call_ai_api, client, model, prompt, system_prompt=system_prompt, api_key=api_key)
                    if content:
                        return content, label, model
                except Exception as e:
                    last_err = e
                    continue
        if last_err:
            raise last_err
        raise Exception("AI returned empty response")

    def _normalize_action_reason(self, action: str, reason: Optional[str], price: float = 0.0, strategy: str = "") -> str:
        r = (reason or "").strip()
        if r:
            for prefix_token in ["买卖价格", "持仓观望", "观望", "买", "卖"]:
                if r.startswith(prefix_token):
                    return r
        a = (action or "").strip().upper()
        # 兼容一些常见的 AI 拼写错误
        if a in ["BUY", "BUIT", "B"]:
            a = "BUY"
        elif a in ["SELL", "CANCEL", "REDUCE", "S"]:
            a = "SELL"
            
        prefix = "观望"
        if a == "BUY":
            prefix = "买"
        elif a in ["SELL", "CANCEL", "REDUCE"]:
            prefix = "卖"
        else:
            if "持仓" in (strategy or ""):
                prefix = "持仓观望"
            else:
                prefix = "观望"
        price_value: float = float(price or 0.0)
        if prefix in ["买", "卖"] and price_value > 0:
            return f"{prefix}{price_value:.2f} {r}".strip()
        return f"{prefix} {r}".strip() if r else prefix

    def _get_relevant_memories(self, strategy_name: str, market_status: str) -> str:
        """获取相关的反思记忆规则"""
        db = SessionLocal()
        try:
            # 优先匹配特定策略，其次通用
            memories = db.query(ReflectionMemory).filter(
                ReflectionMemory.is_active == True,
                or_(
                    ReflectionMemory.strategy_name == strategy_name,
                    ReflectionMemory.strategy_name == "通用",
                    ReflectionMemory.strategy_name == None
                )
            ).order_by(desc(ReflectionMemory.weight), desc(ReflectionMemory.created_at)).limit(5).all()
            
            if not memories:
                return "【历史反思记忆 (必须遵守的软约束)】\n- 暂无匹配记忆"
            
            rules = ["【历史反思记忆 (必须遵守的软约束)】"]
            for m in memories:
                source_label = ""
                if m.source_event_type == "PATTERN_CASE":
                    source_label = "[失败案例]"
                elif m.source_event_type == "PLAN_TRACKING":
                    source_label = "[踏空/避险]"
                rules.append(f"- {source_label}{m.condition} -> {m.action} (理由: {m.reason})")
            
            return "\n".join(rules)
        except Exception as e:
            logger.error(f"Error fetching memories: {e}")
            return ""
        finally:
            db.close()

    # --- Public Facade Methods (Backward Compatibility) ---
    def calculate_technical_indicators(self, kline_data: List[Dict[str, Any]], cache_key: Optional[str] = None) -> pd.DataFrame:
        """
        计算或返回预计算的技术指标
        """
        if not kline_data:
            return pd.DataFrame()
            
        # 检查是否已有核心预计算指标 (采样检查)
        has_precomputed = False
        if len(kline_data) > 0:
            last_item = kline_data[-1]
            # 如果包含 ma20 和 macd，且不为 None，说明已经有了预计算数据
            if all(k in last_item and last_item[k] is not None for k in ['ma20', 'macd']):
                has_precomputed = True
                
        if has_precomputed:
            df = pd.DataFrame(kline_data)
            # 补全前端可能需要的字段名映射 (如果 technical_indicators._calculate_indicators 有特殊逻辑)
            if 'macd_dea' in df.columns and 'macd_signal' not in df.columns:
                df['macd_signal'] = df['macd_dea']
            return df
            
        return technical_indicators.calculate(kline_data, cache_key)

    async def get_trading_status(self) -> str:
        return await prompt_builder.get_trading_status()

    # --- Core Analysis Methods ---

    async def analyze_stock(self, symbol: str, kline_data: Optional[List[Dict[str, Any]]] = None, basic_info: Optional[Dict[str, Any]] = None, realtime_quote: Optional[Dict[str, Any]] = None, weekly_kline: Optional[List[Dict[str, Any]]] = None, monthly_kline: Optional[List[Dict[str, Any]]] = None, kline_5m: Optional[List[Dict[str, Any]]] = None, kline_30m: Optional[List[Dict[str, Any]]] = None, sector_info: Optional[Dict[str, Any]] = None, sector_task: Any = None, raw_trading_context: Optional[str] = None, prev_score: Optional[int] = None, strategy: Optional[str] = "default", preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        执行分析 (异步并行版) - 增强多周期原始数据支持
        """
        # 兼容性处理：不再提前等待 sector_task，避免因超时导致任务被取消 (CancelledError)
        # if sector_task and not sector_info: ... (Removed)
        # Ensure basic_info is a dict
        if isinstance(basic_info, str):
            # 如果错误地传了字符串（如 context_str），尝试将其作为 raw_trading_context
            if not raw_trading_context:
                raw_trading_context = basic_info
            logger.warning(f"AI 服务: {symbol} 的 basic_info 被传为了字符串，已重置为字典")
            basic_info = {}
            
        if not isinstance(basic_info, dict):
            basic_info = {}
        
        # --- [核心增强] 如果缺少多周期数据或上下文，从统一接口获取 ---
        if not all([kline_data, weekly_kline, monthly_kline, kline_5m, kline_30m, raw_trading_context]):
            try:
                from app.services.market.market_data_service import market_data_service
                from app.services.chat_service import chat_service
                
                cache_scope = "analysis"
                data = await market_data_service.get_ai_context_data(symbol, no_side_effect=False, cache_scope=cache_scope)
                
                # 2. 补充缺失的原始数据
                if not kline_data: kline_data = data.get('kline_d', [])
                if not weekly_kline: weekly_kline = data.get('weekly_k', [])
                if not monthly_kline: monthly_kline = data.get('monthly_k', [])
                if not kline_5m: kline_5m = data.get('kline_5m', [])
                if not kline_30m: kline_30m = data.get('kline_30m', [])
                if not realtime_quote: realtime_quote = data.get('quote')
                
                # 3. 补充基础信息 (如果缺失)
                if not basic_info or not basic_info.get('name'):
                    basic_info = basic_info or {}
                    basic_info['name'] = data.get('name', symbol)
                
                # 4. 补充交易上下文 (格式化的字符串)
                if not raw_trading_context:
                    # [优化] 分析模式下不需要 K 线明细 (由 PromptBuilder 生成 CSV)，避免重复
                    raw_trading_context = await chat_service.get_ai_trading_context(symbol, cache_scope=cache_scope, include_kline=False)
                    
            except Exception as e:
                logger.warning(f"AI 服务: 自动补充多周期数据失败 {symbol}: {e}")

        # --- [新增] 检查分值一致性缓存 ---
        if prev_score is None:
            cached_data = self._score_consistency_cache.get(symbol)
            if cached_data:
                cached_score, cached_ts = cached_data
                # 缓存 4 小时有效 (一个交易时段)
                if time.time() - cached_ts < 4 * 3600:
                    prev_score = cached_score
                    logger.info(f"AI 服务: {symbol} 使用缓存评分 {prev_score}")
        
        # --- Fix: Fetch Industry and Concepts if missing ---
        # 1. Fetch Industry from DB if not present
        if not basic_info.get('industry'):
            try:
                from app.db.session import SessionLocal
                from app.models.stock_models import Stock
                db = SessionLocal()
                def fetch_industry():
                    try:
                        stock_record = db.query(Stock).filter(Stock.ts_code == symbol).first()
                        return stock_record.industry if stock_record else ""
                    finally:
                        db.close()
                basic_info['industry'] = await asyncio.to_thread(fetch_industry)
            except Exception as e:
                logger.error(f"Error fetching industry for {symbol}: {e}")

        # 2. Fetch Concepts if not present
        if not basic_info.get('concepts'):
            try:
                from app.services.data_provider import data_provider
                concepts = await data_provider.get_stock_concepts(symbol)
                basic_info['concepts'] = concepts
            except Exception as e:
                logger.error(f"Error fetching concepts for {symbol}: {e}")
        # ---------------------------------------------------

        name = basic_info.get('name', '')
        
        # 并行执行：指标计算 (CPU) 和 资讯搜索 (IO)
        search_task = asyncio.create_task(search_service.search_stock_info(symbol, name))
        
        # 指标计算任务 (如果提供了原始 K 线)
        df = pd.DataFrame()
        if kline_data:
            # 优先检查是否已经包含预计算指标
            has_precomputed = all(k in kline_data[-1] and kline_data[-1][k] is not None for k in ['ma5', 'ma20', 'macd_diff'])
            if has_precomputed:
                df = pd.DataFrame(kline_data)
            else:
                df = await asyncio.to_thread(self.calculate_technical_indicators, kline_data)

        # [增强] 使用分笔成交计算当日最高/最低价时刻乖离率，覆盖日线 bias5_high/bias5_low
        if not df.empty:
            try:
                from app.services.market.market_utils import is_trading_time
                from app.services.tdx_data_service import tdx_service

                should_calc = is_trading_time()
                if not should_calc:
                    last_time = df.iloc[-1].get('trade_date') or df.iloc[-1].get('time')
                    last_date_str = str(last_time)[:10].replace('-', '')
                    today_str = datetime.now().strftime('%Y%m%d')
                    should_calc = last_date_str == today_str

                if should_calc:
                    bias_info = await asyncio.to_thread(tdx_service.compute_intraday_bias_at_extremes, symbol, 5)
                    if bias_info:
                        if bias_info.get("bias_high") is not None:
                            df.at[df.index[-1], "bias5_high"] = float(bias_info["bias_high"])
                        if bias_info.get("bias_low") is not None:
                            df.at[df.index[-1], "bias5_low"] = float(bias_info["bias_low"])
            except Exception as e:
                logger.warning(f"Intraday bias calc failed for {symbol}: {e}")
            
        # 如果既没有 K 线数据，也没有结构化的交易上下文，则报错
        if df.empty and not raw_trading_context:
            return {"error": "Not enough data (Both kline and context are missing)"}

        # 周线和月线同理
        if weekly_kline and len(weekly_kline) > 0:
            if all(k in weekly_kline[-1] and weekly_kline[-1][k] is not None for k in ['ma20']):
                df_w = pd.DataFrame(weekly_kline)
            else:
                df_w = await asyncio.to_thread(self.calculate_technical_indicators, weekly_kline)
        else:
            df_w = None

        if monthly_kline and len(monthly_kline) > 0:
            if all(k in monthly_kline[-1] and monthly_kline[-1][k] is not None for k in ['ma20']):
                df_m = pd.DataFrame(monthly_kline)
            else:
                df_m = await asyncio.to_thread(self.calculate_technical_indicators, monthly_kline)
        else:
            df_m = None

        # [新增] 5分钟和30分钟线处理
        df_5m = pd.DataFrame(kline_5m) if kline_5m and len(kline_5m) > 0 else None
        df_30m = pd.DataFrame(kline_30m) if kline_30m and len(kline_30m) > 0 else None

        # 等待所有任务完成
        tasks_map = {'search': search_task}
        if sector_task:
            tasks_map['sector'] = sector_task

        search_info = ""
        sector_result = None

        try:
            # 60秒超时 (从 30s 增加到 60s 以应对板块分析较慢的情况)
            done, pending = await asyncio.wait(tasks_map.values(), timeout=60.0)
            
            if search_task in done:
                try:
                    search_info = search_task.result()
                except Exception as e:
                    logger.warning(f"AI 服务: 搜索任务失败: {e}")
                    search_info = "搜索服务异常。"
            else:
                logger.warning(f"AI 服务: {symbol} 搜索超时")
                search_info = "搜索超时。"
                
            if sector_task and sector_task in done:
                try:
                    sector_result = sector_task.result()
                except Exception as e:
                    logger.warning(f"AI 服务: 板块分析任务失败: {e}")
            elif sector_task:
                logger.warning(f"AI 服务: {symbol} 板块分析超时")

        except Exception as e:
            logger.error(f"AI 服务: 任务等待失败: {e}")
            search_info = "服务响应异常。"

        # 如果 sector_task 返回了有效结果，使用它
        effective_sector_info = sector_result if (sector_result and not sector_result.get('error')) else sector_info

        # 生成 Prompt (注入 raw_trading_context)
        prompt = await prompt_builder.generate_analysis_prompt(
            symbol, df, basic_info, search_info, realtime_quote, df_w, df_m, df_30m, df_5m, effective_sector_info,
            raw_trading_context=raw_trading_context,
            prev_score=prev_score,
            strategy=strategy or "default"
        )
        
        try:
            content, used_model, _model = await self._call_ai_best_effort(prompt, preferred_provider=preferred_provider, api_key=api_key)
        except Exception as e:
            logger.warning(f"AI model all failed: {e}")
            content = ""
            used_model = "Unknown"
        
        if not content:
            return {
                "symbol": symbol,
                "analysis": "AI 分析暂时不可用，请稍后重试。",
                "timestamp": datetime.now().isoformat(),
                "data_source": "System",
                "source": "System"
            }

        # 清理并解析 JSON
        try:
            import re
            
            def repair_json(s):
                """简单的 JSON 修复逻辑，处理截断、未闭合的引号以及非法控制字符"""
                s = s.strip()
                
                # [新增] 移除 <think>...</think> 块，这常见于 DeepSeek-R1 等推理模型
                s = re.sub(r'<think>.*?</think>', '', s, flags=re.DOTALL)
                
                # 移除 markdown 代码块标记
                s = re.sub(r'```json\s*|\s*```', '', s).strip()
                
                # 关键修复：移除 JSON 字符串中非法的控制字符 (0-31)，这些字符会导致 json.loads 失败
                # 使用正则替换掉除了 \n, \r, \t 以外的控制字符
                s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', s)

                # 如果没有以 { 开头，尝试找到第一个 {
                if not s.startswith('{'):
                    start = s.find('{')
                    if start != -1:
                        s = s[start:]
                
                # 进一步清理末尾非 JSON 内容 (如果存在)
                end = s.rfind('}')
                if end != -1:
                    s = s[:end+1]

                # 处理未闭合的引号
                # 这里的逻辑比较简单，可能不完全准确，但能处理大部分截断情况
                quotes_count = s.count('"') - s.count('\\"')
                if quotes_count % 2 != 0:
                    # 如果最后一个字符不是引号，且看起来是在字符串中间截断的
                    if not s.endswith('"'):
                        s += '"'
                
                # 处理未闭合的大括号
                open_braces = s.count('{')
                close_braces = s.count('}')
                if open_braces > close_braces:
                    s += '}' * (open_braces - close_braces)
                
                return s

            # 预处理：移除 markdown 代码块标记和 think 块
            # 先移除 think 块，因为它可能包含干扰解析的 JSON 片段
            content_no_think = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            
            # 提取 JSON 块
            clean_json = content_no_think
            # 尝试寻找 ```json ... ```
            json_match = re.search(r'```json\s*(.*?)\s*```', content_no_think, re.DOTALL)
            if json_match:
                clean_json = json_match.group(1)
            else:
                # 尝试寻找第一个 { 到最后一个 }
                start_idx = content_no_think.find('{')
                end_idx = content_no_think.rfind('}')
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    clean_json = content_no_think[start_idx:end_idx+1]
            
            clean_json = clean_json.strip()
            
            # 尝试修复并解析
            ai_data = None
            try:
                # 尝试直接解析 (增加 strict=False 以容忍 JSON 字符串内部的原始换行符)
                ai_data = json.loads(clean_json, strict=False)
            except json.JSONDecodeError as e:
                # 如果直接解析失败，先尝试移除不可见控制字符
                try:
                    # 再次强化清理：移除所有不可见的控制字符（0-31），除了必要的换行和制表符
                    fixed_json = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', clean_json)
                    ai_data = json.loads(fixed_json, strict=False)
                except json.JSONDecodeError:
                    # 如果还是失败，进入更激进的修复逻辑
                    repaired = repair_json(clean_json)
                    try:
                        ai_data = json.loads(repaired, strict=False)
                    except json.JSONDecodeError:
                        # [新增] 针对常见的 "Expecting ',' delimiter" 错误，这通常是因为字符串内部有未转义的双引号
                        # 尝试通过正则寻找并替换
                        try:
                            # 寻找 "full_report": "..." 结构，并将其内部的未转义双引号转义
                            def escape_inner_quotes(match):
                                prefix = match.group(1) # "full_report": "
                                content_inner = match.group(2) # ...
                                suffix = match.group(3) # "
                                # 转义内容中未转义的双引号 (排除 \" )
                                fixed_content = re.sub(r'(?<!\\)"', r'\"', content_inner)
                                return f'{prefix}{fixed_content}{suffix}'
                            
                            experimental_json = re.sub(r'("(?:full_report|analysis|rejection_reason)"\s*:\s*")(.*?)("\s*[,}])', escape_inner_quotes, clean_json, flags=re.DOTALL)
                            ai_data = json.loads(experimental_json, strict=False)
                        except Exception as final_err:
                            logger.error(f"AI JSON 深度修复后依然解析失败: {final_err}. Clean content: {clean_json[:500]}...")
                            raise e
            
            # 组装返回结果
            full_analysis = ai_data.get('full_report') or ai_data.get('analysis') or content_no_think
            # 关键修复：处理换行符和排版
            if isinstance(full_analysis, str):
                # 1. 将 AI 可能误输出的 \\n 替换为真实换行
                full_analysis = full_analysis.replace('\\n', '\n')
                full_analysis = full_analysis.replace('\u00a0', ' ')
                
                # 2. 修复被挤压在一行的标题 (针对部分模型如 DeepSeek V3 不遵守换行指令的情况)
                # 在 # 1. , # 2. 等标题前强制增加换行
                full_analysis = re.sub(r'([^\n])\s*(#\s*\d\.)', r'\1\n\n\2', full_analysis)
                full_analysis = re.sub(r'([^\n])\s*((?:[1-9]\d*)\.\s*(?:操盘结论|主力意图解密|题材与风口|风险提示|交易计划|操作建议|趋势判断|关键结论|结论|建议))', r'\1\n\n\2', full_analysis)
                full_analysis = re.sub(r'([^\n])\s*((?:一句话定性|核心判断|月线级别|周线级别|主力行为判定|日线形态确认|关键K线解析|支撑压力体系|乖离率状态|风口判定|行业逻辑|题材逻辑)[：:])', r'\1\n\2', full_analysis)
                full_analysis = re.sub(r'\s*--\s*', '\n\n--\n\n', full_analysis)
                
                # 3. 在 **关键词**： 前增加换行 (如果是紧跟在一段文字后面)
                full_analysis = re.sub(r'([^\n])\s*(\*\*[^*]+\*\*[:：])', r'\1\n\2', full_analysis)
                
                # 4. 修复列表项挤在一起的情况
                full_analysis = re.sub(r'([^\n])\s*(-\s+)', r'\1\n\2', full_analysis)
                
                # 5. 移除多余的空行
                full_analysis = re.sub(r'\n{3,}', '\n\n', full_analysis)
                
                full_analysis = full_analysis.strip()

            score = ai_data.get('score', 0)
            
            # [新增] 更新分值一致性缓存
            if score > 0:
                self._score_consistency_cache[symbol] = (score, time.time())
                logger.info(f"AI 服务: 已缓存 {symbol} 的评分 {score}")

            result = {
                "symbol": symbol,
                "is_worth_trading": ai_data.get('is_worth_trading', False),
                "score": score,
                "rejection_reason": ai_data.get('rejection_reason', ""),
                "analysis": full_analysis,
                "timestamp": datetime.now().isoformat(),
                "data_source": "TDX + Web Search",
                "source": used_model
            }
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="stock_analysis",
                    ts_code=symbol,
                    strategy_name=None,
                    request_payload={
                        "symbol": symbol,
                        "prev_score": prev_score,
                        "basic_info": basic_info,
                        "realtime_quote": realtime_quote,
                        "raw_trading_context": raw_trading_context,
                    },
                    response_payload=result,
                )
            except Exception as _e:
                logger.warning(f"AIReport save stock_analysis failed: {_e}")
            return result
        except Exception as e:
            logger.error(f"解析 AI 返回 JSON 失败: {e}, Content: {content[:500]}")
            # 降级处理：即使解析失败，也要清理内容，移除 <think> 块和末尾可能存在的 JSON
            fallback_analysis = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            # 移除可能存在的 JSON 块（防止显示乱码）
            fallback_analysis = re.sub(r'```json.*?```', '', fallback_analysis, flags=re.DOTALL).strip()
            
            result = {
                "symbol": symbol,
                "is_worth_trading": False, # 解析失败默认不交易
                "score": 0,
                "analysis": fallback_analysis or "AI 分析解析失败，请检查模型输出格式。",
                "timestamp": datetime.now().isoformat(),
                "data_source": "TDX + Web Search",
                "source": used_model
            }
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="stock_analysis",
                    ts_code=symbol,
                    strategy_name=None,
                    request_payload={
                        "symbol": symbol,
                        "prev_score": prev_score,
                        "basic_info": basic_info,
                        "realtime_quote": realtime_quote,
                        "raw_trading_context": raw_trading_context,
                        "parse_error": str(e),
                    },
                    response_payload=result,
                )
            except Exception as _e:
                logger.warning(f"AIReport save stock_analysis failed: {_e}")
            return result

    async def analyze_selling_opportunity(self, symbol: str, current_price: float, avg_price: float, pnl_pct: float, hold_days: int, market_status: str, account_info: dict, **kwargs) -> dict:
        """
        [V3 旗舰版] 实时持仓卖出决策 (支持原始数据上下文)
        """
        # 获取可选参数
        raw_trading_context = kwargs.get('raw_trading_context')
        handicap_info = kwargs.get('handicap_info', "")
        search_info = kwargs.get('search_info', "")
        vol = kwargs.get('vol', 0)
        available_vol = kwargs.get('available_vol', 0)
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        
        # --- [新增] 动态止盈与触发上下文 ---
        trigger_reason = kwargs.get('trigger_reason', "常规巡检")
        high_price = kwargs.get('high_price', 0.0)
        high_pnl_pct = kwargs.get('high_pnl_pct', 0.0)

        # --- [新增] 检查分值一致性缓存 ---
        prev_score = kwargs.get('prev_score')
        if prev_score is None:
            cached_data = self._score_consistency_cache.get(symbol)
            if cached_data:
                cached_score, cached_ts = cached_data
                if time.time() - cached_ts < 4 * 3600:
                    prev_score = cached_score

        prev_score_context = ""
        if prev_score is not None:
            if prev_score >= 80:
                prev_score_context = f"\n**【重要参考】该标的在选股系统中的评分为 {prev_score} 分。这表示它是一个高质量的趋势标的。在考虑卖出决策时，请务必区分“主力洗盘”与“趋势反转”。除非月线或周线级别出现放量破位，否则不要轻易清仓。**\n"
            else:
                prev_score_context = f"\n**【重要参考】该标的前次评分为 {prev_score} 分。请在决策时参考此分值。**\n"

        # 如果外部没传 context，内部尝试获取
        if not raw_trading_context:
            from app.services.chat_service import chat_service
            raw_trading_context = await chat_service.get_ai_trading_context(symbol, cache_scope="analysis")
        
        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()
        memory_context = self._get_relevant_memories("通用", market_status)

        # --- [新增] 针对复盘场景优化提示词 ---
        review_context_instruction = ""
        if "复盘" in market_status:
            review_context_instruction = """
## Review Mode Special Instructions (复盘模式专项指令):
- 你现在是在进行【盘后复盘】，目标是为【次日】制定交易计划。
- **视角转换**: 虽然你被称为“风控官”，但在复盘模式下，你的任务是评估标的是否具备“继续持仓”或“次日买入”的价值。
- **对于强势股 (如连板、龙头、成交额前列)**: 高 BIAS 是常态。不要仅因为 BIAS 高就拒绝。你应该观察 30min 走势是否稳健，是否有“分歧换手后转强”的机会。
- **寻找买点**: 重点寻找“缩量回调支撑位”、“分歧转一致的买点”或“分力转合力的契机”。
- **敢于看多**: 如果大势向好且标的处于强势主线，应更积极地寻找买入逻辑。
"""

        # --- [新增] 针对 9:25 开盘二次确认场景优化提示词 ---
        is_opening_reconfirm = "【09:25 开盘二次确认】" in (market_status or "")
        opening_reconfirm_instruction = ""
        if is_opening_reconfirm:
            opening_reconfirm_instruction = """
## 09:25 开盘二次确认专项指令 (Opening Auction Reconfirmation):
- **背景**: 当前是 09:25 集合竞价刚结束，开盘价已确定，但尚未正式开盘 (09:30)。
- **目标**: 对【昨日预选】的计划进行最终确认。
- **事实描述**:
    1. 开盘价相对前收的幅度反映竞价强度，高开或低开对应不同的市场情绪信号。
    2. 开盘位置相对关键压力位与竞价量能反映承接力度与兑现风险。
    3. 低开且板块走弱属于风险信号样本。
    4. 若维持买入决策，需按输出格式给出 action 与委托参数。
"""

        # --- [新增] 针对盘中实时筛选计划优化提示词 ---
        intraday_plan_instruction = ""
        is_intraday_plan = kwargs.get('is_intraday_plan', False)
        if is_intraday_plan:
            intraday_plan_instruction = """
## Intraday Real-time Plan Instructions (盘中筛选计划专项指令):
- **背景**: 该计划是【今日盘中】根据实时波动筛选出的机会，具有极强的时效性。
- **决策准则**:
    1. **分时横盘低吸 (买点之一)**: 若上午已出现一波放量拉升，之后股价长期横盘并围绕分时均价线震荡，且横盘期间量能逐步缩小，这是更优的低风险买点之一，应优先 **BUY**。
    2. **强势回调敢买**: 回调末端缩量更健康，强势趋势中的回调企稳应优先 **BUY**；若放量上冲已显著拉高，避免追价。出现强势骑线阳（开盘在均线下方、盘中突破并站稳均线）可主动 **BUY**。
    3. **即时性**: 若横盘区出现小级别止跌与轻微放量承接，结合 日/周/5min/30min 上行趋势，可立即 **BUY**。
"""

        # --- 获取反思记忆 (Phase 2) ---
        # 卖出决策使用通用反思记忆
        memory_context = self._get_relevant_memories("通用", market_status)
        sell_keywords = ["卖出", "减仓", "止损", "止盈", "滞涨", "背离", "破位", "高位", "放量阴", "上影", "回撤", "补跌"]
        sell_memory_context = await learning_service.get_reflection_memories_by_keywords(
            ["持仓卖出", "持仓减仓", "持仓持有", "通用"],
            sell_keywords,
            limit=6,
            source_event_type="PATTERN_CASE"
        )
        if sell_memory_context:
            sell_memory_context = "【卖出经验总结】\n" + sell_memory_context
        else:
            sell_memory_context = "【卖出经验总结】\n- 暂无匹配记忆"

        # --- 获取策略统计上下文 (Phase 2) ---
        # 尝试从 market_status 中解析市场温度
        market_temperature = 50.0
        try:
            db_temp = SessionLocal()
            today_sentiment = db_temp.query(MarketSentiment).filter(
                MarketSentiment.date == datetime.now().date()
            ).first()
            if today_sentiment:
                market_temperature = today_sentiment.market_temperature
        except Exception as e:
            logger.warning(f"Failed to fetch market temperature: {e}")
        finally:
            if 'db_temp' in locals():
                db_temp.close()
        
        # 获取该股票对应的买入策略
        strategy_name = "通用"
        plan_id = kwargs.get('plan_id')
        if plan_id:
            try:
                from app.db.session import SessionLocal
                db = SessionLocal()
                from app.models.stock_models import TradingPlan
                plan = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
                if plan:
                    strategy_name = plan.strategy_name
                db.close()
            except Exception as e:
                logger.warning(f"Failed to fetch plan for strategy context: {e}")
        
        strategy_context = await learning_service.get_strategy_context(strategy_name, market_temperature)

        # --- 获取成功模式上下文 (Phase 2) ---
        pattern_context = await learning_service.get_successful_pattern_context(strategy_name, limit=3)

        # --- 获取失败模式上下文 (Phase 2) ---
        failed_pattern_context = await learning_service.get_failed_pattern_context(strategy_name, limit=3)
        
        prompt = f"""
# Role: 资深交易组长 (Trading Head) & 风控专家
## Mission:
分析标的 {symbol} 的走势并给出决策。
你的目标是在保护本金的同时，捕获最强势的趋势机会。

{review_context_instruction}
{opening_reconfirm_instruction}
{intraday_plan_instruction}

{core_rules}
{auto_rules}
{memory_context}
{memory_context}
{memory_context}
{prev_score_context}
{memory_context}
{sell_memory_context}
{strategy_context}
{pattern_context}
{failed_pattern_context}

## Core Data Context (核心数据上下文):
【实时市场大势】
{market_status}

【实时盘口信息】
{handicap_info}

【板块与资讯 (包含行业联动信息)】
{search_info}

【账户资金状况】
- 总资产: {account_info.get('total_assets', 0):.2f}
- 持仓市值: {account_info.get('market_value', 0):.2f}
- 当前仓位占比: {(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%
- 当前总盈亏: {account_info.get('total_pnl_pct', 0):.2f}%

【持仓实时信息】
- 成本价: {avg_price:.2f}
- 当前价: {current_price:.2f}
- 当前盈亏: {pnl_pct:.2f}%
- 持仓期间最高价: {high_price:.2f}
- 持仓期间最高盈亏: {high_pnl_pct:.2f}%
- 持仓天数: {hold_days}天
- 持仓量: {vol}股 (可用: {available_vol}股)
- T+1规则: 持仓天数为0或可用股数为0，当天禁止卖出，只能给出HOLD

【本次触发原因】
**{trigger_reason}**
(注：触发原因仅为评估起点。请基于全周期行情，综合判断是否符合**任何**卖出理由。盈利保护优先级最高。)

【全周期原始行情数据 (Raw Data Context)】
        以下是该标的的 **30日日线 / 12周周线 / 6月月线** 以及 **30分钟 / 5分钟** 原始 K 线数据。
        **特别注意**：对于盘中决策，请务必分析 30分钟和 5分钟 K 线。
        **卖出信号声明**：
        1. **顶背离只是卖出信号之一**，即便没有顶背离，只要出现“放量滞涨”、“缩量阴跌”、“跌破 5min/30min 关键支撑线”、“大单持续流出”等任何走弱迹象，均应果断执行卖出决策。
        2. **盈利保护**：如果触发原因是“阶梯止盈”、“保本保护”或“触及计划止盈线”，无论技术形态是否完美，均应优先锁定部分利润。
        {raw_trading_context}

## Decision Logic (决策逻辑):
        1. **利润回吐警戒线 (Profit Protection Alert - Highest Priority)**:
           - **核心原则**：如果当前盈亏已从持仓期间最高点回撤超过 **3%** (针对盈利 > 5% 的个股) 或回撤超过 **5%** (针对盈利 > 15% 的个股)，必须优先考虑 SELL 或 REDUCE。
           - **绝对指令**：**严禁让盈利的股票变成亏损**。如果回撤触及保本线 (盈亏回落至 0.5% 附近)，必须果断卖出。
        2. **全维度走弱判断**:
           - **核心原则**：只要符合**任何**一条卖出逻辑（如：止盈止损、技术位破位、量价背离、板块崩盘、相对强度急剧转弱等），即可执行 SELL 或 REDUCE。
           - **不要死等顶背离**：顶背离是强势股见顶的一种形式，但不是唯一形式。尖顶回落、阴跌破位同样是明确卖点。
        2. **盈利保护与止盈 (Profit Protection)**:
           - 盈利保护逻辑（阶梯止盈、保本、计划止盈）独立生效，不需要其他技术信号配合。
        3. **相对强度与板块优先 (Relative Strength & Sector King)**: 
           - 观察【板块与资讯】中的行业涨跌幅 vs 个股涨跌幅。
           - **核心原则**：如果个股涨幅远超板块涨幅，说明该股具备极强的相对强度，是独立牛股。但在相对强度极强的情况下，若触发了盈利保护或出现了明确的分钟级别破位，仍应执行减仓或卖出。
        4. **分钟级别走弱确认 (Intraday Weakness Check)**: 
           - 观察 30 分钟和 5 分钟 K 线。
           - 卖出理由包括但不限于：价格新高但 MACD/RSI 不跟（背离）、高位放量滞涨（出货嫌疑）、缩量阴跌跌破均线、分时图走势出现“尖刀波”回落等。
        5. **多周期趋势共振**:
           - 如果日线级别依然健康，且 30/5 分钟级别未见任何走弱信号，且未触发盈利保护，应继续 HOLD。
        6. **止损参考**: 如果 30 日趋势线或关键支撑位(如 MA20) 跌破，或当前亏损较大，请评估是否需要 SELL 以控制风险。
        7. **仓位风险控制 (核心)**: 总仓位越高，风控越应严苛。
        8. **量价辩证与落袋为安 (Volume-Price Dialectics)**: 
           - **缩量上涨**不一定是坏事（可能代表筹码锁定良好，抛压轻），**放量上涨**也不一定是好事（高位放量滞涨往往是主力出货）。
           - **核心原则**：看量价位置。低位放量是启动信号，高位放量滞涨是危险信号；缩量上涨若未破位则持有，缩量阴跌则需警惕。
           - 当月线级别触及历史强压力区，或出现高位放量滞涨、缩量阴跌破位时，决定 REDUCE 或 SELL。

## Output Format (Strict JSON):
{{
    "action": "SELL" | "HOLD" | "REDUCE" | "T",
    "reason": "以第一人称'我'开头，简述你的多周期趋势判断和卖出/持有/做T理由 (限 50 字)",
    "confidence": 0-100,
    "order_type": "MARKET" | "LIMIT",
    "price": float,
    "sell_order_type": "MARKET" | "LIMIT",
    "sell_price": float,
    "buy_order_type": "MARKET" | "LIMIT",
    "buy_price": float,
    "volume": int
}}
当 action = "T" 时必须给出 sell_price、buy_price、volume，并且 volume 不能超过可用持仓；做T采用先卖后买的顺序。
"""
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个极度理性的风控官，只输出 JSON。", preferred_provider=preferred_provider, api_key=api_key)
            
            import re
            clean_json = re.sub(r'```json\s*|\s*```', '', response).strip()
            # 找到第一个 {
            start_idx = clean_json.find('{')
            if start_idx != -1:
                clean_json = clean_json[start_idx:]
            
            decision = json.loads(clean_json, strict=False)
            logger.info(f"AI Selling Decision for {symbol}: {decision.get('action')} - {decision.get('reason')}")
            # 确保包含必需字段
            if 'order_type' not in decision: decision['order_type'] = "MARKET"
            if 'price' not in decision: decision['price'] = current_price
            act = str((decision.get('action') or "HOLD")).upper()
            if act == "T":
                if 'sell_order_type' not in decision: decision['sell_order_type'] = decision['order_type']
                if 'buy_order_type' not in decision: decision['buy_order_type'] = decision['order_type']
                if 'sell_price' not in decision: decision['sell_price'] = decision.get('price', current_price)
                if 'buy_price' not in decision: decision['buy_price'] = current_price
                v = int(decision.get('volume') or 0)
                if v <= 0:
                    step = 100
                    v = int((available_vol or 0) * 0.3 // step * step)
                    decision['volume'] = max(v, 0)
            if 'reason' in decision:
                decision['reason'] = self._normalize_auto_reason(str(decision.get('reason') or ""))
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="selling_opportunity",
                    ts_code=symbol,
                    strategy_name="持仓卖出决策",
                    request_payload={
                        "symbol": symbol,
                        "current_price": current_price,
                        "avg_price": avg_price,
                        "pnl_pct": pnl_pct,
                        "hold_days": hold_days,
                        "market_status": market_status,
                        "account_info": account_info,
                        "handicap_info": handicap_info,
                        "search_info": search_info,
                        "raw_trading_context": raw_trading_context,
                        "vol": vol,
                        "available_vol": available_vol,
                        "prev_score": prev_score,
                    },
                    response_payload=decision,
                )
            except Exception as _e:
                logger.warning(f"AIReport save selling_opportunity failed: {_e}")
            return decision
        except Exception as e:
            logger.error(f"V3 卖出决策分析失败: {e}")
            decision = {"action": "HOLD", "reason": "AI 分析暂时故障，决定暂时持股", "order_type": "MARKET", "price": current_price}
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="selling_opportunity",
                    ts_code=symbol,
                    strategy_name="持仓卖出决策",
                    request_payload={
                        "symbol": symbol,
                        "current_price": current_price,
                        "avg_price": avg_price,
                        "pnl_pct": pnl_pct,
                        "hold_days": hold_days,
                        "market_status": market_status,
                        "account_info": account_info,
                        "handicap_info": handicap_info,
                        "search_info": search_info,
                        "raw_trading_context": raw_trading_context,
                        "vol": vol,
                        "available_vol": available_vol,
                        "prev_score": prev_score,
                        "error": str(e),
                    },
                    response_payload=decision,
                )
            except Exception as _e:
                logger.warning(f"AIReport save selling_opportunity failed: {_e}")
            return decision

    async def analyze_sell_signal(self, symbol: str, current_price: float, avg_price: float, hold_days: int, vol: int, available_vol: int, market_status: str, minute_str: str, **kwargs) -> dict:
        """
        卖出信号分析 (纯 JSON)
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        pnl_pct = (current_price - avg_price) / avg_price * 100
        account_str = f"浮动盈亏: {pnl_pct:.2f}%"

        raw_trading_context = ""
        try:
            from app.services.chat_service import chat_service
            raw_trading_context = await asyncio.wait_for(chat_service.get_ai_trading_context(symbol, cache_scope="analysis"), timeout=25.0)
        except Exception as e:
            logger.warning(f"Sell-signal: Failed to fetch unified trading context for {symbol}: {e}")
        
        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()
        
        prompt = f"""
        # Role: 严酷的风控官 (Risk Manager)
        
        ## Mission:
        判断是否卖出持仓 {symbol}。
        你的决策直接决定这笔交易的最终盈亏。请像对待自己的钱一样对待这笔持仓。
        
        {core_rules}
        {auto_rules}
        
        【市场环境】
        {market_status}

        【当前账户状态】
        {account_str}
        
        【持仓信息】
        - 成本价: {avg_price:.2f}
        - 当前价: {current_price:.2f}
        - 浮动盈亏: {pnl_pct:.2f}%
        - 持仓天数: {hold_days}天
        - 持仓量: {vol}股 (可用: {available_vol}股)
        
        【最近5分钟走势】
        {minute_str}

        【多周期原始行情 (日/周/月 + 30分钟/5分钟，如上下文中提供)】
        {raw_trading_context}
        
        【决策逻辑 (基金经理视角)】
        1. **止损铁律**: 亏损超过 -5% 且市场情绪转弱，或者技术破位，必须坚决 SELL，截断亏损！
        2. **落袋为安**: 如果盈利超过 10% 且出现滞涨或高位放量阴线，必须 SELL，保住利润！
        3. **30分钟顶背离风险**: 观察 30 分钟 K 线与 MACD。**由你判断其突破动能**：若股价在高位滞涨或创新高，但 MACD 走弱形成背离，且你判定无明显的加速突破迹象，应果断 SELL 或 REDUCE。
        4. **30分钟底背离观察**: 若股价在低位出现底背离（股价创新低但 MACD 指标抬升），**请你深度评估反弹力度**。若你判定反弹有力（如放量拉升、MACD金叉等）且底背离有效，可适当推迟止损，等待反弹减速再操作。
        5. **强弱分化处理**: 市场退潮或冰点期，若个股仍强势可继续 HOLD；若走弱才减仓或清仓。
        6. **耐心持股**: 只有在趋势良好且未触发任何卖出信号（包括 30 分钟背离）时，才允许 HOLD。
        
        请输出 JSON 格式决策：
        {{
            "action": "SELL" | "HOLD" | "REDUCE",
            "reason": "以第一人称'我'开头，简述决策理由 (例如：'我决定止损，因为触发了-5%风控线...')",
            "order_type": "MARKET" | "LIMIT",
            "price": float
        }}
        """
        
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个为结果负责的实盘基金经理，只输出 JSON。", preferred_provider=preferred_provider, api_key=api_key)
            
            import re
            clean_json = re.sub(r'```json\s*|\s*```', '', response).strip()
            decision = json.loads(clean_json, strict=False)
            # 确保包含必需字段
            if 'order_type' not in decision: decision['order_type'] = "MARKET"
            if 'price' not in decision: decision['price'] = current_price
            if 'reason' in decision:
                decision['reason'] = self._normalize_auto_reason(str(decision.get('reason') or ""))
            return decision
        except Exception as e:
            logger.error(f"AI 卖出信号分析失败: {e}")
            # 降级风控
            if pnl_pct < -8.0:
                return {"action": "SELL", "reason": "触及硬性止损线 (-8%)", "order_type": "MARKET", "price": current_price}
            return {"action": "HOLD", "reason": "AI分析失败，决定持股观察", "order_type": "MARKET", "price": current_price}

    async def analyze_holding_strategy(self, symbol: str, name: str, current_price: float, 
                                 avg_price: float, profit_pct: float, 
                                 market_summary: str, kline_context: str,
                                 vol: int = 0, available_vol: int = 0, hold_days: int = 0, **kwargs) -> dict:
        """
        盘后持仓分析
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        status = "盈利" if profit_pct > 0 else "亏损"

        if not kline_context:
            try:
                from app.services.chat_service import chat_service
                kline_context = await asyncio.wait_for(chat_service.get_ai_trading_context(symbol, cache_scope="analysis"), timeout=25.0)
            except Exception as e:
                logger.warning(f"Holding-strategy: Failed to fetch unified trading context for {symbol}: {e}")
        
        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()
        
        prompt = f"""
# Role: 首席基金经理 (实盘账户负责人)

## Mission:
你正在复盘自己管理的实盘账户持仓 {name} ({symbol})。
【责任声明】这是你管理的实盘账户。请务必为明日的账户净值负责。

{core_rules}
{auto_rules}

持仓详情如下：
- 成本价: {avg_price:.2f}
- 现价: {current_price:.2f}
- 持仓量: {vol}股 (可用: {available_vol}股)
- 持仓天数: {hold_days}天
- 盈亏状况: {status} {profit_pct:.2f}%

请基于今日收盘情况和市场大势，制定**明天的操作计划**。

## Market Context (大盘环境):
{market_summary}

## Stock Context (个股走势):
{kline_context}

## Decision Rules:
1. **止盈保护**: 如果盈利丰厚且出现高位滞涨/顶背离（重点观察 30 分钟背离强度），坚决减仓或清仓。
2. **止损纪律**: 如果亏损扩大且跌破关键支撑 (如 MA20)，坚决止损。**注意**：若 30 分钟底背离成立且你判定反弹有力，可暂缓止损。
3. **弱转强**: 如果今日弱势但未破位，可观察明日是否弱转强。
4. **强更强**: 如果今日涨停或大涨，明日溢价预期高，决定持有或冲高止盈。

## Output Format (JSON Only):
请仅返回如下 JSON 格式，不要包含 Markdown 标记：
{{
    "action": "HOLD" | "SELL" | "REDUCE" | "BUY",  // 动作: 持有/清仓/减仓/加仓
    "stop_loss_price": float,              // 更新后的止损价 (必填)
    "take_profit_price": float,            // 更新后的止盈价 (必填)
    "reason": "string"                     // 以第一人称'我'开头，简述决策理由
}}
"""
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, preferred_provider=preferred_provider, api_key=api_key)
            
            clean_resp = response.replace('```json', '').replace('```', '').strip()
            result = json.loads(clean_resp, strict=False)
            if "reason" in result:
                result["reason"] = self._normalize_auto_reason(str(result.get("reason") or ""))
            return result
        except Exception as e:
            logger.error(f"持仓分析失败: {e}")
            return {
                "action": "HOLD",
                "stop_loss_price": current_price * 0.95,
                "take_profit_price": current_price * 1.10,
                "reason": "AI分析失败，执行默认风控策略"
            }

    async def analyze_stock_for_plan(self, context_str: str, is_noon: bool = False, **kwargs) -> dict:
        """
        简单分析股票并生成交易计划 (JSON格式)
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        account_info = kwargs.get('account_info') or {}
        positions = account_info.get('positions') or []
        total_assets = float(account_info.get('total_assets') or 0.0)
        available_cash = float(account_info.get('available_cash') or 0.0)
        market_value = float(account_info.get('market_value') or 0.0)
        total_pnl_pct = float(account_info.get('total_pnl_pct') or 0.0)
        extra_context = ""
        ts_code = None
        try:
            import re
            m = re.search(r'(?<!\d)(\d{6})\.(SH|SZ|BJ)(?!\w)', context_str)
            if m:
                ts_code = f"{m.group(1)}.{m.group(2)}"
                from app.services.chat_service import chat_service
                extra_context = await asyncio.wait_for(chat_service.get_ai_trading_context(ts_code, cache_scope="analysis"), timeout=25.0)
        except Exception as e:
            logger.warning(f"Stock-for-plan: Failed to attach unified trading context: {e}")
        is_held = False
        if ts_code:
            is_held = any(str(p.get("ts_code") or "") == ts_code for p in positions)

        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()
        
        prompt = f"""
        # Role: 首席交易员 (Chief Trader)
        
        ## Mission:
        分析该标的的交易机会并制定计划。
        
        {core_rules}
        {auto_rules}
        
        Account Snapshot:
        - total_assets: {total_assets:.2f}
        - available_cash: {available_cash:.2f}
        - market_value: {market_value:.2f}
        - total_pnl_pct: {total_pnl_pct:.2f}%
        - positions_count: {len(positions)}
        - is_held: {"YES" if is_held else "NO"}

        Analyze this stock data:
        {context_str}

        Unified trading context (日/周/月 + 30分钟/5分钟，如上下文中提供):
        {extra_context}
        
        Scenario: {'Intraday Noon Review' if is_noon else 'Post-market Review'}
        
        Determine if this is a good buying opportunity for {'today afternoon' if is_noon else 'tomorrow'}.
        Criteria: Strong trend, breakout, or good risk/reward support level.
        If is_held is NO, do not output HOLD/SELL/REDUCE.
        
        Output JSON only:
        {{
            "action": "BUY" or "BUY_NOW" or "WAIT" or "CANCEL" or "SELL" or "REDUCE",
            "strategy": "Strategy Name" (e.g. Low Suck, Breakout),
            "reason": "Brief reason (max 20 words)",
            "buy_price": float (limit price, 0 for market),
            "stop_loss": float,
            "take_profit": float,
            "confidence": score 0-100
        }}
        """
        
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, preferred_provider=preferred_provider, api_key=api_key)
            clean_resp = response.replace('```json', '').replace('```', '').strip()
            decision = json.loads(clean_resp, strict=False)
            if isinstance(decision, dict) and "reason" in decision:
                decision["reason"] = self._normalize_auto_reason(str(decision.get("reason") or ""))
            return decision
        except Exception as e:
            logger.error(f"Stock analysis for plan failed: {e}")
            return {}

    async def analyze_realtime_trade_signal_v3(self, symbol: str, strategy: str, current_price: float, buy_price: float, raw_trading_context: str = None, plan_reason: str = "", market_status: str = "", search_info: str = "", account_info: dict = {}, **kwargs) -> dict:
        """
        [V3 旗舰版] 盘中实时交易决策 (注入 30日/12周/6月 原始数据)
        """
        # 兼容旧的参数名 raw_context
        if raw_trading_context is None:
            raw_trading_context = kwargs.get('raw_context', "")

        if not raw_trading_context:
            try:
                from app.services.chat_service import chat_service
                raw_trading_context = await asyncio.wait_for(chat_service.get_ai_trading_context(symbol, cache_scope="analysis"), timeout=25.0)
            except Exception as e:
                logger.warning(f"Realtime-signal: Failed to fetch unified trading context for {symbol}: {e}")
            
        # --- [新增] 检查 9:25 开盘确认场景 ---
        is_opening_reconfirm = "【09:25 开盘二次确认】" in market_status
        opening_reconfirm_instruction = ""
        if is_opening_reconfirm:
            opening_reconfirm_instruction = """
## 09:25 开盘二次确认专项指令 (Opening Auction Reconfirmation):
- **背景**: 当前是 09:25 集合竞价刚结束，开盘价已确定，但尚未正式开盘 (09:30)。
- **目标**: 对【昨日预选】的计划进行最终确认。
- **决策准则**:
    1. **开盘强度**: 如果开盘价远超预期 (高开 > 4%)，需判断是否为“缩量秒板”潜力或“利好兑现高开低走”风险。
    2. **开盘位置**: 如果开盘在关键压力位之上，且竞价量能达标，应果断执行 **BUY**。
    3. **风险拦截**: 如果竞价表现极弱 (低开 < -3%) 或板块集体走弱，应果断 **CANCEL** 或 **WAIT**。
    4. **一致性**: 如果你决定维持昨天的买入决策，请输出 **BUY** 并给出 **MARKET** 或 **LIMIT** 委托。
"""

        # --- [新增] 针对盘中实时筛选计划优化提示词 ---
        intraday_plan_instruction = ""
        is_intraday_plan = kwargs.get('is_intraday_plan', False)
        if is_intraday_plan:
            intraday_plan_instruction = """
## Intraday Real-time Plan Instructions (盘中筛选计划专项指令):
- **背景**: 该计划是【今日盘中】根据实时波动筛选出的机会，具有极强的时效性。
- **事实描述**:
    1. 盘中筛选代表当前形态已触发策略信号，具有较强时效性。
    2. 1min/5min 的量价配合可反映短线资金参与强度。
"""
        
        # --- [新增] 获取进化后的策略参数 (Phase 3) ---
        # 尝试从 market_status 中解析温度 (简单处理，假设 market_status 文本包含温度信息，或默认 50)
        temp = 50.0 
        # TODO: 解析 market_status 获取真实温度，暂用默认
        policy_params = evolution_service.get_active_config(strategy, temp)
        
        max_pos = policy_params.get("max_position_pct", 0.1)
        stop_loss_pct = policy_params.get("stop_loss_pct", 0.05)
        
        # --- [新增] 检查分值一致性缓存 ---
        prev_score = kwargs.get('prev_score')
        if prev_score is None:
            cached_data = self._score_consistency_cache.get(symbol)
            if cached_data:
                cached_score, cached_ts = cached_data
                if time.time() - cached_ts < 4 * 3600:
                    prev_score = cached_score

        prev_score_context = ""
        if prev_score is not None:
            if prev_score >= 80:
                prev_score_context = f"\n**【重要参考】该标的在系统中的最新评分为 {prev_score} 分，代表历史评分记录与当下判断存在一致性约束。**\n"
            else:
                prev_score_context = f"\n**【重要参考】该标的前次评分为 {prev_score} 分，用于保持逻辑连贯性。**\n"

        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()

        # --- [新增] 针对复盘场景优化提示词 ---
        review_context_instruction = ""
        if "复盘" in (market_status or ""):
            review_context_instruction = """
## Review Mode Special Instructions (复盘模式专项指令):
- 你现在是在进行【盘后复盘】，目标是为【次日】制定交易计划。
- 复盘模式强调对次日计划的记录与可验证性。
- 仅描述可观察的量价/趋势/结构事实与风险信号。
"""

        # --- 获取反思记忆 (Phase 2) ---
        memory_context = self._get_relevant_memories(strategy, market_status)

        # --- 获取策略统计上下文 (Phase 2) ---
        # 尝试从 market_status 中解析市场温度
        market_temperature = 50.0
        try:
            db_temp_2 = SessionLocal()
            today_sentiment = db_temp_2.query(MarketSentiment).filter(
                MarketSentiment.date == datetime.now().date()
            ).first()
            if today_sentiment:
                market_temperature = today_sentiment.market_temperature
        except Exception as e:
            logger.warning(f"Failed to fetch market temperature: {e}")
        finally:
            if 'db_temp_2' in locals():
                db_temp_2.close()
        
        strategy_context = await learning_service.get_strategy_context(strategy, market_temperature)

        # --- 获取成功模式上下文 (Phase 2) ---
        pattern_context = await learning_service.get_successful_pattern_context(strategy, limit=3)

        # --- 获取失败模式上下文 (Phase 2) ---
        failed_pattern_context = await learning_service.get_failed_pattern_context(strategy, limit=3)

        prompt = f"""
# Role: 首席交易官 (Chief Trading Officer) - 盘中实战模式

## Task: 
针对标的 {symbol} 进行实时决策。
你目前正处于开盘交易时段，你的每一个决策都涉及真实资金的买入与卖出。

{core_rules}
{auto_rules}
{review_context_instruction}
{opening_reconfirm_instruction}
{intraday_plan_instruction}
{prev_score_context}
{memory_context}
{strategy_context}
{pattern_context}
{failed_pattern_context}

## Policy Constraints (策略风控参数 - 参考):
- 单笔最大仓位: {max_pos*100}% (系统进化设定)
- 止损幅度: -{stop_loss_pct*100}% (系统进化设定)

## Core Data Context (核心数据上下文):
【实时市场大势】
{market_status}

【账户资金状况】
- 总资产: {account_info.get('total_assets', 0):.2f}
- 可用资金: {account_info.get('available_cash', 0):.2f}
- 当前仓位: {(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%
- 持仓明细: {account_info.get('current_positions', '无')}
- 挂单状态: {account_info.get('pending_orders', '无')}

【个股实时行情】
- 当前价格: {current_price:.2f} (对比计划买入价: {buy_price:.2f})

【外部实时资讯 & 盘口信息】
{search_info}

【核心原始数据 (Raw Data Context - 多周期)】
以下是该标的的日K/周K/月K以及分钟K(30min/5min，如上下文中提供)原始数据，请务必基于此进行多周期共振分析：
{raw_trading_context}

【预设交易计划】
- 策略名称: {strategy}
- 计划初衷: {plan_reason}

## Decision Logic (事实与硬约束):
1. **学习记录**：系统会记录成交结果与回撤表现，用于后续学习与优化。
2. **买卖交易硬规则 (不可变)**：
   - 只有在 action 明确输出 BUY/SELL/CANCEL 时才允许挂单或撤单。
   - BUY 必须给出明确价格，reason 必须以“买/价格”开头；SELL/CANCEL 必须以“卖/价格”开头。
   - 未满足硬规则则不挂单。
3. **撤单与观察的事实边界**:
   - 撤单用于撤回挂单并解冻资金；持续跟踪观察不等于挂单成交。
   - 原本逻辑被破坏（放量破位、趋势走坏、市场突发跳水、价格显著偏离最佳区间）是撤单事实依据。
   - 未到买点、小级别回落或轻微背离属于观察事实范畴。
4. **涨停/封板规则**:
   - 如果五档盘口显示卖盘几乎为 0、买一封单显著，且当前价已到涨停或贴近涨停价，禁止追价买入，输出 WAIT 或 CANCEL。
   - 若仍需挂单，限价必须不高于涨停价。
5. **仓位与风险事实**: 当前仓位较高 ({(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%)。
6. **基本面风险事实**: 基本面仅作退市风险过滤；只有上下文明确存在 ST/退市风险时才提及。
7. **趋势事实**: “月线看大势，周线看趋势，日线结合分钟线找买点”作为结构化描述框架。
8. **趋势阶段事实**: 观察 30分钟与日线的突破、均线发散、BIAS 与量能配合。
9. **盈亏比事实**: 评估当前价到下方支撑（如 MA20 或最近放量阳线中位）的距离。
10. **底背离事实**: 记录 30min 底背离与反弹力度。
11. **市场共振事实**: 记录个股相对大盘与板块的强弱关系。
12. **解析一致性**: 如果决定买入，action 输出 "BUY"，reason 以 "买" 开头。

## Output Format (Strict JSON):
{{
    "action": "BUY" | "WAIT" | "CANCEL",
    "reason": "必须以【买/卖/买卖价格/持仓观望/观望】之一开头；BUY用“买”，CANCEL用“卖”，WAIT用“观望/持仓观望”；随后给出理由 (限 50 字)",
    "confidence": 0-100,
    "order_type": "MARKET" | "LIMIT",
    "price": float,
    "reference_price": float,
    "plan_price": float
}}
"""
        try:
            has_mimo = ai_client.mimo_client is not None
            api_key = kwargs.get('api_key')

            async def _call_model(client, model, name):
                try:
                    raw = await asyncio.to_thread(ai_client.call_ai_api, client, model, prompt, system_prompt="你是客观记录事实的实盘交易员，只输出 JSON。", api_key=api_key)
                    import re
                    clean = re.sub(r'```json\s*|\s*```', '', raw).strip()
                    start = clean.find('{')
                    if start != -1: clean = clean[start:]
                    data = json.loads(clean)
                    if 'order_type' not in data:
                        data['order_type'] = "MARKET"
                    reason_text = str(data.get("reason") or "").strip()
                    data['_explicit_price'] = "price" in data and float(data.get("price") or 0) > 0
                    data['_explicit_signal'] = reason_text.startswith("买") or reason_text.startswith("卖")
                    if 'price' not in data:
                        data['price'] = current_price
                    if 'reference_price' not in data:
                        data['reference_price'] = current_price
                    if 'plan_price' not in data:
                        data['plan_price'] = float(data.get("price") or current_price)
                    return data
                except Exception as e:
                    logger.warning(f"AI {name} failed: {e}")
                    return None

            if not has_mimo:
                raise Exception("MiMo client unavailable")

            async with self.ai_semaphore:
                decision = await _call_model(ai_client.mimo_client, settings.MIMO_MODEL, "MiMo")
            if not decision:
                raise Exception("MiMo decision failed")

            action = str(decision.get('action') or "")
            decision['reason'] = self._normalize_action_reason(
                action,
                str(decision.get('reason', '') or ""),
                price=decision.get('price', current_price),
                strategy=strategy
            )
            decision['reason'] = self._normalize_auto_reason(decision.get('reason', ''))
            if is_intraday_plan and action.upper() != "BUY":
                cached = self._realtime_decision_cache.get(symbol)
                if cached:
                    cached_ts = float(cached.get("ts") or 0)
                    if time.time() - cached_ts <= 120:
                        cached_price = float(cached.get("price") or 0)
                        if cached_price > 0 and current_price > 0:
                            diff = abs(current_price - cached_price) / cached_price
                            if diff <= 0.008:
                                cached_decision = cached.get("decision")
                                if isinstance(cached_decision, dict):
                                    decision = cached_decision.copy()
                                    action = str(decision.get("action") or "")
                                    decision["reason"] = self._normalize_action_reason(
                                        action,
                                        f"{decision.get('reason', '')} 短时延续",
                                        price=decision.get('price', current_price),
                                        strategy=strategy
                                    )
                                    decision["reason"] = self._normalize_auto_reason(decision.get("reason", ""))
            if action.upper() == "BUY":
                self._realtime_decision_cache[symbol] = {
                    "ts": time.time(),
                    "price": float(decision.get("price") or current_price),
                    "decision": decision.copy(),
                }
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="realtime_trade_signal_v3",
                    ts_code=symbol,
                    strategy_name=strategy,
                    request_payload={
                        "symbol": symbol,
                        "strategy": strategy,
                        "current_price": current_price,
                        "buy_price": buy_price,
                        "plan_reason": plan_reason,
                        "market_status": market_status,
                        "search_info": search_info,
                        "account_info": account_info,
                        "raw_trading_context": raw_trading_context,
                        "prev_score": prev_score,
                    },
                    response_payload=decision,
                )
            except Exception as _e:
                logger.warning(f"AIReport save realtime_trade_signal_v3 failed: {_e}")
            return decision
        except Exception as e:
            logger.error(f"V3 实时交易决策分析失败: {e}")
            decision = {"action": "WAIT", "reason": "观望 AI 分析暂时故障，决定观望"}
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="realtime_trade_signal_v3",
                    ts_code=symbol,
                    strategy_name=strategy,
                    request_payload={
                        "symbol": symbol,
                        "strategy": strategy,
                        "current_price": current_price,
                        "buy_price": buy_price,
                        "plan_reason": plan_reason,
                        "market_status": market_status,
                        "search_info": search_info,
                        "account_info": account_info,
                        "raw_trading_context": raw_trading_context,
                        "prev_score": prev_score,
                        "error": str(e),
                    },
                    response_payload=decision,
                )
            except Exception as _e:
                logger.warning(f"AIReport save realtime_trade_signal_v3 failed: {_e}")
            return decision

    async def analyze_portfolio_adjustment(self, market_status: str, positions: List[Dict], account_info: Dict, **kwargs) -> List[Dict]:
        """
        [V3 旗舰版] 基于多周期原始数据的组合级调仓决策
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        if not positions:
            return []

        pos_desc = []
        # 并行获取所有持仓的多周期交易上下文，大幅提高效率
        from app.services.chat_service import chat_service
        
        context_tasks = [chat_service.get_ai_trading_context(p['ts_code'], cache_scope="analysis") for p in positions]
        all_contexts = await asyncio.gather(*context_tasks)
        
        for i, p in enumerate(positions):
            raw_context = all_contexts[i]
            ts_code = p['ts_code']
            
            # 获取该标的分值缓存
            score_str = ""
            cached_data = self._score_consistency_cache.get(ts_code)
            if cached_data:
                score_str = f" [系统评分: {cached_data[0]}分]"

            p_str = f"""
            - {p['name']}({ts_code}){score_str}: 
              盈亏: {p['pnl_pct']:.2f}%, 持仓: {p['vol']}股, 现价: {p['current_price']:.2f}
              【原始行情数据 (30日/12周/6月)】:
              {raw_context}
            """
            pos_desc.append(p_str)
        
        pos_all_str = "\n".join(pos_desc)

        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()

        prompt = f"""
# Role: 资深实盘投资经理 (Portfolio Manager)

## Task:
你正在审视账户的整体持仓情况。请结合当前市场大势和每只持仓股的“日/周/月”多周期原始行情，判断是否需要**立即**进行调仓操作。

{core_rules}
{auto_rules}

## Market Context (市场大势):
{market_status}

## Account Info (账户资金):
- 总资产: {account_info.get('total_assets', 0):.2f}
- 可用资金: {account_info.get('available_cash', 0):.2f}
- 总盈亏比例: {account_info.get('total_pnl_pct', 0):.2f}%

## Portfolio Status (当前持仓详情):
{pos_all_str}

## Decision Logic (调仓逻辑):
1. **仓位动态平衡 (核心)**: 当前仓位为 {(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%。如果仓位超过 80%，应采取防御姿态，优先考虑 SELL 或 REDUCE 那些趋势转弱或处于高位滞涨的标的，保留流动性。
2. **去弱留强**: 仔细分析每只股的原始数据。如果日线/周线趋势已经走坏（如跌破关键均线、放量下跌），即使目前盈利也要果断 SELL。
3. **多周期共振**: 只有月线、周线、日线均处于向上趋势，或月线支撑位企稳的标的，才允许继续 HOLD 或 BUY_MORE。
4. **风控优先**: 如果大盘环境恶化，优先决定 REDUCE 或 SELL 那些表现弱于大盘的标的，腾出现金。
5. **月线视角**: 务必关注月线原始数据，判断当前是否处于长期大顶或强阻力位。

## Output Format (Strict JSON List):
只输出需要**改变状态**的个股。如果全部 HOLD，输出空列表 `[]`。
[
    {{
        "ts_code": "000001.SZ",
        "action": "SELL" | "REDUCE" | "BUY_MORE",
        "reason": "以第一人称'我'开头，简述多周期趋势判断理由 (限 40 字)",
        "order_type": "MARKET" | "LIMIT", // MARKET: 现价, LIMIT: 限价
        "price": float // 如果是 LIMIT，请给出具体限价；如果是 MARKET，此值会被忽略但仍需提供
    }}
]
"""
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个极度理性的实盘投资经理，只输出 JSON 列表。", preferred_provider=preferred_provider, api_key=api_key)
            
            import re
            clean_resp = re.sub(r'```json\s*|\s*```', '', response).strip()
            # 找到第一个 [
            start_idx = clean_resp.find('[')
            if start_idx != -1:
                clean_resp = clean_resp[start_idx:]
            
            # 兼容 AI 可能输出单个对象而不是列表的情况
            if clean_resp.startswith('{'):
                clean_resp = f"[{clean_resp}]"
            
            result = json.loads(clean_resp, strict=False)
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"V3 组合调仓分析失败: {e}")
            return []

    async def analyze_rebalance_signal(self, *args, **kwargs) -> List[Dict]:
        """
        调仓信号分析 (别名，为了向后兼容)
        """
        return await self.analyze_portfolio_adjustment(*args, **kwargs)

    async def analyze_late_session_opportunity(self, symbol: str, current_price: float, market_status: str, **kwargs) -> dict:
        """
        [V3] 尾盘选股深度分析与决策 (注入 30日/12周/6月 数据)
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        from app.services.chat_service import chat_service
        raw_context = await chat_service.get_ai_trading_context(symbol, cache_scope="analysis")

        # --- [新增] 检查分值一致性缓存 ---
        prev_score_context = ""
        cached_data = self._score_consistency_cache.get(symbol)
        if cached_data:
            cached_score, cached_ts = cached_data
            if time.time() - cached_ts < 4 * 3600:
                if cached_score >= 80:
                    prev_score_context = f"\n**【重要参考】该标的在系统中的最新评分为 {cached_score} 分，属于高确定性品种。尾盘分析应优先寻找买入机会，除非出现极端破位。**\n"
                else:
                    prev_score_context = f"\n**【重要参考】该标的前次评分为 {cached_score} 分。**\n"

        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()

        prompt = f"""
        # Role: 资深超短线交易员 (尾盘突击专家)
        
        ## Task
        现在是尾盘阶段 (14:45)。系统初步筛选出 {symbol} 为潜在买入标的。
        请结合当前市场环境和提供的“日/周/月 + 30分钟/5分钟”原始行情数据，做出最终买入决策。
        
        {core_rules}
        {auto_rules}
        {prev_score_context}
        
        【个股实时行情】
        - 代码: {symbol}
        - 现价: {current_price}
        
        【市场大势】
        {market_status}

        【多周期原始行情 (日/周/月 + 30分钟/5分钟)】
        {raw_context}
        
        【决策逻辑】
        1. **趋势确认**: 必须执行“月线看大势，周线看趋势，日线找买点”。如果月线或周线趋势不佳，坚决 WAIT。
        2. **次日溢价**: 只有预期次日能高开或有冲高动作时，才执行 BUY。
        3. **量价配合**: 观察原始数据中的量价关系，确认是否有主力资金在尾盘扫货。
        
        ## Output Format (JSON Only):
        {{
            "action": "BUY" | "WAIT",
            "reason": "简述理由 (50字以内)",
            "order_type": "MARKET" | "LIMIT", // MARKET: 现价, LIMIT: 限价
            "price": float // 如果是 LIMIT，请给出具体限价；如果是 MARKET，此值会被忽略但仍需提供
        }}
        """
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个追求尾盘套利的资深交易员。", preferred_provider=preferred_provider, api_key=api_key)
            
            import re
            clean_resp = re.sub(r'```json\s*|\s*```', '', response).strip()
            decision = json.loads(clean_resp, strict=False)
            if "reason" in decision:
                decision["reason"] = self._normalize_auto_reason(str(decision.get("reason") or ""))
            return decision
        except Exception as e:
            logger.error(f"Late session analysis failed for {symbol}: {e}")
            return {"action": "WAIT", "reason": "观望 AI 分析暂时故障，决定观望"}

    async def decide_late_session_strategy(self, candidates: List[Dict], positions: List[Dict], market_status: str, account_info: dict = None, force_trade: bool = False, **kwargs) -> dict:
        """
        [V3 旗舰版] 尾盘综合决策 (14:45): 整合选股与持仓，进行全局最优配比
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        if account_info is None:
            account_info = {}
            
        force_trade_context = ""
        if force_trade:
            # 检查仓位是否已满 (超过 95% 视为满仓)
            current_pos_pct = (account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100)
            if current_pos_pct > 95:
                force_trade_context = """
**【强制交易指令 (已豁免)】**
今日虽无买入，但检测到当前仓位已接近满仓 (>95%)，因此**豁免强制买入**。
请重点关注持仓管理，若有风险标的应执行卖出。
"""
            else:
                force_trade_context = """
**【强制买入指令 (FORCE BUY)】**
今日账户尚未进行任何买入操作。为了保持策略活跃度，**必须**在候选股中选择一只形态最佳、风险可控的标的进行开仓。
**严禁给出 "WAIT" (观望) 的建议。你必须买入一只股票。**
"""

        cand_desc = []
        for c in candidates:
            ts_code = c['ts_code']
            # 获取分值缓存
            score_str = ""
            cached_data = self._score_consistency_cache.get(ts_code)
            if cached_data:
                score_str = f" [系统评分: {cached_data[0]}分]"

            cand_desc.append(f"- {c['name']}({ts_code}){score_str}: 现价{c['price']:.2f}, 来源:{c['source']}, 理由:{c['reason']}")
            # 注入多周期原始数据 (在 trading_service 中已经注入到 c['raw_trading_context'] 了)
            if not c.get('raw_trading_context'):
                try:
                    from app.services.chat_service import chat_service
                    c['raw_trading_context'] = await asyncio.wait_for(chat_service.get_ai_trading_context(ts_code, cache_scope="analysis"), timeout=25.0)
                except Exception as e:
                    logger.warning(f"Late-session: Failed to fetch unified trading context for {ts_code}: {e}")
            if c.get('raw_trading_context'):
                cand_desc.append(f"  [原始数据]:\n{c['raw_trading_context']}\n")

        pos_desc = []
        for p in positions:
            ts_code = p['ts_code']
            # 获取分值缓存
            score_str = ""
            cached_data = self._score_consistency_cache.get(ts_code)
            if cached_data:
                score_str = f" [系统评分: {cached_data[0]}分]"

            pos_desc.append(f"- {p['name']}({ts_code}){score_str}: 盈亏{p['pnl_pct']:.2f}%, 现价{p['current_price']:.2f}, {'可卖' if p['can_sell'] else '不可卖'}")
            if not p.get('raw_trading_context'):
                try:
                    from app.services.chat_service import chat_service
                    # [优化] 延长外部超时至 30s
                    p['raw_trading_context'] = await asyncio.wait_for(chat_service.get_ai_trading_context(ts_code, cache_scope="analysis"), timeout=30.0)
                except Exception as e:
                    logger.warning(f"Late-session: Failed to fetch unified trading context for {ts_code}: {e}")
            if p.get('raw_trading_context'):
                pos_desc.append(f"  [原始数据]:\n{p['raw_trading_context']}\n")

        core_rules = prompt_builder.get_core_analysis_rules()
        auto_rules = prompt_builder.get_auto_decision_rules()

        prompt = f"""
# Role: 首席投资官 (CIO) - 尾盘终极决策

## Task: 
现在是 14:45 (尾盘)，你需要对目前的持仓和今日筛选出的潜力股进行全局审视，做出今日最后的交易决策。

{core_rules}
{auto_rules}

## Market Context (市场大势):
{market_status}

## Account Status (账户资金状况):
- 总资产: {account_info.get('total_assets', 0):.2f}
- 可用现金: {account_info.get('available_cash', 0):.2f}
- 当前总盈亏: {account_info.get('total_pnl_pct', 0):.2f}%
- 当前仓位占比: {(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%

## Portfolio Status (当前持仓):
{chr(10).join(pos_desc) if pos_desc else "空仓"}

## New Opportunities (今日候选潜力股):
{chr(10).join(cand_desc) if cand_desc else "无"}

## Decision Logic (决策逻辑):
1. **强制买入规则 (Priority 0)**: 
   - 如果收到【强制买入指令】，你**必须**生成一个 `new_buy_decision`，除非所有候选股都已停牌或跌停。
   - 即使所有候选股评分都不高，也要选择相对最好的一个进行防御性建仓（轻仓试错）。
   - 如果收到【豁免指令】，则无需强制买入，按常规逻辑处理。
2. **仓位平衡 (核心)**: 当前总仓位为 {(account_info.get('market_value', 0) / account_info.get('total_assets', 1) * 100):.1f}%。
3. **去弱留强**: 尾盘是确认强弱的最佳时机。
4. **月线优先**: 务必查看提供的原始月线数据。

## Output Format (Strict JSON):
{{
    "position_decisions": [
        {{ 
            "ts_code": "string", 
            "action": "SELL" | "REDUCE" | "HOLD", 
            "reason": "string",
            "order_type": "MARKET" | "LIMIT",
            "price": float
        }}
    ],
    "new_buy_decision": {{
        "action": "BUY" | "WAIT", // 如果有强制指令，严禁 WAIT
        "target_code": "string",
        "order_type": "MARKET" | "LIMIT",
        "price": float,
        "reason": "string"
    }},
    "market_view": "string"
}}
"""
        try:
            response, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个追求复利增长的首席投资官，只输出 JSON。", preferred_provider=preferred_provider, api_key=api_key)
            
            import re
            clean_json = re.sub(r'```json\s*|\s*```', '', response).strip()
            # 找到第一个 {
            start_idx = clean_json.find('{')
            if start_idx != -1:
                clean_json = clean_json[start_idx:]
            
            decision = json.loads(clean_json, strict=False)
            # 确保 position_decisions 中的每个决策都包含必需字段
            if 'position_decisions' in decision:
                for d in decision['position_decisions']:
                    if 'order_type' not in d: d['order_type'] = "MARKET"
                    if 'price' not in d:
                        # 尝试从候选人或持仓中找当前价，如果没传，设为 0
                        d['price'] = 0
                    if "reason" in d:
                        d["reason"] = self._normalize_auto_reason(str(d.get("reason") or ""))
            if "new_buy_decision" in decision and isinstance(decision.get("new_buy_decision"), dict):
                nb = decision["new_buy_decision"]
                if "reason" in nb:
                    nb["reason"] = self._normalize_auto_reason(str(nb.get("reason") or ""))
            if "market_view" in decision:
                decision["market_view"] = self._normalize_auto_reason(str(decision.get("market_view") or ""))
            return decision
        except Exception as e:
            logger.error(f"V3 尾盘决策分析失败: {e}")
            return {"position_decisions": [], "new_buy_decision": {"action": "WAIT"}, "market_view": "AI 异常，维持现状"}

    def calculate_smart_trailing_stop(self, kline_data: List[Dict[str, Any]], window: int = 20) -> Optional[float]:
        """
        计算智能移动止盈支撑线
        算法: 最近 N 根 K 线的成交量加权均价 (VWAP)
        """
        if not kline_data or len(kline_data) < 2:
            return None
            
        try:
            # 取最近 window 根
            recent_data = kline_data[-window:]
            
            total_vol = 0.0
            total_amount = 0.0
            
            for k in recent_data:
                vol = float(k.get('vol', 0))
                # 如果有 amount (成交额)，直接用 amount；否则用 vol * close 近似
                amount = float(k.get('amount', 0))
                if amount == 0:
                    amount = vol * float(k.get('close', 0))
                    
                total_vol += vol
                total_amount += amount
                
            if total_vol == 0:
                return float(recent_data[-1].get('close', 0)) * 0.95 # Fallback
                
            vwap = total_amount / total_vol
            return vwap
        except Exception as e:
            logger.error(f"Error calculating trailing stop: {e}")
            return None

    async def analyze_market_snapshot(self, snapshot_data: dict, force_refresh: bool = False, **kwargs) -> str:
        """
        AI 盘中市场深度分析 (每30分钟, 带缓存)
        """
        preferred_provider = kwargs.get('preferred_provider', "Xiaomi MiMo")
        api_key = kwargs.get('api_key')
        now_ts = time.time()
        # 如果缓存有效且未强制刷新，直接返回
        cache_ts = float(self._market_status_cache.get("timestamp") or 0.0)
        if not force_refresh and self._market_status_cache.get("content") and \
           (now_ts - cache_ts < 1800): # 30 mins
            return str(self._market_status_cache["content"])

        now_str = datetime.now().strftime('%H:%M')

        from app.services.learning_service import learning_service
        market_temperature = 50.0
        try:
            db_temp = SessionLocal()
            today_sentiment = db_temp.query(MarketSentiment).filter(
                MarketSentiment.date == datetime.now().date()
            ).first()
            if today_sentiment:
                market_temperature = today_sentiment.market_temperature
        except Exception as e:
            logger.warning(f"Failed to fetch market temperature: {e}")
        finally:
            if 'db_temp' in locals():
                db_temp.close()

        memory_context = await learning_service.get_reflection_memories("通用", market_temperature)
        if not memory_context:
            memory_context = "【策略反思与长期记忆 (基于历史成败提炼)】\n- 暂无匹配记忆"
        
        # 3. 搜索全网宏观资讯 (新增：重大国际国内新闻)
        macro_news = ""
        try:
            macro_news = await asyncio.wait_for(search_service.search_market_news(), timeout=15.0)
            if macro_news:
                macro_news = f"\n【实时重大宏观资讯 (Serper 全网检索)】\n{macro_news}"
        except Exception as e:
            logger.warning(f"AI 市场快照分析: 宏观新闻获取失败: {e}")

        prompt = f"""
        你是一位顶级A股操盘手，正在进行盘中即时盯盘。
        现在时间是 {now_str}。请根据以下市场快照数据，快速判断当前的市场情绪和资金流向，并给出操作决定。

        {memory_context}
        {macro_news}
        
        【市场快照】
        - 上证指数: {snapshot_data.get('sh_index', '未知')}
        - 创业板指: {snapshot_data.get('cy_index', '未知')}
        - 涨跌家数: 涨{snapshot_data.get('up_count', 0)} / 跌{snapshot_data.get('down_count', 0)}
        - 涨停家数: {snapshot_data.get('limit_up_count', 0)} (炸板率: {snapshot_data.get('bomb_ratio', 0)}%)
        - 成交量: {snapshot_data.get('total_volume', '未知')}
        - 领涨板块: {snapshot_data.get('top_sectors', '未知')}
        
        【任务要求】
        1. **情绪定性**: 当前市场是【极强/较强/震荡/分歧/退潮/冰点】中的哪一种？
        2. **风险点**: 盘中观察到的核心风险点（如：高位股杀跌、放量滞涨等）。
        3. **机会点**: 资金主要攻击的方向或潜伏的板块。
        4. **操作指南**: 当前应【积极进攻/观望为主/减仓防守】？
        5. **重大新闻响应**: 如果存在重大新闻（见 macro_news），请说明其对当前盘面或仓位的具体影响。
        
        请用专业、犀利、实战派的语言描述，不超过 200 字。
        """
        
        try:
            result, _label, _model = await self._call_ai_best_effort(prompt, system_prompt="你是一个犀利的盘中解说员，拒绝废话，直击要害。", preferred_provider=preferred_provider, api_key=api_key)
            result = self._normalize_auto_reason(result)
            # Update cache
            self._market_status_cache = {
                "timestamp": now_ts,
                "content": result,
                "data": snapshot_data
            }
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="market_snapshot",
                    ts_code=None,
                    strategy_name=None,
                    request_payload={"snapshot_data": snapshot_data, "force_refresh": force_refresh},
                    response_payload={"content": result},
                )
            except Exception as _e:
                logger.warning(f"AIReport save market_snapshot failed: {_e}")
            return result
        except Exception as e:
            logger.error(f"AI 市场快照分析失败: {e}")
            fallback = f"市场震荡中，涨{snapshot_data.get('up_count')}跌{snapshot_data.get('down_count')}，决定多看少动。"
            try:
                from app.services.ai_report_service import ai_report_service
                await ai_report_service.save_report(
                    analysis_type="market_snapshot",
                    ts_code=None,
                    strategy_name=None,
                    request_payload={"snapshot_data": snapshot_data, "force_refresh": force_refresh, "error": str(e)},
                    response_payload={"content": fallback},
                )
            except Exception as _e:
                logger.warning(f"AIReport save market_snapshot failed: {_e}")
            return fallback

# Global instance
analysis_service = AnalysisService()

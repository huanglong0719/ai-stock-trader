import asyncio
from typing import List, Dict, Any, Optional
from sqlalchemy import func, desc, or_
from datetime import date, datetime, timedelta
import pandas as pd
import json
from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, StrategyStats, MarketSentiment, PatternCase, OutcomeEvent, DailyBar, ReflectionMemory, TempMemory
from app.models.chat_models import ChatMessage
from app.repositories.trading_repository import TradingRepository
from app.services.logger import logger
from app.services.data_provider import data_provider
from app.services.ai.ai_client import ai_client
from app.core.config import settings

class LearningService:
    def __init__(self):
        pass

    async def perform_daily_learning(self):
        """
        每日离线学习任务 (通常在收盘后或夜间执行)
        1. 统计各策略的历史表现 (胜率、盈亏比)
        2. 更新 StrategyStats 表
        3. 执行 TRACKING 闭环：更新未执行计划的后续表现，生成 OutcomeEvent
        4. 提取历史交易模式 (PatternCase)
        5. 生成反思记忆 (ReflectionMemory)
        """
        logger.info("Starting daily offline learning task...")
        
        try:
            # 1. 策略统计
            await asyncio.to_thread(self._update_strategy_stats_sync)
            
            # 2. 追踪闭环 (Phase 1)
            await self._perform_tracking_update()
            
            # 3. 提取模式案例 (Phase 2)
            await self._extract_pattern_cases()
            
            # 4. 记忆生成 (Phase 3)
            await self._generate_memories()
            
            # 5. 涨停板复盘研究 (Phase 4)
            await self._analyze_daily_limit_ups()
            
            # 6. 记忆进化与归类 (Phase 5)
            await self._evolve_memories()

            await self.upsert_daily_temp_memories()
            
            logger.info("Daily offline learning task completed.")
        except Exception as e:
            logger.error(f"Error in daily learning: {e}", exc_info=True)

    async def _extract_pattern_cases(self):
        """
        将已平仓的交易计划转化为模式案例 (PatternCase)
        """
        logger.info("Extracting pattern cases from trading history...")
        db = SessionLocal()
        try:
            # 获取最近 30 天内已执行且已平仓的计划
            check_date = date.today() - timedelta(days=30)
            plans = db.query(TradingPlan).filter(
                TradingPlan.executed == True,
                TradingPlan.exit_price.isnot(None),
                TradingPlan.date >= check_date
            ).all()

            for plan in plans:
                # 检查是否已存在
                existing = db.query(PatternCase).filter(
                    PatternCase.ts_code == plan.ts_code,
                    PatternCase.trade_date == plan.date
                ).first()
                if existing:
                    continue

                # 获取当时的市场环境
                sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == plan.date).first()
                env = {
                    "market_temperature": sentiment.market_temperature if sentiment else 50.0,
                    "up_count": sentiment.up_count if sentiment else 0,
                    "limit_up_count": sentiment.limit_up_count if sentiment else 0,
                    "main_theme": sentiment.main_theme if sentiment else "未知"
                }

                # 计算收益率
                pnl_pct = plan.real_pnl_pct if plan.real_pnl_pct is not None else (plan.pnl_pct or 0.0)
                
                # 计算持仓天数 (简化：如果是 T+N 卖出，这里尝试从 track_data 估算)
                hold_days = 1
                if plan.track_days:
                    hold_days = plan.track_days

                case = PatternCase(
                    ts_code=plan.ts_code,
                    trade_date=plan.date,
                    pattern_type=plan.strategy_name,
                    market_environment=json.dumps(env, ensure_ascii=False),
                    profit_pct=pnl_pct,
                    hold_days=hold_days,
                    is_successful=(pnl_pct > 0)
                )
                db.add(case)
                logger.info(f"Extracted PatternCase for {plan.ts_code} on {plan.date}, Profit: {pnl_pct:.2f}%")
            
            db.commit()
        finally:
            db.close()

    async def _generate_memories(self):
        """
        从 OutcomeEvent 生成 ReflectionMemory
        """
        logger.info("Starting memory generation...")
        db = SessionLocal()
        try:
            # 1. 获取最近的 OutcomeEvent (Missed 或 RiskAvoided)
            # 仅处理最近 3 天产生的事件，避免重复处理历史
            check_start_date = date.today() - timedelta(days=3)
            
            events = db.query(OutcomeEvent).filter(
                OutcomeEvent.event_date >= check_start_date,
                OutcomeEvent.event_type.like("PLAN_TRACKING%"),
                or_(OutcomeEvent.evaluation_label == "Missed", OutcomeEvent.evaluation_label == "RiskAvoided")
            ).all()
            
            for event in events:
                # 检查是否已生成过记忆
                existing_memory = db.query(ReflectionMemory).filter(
                    ReflectionMemory.source_event_id == event.id
                ).first()
                
                if existing_memory:
                    continue
                    
                # 2. 准备上下文
                payload = json.loads(event.payload_json or "{}")
                plan_id = payload.get("plan_id")
                
                plan = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
                if not plan:
                    continue
                    
                # 获取当时的市场情绪 (如果有)
                sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == event.event_date).first()
                temp = sentiment.market_temperature if sentiment else 50.0
                
                # 3. 调用 AI 生成规则
                # [优化] 增加随机延迟，避免触发 API 限流 (Rate Limit Exceeded)
                await asyncio.sleep(1.5) 
                
                prompt = self._build_memory_prompt(event, plan, sentiment)
                
                try:
                    memory_content = await self._call_ai_for_memory(prompt)
                    if memory_content:
                        # 4. 保存记忆
                        bucket = "MID"
                        if temp < 30: bucket = "LOW"
                        elif temp > 70: bucket = "HIGH"
                        
                        memory = ReflectionMemory()
                        memory.condition = str(memory_content.get("condition") or "")
                        memory.action = str(memory_content.get("action") or "")
                        memory.reason = str(memory_content.get("reason") or "")
                        memory.strategy_name = str(plan.strategy_name or "")
                        memory.market_temperature_bucket = bucket
                        memory.source_event_id = int(event.id or 0)
                        memory.source_event_type = "PLAN_TRACKING"
                        db.add(memory)
                        logger.info(f"Generated Memory for event {event.id}: {memory_content}")
                        db.commit() # 逐条提交防止中断
                except Exception as e:
                    logger.error(f"Error generating memory for event {event.id}: {e}")

            # 5. 为失败交易案例生成反思记忆
            # 获取最近 3 天的失败案例（亏损案例）
            failed_patterns = db.query(PatternCase).filter(
                PatternCase.is_successful == False,
                PatternCase.created_at >= datetime.now() - timedelta(days=3)
            ).all()

            for pattern in failed_patterns:
                # 检查是否已生成过记忆
                existing_memory = db.query(ReflectionMemory).filter(
                    ReflectionMemory.source_event_id == pattern.id,
                    ReflectionMemory.source_event_type == "PATTERN_CASE"
                ).first()

                if existing_memory:
                    continue

                # 获取对应的交易计划
                plan = db.query(TradingPlan).filter(
                    TradingPlan.ts_code == pattern.ts_code,
                    TradingPlan.date == pattern.trade_date,
                    TradingPlan.executed == True
                ).first()

                if not plan:
                    continue

                # 获取当时的市场情绪
                sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == pattern.trade_date).first()
                temp = sentiment.market_temperature if sentiment else 50.0

                # 调用 AI 生成失败案例反思记忆
                # [优化] 增加随机延迟，避免触发 API 限流
                await asyncio.sleep(1.5)

                prompt = self._build_failed_pattern_prompt(pattern, plan, sentiment)

                try:
                    memory_content = await self._call_ai_for_memory(prompt)
                    if memory_content:
                        # 保存记忆
                        bucket = "MID"
                        if temp < 30: bucket = "LOW"
                        elif temp > 70: bucket = "HIGH"

                        memory = ReflectionMemory()
                        memory.condition = str(memory_content.get("condition") or "")
                        memory.action = str(memory_content.get("action") or "")
                        memory.reason = str(memory_content.get("reason") or "")
                        memory.strategy_name = str(pattern.pattern_type or "")
                        memory.market_temperature_bucket = bucket
                        memory.source_event_id = int(pattern.id or 0)
                        memory.source_event_type = "PATTERN_CASE"
                        db.add(memory)
                        logger.info(f"Generated Memory for failed pattern {pattern.id}: {memory_content}")
                        db.commit()
                except Exception as e:
                    logger.error(f"Error generating memory for failed pattern {pattern.id}: {e}")
                    
        finally:
            db.close()

    def _build_memory_prompt(self, event, plan, sentiment):
        return f"""
        请基于以下交易复盘案例，提炼一条通用的“反思记忆规则”。
        
        【你的核心交易哲学】
        1. **客观结构优先**：规则应侧重于技术结构（如量比、支撑、形态），而非单纯的市场情绪（温度）。不要生成“因为温度高就必须卖出”这种单纯基于恐惧的规则。
        2. **买点纪律**：鼓励“回调企稳”的买点，反对追高。
        
        【案例背景】
        - 策略: {plan.strategy_name}
        - 原始决策: {plan.ai_decision} (理由: {plan.reason})
        - 最终结果: {event.evaluation_label} (说明: {event.evaluation_json})
        - 市场环境: 温度={sentiment.market_temperature if sentiment else '未知'}, 描述={sentiment.summary if sentiment else '未知'}
        
        【要求】
        1. 必须抽象为“条件 -> 动作 -> 理由”的三段式结构。
        2. 不要包含具体的股票代码、价格数字。
        3. 规则必须是可执行的软约束。
        4. 格式必须是 JSON: {{"condition": "...", "action": "...", "reason": "..."}}
        
        【示例】
        Missed案例 (踏空):
        {{
            "condition": "[温度>60][板块效应强] 出现分歧回踩时",
            "action": "不要急于取消计划，可适当放宽回踩幅度",
            "reason": "强势市场中资金承接力强，容易深V反包"
        }}
        
        RiskAvoided案例 (避险):
        {{
            "condition": "[温度<30][炸板率高] 出现放量滞涨时",
            "action": "严格执行取消或降低仓位",
            "reason": "退潮期承接断裂，容易引发补跌"
        }}
        """

    def _build_failed_pattern_prompt(self, pattern, plan, sentiment):
        return f"""
        请基于以下失败交易案例，提炼一条通用的"反思记忆规则"。
        
        【你的核心交易哲学】
        1. **客观结构优先**：分析失败原因时，请优先从结构（如假突破、量价背离、支撑失效）找原因，而不要仅仅归咎于“市场不好”。
        2. **买点纪律**：如果失败是因为追高，请生成“等待回调企稳”的规则。
        
        【案例背景】
        - 策略: {pattern.pattern_type}
        - 交易日期: {pattern.trade_date}
        - 买入决策: {plan.ai_decision} (理由: {plan.reason})
        - 最终结果: 亏损 {pattern.profit_pct:.2f}%
        - 持有天数: {pattern.hold_days}天
        - 市场环境: 温度={sentiment.market_temperature if sentiment else '未知'}, 描述={sentiment.summary if sentiment else '未知'}
        
        【要求】
        1. 必须抽象为"条件 -> 动作 -> 理由"的三段式结构。
        2. 不要包含具体的股票代码、价格数字。
        3. 规则必须是可执行的软约束。
        4. 格式必须是 JSON: {{"condition": "...", "action": "...", "reason": "..."}}
        
        【示例】
        买入后直接亏损案例:
        {{
            "condition": "[温度<40][板块弱势] 买入后次日直接低开",
            "action": "立即止损，不要等待反弹",
            "reason": "弱势市场承接力弱，容易持续下跌"
        }}
        
        盈利转亏损案例:
        {{
            "condition": "[温度>70][连续涨停] 盈利超过10%后出现放量滞涨",
            "action": "及时止盈，不要贪婪",
            "reason": "高潮期获利盘涌出，容易A杀"
        }}
        """

    async def _call_ai_for_memory(self, prompt):
        candidates = []
        if ai_client.mimo_client:
            candidates.append((ai_client.mimo_client, settings.MIMO_MODEL))
        if getattr(ai_client, "nim_client", None):
            candidates.append((ai_client.nim_client, settings.NVIDIA_NIM_MODEL))
        if ai_client.ds_client:
            candidates.append((ai_client.ds_client, "deepseek-chat"))

        if not candidates:
            return None

        response = None
        for client, model in candidates:
            try:
                response = await asyncio.to_thread(
                    ai_client.call_ai_api,
                    client,
                    model,
                    prompt,
                    system_prompt="你是一个极度理性的交易风控官，负责总结交易经验。"
                )
                if response:
                    break
            except Exception:
                continue
        if not response:
            return None
        
        try:
            # 清理 Markdown
            import re
            clean_json = re.sub(r'```json\s*|\s*```', '', response).strip()
            start_idx = clean_json.find('{')
            end_idx = clean_json.rfind('}')
            if start_idx != -1 and end_idx != -1:
                clean_json = clean_json[start_idx:end_idx+1]
            
            return json.loads(clean_json, strict=False)
        except Exception as e:
            logger.error(f"Failed to parse AI memory response: {response}, error: {e}")
            return None

    async def _analyze_daily_limit_ups(self):
        """
        每日涨停板复盘研究
        研究方向：
        1. 涨停前状态 (Pre-limit-up Status)
        2. 涨停原因 (Reason)
        3. 共性总结 (Common Characteristics)
        """
        logger.info("Starting daily limit-up analysis...")
        from app.services.market.market_data_service import market_data_service
        from app.services.chat_service import chat_service
        
        # 1. 获取今日涨停股
        # 注意：这里调用 market_data_service 的新方法
        limit_up_codes = market_data_service.get_limit_up_stocks()
        
        # 如果缓存为空（可能没开盘或获取失败），尝试兜底获取（比如从 tdx_service 直接获取）
        # 但 get_limit_up_stocks 已经做了封装，这里信任它
        if not limit_up_codes:
            logger.info("No limit-up stocks found today (or data missing).")
            return

        # 2. 选取样本 (前 15 个，避免 AI token 溢出)
        # 理想情况下应该随机采样或按成交额排序，这里简单取前 15 个
        target_codes = limit_up_codes[:15]
        logger.info(f"Found {len(limit_up_codes)} limit-up stocks. Analyzing sample: {target_codes}")
        
        async def _analyze_one(code):
            try:
                # 获取上下文 (复用 chat_service，包含 K 线和基本面)
                # 限制上下文长度，只取最近的
                context = await chat_service.get_ai_trading_context(code)
                
                # 截断 context 以节省 token (只保留最近 10 天日线 + 核心指标)
                # 简单做法：直接传给 AI，让 AI 自己看 (chat_service 已经有压缩逻辑)
                
                prompt = f"""
请分析这只今日涨停的股票 ({code})。
基于提供的行情数据，简要回答：
1. 涨停前状态：涨停前的K线形态（如：底部盘整、上升中继、高位反包、超跌反弹等）。
2. 涨停原因：结合板块或技术面推测今日涨停原因。
请用一句话概括，格式："{code}: [状态] ... [原因] ..."
"""
                response = await asyncio.to_thread(ai_client.call_ai_best_effort, prompt, system_prompt="你是一个资深复盘专家。")
                return response
            except Exception as e:
                logger.error(f"Error analyzing limit-up {code}: {e}")
                return None

        # 并发执行分析
        tasks = [_analyze_one(code) for code in target_codes]
        # 限制并发数为 5，避免瞬间压垮 API
        results = []
        for i in range(0, len(tasks), 5):
            batch = tasks[i:i+5]
            batch_results = await asyncio.gather(*batch)
            results.extend([r for r in batch_results if r])
        
        if not results:
            logger.warning("No analysis results generated.")
            return

        # 3. 汇总共性
        summary_prompt = "以下是今日部分涨停股的分析样本：\n" + "\n".join(results) + "\n\n"
        summary_prompt += """
请综合以上样本，总结今日涨停板的共性特征 (Limit-Up Research Summary)：
1. 【形态共性】：涨停前主要是什么形态？(如：大部分是超跌反弹，还是高位突破？)
2. 【逻辑共性】：主要集中在哪些板块或概念？
3. 【情绪总结】：今日接力情绪如何？(如：首板为主，还是连板强势？)

输出一段简练的“今日涨停复盘总结”，约 100-200 字。
"""
        try:
            summary = await asyncio.to_thread(ai_client.call_ai_best_effort, summary_prompt, system_prompt="你是一个资深复盘专家。")
            logger.info(f"Limit-up Summary: {summary}")
            
            # 4. 保存总结到 ReflectionMemory
            db = SessionLocal()
            try:
                # 检查今日是否已存在，存在则更新
                today_str = date.today().strftime("%Y-%m-%d")
                condition_key = f"Limit-Up Research {today_str}"
                
                existing = db.query(ReflectionMemory).filter(
                    ReflectionMemory.strategy_name == "LIMIT_UP_RESEARCH",
                    ReflectionMemory.condition == condition_key
                ).first()
                
                if existing:
                    existing.reason = summary
                    existing.updated_at = datetime.now()
                else:
                    mem = ReflectionMemory(
                        strategy_name="LIMIT_UP_RESEARCH",
                        market_temperature_bucket="ALL",
                        condition=condition_key,
                        action="RESEARCH",
                        reason=summary,
                        weight=1.0,
                        source_event_id=0,
                        source_event_type="DAILY_RESEARCH",
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    db.add(mem)
                db.commit()
                logger.info("Limit-up research summary saved to ReflectionMemory.")
            except Exception as e:
                logger.error(f"Error saving limit-up summary: {e}")
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error generating limit-up summary: {e}")

    async def _evolve_memories(self):
        """
        记忆进化与归类 (Memory Evolution)
        让 AI 交易员自我审查历史记忆：
        1. 归类 (Categorization)
        2. 评分 (Scoring)
        3. 剔除/优化 (Pruning/Refining)
        """
        logger.info("Starting memory evolution...")
        db = SessionLocal()
        try:
            # 1. 获取所有活跃记忆
            memories = db.query(ReflectionMemory).filter(
                ReflectionMemory.is_active == True
            ).order_by(desc(ReflectionMemory.created_at)).limit(50).all() # 限制处理数量，优先处理最新的
            
            if not memories:
                logger.info("No active memories to evolve.")
                return

            # 2. 构建审查 Prompt
            memory_list_text = ""
            for mem in memories:
                memory_list_text += f"""
ID: {mem.id}
Condition: {mem.condition}
Action: {mem.action}
Reason: {mem.reason}
Current Weight: {mem.weight}
---"""

            prompt = f"""
作为 AI 交易员的自我进化模块，请审查以下历史交易记忆（规则）。
你的任务是：
1. 【归类】：为每条规则打上标签（如：风险控制、抄底技巧、追涨策略、大盘研判、板块轮动、情绪周期）。
2. 【评分】：评估规则的有效性和普适性 (0-10分)。
   - 对于系统自动生成的规则：低于 4 分将被标记为剔除。
   - 对于人工植入的规则 (MANUAL_INJECTION)：请基于近期市场表现评估其是否需要微调。如果发现有更好的操作细节（如止损位调整、买点优化），请在 refined_reason 中提出建议，但不要直接修改核心逻辑。
3. 【进化】：如果规则描述模糊或有误，请提供优化后的描述。

【输入记忆列表】
{memory_list_text}

【输出格式】
必须是 JSON 列表，包含所有 ID 的处理结果：
[
    {{
        "id": 123,
        "category": "风险控制",
        "score": 8.5,
        "action": "KEEP",  // KEEP, DELETE, UPDATE
        "refined_condition": "...", // 仅在 action=UPDATE 时提供
        "refined_action": "...",
        "refined_reason": "..."
    }},
    ...
]
"""
            # 3. 调用 AI
            response = await asyncio.to_thread(ai_client.call_ai_best_effort, prompt, system_prompt="你是一个严谨的交易系统架构师。")
            if not response:
                return

            # 4. 解析并执行更新
            try:
                import re
                clean_json = re.sub(r'```json\s*|\s*```', '', response).strip()
                start_idx = clean_json.find('[')
                end_idx = clean_json.rfind(']')
                if start_idx != -1 and end_idx != -1:
                    clean_json = clean_json[start_idx:end_idx+1]
                
                evolutions = json.loads(clean_json)
                
                for item in evolutions:
                    mem_id = item.get("id")
                    category = item.get("category")
                    score = float(item.get("score") or 5.0)
                    action = item.get("action")
                    
                    memory: Optional[ReflectionMemory] = db.query(ReflectionMemory).filter(ReflectionMemory.id == mem_id).first()
                    if not memory:
                        continue
                    
                    # 更新分类和权重
                    if category:
                        current_strat = memory.strategy_name or ""
                        # 避免重复添加前缀
                        if not current_strat.startswith(f"[{category}]") and "]" not in current_strat[:10]:
                            memory.strategy_name = f"[{category}] {current_strat}"
                    
                    # [优化] 人工植入的记忆权重始终较高，但会根据 AI 评分微调 (9.0 - 10.0)
                    if memory.source_event_type == "MANUAL_INJECTION":
                        base_weight = 9.0
                        adjusted_score = max(0, min(1.0, (score - 5) / 5)) # 将 5-10 分映射到 0-1
                        memory.weight = base_weight + adjusted_score
                    else:
                        memory.weight = score / 10.0 # 归一化到 0-1
                    
                    # 执行动作
                    if memory.source_event_type == "MANUAL_INJECTION":
                        # 人工记忆不仅 KEEP，还允许 UPDATE 优化
                        if action == "UPDATE":
                             if item.get("refined_condition"):
                                memory.condition = item.get("refined_condition")
                             if item.get("refined_action"):
                                memory.action = item.get("refined_action")
                             if item.get("refined_reason"):
                                # 保留原人工植入标记，追加 AI 优化建议
                                original_reason = memory.reason
                                new_suggestion = item.get("refined_reason")
                                if "[AI优化]" not in original_reason:
                                    memory.reason = f"{original_reason} [AI优化]: {new_suggestion}"
                                else:
                                    # 替换旧的 AI 优化
                                    base_reason = original_reason.split("[AI优化]")[0].strip()
                                    memory.reason = f"{base_reason} [AI优化]: {new_suggestion}"
                             logger.info(f"Manual Memory {mem_id} refined by AI (Score: {score})")
                        else:
                             logger.info(f"Manual Memory {mem_id} kept (Score: {score})")
                    else:
                        # 普通自动记忆
                        if action == "DELETE" or score < 4.0:
                            memory.is_active = False
                            logger.info(f"Memory {mem_id} deactivated (Score: {score})")
                        elif action == "UPDATE":
                            if item.get("refined_condition"):
                                memory.condition = item.get("refined_condition")
                            if item.get("refined_action"):
                                memory.action = item.get("refined_action")
                            if item.get("refined_reason"):
                                memory.reason = item.get("refined_reason")
                            logger.info(f"Memory {mem_id} updated/refined")
                        
                db.commit()
                logger.info("Memory evolution completed.")
                
            except Exception as e:
                logger.error(f"Error parsing memory evolution response: {e}")

        except Exception as e:
            logger.error(f"Error in memory evolution: {e}")
        finally:
            db.close()


    async def _perform_tracking_update(self):
        """
        执行追踪闭环：
        1. 扫描所有 executed=False 且 (track_status=None 或 TRACKING) 的计划
        2. 获取后续 T+1 ~ T+5 的行情
        3. 更新 track_data
        4. 若满足 T+3/T+5 条件，生成 OutcomeEvent 并打标 (Missed/RiskAvoided/Neutral)
        """
        logger.info("Starting tracking update...")
        db = SessionLocal()
        try:
            # 1. 找出需要追踪的计划
            # 条件：未执行，且 (状态为TRACKING 或 (状态为NONE/NULL 且日期在最近10天内))
            # 限制最近10天是为了避免扫描太久远的历史
            check_start_date = date.today() - timedelta(days=10)
            
            plans = db.query(TradingPlan).filter(
                TradingPlan.executed == False,
                or_(
                    TradingPlan.track_status == "TRACKING",
                    (TradingPlan.track_status == None) & (TradingPlan.date >= check_start_date),
                    (TradingPlan.track_status == "NONE") & (TradingPlan.date >= check_start_date)
                )
            ).all()

            logger.info(f"Found {len(plans)} plans to track.")
            
            for plan in plans:
                try:
                    await self._process_single_plan_tracking(db, plan)
                except Exception as e:
                    logger.error(f"Error tracking plan {plan.id}: {e}")
                    
            db.commit()
            
        finally:
            db.close()

    async def _process_single_plan_tracking(self, db, plan: TradingPlan):
        # 确定基准价格
        base_price = plan.buy_price_limit
        if not base_price or base_price <= 0:
            base_price = plan.decision_price
        
        # 如果还是没有基准价格，尝试获取最近一个交易日的收盘价作为基准 (兜底)
        # 兼容休息日生成的计划 (plan.date 可能是周末，需取前一个交易日收盘价)
        if not base_price or base_price <= 0:
            daily_bar = db.query(DailyBar).filter(
                DailyBar.ts_code == plan.ts_code,
                DailyBar.trade_date <= plan.date
            ).order_by(DailyBar.trade_date.desc()).first()
            
            if daily_bar:
                base_price = daily_bar.close
            else:
                logger.warning(f"Plan {plan.id} ({plan.ts_code} on {plan.date}) has no base price and no historical bar found. Skipping.")
                return

        # 获取计划日期之后的行情 (T+1 ~ T+5)
        # 限制获取最近 10 个交易日的数据，足以覆盖 T+5
        bars = db.query(DailyBar).filter(
            DailyBar.ts_code == plan.ts_code,
            DailyBar.trade_date > plan.date
        ).order_by(DailyBar.trade_date.asc()).limit(10).all()
        
        if not bars:
            return

        # 初始化或加载 track_data
        track_data = {}
        if plan.track_data:
            try:
                track_data = json.loads(plan.track_data)
            except:
                track_data = {}
        
        # 更新每一天的数据
        highest_gain = -999.0
        max_drop = 999.0
        
        days_tracked = 0
        for i, bar in enumerate(bars):
            days_tracked = i + 1
            if days_tracked > 5: # 只追踪 T+5
                break
                
            # 计算相对于基准价格的涨跌幅
            # 使用最高/最低价来计算最大潜在收益/风险
            high_pct = (bar.high - base_price) / base_price * 100
            low_pct = (bar.low - base_price) / base_price * 100
            close_pct = (bar.close - base_price) / base_price * 100
            
            day_key = f"T+{days_tracked}"
            track_data[day_key] = {
                "date": bar.trade_date.strftime("%Y-%m-%d"),
                "high_pct": round(high_pct, 2),
                "low_pct": round(low_pct, 2),
                "close_pct": round(close_pct, 2)
            }
            
            # 更新区间极值 (仅在 T+1 ~ T+3 窗口内用于判定 Missed/RiskAvoided，或者 T+5 也可以)
            # 文档提到 "T+3 最高涨幅"
            if days_tracked <= 3:
                highest_gain = max(highest_gain, high_pct)
                max_drop = min(max_drop, low_pct)
            elif days_tracked <= 5:
                # 也可以继续更新，视具体规则而定，这里暂且记录全周期的
                highest_gain = max(highest_gain, high_pct)
                max_drop = min(max_drop, low_pct)

        # 更新 Plan 状态
        plan.track_data = json.dumps(track_data)
        plan.track_days = days_tracked
        plan.track_status = "TRACKING"
        
        # 判定逻辑 (Phase 1 规则)
        # Missed（踏空）：取消后 T+3 最高涨幅 ≥ +6%
        # RiskAvoided（避险成功）：取消后 T+3 最大跌幅 ≤ -4%
        # Neutral：无显著波动或不满足阈值
        
        label = "Neutral"
        is_finalized = False
        
        # 提前结算逻辑：如果T+1就出现极端行情（涨停/跌停），提前结算
        if days_tracked >= 1:
            # T+1就涨停：提前判定为Missed
            if highest_gain >= 9.5:
                label = "Missed"
                is_finalized = True
                logger.info(f"Early settlement for plan {plan.id}: T+{days_tracked} limit-up (gain={highest_gain:.2f}%)")
            # T+1就跌停：提前判定为RiskAvoided
            elif max_drop <= -9.5:
                label = "RiskAvoided"
                is_finalized = True
                logger.info(f"Early settlement for plan {plan.id}: T+{days_tracked} limit-down (drop={max_drop:.2f}%)")
        
        # 如果没有提前结算，按原T+3/T+5逻辑
        if not is_finalized and days_tracked >= 3:
            if highest_gain >= 6.0:
                label = "Missed"
                is_finalized = True
            elif max_drop <= -4.0:
                label = "RiskAvoided"
                is_finalized = True
            elif days_tracked >= 5:
                label = "Neutral"
                is_finalized = True
        
        # 如果已经完结，或者虽然没到T+3但已经触发了某些极端条件(比如T+1就涨停)，也可以提前结算
        # 这里严格按照 T+3 判定或 T+5 完结
        
        if is_finalized:
            plan.track_status = "FINISHED"
            plan.ai_evaluation = label
            
            # 生成 OutcomeEvent
            # 使用 plan.id 作为唯一标识，避免同一天同一股票多个计划导致冲突
            event_type_key = f"PLAN_TRACKING:{plan.id}"
            
            existing_event = db.query(OutcomeEvent).filter(
                OutcomeEvent.ts_code == plan.ts_code,
                OutcomeEvent.event_type == event_type_key,
                OutcomeEvent.event_date == plan.date
            ).first()
            
            if not existing_event:
                event = OutcomeEvent(
                    ts_code=plan.ts_code,
                    event_type=event_type_key,
                    event_date=plan.date,
                    payload_json=json.dumps({
                        "plan_id": plan.id,
                        "strategy": plan.strategy_name,
                        "base_price": base_price,
                        "highest_gain_t3": highest_gain,
                        "max_drop_t3": max_drop,
                        "track_data": track_data
                    }),
                    evaluation_label=label,
                    evaluation_json=json.dumps({
                        "reason": f"T+{days_tracked} max_gain={highest_gain:.2f}%, max_drop={max_drop:.2f}%"
                    })
                )
                db.add(event)
                logger.info(f"Generated OutcomeEvent for plan {plan.id}: {label}")


    def _update_strategy_stats_sync(self):
        """
        全量统计策略表现并更新 StrategyStats (同步版，供 to_thread 调用)
        """
        db = SessionLocal()
        try:
            self._update_strategy_stats(db)
        finally:
            db.close()

    def _update_strategy_stats(self, db):
        """
        全量统计策略表现并更新 StrategyStats
        """
        # 1. 获取所有已平仓的计划
        plans = db.query(TradingPlan).filter(
            TradingPlan.executed == True,
            TradingPlan.exit_price.isnot(None)
        ).all()
        
        if not plans:
            logger.info("No executed plans found for statistics.")
            return

        # 2. 按策略分组统计
        stats_map: Dict[str, Dict[str, Any]] = {}
        
        for plan in plans:
            strategy = plan.strategy_name
            if not strategy: continue
            
            if strategy not in stats_map:
                stats_map[strategy] = {
                    'total_trades': 0,
                    'win_trades': 0,
                    'total_pnl_pct': 0.0,
                    'pnl_list': []
                }
            
            s = stats_map[strategy]
            s['total_trades'] = s['total_trades'] + 1
            if plan.real_pnl and plan.real_pnl > 0:
                s['win_trades'] = s['win_trades'] + 1
            elif not plan.real_pnl and plan.pnl_pct and plan.pnl_pct > 0:
                 # 兼容逻辑
                 s['win_trades'] = s['win_trades'] + 1
            
            pnl = plan.real_pnl_pct if plan.real_pnl_pct is not None else plan.pnl_pct
            if pnl is None: pnl = 0.0
            
            s['total_pnl_pct'] = s['total_pnl_pct'] + pnl
            if isinstance(s['pnl_list'], list):
                s['pnl_list'].append(pnl)

        # 3. 计算指标并写入数据库
        for strategy, data in stats_map.items():
            total = data['total_trades']
            if total == 0: continue
            
            win_rate = (data['win_trades'] / total) * 100
            avg_pnl = data['total_pnl_pct'] / total
            
            # 计算最大回撤 (基于单利累计收益曲线)
            cum_pnl = [0.0]
            current_cum = 0.0
            pnl_list = data.get('pnl_list', [])
            if isinstance(pnl_list, list):
                for p in pnl_list:
                    current_cum += p
                    cum_pnl.append(current_cum)
            
            max_dd = 0.0
            peak = -999999.0
            for val in cum_pnl:
                if val > peak:
                    peak = val
                dd = peak - val
                if dd > max_dd:
                    max_dd = dd
            
            # 更新或创建 StrategyStats
            stat_record = db.query(StrategyStats).filter(StrategyStats.strategy_name == strategy).first()
            if not stat_record:
                stat_record = StrategyStats(strategy_name=strategy)
                db.add(stat_record)
            
            stat_record.total_trades = total
            stat_record.win_trades = data['win_trades']
            stat_record.total_pnl_pct = data['total_pnl_pct']
            stat_record.win_rate = win_rate
            stat_record.avg_pnl_pct = avg_pnl
            stat_record.max_drawdown = max_dd
            stat_record.updated_at = datetime.now()
            
            logger.info(f"Strategy Stats Updated: {strategy} | WinRate: {win_rate:.1f}% | AvgPnL: {avg_pnl:.2f}% | MaxDD: {max_dd:.2f}%")
        
        db.commit()

    async def get_strategy_context(self, strategy_name: str, market_temperature: float = None) -> str:
        """
        为 AI 提供策略表现上下文 (Prompt Injection)
        """
        def _get_context():
            db = SessionLocal()
            try:
                stat = db.query(StrategyStats).filter(StrategyStats.strategy_name == strategy_name).first()
                if not stat:
                    return ""
                
                win_rate = float(stat.win_rate or 0.0)
                avg_pnl = float(stat.avg_pnl_pct or 0.0)
                context = f"\n[历史数据] 策略【{strategy_name}】历史胜率 {win_rate:.1f}%, 平均盈亏 {avg_pnl:.2f}%."
                
                # 简单的动态上下文 (Phase 2)
                if market_temperature is not None:
                    if market_temperature < 30 and win_rate < 40:
                        context += " 注意：在低情绪市场下该策略历史表现较差，建议谨慎或降低仓位。"
                    elif market_temperature > 70 and win_rate > 60:
                        context += " 注意：在市场高潮期该策略表现优异。"
                
                return context
            finally:
                db.close()
        
        return await asyncio.to_thread(_get_context)

    async def get_successful_pattern_context(self, strategy_name: str, limit: int = 3) -> str:
        """
        获取成功模式的上下文 (Prompt Injection)
        返回该策略下盈利最高的几个成功案例的 K 线形态和市场环境
        """
        def _get_patterns():
            db = SessionLocal()
            try:
                # [核心修正] 提高成功案例门槛
                # 原为 3.0%，现改为 5.0%，过滤掉微利和运气成分
                # 3% 在 A 股往往只是一个日内波动，不足以证明策略的有效性
                patterns = db.query(PatternCase).filter(
                    PatternCase.pattern_type == strategy_name,
                    PatternCase.profit_pct >= 5.0
                ).order_by(desc(PatternCase.profit_pct)).limit(limit).all()
                
                if not patterns:
                    return ""
                
                context = "\n[成功模式参考] 该策略历史上有以下高盈利案例，可作为参考：\n"
                for i, p in enumerate(patterns, 1):
                    context += f"\n案例{i}: 交易日期 {p.trade_date}, 盈利 {p.profit_pct:.2f}%, 持有 {p.hold_days} 天\n"
                    
                    # 尝试解析市场环境
                    if p.market_environment:
                        try:
                            env = json.loads(p.market_environment)
                            if env.get('market_temperature'):
                                context += f"  - 市场温度: {env.get('market_temperature')}\n"
                            if env.get('limit_up_count'):
                                context += f"  - 涨停数量: {env.get('limit_up_count')}\n"
                        except:
                            pass
                
                return context
            finally:
                db.close()
        
        return await asyncio.to_thread(_get_patterns)

    async def get_failed_pattern_context(self, strategy_name: str, limit: int = 3) -> str:
        """
        获取失败模式的上下文 (Prompt Injection)
        返回该策略下亏损最大的几个失败案例的 K 线形态和市场环境
        """
        def _get_patterns():
            db = SessionLocal()
            try:
                patterns = db.query(PatternCase).filter(
                    PatternCase.pattern_type == strategy_name,
                    PatternCase.profit_pct < 0.0
                ).order_by(PatternCase.profit_pct.asc()).limit(limit).all()
                
                if not patterns:
                    return ""
                
                context = "\n[失败案例参考] 该策略历史上有以下亏损案例，需要避免类似情况：\n"
                for i, p in enumerate(patterns, 1):
                    context += f"\n案例{i}: 交易日期 {p.trade_date}, 亏损 {p.profit_pct:.2f}%, 持有 {p.hold_days} 天\n"
                    
                    # 尝试解析市场环境
                    if p.market_environment:
                        try:
                            env = json.loads(p.market_environment)
                            if env.get('market_temperature'):
                                context += f"  - 市场温度: {env.get('market_temperature')}\n"
                            if env.get('limit_up_count'):
                                context += f"  - 涨停数量: {env.get('limit_up_count')}\n"
                        except:
                            pass
                
                return context
            finally:
                db.close()
        
        return await asyncio.to_thread(_get_patterns)

    async def add_manual_reflection_memory(self, strategy_name: str, condition: str, action: str, reason: str, market_temperature_bucket: str = "MID") -> bool:
        """
        [新功能] 手动植入交易记忆 (上帝视角注入)
        允许用户直接向 AI 灌输经验，而非等待自动学习
        """
        def _add_memory():
            db = SessionLocal()
            try:
                # 检查是否存在相似记忆
                existing = db.query(ReflectionMemory).filter(
                    ReflectionMemory.strategy_name == strategy_name,
                    ReflectionMemory.condition == condition,
                    ReflectionMemory.action == action
                ).first()
                
                if existing:
                    # 如果存在，更新权重和理由
                    existing.reason = reason
                    existing.weight = 10.0 # 手动植入的权重设为最高
                    existing.is_active = True
                    logger.info(f"Updated existing manual memory for {strategy_name}")
                else:
                    memory = ReflectionMemory(
                        strategy_name=strategy_name,
                        condition=condition,
                        action=action,
                        reason=reason,
                        market_temperature_bucket=market_temperature_bucket,
                        weight=10.0, # 手动植入默认为最高权重
                        source_event_id=0,
                        source_event_type="MANUAL_INJECTION",
                        is_active=True
                    )
                    db.add(memory)
                    logger.info(f"Injected new manual memory for {strategy_name}")
                
                db.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding manual memory: {e}")
                return False
            finally:
                db.close()
                
        return await asyncio.to_thread(_add_memory)

    async def export_all_memories(self, format_type: str = "json") -> str:
        """导出所有活跃记忆"""
        def _export():
            db = SessionLocal()
            try:
                memories = db.query(ReflectionMemory).filter(ReflectionMemory.is_active == True).all()
                data = []
                for m in memories:
                    data.append({
                        "strategy_name": m.strategy_name,
                        "condition": m.condition,
                        "action": m.action,
                        "reason": m.reason,
                        "market_temperature_bucket": m.market_temperature_bucket,
                        "weight": m.weight,
                        "source_event_type": m.source_event_type,
                        "created_at": m.created_at.isoformat() if m.created_at else None
                    })
                
                if format_type.lower() == "csv":
                    df = pd.DataFrame(data)
                    return df.to_csv(index=False)
                else:
                    return json.dumps(data, ensure_ascii=False, indent=2)
            finally:
                db.close()
        return await asyncio.to_thread(_export)

    async def import_memories(self, content: str, format_type: str = "json") -> Dict[str, int]:
        """导入记忆 (合并模式)"""
        def _import():
            db = SessionLocal()
            try:
                if format_type.lower() == "csv":
                    from io import StringIO
                    df = pd.read_csv(StringIO(content))
                    # Fill NaN with None/empty string to avoid validation errors
                    df = df.fillna("")
                    data = df.to_dict(orient="records")
                else:
                    data = json.loads(content)
                
                added = 0
                updated = 0
                skipped = 0
                
                for item in data:
                    # 验证必要字段
                    if not all(k in item and item[k] for k in ["strategy_name", "condition", "action", "reason"]):
                        skipped += 1
                        continue
                        
                    # 查重
                    existing = db.query(ReflectionMemory).filter(
                        ReflectionMemory.strategy_name == item["strategy_name"],
                        ReflectionMemory.condition == item["condition"],
                        ReflectionMemory.action == item["action"]
                    ).first()
                    
                    if existing:
                        # 策略：如果导入的是人工记忆，或者权重更高，则更新
                        new_weight = float(item.get("weight") or 5.0)
                        if item.get("source_event_type") == "MANUAL_INJECTION" or new_weight > (existing.weight or 0):
                            existing.reason = item["reason"]
                            existing.weight = new_weight
                            existing.market_temperature_bucket = item.get("market_temperature_bucket", "MID")
                            existing.is_active = True
                            updated += 1
                        else:
                            skipped += 1
                    else:
                        mem = ReflectionMemory(
                            strategy_name=item["strategy_name"],
                            condition=item["condition"],
                            action=item["action"],
                            reason=item["reason"],
                            market_temperature_bucket=item.get("market_temperature_bucket", "MID"),
                            weight=float(item.get("weight") or 5.0),
                            source_event_type=item.get("source_event_type", "IMPORTED"),
                            source_event_id=0,
                            is_active=True,
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        db.add(mem)
                        added += 1
                
                db.commit()
                return {"added": added, "updated": updated, "skipped": skipped}
            except Exception as e:
                logger.error(f"Import failed: {e}")
                raise e
            finally:
                db.close()
        return await asyncio.to_thread(_import)

    async def get_reflection_memories(self, strategy_name: str, market_temperature: float = None, limit: int = 5) -> str:
        """
        获取针对特定策略和市场环境的反思记忆 (Prompt Injection)
        """
        def _get_memories():
            db = SessionLocal()
            try:
                bucket = "MID"
                if market_temperature is not None:
                    if market_temperature < 30: bucket = "LOW"
                    elif market_temperature > 70: bucket = "HIGH"
                
                query = db.query(ReflectionMemory).filter(
                    ReflectionMemory.strategy_name == strategy_name,
                    ReflectionMemory.is_active == True
                )
                
                # 优先获取手动植入的记忆 (MANUAL_INJECTION)
                manual_memories = query.filter(
                    ReflectionMemory.source_event_type == "MANUAL_INJECTION"
                ).limit(limit).all()
                
                # 优先获取匹配分桶的记忆，如果没有则获取全部
                bucket_memories = query.filter(ReflectionMemory.market_temperature_bucket == bucket).order_by(desc(ReflectionMemory.weight), desc(ReflectionMemory.created_at)).limit(limit).all()
                
                # 合并记忆 (去重)
                seen_ids = set([m.id for m in manual_memories])
                final_memories = list(manual_memories)
                
                for m in bucket_memories:
                    if m.id not in seen_ids and len(final_memories) < limit:
                        final_memories.append(m)
                        seen_ids.add(m.id)
                
                if not final_memories:
                    # 如果没有分桶匹配，则获取通用的
                    bucket_memories = query.order_by(desc(ReflectionMemory.weight), desc(ReflectionMemory.created_at)).limit(limit).all()
                    for m in bucket_memories:
                        if m.id not in seen_ids and len(final_memories) < limit:
                            final_memories.append(m)
                
                if not final_memories:
                    return ""
                
                context = "\n【策略反思与长期记忆 (包含人工植入与历史提炼)】\n"
                for i, m in enumerate(final_memories, 1):
                    prefix = "[人工置顶] " if m.source_event_type == "MANUAL_INJECTION" else ""
                    context += f"- 记忆{i}: {prefix}当【{m.condition}】时，建议【{m.action}】，原因：{m.reason}\n"
                
                return context
            finally:
                db.close()
        
        return await asyncio.to_thread(_get_memories)

    async def upsert_daily_temp_memories(self, target_date: Optional[date] = None, keep_days: int = 7) -> Dict[str, int]:
        def _upsert():
            db = SessionLocal()
            try:
                d = target_date or date.today()
                start_dt = datetime.combine(d, datetime.min.time())
                end_dt = datetime.combine(d, datetime.max.time())

                msgs = db.query(ChatMessage).filter(
                    ChatMessage.created_at >= start_dt,
                    ChatMessage.created_at <= end_dt
                ).order_by(ChatMessage.created_at.asc()).all()
                lines: List[str] = []
                for m in msgs:
                    role = "用户" if (m.role or "").lower() == "user" else "AI"
                    content = str(m.content or "").strip()
                    if not content:
                        continue
                    if len(content) > 120:
                        content = content[:120] + "..."
                    lines.append(f"{role}: {content}")
                chat_content = "\n".join(lines[-30:]) if lines else "无聊天记录"

                plans = db.query(TradingPlan).filter(TradingPlan.date == d).all()
                entrust_lines: List[str] = []
                cancel_lines: List[str] = []
                for p in plans:
                    action = TradingRepository._infer_plan_action(p)
                    review_txt = str(p.review_content or "")
                    price = p.limit_price or p.buy_price_limit or p.decision_price
                    price_text = f"{float(price):.2f}" if price and float(price) > 0 else "无"
                    base = f"{p.ts_code} {action} @ {price_text}"
                    if (p.track_status or "").upper() == "CANCELLED" or "撤单" in review_txt or "取消" in review_txt:
                        tail = review_txt[:80] if review_txt else ""
                        cancel_lines.append(f"{base} {tail}".strip())
                        continue
                    if float(p.frozen_amount or 0.0) > 0 or "AI挂单" in review_txt or "AI确认" in review_txt or "买入待成" in review_txt:
                        tail = review_txt[:80] if review_txt else ""
                        entrust_lines.append(f"{base} {tail}".strip())

                entrust_content = "\n".join(entrust_lines[:40]) if entrust_lines else "无挂单记录"
                cancel_content = "\n".join(cancel_lines[:40]) if cancel_lines else "无撤单记录"

                for category, content in [
                    ("chat", chat_content),
                    ("order_entrust", entrust_content),
                    ("order_cancel", cancel_content),
                ]:
                    existing = db.query(TempMemory).filter(
                        TempMemory.memory_date == d,
                        TempMemory.category == category
                    ).first()
                    if existing:
                        existing.content = content
                        existing.updated_at = datetime.now()
                    else:
                        db.add(TempMemory(
                            memory_date=d,
                            category=category,
                            content=content,
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        ))

                expire_date = d - timedelta(days=keep_days)
                db.query(TempMemory).filter(TempMemory.memory_date < expire_date).delete()
                db.commit()
                return {
                    "chat": len(lines),
                    "entrust": len(entrust_lines),
                    "cancel": len(cancel_lines)
                }
            finally:
                db.close()
        return await asyncio.to_thread(_upsert)

    async def get_temp_memories(self, days: int = 3, limit_chars: int = 1600) -> str:
        def _load():
            db = SessionLocal()
            try:
                start_date = date.today() - timedelta(days=max(days, 1) - 1)
                rows = db.query(TempMemory).filter(
                    TempMemory.memory_date >= start_date
                ).order_by(TempMemory.memory_date.desc(), TempMemory.category.asc()).all()
                if not rows:
                    return ""
                lines: List[str] = ["【临时记忆】"]
                for r in rows:
                    label = "聊天"
                    if r.category == "order_entrust":
                        label = "挂单"
                    elif r.category == "order_cancel":
                        label = "撤单"
                    content = str(r.content or "").strip()
                    if not content:
                        continue
                    if len(content) > 600:
                        content = content[:600] + "..."
                    lines.append(f"- {r.memory_date} {label}: {content}")
                text = "\n".join(lines)
                if len(text) > limit_chars:
                    text = text[:limit_chars] + "..."
                return text
            finally:
                db.close()
        return await asyncio.to_thread(_load)

    async def deactivate_conflicting_memories(self, style: str = "aggressive") -> int:
        keywords = []
        if style == "aggressive":
            keywords = [
                "不追", "观望", "等待", "谨慎", "保守", "回避", "避险",
                "不要追", "不宜追", "不要买", "不做", "减少操作", "控制仓位", "谨慎参与",
                "宁可错过", "不轻易", "观望为主", "防守"
            ]

        if not keywords:
            return 0

        def _deactivate():
            db = SessionLocal()
            try:
                query = db.query(ReflectionMemory).filter(ReflectionMemory.is_active == True)
                filters = []
                for kw in keywords:
                    filters.append(ReflectionMemory.condition.contains(kw))
                    filters.append(ReflectionMemory.action.contains(kw))
                    filters.append(ReflectionMemory.reason.contains(kw))
                memories = query.filter(or_(*filters)).all()
                for mem in memories:
                    mem.is_active = False
                db.commit()
                return len(memories)
            finally:
                db.close()

        return await asyncio.to_thread(_deactivate)

    async def get_reflection_memories_by_keywords(self, strategy_names: List[str], keywords: List[str], limit: int = 5, source_event_type: Optional[str] = None) -> str:
        def _get_memories():
            if not strategy_names or not keywords:
                return ""
            db = SessionLocal()
            try:
                query = db.query(ReflectionMemory).filter(ReflectionMemory.is_active == True)
                query = query.filter(or_(ReflectionMemory.strategy_name.in_(strategy_names), ReflectionMemory.strategy_name.is_(None)))
                if source_event_type:
                    query = query.filter(ReflectionMemory.source_event_type == source_event_type)
                keyword_filters = []
                for kw in keywords:
                    keyword_filters.append(ReflectionMemory.condition.contains(kw))
                    keyword_filters.append(ReflectionMemory.action.contains(kw))
                    keyword_filters.append(ReflectionMemory.reason.contains(kw))
                if not keyword_filters:
                    return ""
                memories = query.filter(or_(*keyword_filters)).order_by(desc(ReflectionMemory.created_at)).limit(limit).all()
                if not memories:
                    return ""
                context = ""
                for i, m in enumerate(memories, 1):
                    context += f"- 记忆{i}: 当【{m.condition}】时，建议【{m.action}】，原因：{m.reason}\n"
                return context
            finally:
                db.close()
        return await asyncio.to_thread(_get_memories)

learning_service = LearningService()

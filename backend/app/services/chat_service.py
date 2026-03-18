import logging
import asyncio
import json
import re
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from sqlalchemy import func, desc
from app.core.config import settings
from app.services.data_provider import data_provider
from app.db.session import SessionLocal
from app.models.chat_models import ChatMessage
from app.models.stock_models import Account, Position, TradingPlan, Stock, MarketSentiment, PatternCase, ReflectionMemory, OutcomeEvent
from app.services.indicators.technical_indicators import technical_indicators
from app.services.market.tdx_formula_service import tdx_formula_service

logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        self.ai_semaphore = asyncio.Semaphore(3)
        self._stock_list_cache = None
        self._last_cache_time = 0.0
        self._context_cache = {} # (ts_code, date_str, minute_str) -> context

    def _chunk_text(self, text: str, max_chars: int) -> List[str]:
        if not text:
            return []
        max_chars = max(500, int(max_chars or 0))
        lines = text.splitlines(True)
        chunks: List[str] = []
        buf: List[str] = []
        size = 0
        for line in lines:
            if size + len(line) > max_chars and buf:
                chunks.append("".join(buf))
                buf = []
                size = 0
            buf.append(line)
            size += len(line)
        if buf:
            chunks.append("".join(buf))
        return chunks

    async def _summarize_text(self, text: str, system_prompt: str) -> str:
        from app.services.ai.ai_client import ai_client as ai_core_client
        async with self.ai_semaphore:
            return await asyncio.to_thread(ai_core_client.call_ai_best_effort, text, system_prompt)

    async def _retrieve_relevant_memories(self, ts_code: str) -> str:
        """
        检索该股票相关的历史记忆与反思
        1. 检索 PatternCase (该股的历史交易模式)
        2. 检索 ReflectionMemory (该股的历史教训)
        """
        db = SessionLocal()
        try:
            memories = []
            
            # 1. 历史成功/失败案例
            patterns = db.query(PatternCase).filter(
                PatternCase.ts_code == ts_code
            ).order_by(desc(PatternCase.trade_date)).limit(3).all()
            
            if patterns:
                memories.append("【历史交易记录】")
                for p in patterns:
                    res_str = "盈利" if p.is_successful else "亏损"
                    memories.append(f"- {p.trade_date} {p.pattern_type}: {res_str} {p.profit_pct:.1f}% (持仓{p.hold_days}天)")
            
            # 2. 深度反思
            # 这里暂时用简单的 SQL 查询，未来可接入向量检索寻找"相似形态"的记忆
            reflections = db.query(ReflectionMemory).join(
                OutcomeEvent, ReflectionMemory.source_event_id == OutcomeEvent.id
            ).filter(
                OutcomeEvent.ts_code == ts_code,
                ReflectionMemory.is_active == True
            ).order_by(desc(ReflectionMemory.created_at)).limit(3).all()
            
            if reflections:
                memories.append("【AI反思记忆】")
                for r in reflections:
                    memories.append(f"- 当【{r.condition}】时，建议【{r.action}】，原因：{r.reason}")
                    
            return "\n".join(memories)
        except Exception as e:
            logger.error(f"Error retrieving memories for {ts_code}: {e}")
            return ""
        finally:
            db.close()

    async def _compress_context(self, raw: str, ts_code: str) -> str:
        """
        [优化] 基于规则的上下文压缩 (Rule-based Compression)
        移除 AI 递归调用，改为截断非核心数据，确保 Context 构建秒级完成。
        """
        try:
            if not getattr(settings, "AI_CONTEXT_SUMMARY_ENABLED", True):
                return raw
            trigger_chars = int(getattr(settings, "AI_CONTEXT_SUMMARY_TRIGGER_CHARS", 15000) or 15000)
            if len(raw) <= trigger_chars:
                return raw

            logger.info(f"Chat: Context for {ts_code} exceeds {trigger_chars} chars ({len(raw)}). Applying rule-based compression.")

            # 1. 提取核心头部 (Quote & Stats) - 必须保留
            pinned_parts: List[str] = []
            
            # 1.1 提取行情行
            quote_line = ""
            pankou_line = ""
            lines = raw.splitlines()
            for ln in lines:
                if ln.startswith("● ") and f"({ts_code})" in ln:
                    quote_line = ln.strip()
                if ln.startswith("● ") and ("盘口" in ln or "买一" in ln or "卖一" in ln):
                    pankou_line = ln.strip()
                if quote_line and pankou_line:
                    break
            if quote_line:
                pinned_parts.append(quote_line)
            if pankou_line:
                pinned_parts.append(pankou_line)

            # 1.2 提取 5年统计 (正则提取整个块)
            stats_match = re.search(r"【历史统计概览 - 5年全景】[\s\S]*?(?=\n【|\Z)", raw)
            if stats_match:
                pinned_parts.append(stats_match.group(0).strip())

            # 2. 智能截断各部分
            def truncate_section(section_name: str, max_len: int, content: str, keep_tail: bool = False) -> str:
                pattern = rf"({section_name})([\s\S]*?)(?=\n【|\Z)"
                match = re.search(pattern, content)
                if not match:
                    return ""
                
                header = match.group(1)
                body = match.group(2).strip()
                
                if len(body) <= max_len:
                    return f"{header}\n{body}"

                if keep_tail:
                    lines = body.splitlines()
                    kept_lines: list[str] = []
                    size = 0
                    for line in reversed(lines):
                        line_len = len(line) + 1
                        if size + line_len > max_len and kept_lines:
                            break
                        kept_lines.append(line)
                        size += line_len
                    kept_body = "\n".join(reversed(kept_lines))
                else:
                    kept_body = body[:max_len]
                    last_newline = kept_body.rfind('\n')
                    if last_newline > max_len * 0.8:
                        kept_body = kept_body[:last_newline]

                return f"{header}\n{kept_body}\n... (已截断过往数据)"

            compressed_parts = []
            
            # 3. 按优先级重组
            # 3.1 账户与持仓 (保留)
            acct_match = re.search(r"【账户委托与持仓】[\s\S]*?(?=\n【|\Z)", raw)
            if acct_match:
                compressed_parts.append(acct_match.group(0).strip())

            section_defs = [
                ("【大级别趋势/信号】", 800, False),
                ("【量价特征分析】", 600, False),
                ("【日K线明细 - 最近30天】", 2200, True),
                ("【日K线最近20根K线(CSV)】", 2200, True),
                ("【日线最近20根K线(CSV)】", 2200, True),
                ("【周K线明细 - 最近12周】", 1200, True),
                ("【周线最近20根K线(CSV)】", 1200, True),
                ("【月K线明细 - 最近6个月】", 900, True),
                ("【月线最近12根K线(CSV)】", 900, True),
                ("【技术风险/机会】", 800, False),
                ("【30分钟K线明细 - 最近48根】", 1600, True),
                ("【30分钟K线最近20根K线(CSV)】", 1600, True),
                ("【5分钟K线明细 - 最近24根】", 1000, True),
                ("【5分钟K线最近20根K线(CSV)】", 1000, True),
            ]
            for section_name, max_len, keep_tail in section_defs:
                section_body = truncate_section(section_name, max_len, raw, keep_tail=keep_tail)
                if section_body:
                    compressed_parts.append(section_body)

            # 3.3 基本面 (Fundamental) - 限制长度
            # 标题可能是 "【五步基本面筛选结果 - 综合得分: 85.0】" 这种动态标题
            fina_match = re.search(r"(【五步基本面筛选结果[\s\S]*?)(?=\n【|\Z)", raw)
            if fina_match:
                fina_header = fina_match.group(1).split('\n')[0]
                fina_body = fina_match.group(0)
                # 截断
                if len(fina_body) > 1000:
                    fina_body = fina_body[:1000] + "\n... (基本面数据过长已截断)"
                compressed_parts.append(fina_body)
            
            # 3.4 搜索资讯 (Search) - 限制长度 (资讯通常最长)
            # 注意：Search Info 通常不在 get_ai_trading_context 内部，而是在外部拼接。
            # 但如果未来加进来了，这里作为一个防御性截断
            search_body = truncate_section("【相关资讯与搜索】", 1000, raw) 
            if search_body: compressed_parts.append(search_body)

            # 3.5 其他部分 (未匹配到的)
            # 简单策略：如果不在这几个核心块里，就不放了，或者放一点点
            
            # 4. 组装
            final_context = "\n\n".join(pinned_parts + compressed_parts)
            
            # 再次检查总长度，如果还是太长，强行截断末尾
            if len(final_context) > trigger_chars:
                final_context = final_context[:trigger_chars] + "\n... (上下文过长，已强制截断)"

            return final_context + "\n\n【上下文已分段压缩 (Rule-based)】"

        except Exception as e:
            logger.warning(f"Chat: Failed to compress context for {ts_code}: {e}")
            return raw

    def _patch_latest_daily_kline(self, kline_d: List[Dict], quote: Dict) -> List[Dict]:
        """
        [Hotfix] 如果数据库日线未同步，利用实时行情构造当日 K 线并追加到列表
        """
        if not quote:
            return kline_d
        
        try:
            # 获取行情时间
            trade_date_str = str(quote.get('trade_date') or datetime.now().strftime('%Y%m%d'))
            # 格式化日期为 YYYY-MM-DD
            if len(trade_date_str) == 8:
                trade_date_str = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:]}"
            elif len(trade_date_str) >= 10:
                trade_date_str = trade_date_str[:10]
            
            # 检查 kline_d 最后一条是否已是今日
            if kline_d:
                last_date = kline_d[-1].get('time', '')
                if last_date == trade_date_str:
                    # 如果已存在，尝试更新它（因为实时行情可能比 DB 更新）
                    last_bar = kline_d[-1]
                    price = float(quote.get('price') or 0)
                    if price > 0:
                        last_bar['close'] = price
                        last_bar['high'] = max(float(last_bar.get('high', 0)), float(quote.get('high', 0)))
                        last_bar['low'] = min(float(last_bar.get('low', 0)), float(quote.get('low', 0)))
                        last_bar['volume'] = max(float(last_bar.get('volume', 0)), float(quote.get('vol', 0)))
                        last_bar['amount'] = max(float(last_bar.get('amount', 0)), float(quote.get('amount', 0)))
                    return kline_d
            
            # 构造今日 K 线
            # 注意：实时行情中的 price 是当前价，即 close
            open_p = float(quote.get('open') or 0)
            price = float(quote.get('price') or 0)
            
            # 必须确保有有效价格，且不是停牌（open=0）
            if open_p <= 0 or price <= 0:
                return kline_d
                
            new_bar = {
                "time": trade_date_str,
                "open": open_p,
                "high": float(quote.get('high') or price),
                "low": float(quote.get('low') or price),
                "close": price,
                "volume": float(quote.get('vol') or 0), # 手
                "amount": float(quote.get('amount') or 0), # 元
                "pct_chg": float(quote.get('pct_chg') or 0),
                "adj_factor": 1.0 # 假设未除权，暂不处理复杂复权
            }
            
            kline_d.append(new_bar)
            return kline_d
            
        except Exception as e:
            logger.warning(f"Failed to patch latest daily kline: {e}")
            return kline_d

    def _infer_plan_action(self, plan: TradingPlan) -> str:
        decision = (getattr(plan, "ai_decision", "") or "").strip().upper()
        if decision in ["CANCEL", "WAIT", "HOLD"]:
            return "HOLD"
        if decision in ["SELL", "REDUCE"]:
            return "SELL"
        if decision == "BUY":
            return "BUY"
        strategy_name = (plan.strategy_name or "").strip()
        if any(k in strategy_name for k in ["卖出", "减仓", "减持", "清仓", "止盈", "止损", "抛售"]):
            return "SELL"
        if any(k in strategy_name for k in ["持有", "观望", "待定"]):
            return "HOLD"
        return "BUY"

    async def _get_ts_code_account_order_context(self, ts_code: str) -> str:
        db = SessionLocal()
        try:
            account = await asyncio.to_thread(db.query(Account).first)
            pos = await asyncio.to_thread(lambda: db.query(Position).filter(Position.ts_code == ts_code, Position.vol > 0).first())
            today = date.today()
            next_plan_date = await asyncio.to_thread(
                lambda: db.query(func.min(TradingPlan.date))
                .filter(TradingPlan.ts_code == ts_code, TradingPlan.date >= today)
                .scalar()
            )
            if next_plan_date is None:
                next_plan_date = await asyncio.to_thread(
                    lambda: db.query(func.max(TradingPlan.date))
                    .filter(TradingPlan.ts_code == ts_code)
                    .scalar()
                )

            plans = []
            if next_plan_date:
                plans = await asyncio.to_thread(
                    lambda: db.query(TradingPlan)
                    .filter(TradingPlan.date == next_plan_date, TradingPlan.ts_code == ts_code)
                    .order_by(TradingPlan.created_at.desc(), TradingPlan.id.desc())
                    .all()
                )

            lines = ["【账户委托与持仓】"]
            if account:
                lines.append(
                    f"- 资金: 总资产 {float(account.total_assets or 0.0):.2f}, 可用 {float(account.available_cash or 0.0):.2f}, 冻结 {float(account.frozen_cash or 0.0):.2f}"
                )
            if pos:
                lines.append(
                    f"- 持仓: {int(pos.vol or 0)}股(可卖{int(pos.available_vol or 0)}), 均价{float(pos.avg_price or 0.0):.2f}, 现价{float(pos.current_price or 0.0):.2f}"
                )
            else:
                lines.append("- 持仓: 无")

            if not plans:
                label_date = (next_plan_date or today).isoformat()
                lines.append(f"- 委托({label_date}): 无")
                return "\n".join(lines)

            label_date = (next_plan_date or today).isoformat()
            lines.append(f"- 委托({label_date}):")
            for p in plans[:5]:
                action = self._infer_plan_action(p)
                status = "已成交" if p.executed else ("观察中" if (p.track_status or "").upper() == "TRACKING" else "待成交")
                order_type = (p.order_type or "MARKET").upper()
                limit_price = float(p.limit_price or p.buy_price_limit or 0.0)
                frozen_amount = float(p.frozen_amount or 0.0)
                frozen_vol = int(p.frozen_vol or 0)
                review = (p.review_content or "").strip()
                review_part = f", 状态说明: {review}" if review else ""
                lines.append(
                    f"  - #{p.id} {action} {status}, {order_type}"
                    f", 委托价{limit_price:.2f}, 冻结{frozen_amount:.2f}({frozen_vol}股){review_part}"
                )

            if len(plans) > 5:
                lines.append(f"  - 其余 {len(plans) - 5} 条已省略")

            return "\n".join(lines)
        finally:
            await asyncio.to_thread(db.close)

    async def _get_stock_list(self):
        """获取并缓存股票列表"""
        now = datetime.now().timestamp()
        if self._stock_list_cache and (now - self._last_cache_time < 3600):
            return self._stock_list_cache
            
        try:
            stocks = await data_provider.get_stock_basic()
            self._stock_list_cache = stocks
            self._last_cache_time = now
            return stocks
        except Exception as e:
            logger.error(f"Chat: Failed to fetch stock list: {e}")
            return []

    async def _get_account_position_state_key(self, ts_code: str) -> tuple[int, int, int, int]:
        def _query():
            db = SessionLocal()
            try:
                account = db.query(Account).first()
                pos = db.query(Position).filter(Position.ts_code == ts_code).first()
                available_cash = int(round(float(account.available_cash or 0.0) * 100)) if account else 0
                frozen_cash = int(round(float(account.frozen_cash or 0.0) * 100)) if account else 0
                vol = int(pos.vol or 0) if pos else 0
                available_vol = int(pos.available_vol or 0) if pos else 0
                return (available_cash, frozen_cash, vol, available_vol)
            finally:
                db.close()
        return await asyncio.to_thread(_query)

    async def extract_stock_codes(self, text: str) -> List[str]:
        """从文本中提取股票代码或名称"""
        found_stocks = []
        
        # 1. 匹配 6 位数字代码 (如 603097)
        # 使用前后非数字判定，避免 \b 在中文环境下的匹配问题
        codes = re.findall(r'(?<!\d)(\d{6})(?!\d)', text)
        for code in codes:
            # 简单补充后缀
            if code.startswith('6'): ts_code = f"{code}.SH"
            elif code.startswith('0') or code.startswith('3'): ts_code = f"{code}.SZ"
            elif code.startswith('4') or code.startswith('8'): ts_code = f"{code}.BJ"
            else: continue
            if ts_code not in found_stocks:
                found_stocks.append(ts_code)
                
        # 2. 匹配股票名称
        stocks = await self._get_stock_list()
        for s in stocks:
            # 只有名称长度大于等于2才进行匹配，避免单字误伤
            if len(s['name']) >= 2 and s['name'] in text and s['ts_code'] not in found_stocks:
                found_stocks.append(s['ts_code'])
        
        if found_stocks:
            logger.info(f"Chat: Extracted stock codes from message: {found_stocks}")
                
        return found_stocks[:10]

    def _format_kline_csv(self, data_list: List[Dict], period_name: str, limit: int = 20) -> str:
        """
        将 K 线数据格式化为 CSV Compact Format
        Dt,O,H,L,C,V,Pct
        """
        if not data_list:
            return ""
        recent = data_list[-limit:]
        lines = [f"【{period_name}最近{limit}根K线(CSV)】\nDt,O,H,L,C,V,Pct"]
        for bar in recent:
            try:
                # bar keys: time, open, high, low, close, volume, pct_chg
                d_str = str(bar.get('time', ''))[:10].replace('-', '').replace('/', '')[2:]
                o = float(bar.get('open', 0))
                h = float(bar.get('high', 0))
                l = float(bar.get('low', 0))
                c = float(bar.get('close', 0))
                v = int(float(bar.get('volume', 0)))
                pct = float(bar.get('pct_chg', 0))
                
                # 简单保留2位小数
                line = f"{d_str},{o:.2f},{h:.2f},{l:.2f},{c:.2f},{v},{pct:.2f}"
                lines.append(line)
            except Exception:
                continue
        return "\n".join(lines)

    async def get_ai_trading_context(self, ts_code: str, pre_fetched_fundamental: Optional[Dict] = None, cache_scope: Optional[str] = None, include_kline: bool = True) -> str:
        """为 AI 获取特定股票的深度交易上下文，用于增强分析准确性"""
        ts_code = data_provider._normalize_ts_code(ts_code)
        # 0. 检查缓存 (按分钟缓存)
        now = datetime.now()
        account_state = (0, 0, 0, 0)
        try:
            account_state = await self._get_account_position_state_key(ts_code)
        except Exception:
            account_state = (0, 0, 0, 0)
        
        # 缓存键增加 include_kline
        cache_key = (ts_code, now.strftime('%Y%m%d'), now.strftime('%H%M'), account_state, include_kline)
        if cache_key in self._context_cache and pre_fetched_fundamental is None:
            return self._context_cache[cache_key]

        context_parts = []
        try:
            # 1. 使用统一数据接口获取全维度数据
            from app.services.market.market_data_service import market_data_service
            if cache_scope:
                with market_data_service.cache_scope(cache_scope):
                    data = await asyncio.wait_for(
                        market_data_service.get_ai_context_data(ts_code, cache_scope=cache_scope),
                        timeout=20.0
                    )
            else:
                data = await asyncio.wait_for(
                    market_data_service.get_ai_context_data(ts_code),
                    timeout=20.0
                )
            
            name = data['name']
            quote = data['quote']
            kline_d = data['kline_d']
            weekly_k = data['weekly_k']
            monthly_k = data['monthly_k']
            kline_5m = data.get('kline_5m') or []
            kline_30m = data.get('kline_30m') or []
            stats = data['stats']
            
            # [Fix] 尝试补全最新的日线数据 (如果 DB 未同步)
            # 只有当 quote 存在且有效时才补全
            if quote and kline_d is not None:
                kline_d = self._patch_latest_daily_kline(kline_d, quote)
            
            # [优化] 如果外部传了基本面数据，则直接使用，避免重复抓取
            fundamental = pre_fetched_fundamental or data.get('fundamental', {})

            # 增加基本面上下文
            if fundamental:
                fina = fundamental.get('fina_indicators', {})
                val = fundamental.get('valuation', {})
                scr = fundamental.get('screening', {})
                profile = fundamental.get('business_profile', {})
                
                fina_parts = [
                    f"【五步基本面筛选结果 - 综合得分: {scr.get('total_score', 0):.1f}】",
                    f"结论: {scr.get('conclusion', '未知')}",
                    f"1. 财务安全: {'通过' if scr.get('step1_safety', {}).get('passed') else '未通过'} - {', '.join(scr.get('step1_safety', {}).get('details', []))}",
                    f"2. 盈利能力: {'通过' if scr.get('step2_profitability', {}).get('passed') else '未通过'} - {', '.join(scr.get('step2_profitability', {}).get('details', []))}",
                    f"3. 成长性: {'通过' if scr.get('step4_growth', {}).get('passed') else '未通过'} - {', '.join(scr.get('step4_growth', {}).get('details', []))}",
                    f"4. 估值分析: {'通过' if scr.get('step5_valuation', {}).get('passed') else '未通过'} - {', '.join(scr.get('step5_valuation', {}).get('details', []))}",
                    f"核心业务画像:",
                    f"  - 主营业务: {profile.get('main_business', '未知')}",
                    f"  - 业务范围: {profile.get('business_scope', '未知')}",
                    f"  - 核心产品: {', '.join(profile.get('main_products', [])) or '未知'}",
                    f"核心财务指标 (报告期 {fina.get('end_date', '无')}):",
                    f"  - ROE: {fina.get('roe', '无')}%",
                    f"  - 毛利率: {fina.get('grossprofit_margin', '无')}%",
                    f"  - 净利率: {fina.get('netprofit_margin', '无')}%",
                    f"  - 营收同比: {fina.get('yoy_revenue', '无')}%",
                    f"  - 负债率: {fina.get('debt_to_assets', '无')}%",
                    f"  - 估值: PE {val.get('pe', '无')}, PB {val.get('pb', '无')}, 市值 {val.get('total_mv', '无'):.1f}亿"
                ]
                
                # [新增] 注入通达信系统定义数据 (EXTERNSTR/EXTERNVALUE/FINONE)
                def _build_tdx_system_parts_sync() -> list[str]:
                    out: list[str] = []
                    for i in range(1, 4):
                        try:
                            s_val = tdx_formula_service.EXTERNSTR(i, ts_code)
                            n_val = tdx_formula_service.EXTERNVALUE(i, ts_code)
                            if s_val or (n_val and n_val != 0):
                                out.append(f"  - 外部系统数据#{i}: {s_val}" + (f" (数值: {n_val})" if n_val else ""))
                        except Exception:
                            continue

                    fin_ids = {1: "总股本", 10: "流通A股", 32: "净利润", 35: "EPS", 44: "ROE"}
                    fin_parts: list[str] = []
                    for fid, fname in fin_ids.items():
                        try:
                            fval = tdx_formula_service.FINONE(fid, ts_code)
                            if fval and fval != 0:
                                fin_parts.append(f"{fname}: {fval}")
                        except Exception:
                            continue

                    if fin_parts:
                        out.append(f"  - 通达信本地财务 (FINONE): {', '.join(fin_parts)}")

                    return out

                try:
                    extern_parts = await asyncio.wait_for(asyncio.to_thread(_build_tdx_system_parts_sync), timeout=2.5)
                except Exception:
                    extern_parts = []

                if extern_parts:
                    fina_parts.append("通达信系统定义数据 (EXTERNSTR/VALUE/FINONE):")
                    fina_parts.extend(extern_parts)
                
                context_parts.append("\n".join(fina_parts))

            # [新增] 必须提供 5分钟/30分钟/日线/周线 完整数据，哪怕数据量不足也要提供空占位符以提示
            # from app.services.market.market_utils import is_trading_time
            # is_trading_now = is_trading_time() # 未使用变量
            
            # 获取日线 (最近 30 天)
            daily_bars = []
            try:
                # 强制 limit=30，确保 AI 有足够日线上下文
                klines_map = await asyncio.wait_for(
                    data_provider.get_kline_batch([ts_code], freq="D", limit=30),
                    timeout=8.0
                )
                daily_bars = klines_map.get(ts_code, [])
            except Exception as e:
                logger.error(f"Failed to fetch daily bars for {ts_code}: {e}")
            
            # 获取周线 (最近 12 周)
            weekly_bars = []
            try:
                klines_map = await asyncio.wait_for(
                    data_provider.get_kline_batch([ts_code], freq="W", limit=12),
                    timeout=8.0
                )
                weekly_bars = klines_map.get(ts_code, [])
            except Exception as e:
                logger.error(f"Failed to fetch weekly bars for {ts_code}: {e}")
                
            # 获取 30分钟 (最近 48 根)
            m30_bars = []
            try:
                klines_map = await asyncio.wait_for(
                    data_provider.get_kline_batch([ts_code], freq="30min", limit=48),
                    timeout=8.0
                )
                m30_bars = klines_map.get(ts_code, [])
            except Exception as e:
                logger.error(f"Failed to fetch 30m bars for {ts_code}: {e}")
                
            # 获取 5分钟 (最近 48 根)
            m5_bars = []
            try:
                klines_map = await asyncio.wait_for(
                    data_provider.get_kline_batch([ts_code], freq="5min", limit=48),
                    timeout=8.0
                )
                m5_bars = klines_map.get(ts_code, [])
            except Exception as e:
                logger.error(f"Failed to fetch 5m bars for {ts_code}: {e}")

            def format_bars(bars, name):
                if not bars:
                    return f"● {name} K线数据: [缺失]"
                lines = [f"● {name} K线数据 (最近 {len(bars)} 根):"]
                # 简化显示：只显示最后 5 根详细数据，其余概略
                for b in bars[-5:]:
                    lines.append(f"  - {b.get('trade_date', '')} {b.get('trade_time', '')}: O={b.get('open')} H={b.get('high')} L={b.get('low')} C={b.get('close')} V={b.get('vol')}")
                return "\n".join(lines)

            context_parts.append(format_bars(daily_bars, "日线"))
            context_parts.append(format_bars(weekly_bars, "周线"))
            context_parts.append(format_bars(m30_bars, "30分钟"))
            context_parts.append(format_bars(m5_bars, "5分钟"))

            # [清理] 移除指标计算等非核心数据干扰，指标由 AI 根据 K 线自行判断或仅作为辅助
            # 原有的指标计算代码已移除，以净化上下文

            from app.services.learning_service import learning_service
            market_temperature = 50.0
            db_temp = SessionLocal()
            try:
                today_sentiment = db_temp.query(MarketSentiment).filter(
                    MarketSentiment.date == datetime.now().date()
                ).first()
                if today_sentiment:
                    market_temperature = today_sentiment.market_temperature
            except Exception:
                market_temperature = 50.0
            finally:
                db_temp.close()

            try:
                memory_context = await asyncio.wait_for(
                    learning_service.get_reflection_memories("通用", market_temperature),
                    timeout=5.0
                )
            except Exception:
                memory_context = ""
            if not memory_context:
                memory_context = "【策略反思与长期记忆 (基于历史成败提炼)】\n- 暂无匹配记忆"
            context_parts.append(memory_context)

            from app.services.market.market_utils import is_trading_time
            is_trading_now = is_trading_time()
            quote_ok = False
            if quote:
                price = float(quote.get('price') or 0)
                pct_chg = quote.get('pct_chg', 0)
                vol = quote.get('vol', 0)
                amount = quote.get('amount', 0)
                turnover = quote.get('turnover_rate', 0)
                volume_ratio = quote.get('volume_ratio', quote.get('vol_ratio', 0))
                vwap = float(quote.get('vwap') or 0)
                trade_time = quote.get('trade_time', quote.get('trade_date', '未知时间'))
                if price > 0:
                    quote_ok = True
                    open_price = quote.get('open', 0)
                    high_price = quote.get('high', 0)
                    low_price = quote.get('low', 0)
                    pre_close = quote.get('pre_close', 0)
                    vol_desc = f"成交量 {vol} (量比 {volume_ratio:.2f}, 换手 {turnover:.2f}%)"
                    vwap_desc = ""
                    if vwap > 0:
                        diff = price - vwap
                        diff_pct = diff / vwap * 100
                        if diff > 0:
                            relation = "高于"
                        elif diff < 0:
                            relation = "低于"
                        else:
                            relation = "等于"
                        vwap_desc = f", 分时均价 {vwap:.4f}, 当前价{relation}均价 {abs(diff):.4f} ({diff_pct:+.2f}%)"
                    context_parts.append(
                        f"● {name} ({ts_code}): 开盘 {open_price}, 最高 {high_price}, 最低 {low_price}, 昨收 {pre_close}, 现价 {price}, 涨跌幅 {pct_chg:.2f}%, {vol_desc}{vwap_desc}, 成交额 {amount}, 数据时间: {trade_time}"
                    )
                    b1_p = 0.0
                    s1_p = 0.0
                    bid_ask = quote.get("bid_ask")
                    if isinstance(bid_ask, dict):
                        def _to_num(v):
                            try:
                                return float(v)
                            except Exception:
                                return 0.0
                        b1_p = _to_num(bid_ask.get("b1_p"))
                        b1_v = _to_num(bid_ask.get("b1_v"))
                        b2_p = _to_num(bid_ask.get("b2_p"))
                        b2_v = _to_num(bid_ask.get("b2_v"))
                        b3_p = _to_num(bid_ask.get("b3_p"))
                        b3_v = _to_num(bid_ask.get("b3_v"))
                        b4_p = _to_num(bid_ask.get("b4_p"))
                        b4_v = _to_num(bid_ask.get("b4_v"))
                        b5_p = _to_num(bid_ask.get("b5_p"))
                        b5_v = _to_num(bid_ask.get("b5_v"))
                        s1_p = _to_num(bid_ask.get("s1_p"))
                        s1_v = _to_num(bid_ask.get("s1_v"))
                        s2_p = _to_num(bid_ask.get("s2_p"))
                        s2_v = _to_num(bid_ask.get("s2_v"))
                        s3_p = _to_num(bid_ask.get("s3_p"))
                        s3_v = _to_num(bid_ask.get("s3_v"))
                        s4_p = _to_num(bid_ask.get("s4_p"))
                        s4_v = _to_num(bid_ask.get("s4_v"))
                        s5_p = _to_num(bid_ask.get("s5_p"))
                        s5_v = _to_num(bid_ask.get("s5_v"))
                        if any([b1_p, b2_p, b3_p, b4_p, b5_p, s1_p, s2_p, s3_p, s4_p, s5_p]):
                            bid_line = f"盘口 买一 {b1_p}({b1_v}) 买二 {b2_p}({b2_v}) 买三 {b3_p}({b3_v}) 买四 {b4_p}({b4_v}) 买五 {b5_p}({b5_v})"
                            ask_line = f"卖一 {s1_p}({s1_v}) 卖二 {s2_p}({s2_v}) 卖三 {s3_p}({s3_v}) 卖四 {s4_p}({s4_v}) 卖五 {s5_p}({s5_v})"
                            context_parts.append(f"● {bid_line} | {ask_line}")
                    limit_up = float(quote.get("limit_up") or 0)
                    limit_down = float(quote.get("limit_down") or 0)
                    limit_source = str(quote.get("limit_source") or "").strip()
                    if not limit_source:
                        limit_source = "行情" if (limit_up or limit_down) else "缺失"
                    limit_status: list[str] = []
                    if limit_up:
                        tol = max(0.01, float(limit_up) * 0.001)
                        if b1_p >= limit_up > 0:
                            limit_status.append("买一触及涨停")
                        if s1_p >= limit_up > 0:
                            limit_status.append("卖一触及涨停")
                        if b1_p > limit_up + tol or s1_p > limit_up + tol:
                            limit_status.append("盘口价高于涨停")
                    if limit_down:
                        tol = max(0.01, float(limit_down) * 0.001)
                        if s1_p and s1_p <= limit_down:
                            limit_status.append("卖一触及跌停")
                        if b1_p and b1_p <= limit_down:
                            limit_status.append("买一触及跌停")
                        if (b1_p and b1_p < limit_down - tol) or (s1_p and s1_p < limit_down - tol):
                            limit_status.append("盘口价低于跌停")
                    limit_label = "、".join(limit_status) if limit_status else ("正常" if (limit_up or limit_down) else "缺失")
                    limit_up_disp = limit_up if limit_up else "未知"
                    limit_down_disp = limit_down if limit_down else "未知"
                    infer_label = ""
                    if not limit_up and not limit_down:
                        if b1_p > 0 and s1_p > 0 and b1_v > 0 and s1_v > 0:
                            infer_label = "未涨跌停"
                        elif b1_p > 0 and s1_p <= 0 and b1_v > 0:
                            infer_label = "可能涨停"
                        elif s1_p > 0 and b1_p <= 0 and s1_v > 0:
                            infer_label = "可能跌停"
                        elif b1_p > 0 and s1_p > 0 and abs(b1_p - s1_p) < 1e-6:
                            if b1_v > 0 and s1_v <= 0:
                                infer_label = "可能涨停"
                            elif s1_v > 0 and b1_v <= 0:
                                infer_label = "可能跌停"
                    infer_desc = f" | 盘口推断 {infer_label}" if infer_label else ""
                    context_parts.append(f"● 涨停价 {limit_up_disp} | 跌停价 {limit_down_disp} | 来源 {limit_source} | 盘口状态 {limit_label}{infer_desc}")

            kline_d_len = len(kline_d) if isinstance(kline_d, list) else 0
            weekly_k_len = len(weekly_k) if isinstance(weekly_k, list) else 0
            monthly_k_len = len(monthly_k) if isinstance(monthly_k, list) else 0
            kline_30m_len = len(kline_30m) if isinstance(kline_30m, list) else 0
            kline_5m_len = len(kline_5m) if isinstance(kline_5m, list) else 0
            history_ok = any([kline_d_len, weekly_k_len, monthly_k_len, kline_30m_len, kline_5m_len])
            full_cycle_ok = (
                kline_d_len >= 30
                and weekly_k_len >= 12
                and monthly_k_len >= 6
                and kline_30m_len >= 48
                and kline_5m_len >= 96
            )

            data_status = [
                "【数据状态】",
                f"- 是否盘中: {'是' if is_trading_now else '否'}",
                f"- 实时行情: {'可用' if quote_ok else '不可用'}",
                f"- 历史K线: {'可用' if history_ok else '不可用'}",
                f"- 全周期历史K线: {'完整' if full_cycle_ok else '不完整'}",
                f"- 周期长度: D={kline_d_len}/30, W={weekly_k_len}/12, M={monthly_k_len}/6, 30m={kline_30m_len}/48, 5m={kline_5m_len}/96",
            ]
            context_parts.append("\n".join(data_status))

            # 2. 注入历史统计概览
            if stats:
                curr_price = None
                try:
                    curr_price = float((quote or {}).get("price") or 0) if quote else None
                    if curr_price is not None and curr_price <= 0:
                        curr_price = None
                except Exception:
                    curr_price = None

                h_5y = float(stats.get("h_5y") or 0)
                l_5y = float(stats.get("l_5y") or 0)
                h_6m = float(stats.get("h_6m") or 0)
                l_6m = float(stats.get("l_6m") or 0)
                h_date = str(stats.get("h_date") or "")
                l_date = str(stats.get("l_date") or "")
                avg_vol_6m = stats.get("avg_vol_6m")
                try:
                    avg_vol_6m = float(avg_vol_6m) if avg_vol_6m is not None else 0.0
                except Exception:
                    avg_vol_6m = 0.0

                pos_desc = ""
                if curr_price is not None and h_5y > 0 and l_5y > 0 and h_5y > l_5y:
                    pos = (curr_price - l_5y) / (h_5y - l_5y) * 100.0
                    to_high = (h_5y / curr_price - 1.0) * 100.0 if curr_price > 0 else 0.0
                    to_low = (curr_price / l_5y - 1.0) * 100.0 if l_5y > 0 else 0.0
                    if pos <= 25:
                        pos_label = "底部区间"
                    elif pos >= 75:
                        pos_label = "顶部区间"
                    else:
                        pos_label = "中位区间"
                    pos_desc = f"- 当前价相对5年区间位置: {pos:.1f}% ({pos_label}) | 距5年高点: {to_high:.1f}% | 距5年低点: {to_low:.1f}%\n"

                stats_summary = f"""
【历史统计概览 - 5年全景】
- 5年区间(前复权): 高 {h_5y:.2f} / 低 {l_5y:.2f}
{pos_desc}- 近半年(前复权): 高 {h_6m:.2f}({h_date}) / 低 {l_6m:.2f}({l_date}) / 日均成交量 {avg_vol_6m:.2f}
"""
                context_parts.append(stats_summary)

            # 3. 注入周线、月线与日线明细 (CSV Compact Mode)
            if include_kline:
                if monthly_k:
                    context_parts.append(self._format_kline_csv(monthly_k, "月线", limit=12))

                if weekly_k:
                    context_parts.append(self._format_kline_csv(weekly_k, "周线", limit=20))

            if kline_30m:
                # 30min 分析 (保留信号分析，仅压缩数据列表)
                min30_csv = ""
                def _analyze_30m_sync():
                    df_30m = technical_indicators.calculate(kline_30m)
                    is_top_div = technical_indicators.detect_top_divergence(df_30m)
                    is_bottom_div = technical_indicators.detect_bottom_divergence(df_30m)
                    is_trend_start = technical_indicators.detect_trend_start(df_30m)

                    ma_status = "均线多头"
                    last_hist = None
                    if len(df_30m) >= 2:
                        last_row = df_30m.iloc[-1]
                        if last_row['close'] < last_row['ma20']:
                            ma_status = "破位 MA20"
                        last_hist = df_30m['macd_hist'].iloc[-1]

                    return {
                        "records": df_30m.to_dict('records'),
                        "is_top_div": bool(is_top_div),
                        "is_bottom_div": bool(is_bottom_div),
                        "is_trend_start": bool(is_trend_start),
                        "ma_status": ma_status,
                        "last_hist": last_hist,
                    }

                analyzed_30m = None
                try:
                    analyzed_30m = await asyncio.wait_for(asyncio.to_thread(_analyze_30m_sync), timeout=4.0)
                except Exception:
                    analyzed_30m = None

                if analyzed_30m:
                    div_status = []
                    if analyzed_30m.get("is_top_div"):
                        div_status.append("30min顶背离")
                    if analyzed_30m.get("ma_status") == "破位 MA20":
                        div_status.append("30min破位MA20")

                    if (not analyzed_30m.get("is_top_div")) and analyzed_30m.get("ma_status") == "均线多头":
                        last_hist = analyzed_30m.get("last_hist")
                        if last_hist is not None and last_hist > 0:
                            div_status.append("30min多头")

                    if analyzed_30m.get("is_bottom_div"):
                        div_status.append("30min底背离(待确认)")
                    if analyzed_30m.get("is_trend_start"):
                        div_status.append("30min趋势启动")

                    div_desc = f"【技术风险/机会】{' | '.join(div_status)}\n" if div_status else "【技术风险/机会】30min走势正常\n"
                    context_parts.append(div_desc) # 信号分析始终保留
                    
                    if include_kline:
                        # 使用 CSV 格式
                        # 30min 需要 MACD 吗? CSV 简单格式没有 MACD。
                        # 但 30min 主要是看背离，背离已经分析在 div_desc 了。
                        # 如果需要 MACD 值，CSV 可以加一列。这里暂时保持基础 OHLCV。
                        context_parts.append(self._format_kline_csv(kline_30m, "30分钟K线", limit=20))
                else:
                    context_parts.append("【技术风险/机会】30min计算失败或超时")

            if kline_5m and include_kline:
                context_parts.append(self._format_kline_csv(kline_5m, "5分钟K线", limit=20))

            if kline_d:
                # 日线分析 (保留信号分析)
                def _build_daily_context_sync():
                    # daily_list = [] # 不再生成详细列表
                    df_d = technical_indicators.calculate(kline_d)
                    df_w = technical_indicators.calculate(weekly_k) if weekly_k else None
                    df_m = technical_indicators.calculate(monthly_k) if monthly_k else None

                    is_d_trend_start = technical_indicators.detect_trend_start(df_d)
                    is_d_top_div = technical_indicators.detect_top_divergence(df_d)
                    is_d_bottom_div = technical_indicators.detect_bottom_divergence(df_d)

                    is_w_breakout = technical_indicators.detect_platform_breakout(df_w, window=12) if df_w is not None else False
                    is_m_breakout = technical_indicators.detect_platform_breakout(df_m, window=6) if df_m is not None else False

                    trend_signals = []
                    if is_m_breakout:
                        trend_signals.append("月线突破(主升浪/平台突破)")
                    if is_w_breakout:
                        trend_signals.append("周线突破(中期趋势确立)")
                    if is_d_trend_start:
                        trend_signals.append("日线趋势启动")
                    if is_d_top_div:
                        trend_signals.append("日线顶背离")
                    if is_d_bottom_div:
                        trend_signals.append("日线底背离")

                    trend_status = f"【大级别趋势/信号】{' | '.join(trend_signals)}\n" if trend_signals else "【大级别趋势/信号】趋势震荡上行中\n"

                    vol_feature = "量价正常"
                    if len(df_d) >= 5:
                        last_5 = df_d.tail(5)
                        price_up = last_5['close'].iloc[-1] > last_5['close'].iloc[0]
                        vol_down = last_5['volume'].iloc[-1] < last_5['volume'].mean() * 0.8
                        if price_up and vol_down:
                            vol_feature = "缩量攀升(疑似锁仓/筹码稳定)"
                        elif (not price_up) and vol_down:
                            vol_feature = "强势调整(回调极度缩量)"

                    return {"trend_status": trend_status, "vol_feature": vol_feature}

                daily_ctx = None
                try:
                    daily_ctx = await asyncio.wait_for(asyncio.to_thread(_build_daily_context_sync), timeout=5.0)
                except Exception:
                    daily_ctx = None

                if daily_ctx:
                    context_parts.append(str(daily_ctx.get("trend_status") or ""))
                    context_parts.append(f"【量价特征分析】{daily_ctx.get('vol_feature')}\n")
                    
                    if include_kline:
                        context_parts.append(self._format_kline_csv(kline_d, "日K线", limit=20))
                else:
                    context_parts.append("【大级别趋势/信号】计算失败或超时")

            try:
                account_ctx = await self._get_ts_code_account_order_context(ts_code)
                if account_ctx:
                    context_parts.append(account_ctx)
            except Exception as e:
                logger.warning(f"Chat: Failed to attach account/entrustment context for {ts_code}: {e}")

            # 4. 注入长期记忆与反思 (New Memory Retrieval)
            try:
                memory_ctx = await self._retrieve_relevant_memories(ts_code)
                if memory_ctx:
                    context_parts.append(f"\n【历史反思与教训】\n{memory_ctx}")
            except Exception as e:
                logger.warning(f"Chat: Failed to attach memory context for {ts_code}: {e}")

            res = "\n".join(context_parts)
            res = await self._compress_context(res, ts_code)
            
            # 存入缓存 (清理旧缓存，防止内存溢出)
            if len(self._context_cache) > 500:
                self._context_cache.clear()
            self._context_cache[cache_key] = res
            
            return res
        except Exception as e:
            logger.error(f"Chat: Error building trading context for {ts_code}: {e}")
            return f"数据获取失败: {str(e)}"

    async def _get_stock_context(self, ts_codes: list) -> str:
        """获取指定股票列表的实时上下文数据（内部调用）"""
        if not ts_codes:
            return ""
            
        logger.info(f"Chat: Fetching real-time context for {ts_codes}")
        
        # [优化] 预先批量计算所有提取到的股票的指标，避免在 get_ai_trading_context 内部逐个串行计算
        # [优化] 仅在交易时间段执行，盘后直接使用预计算数据
        from app.services.market.market_utils import is_trading_time
        if is_trading_time():
            try:
                from app.services.indicator_service import indicator_service
                # 这里的计算是增量的且带缓存的，速度很快
                try:
                    await asyncio.wait_for(indicator_service.calculate_for_codes(ts_codes, force_recalc_today=True), timeout=6.0)
                except Exception as e:
                    logger.warning(f"Chat: Pre-calculating indicators skipped (timeout or error): {e}")
            except Exception as e:
                logger.warning(f"Chat: Pre-calculating indicators failed: {e}")

        queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue()
        results: List[Any] = [None] * len(ts_codes)
        concurrency = 5
        for i, code in enumerate(ts_codes):
            await queue.put((i, code))
        for _ in range(concurrency):
            await queue.put(None)

        async def _worker():
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    idx, code = item
                    try:
                        results[idx] = await asyncio.wait_for(self.get_ai_trading_context(code, cache_scope="chat"), timeout=45.0)
                    except Exception as e:
                        results[idx] = e
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
        await queue.join()
        await asyncio.gather(*workers, return_exceptions=True)
        
        contexts: List[str] = []
        for i, res in enumerate(results):
            ts_code = ts_codes[i]
            need_fallback = False
            if isinstance(res, Exception):
                logger.error(f"Chat: Failed to get context for {ts_code}: {res}")
                need_fallback = True
            elif isinstance(res, str) and ("数据获取失败" in res or "无可用数据" in res):
                need_fallback = True

            if need_fallback:
                retry_ctx = await self._retry_full_stock_context(ts_code, retries=2)
                if retry_ctx:
                    contexts.append(retry_ctx)
                else:
                    fallback = await self._build_minimal_stock_context(ts_code)
                    contexts.append(fallback or f"● {ts_code}: 数据获取失败")
            else:
                contexts.append(res or f"● {ts_code}: 无可用数据")
        
        context_parts = ["\n【提及股票实时行情与多周期原始数据】"]
        context_parts.extend(contexts)
            
        return "\n".join(context_parts)

    async def _build_minimal_stock_context(self, ts_code: str) -> str:
        ts_code = data_provider._normalize_ts_code(ts_code)
        from app.services.market.market_data_service import market_data_service
        quote = None
        kline_d: list[dict] = []
        stock_name = ""
        try:
            quote = await asyncio.wait_for(data_provider.get_realtime_quote(ts_code, cache_scope="chat"), timeout=6.0)
        except Exception:
            quote = None
        try:
            with market_data_service.cache_scope("chat"):
                kline_d = await asyncio.wait_for(
                    market_data_service.get_kline(
                        ts_code,
                        freq="D",
                        limit=5,
                        local_only=True,
                        include_indicators=False,
                        adj="qfq",
                        is_ui_request=False,
                        cache_scope="chat",
                    ),
                    timeout=8.0,
                )
        except Exception:
            kline_d = []
        db = SessionLocal()
        try:
            stock = db.query(Stock).filter(Stock.ts_code == ts_code).first()
            stock_name = str(stock.name or "") if stock else ""
        except Exception:
            stock_name = ""
        finally:
            db.close()

        parts: list[str] = []
        if quote:
            price = float(quote.get("price") or 0)
            pct_chg = float(quote.get("pct_chg") or 0)
            vol = quote.get("vol", 0)
            amount = quote.get("amount", 0)
            volume_ratio = float(quote.get("volume_ratio") or 0)
            trade_time = quote.get("trade_time", quote.get("trade_date", "未知时间"))
            if price > 0:
                name_text = f"{stock_name} " if stock_name else ""
                parts.append(
                    f"● {name_text}{ts_code}: 当前价 {price}, 涨跌幅 {pct_chg:.2f}%, 成交量 {vol} (量比 {volume_ratio:.2f}), 成交额 {amount}, 数据时间: {trade_time}"
                )
        if kline_d:
            parts.append(self._format_kline_csv(kline_d, "日K线(简)", limit=5))
        if parts:
            return "\n".join(parts)
        name_text = f"{stock_name} " if stock_name else ""
        return f"● {name_text}{ts_code}: 数据获取失败"

    async def _retry_full_stock_context(self, ts_code: str, retries: int = 2) -> str:
        for i in range(retries + 1):
            try:
                res = await asyncio.wait_for(self.get_ai_trading_context(ts_code, cache_scope="chat"), timeout=90.0)
                if res and "数据获取失败" not in res and "无可用数据" not in res:
                    return res
            except Exception as e:
                logger.warning(f"Chat: Retry context failed for {ts_code} ({i + 1}/{retries + 1}): {e}")
            await asyncio.sleep(0.6 * (i + 1))
        return ""

    async def save_message(self, role: str, content: str):
        """保存聊天记录到数据库"""
        db = SessionLocal()
        try:
            msg = ChatMessage(role=role, content=content)
            await asyncio.to_thread(db.add, msg)
            await asyncio.to_thread(db.commit)
        except Exception as e:
            logger.error(f"Chat: Failed to save message: {e}")
            await asyncio.to_thread(db.rollback)
        finally:
            await asyncio.to_thread(db.close)

    async def get_history(self, limit: int = 50):
        """获取历史聊天记录"""
        db = SessionLocal()
        try:
            messages = await asyncio.to_thread(db.query(ChatMessage).order_by(ChatMessage.created_at.asc()).all)
            if len(messages) > limit:
                messages = messages[-limit:]
            return messages
        except Exception as e:
            logger.error(f"Chat: Failed to get history: {e}")
            return []
        finally:
            await asyncio.to_thread(db.close)

    async def _get_account_context(self) -> str:
        """获取账户当前的上下文信息（资金、持仓、今日计划）"""
        def _load_account_data() -> tuple[Dict[str, float] | None, List[Dict[str, Any]], List[Dict[str, Any]], date | None]:
            db = SessionLocal()
            try:
                account = db.query(Account).first()
                account_dict: Dict[str, float] | None = None
                if account:
                    account_dict = {
                        "total_assets": float(account.total_assets or 0),
                        "available_cash": float(account.available_cash or 0),
                        "total_pnl": float(account.total_pnl or 0),
                        "total_pnl_pct": float(account.total_pnl_pct or 0),
                    }

                positions = db.query(Position).filter(Position.vol > 0).all()
                positions_list: List[Dict[str, Any]] = []
                for p in positions:
                    positions_list.append(
                        {
                            "ts_code": p.ts_code,
                            "vol": int(p.vol or 0),
                            "avg_price": float(p.avg_price or 0),
                            "current_price": float(p.current_price or 0),
                            "pnl_pct": float(p.pnl_pct or 0),
                        }
                    )

                today = date.today()
                next_plan_date = db.query(func.min(TradingPlan.date)).filter(TradingPlan.date >= today).scalar()
                if next_plan_date is None:
                    next_plan_date = db.query(func.max(TradingPlan.date)).scalar()

                plans_list: List[Dict[str, Any]] = []
                if next_plan_date:
                    plans = db.query(TradingPlan).filter(TradingPlan.date == next_plan_date).all()
                    for pl in plans:
                        plans_list.append(
                            {
                                "ts_code": pl.ts_code,
                                "strategy_name": pl.strategy_name,
                                "executed": bool(pl.executed),
                                "buy_price_limit": float(pl.buy_price_limit or 0),
                            }
                        )

                return account_dict, positions_list, plans_list, next_plan_date
            finally:
                db.close()

        try:
            account, positions, plans, plan_date = await asyncio.to_thread(_load_account_data)
            context = ["【当前账户状态】"]
            if account:
                context.append(f"- 总资产: {account['total_assets']:.2f}, 可用现金: {account['available_cash']:.2f}")
                context.append(f"- 总盈亏: {account['total_pnl']:.2f} ({account['total_pnl_pct']:+.2f}%)")

            if positions:
                context.append("\n【当前持仓】")
                for pos in positions:
                    context.append(
                        f"- {pos['ts_code']}: 持仓 {pos['vol']}, 均价 {pos['avg_price']}, 现价 {pos['current_price']}, 盈亏 {pos['pnl_pct']:+.2f}%"
                    )

            if plans:
                label_date = plan_date.isoformat() if plan_date else date.today().isoformat()
                context.append(f"\n【交易计划（{label_date}）】")
                for p in plans:
                    status = "已执行" if p["executed"] else "待执行"
                    context.append(f"- {p['ts_code']} ({p['strategy_name']}): {status}, 目标价 {p['buy_price_limit']}")
            else:
                context.append("\n【交易计划】无")

            return "\n".join(context)
        except Exception as e:
            logger.error(f"Chat: Error getting account context: {e}")
            return "获取账户信息失败"

    async def _get_recent_market_status_context(self) -> str:
        def _load_latest():
            db = SessionLocal()
            try:
                return (
                    db.query(MarketSentiment)
                    .order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc())
                    .first()
                )
            finally:
                db.close()

        try:
            latest = await asyncio.to_thread(_load_latest)
            if not latest:
                return "【近期市场状态检查】无可用市场状态"
            summary = (latest.summary or "").strip()
            if len(summary) > 400:
                summary = summary[:400]
            return "\n".join(
                [
                    "【近期市场状态检查】",
                    f"- 日期: {latest.date}",
                    f"- 上涨家数: {int(latest.up_count or 0)}, 下跌家数: {int(latest.down_count or 0)}",
                    f"- 涨停: {int(latest.limit_up_count or 0)}, 跌停: {int(latest.limit_down_count or 0)}",
                    f"- 成交额(亿元): {float(latest.total_volume or 0.0):.2f}",
                    f"- 市场温度: {float(latest.market_temperature or 0.0):.1f}",
                    f"- 主线题材: {latest.main_theme or '暂无'}",
                    f"- 市场总结: {summary or '暂无'}",
                ]
            )
        except Exception as e:
            logger.warning(f"Chat: Failed to load recent market status: {e}")
            return "【近期市场状态检查】获取失败"

    async def _get_recent_decision_records_context(self) -> str:
        try:
            from app.services.ai_report_service import ai_report_service
            reports = await ai_report_service.list_reports(days=7, limit=50)
            focus_types = {"realtime_trade_signal_v3", "selling_opportunity", "stock_analysis"}
            rows = [r for r in reports if (r.get("analysis_type") or "") in focus_types]
            if not rows:
                return "【近期AI决策记录检查】无近期决策记录"
            lines = ["【近期AI决策记录检查】"]
            for r in rows[:12]:
                action = ""
                price = ""
                reason = ""
                try:
                    resp = r.get("response_json")
                    if resp:
                        data = json.loads(resp)
                        if isinstance(data, dict):
                            action = str(data.get("action") or "")
                            price = str(data.get("price") or "")
                            reason = str(data.get("reason") or "")
                except Exception:
                    action = ""
                tail = ""
                if action:
                    tail += f", 动作 {action}"
                if price:
                    tail += f", 价格 {price}"
                if reason:
                    tail += f", 理由 {reason[:80]}"
                lines.append(
                    f"- {r.get('created_at') or ''} | {r.get('analysis_type') or ''} | {r.get('ts_code') or ''}{tail}"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"Chat: Failed to load recent decision records: {e}")
            return "【近期AI决策记录检查】获取失败"

    async def process_user_message(self, user_content: str, preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None, dry_run: bool = False):
        """处理用户聊天消息（重命名自 get_ai_response 以匹配接口）"""
        # 1. 保存用户消息
        await self.save_message("user", user_content)
        from app.services.ai.ai_client import ai_client as ai_core_client
        if not api_key and not ai_core_client.get_available_providers():
            error_msg = "AI 平台未配置，请先在系统配置中填写可用的 API KEY。"
            await self.save_message("assistant", error_msg)
            return error_msg

        async def _cancel_plans_by_codes(codes: List[str], reason: str):
            if not codes:
                return
            db = SessionLocal()
            try:
                plans = db.query(TradingPlan).filter(
                    TradingPlan.executed == False,
                    TradingPlan.ts_code.in_(codes)
                ).all()
                plan_ids = [p.id for p in plans if p.id and (p.track_status or "").upper() != "CANCELLED"]
            finally:
                db.close()
            from app.services.trading_service import trading_service
            for pid in plan_ids:
                await trading_service.cancel_plan(int(pid), reason)

        async def _add_plans_by_codes(codes: List[str], reason: str):
            if not codes:
                return
            from app.services.trading_service import trading_service
            for code in codes:
                await trading_service.create_plan(
                    ts_code=code,
                    strategy_name="选股监控-AI聊天",
                    buy_price=0.0,
                    stop_loss=0.0,
                    take_profit=0.0,
                    position_pct=0.1,
                    reason=reason,
                    plan_date=date.today(),
                    score=0.0,
                    source="system",
                    order_type="MARKET",
                    limit_price=0.0,
                    ai_decision="WAIT",
                    is_sell=False,
                )

        async def _place_trade_plans(codes: List[str], reason: str, action: str):
            if not codes:
                return
            from app.services.trading_service import trading_service
            quotes = await data_provider.get_realtime_quotes(codes, local_only=False, cache_scope="trading")
            for code in codes:
                norm = data_provider._normalize_ts_code(code)
                quote = quotes.get(code) or quotes.get(norm) or {}
                price = float(quote.get("price", 0) or 0.0)
                if price <= 0:
                    logger.warning(f"Chat: Skip {action} for {code}, missing realtime price")
                    continue
                if action == "BUY":
                    await trading_service.create_plan(
                        ts_code=code,
                        strategy_name="AI聊天执行-买入",
                        buy_price=price,
                        stop_loss=0.0,
                        take_profit=0.0,
                        position_pct=0.1,
                        reason=reason,
                        plan_date=date.today(),
                        score=0.0,
                        source="system",
                        order_type="MARKET",
                        limit_price=price,
                        ai_decision="BUY",
                        is_sell=False,
                    )
                elif action == "SELL":
                    await trading_service.create_plan(
                        ts_code=code,
                        strategy_name="AI聊天执行-卖出",
                        buy_price=price,
                        stop_loss=0.0,
                        take_profit=0.0,
                        position_pct=1.0,
                        reason=reason,
                        plan_date=date.today(),
                        score=0.0,
                        source="system",
                        order_type="MARKET",
                        limit_price=price,
                        ai_decision="SELL",
                        is_sell=True,
                    )

        async def _handle_text_commands(text: str, reason_prefix: str):
            clear_keywords = ["清除", "移出", "不再跟踪", "不再监控", "取消", "放弃跟踪", "剔除", "清理"]
            add_keywords = ["加入监控", "加入观察", "纳入监控", "进入监控", "加入计划", "加入监控列表", "纳入观察"]
            buy_keywords = ["买入", "执行买入", "立即买入", "建仓", "加仓"]
            sell_keywords = ["卖出", "执行卖出", "立即卖出", "减仓", "清仓", "止盈", "止损"]
            if not text:
                return
            if any(k in text for k in clear_keywords):
                codes = await self.extract_stock_codes(text)
                if codes:
                    await _cancel_plans_by_codes(codes, f"{reason_prefix}: {text[:120]}")
            if any(k in text for k in add_keywords):
                codes = await self.extract_stock_codes(text)
                if codes:
                    await _add_plans_by_codes(codes, f"{reason_prefix}: {text[:120]}")
            if any(k in text for k in buy_keywords):
                codes = await self.extract_stock_codes(text)
                if codes:
                    await _place_trade_plans(codes, f"{reason_prefix}: {text[:120]}", "BUY")
            if any(k in text for k in sell_keywords):
                codes = await self.extract_stock_codes(text)
                if codes:
                    await _place_trade_plans(codes, f"{reason_prefix}: {text[:120]}", "SELL")

        # 2. 获取上下文 (大盘、账户、提及的个股)
        context_str = ""
        try:
            # 2.1 大盘概览
            overview = await data_provider.get_market_overview()
            if overview:
                sh_index = overview.get('sh_index', '未知')
                sh_pct = overview.get('sh_pct', '0')
                if not sh_index or sh_index == '未知':
                    # 尝试从 sh 键获取 (market_data_service 返回的格式可能是 {sh: {...}})
                    sh_data = overview.get('sh', {})
                    sh_index = sh_data.get('price', '未知')
                    sh_pct = sh_data.get('pct_chg', '0')
                context_str += f"\n【当前大盘状态】\n上证指数: {sh_index}, 涨跌幅: {sh_pct}%"

            market_status_context = await self._get_recent_market_status_context()
            decision_records_context = await self._get_recent_decision_records_context()
            context_str += f"\n{market_status_context}\n{decision_records_context}"
            
            # 2.2 账户信息
            account_context = await self._get_account_context()
            context_str += f"\n{account_context}\n"

            from app.services.learning_service import learning_service
            market_temperature = 50.0
            db_temp = SessionLocal()
            try:
                today_sentiment = db_temp.query(MarketSentiment).filter(
                    MarketSentiment.date == datetime.now().date()
                ).first()
                if today_sentiment:
                    market_temperature = today_sentiment.market_temperature
            except Exception:
                market_temperature = 50.0
            finally:
                db_temp.close()

            memory_context = await learning_service.get_reflection_memories("通用", market_temperature)
            if not memory_context:
                memory_context = "【策略反思与长期记忆 (基于历史成败提炼)】\n- 暂无匹配记忆"
            context_str += f"\n{memory_context}\n"

            temp_memory = await learning_service.get_temp_memories()
            if temp_memory:
                context_str += f"\n{temp_memory}\n"
            
            # 2.3 个股行情
            ts_codes = await self.extract_stock_codes(user_content)
            if ts_codes:
                stock_context = await self._get_stock_context(ts_codes)
                context_str += f"\n{stock_context}\n"
            
        except Exception as e:
            logger.warning(f"Chat: Failed to gather context: {e}")

        # 3. 构建历史记录
        history = await self.get_history(limit=5)
        conversation = []
        for h in history:
            conversation.append(f"{h.role}: {h.content}")

        # 4. 构建 System Prompt
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        system_prompt = f"""
        你是一个负责实盘交易的 AI 基金经理。
        你的名字是“AI 交易员”。
        你的性格是：专业、冷静、严谨，但沟通时态度友善。
        
        【你的核心交易风格：激进顺势】
        1. **结构为先、进攻为主**：你的决策核心是技术结构（量比、形态、支撑），市场情绪只是参考背景。强势结构未坏时，你更倾向主动进攻而非保守等待。
        2. **回调敢买**：在强势趋势中，股价下跌回调出现承接与企稳信号时，应果断 BUY，追求波段利润。
        3. **顺势卖出**：上涨中出现放量加速、情绪亢奋或冲高乏力时，允许主动 SELL 锁定利润。
        
        【当前环境】
        - 系统时间: {current_time}
        
        【上下文信息】
        {context_str}

        【回答强制约束】
        1. **数据诚实**：必须基于【上下文信息】回答。如果上下文中没有某只股票的数据（如K线缺失），必须明确告知用户“暂无该股数据”，**严禁编造**价格、涨跌幅或走势。
        2. **核对检查**：回答前必须先核对【近期市场状态检查】与【近期AI决策记录检查】。
        3. **价量为王**：在分析任何股票时，必须遵循“量在价先”的原则。
           - **量比优先**：判断放量/缩量时，**必须优先参考量比 (Volume Ratio)** 指标，而非仅看成交量绝对值或均量线。
           - **量价配合**：放量上涨是资金介入，缩量回调是筹码锁定，这属于健康走势。
           - **量价背离**：无量上涨（诱多风险）、放量滞涨（筹码派发）、缩量空跌（承接无力），必须在分析中重点警示。
        4. **周期辨析**：用户询问“周线”时，必须使用【周K线明细】中的数据；询问“日线”或具体日期时，使用【日K线明细】。严禁将日线价格当作周线回答。
        5. **严禁幻觉**：仅当该维度数据确实未出现在【上下文信息】中时，才允许回答“本次上下文未提供该维度精确数据”。
        6. **严谨复核**：当用户质疑数据或指正错误时，**严禁盲目采信用户说法**。必须重新仔细核对【上下文信息】中的原始数据：
           - 若发现确实是自己之前的回答与上下文不符，立即修正并道歉。
           - 若上下文数据确实与用户说法冲突（例如用户说涨停，但数据显示未涨停），应坚持引用系统数据，同时友好提示“根据当前系统数据（时间xx:xx）显示...”，并说明可能存在数据源延迟，绝不为了迎合用户而编造数据。
        7. **交易执行约束**：严禁声称“已成交/已挂单/已下单/已执行”，除非【上下文信息】明确包含成交或委托记录。
        8. **输出限制**：仅输出核心观点与操作结论，不要复述或罗列收到的上下文原始数据（如K线明细、行情列表、盘口数据等）。
        """

        # 5. 调用 AI
        try:
            prompt = "\n".join(conversation)
            if dry_run:
                return {
                    "system_prompt": system_prompt,
                    "prompt": prompt,
                    "context": context_str
                }
            
            # [核心改进] 使用信号量控制并发，并添加超时控制
            timeout_sec = 75.0
            async with self.ai_semaphore:
                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        ai_core_client.call_ai_best_effort,
                        prompt,
                        system_prompt,
                        preferred_provider=preferred_provider,
                        api_key=api_key
                    ),
                    timeout=timeout_sec
                )
            
            def _has_trade_evidence(ctx: str) -> bool:
                if not ctx:
                    return False
                for k in ["已执行", "成交记录", "成交价", "委托编号", "挂单"]:
                    if k in ctx:
                        return True
                return False

            def _sanitize_execution_claims(text: str) -> str:
                replaced = text
                for old, new in [
                    ("已成交", "拟成交"),
                    ("已挂单", "拟挂单"),
                    ("已下单", "拟下单"),
                    ("已执行", "拟执行"),
                    ("成交完成", "成交待确认"),
                    ("买入成功", "买入待确认"),
                    ("卖出成功", "卖出待确认"),
                ]:
                    replaced = replaced.replace(old, new)
                return replaced

            if response and not _has_trade_evidence(context_str):
                for k in ["已成交", "已挂单", "已下单", "已执行", "成交完成", "买入成功", "卖出成功"]:
                    if k in response:
                        response = "【交易执行校验】系统未发现成交/挂单记录，以下内容仅为分析建议。\n" + _sanitize_execution_claims(response)
                        break

            if response:
                await _handle_text_commands(response or "", "AI回复")

            # 保存并返回 AI 回复
            if response:
                await self.save_message("assistant", response)
                return response
            else:
                raise Exception("AI returned empty response")
            
        except asyncio.TimeoutError:
            error_msg = "抱歉，AI 交易员响应超时，请稍后重试。"
            await self.save_message("assistant", error_msg)
            return error_msg
        except Exception as e:
            logger.warning(f"Chat: AI API call failed: {e}")
            msg = str(e)
            if "Connection error" in msg or "All connection attempts failed" in msg:
                error_msg = "抱歉，AI 交易员暂时无法连接（外网连接异常/被拦截）。请检查网络或代理后重试。"
            else:
                error_msg = f"抱歉，AI 交易员暂时无法连接 ({msg})。请稍后再试。"
            await self.save_message("assistant", error_msg)
            return error_msg

    async def get_ai_response(self, user_id: str, message: str) -> str:
        # 保留原方法作为兼容，直接调用新方法
        return await self.process_user_message(message)

chat_service = ChatService()

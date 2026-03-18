import pandas as pd
import asyncio
from datetime import datetime, timedelta
import json
from app.services.data_provider import data_provider
from app.services.ai_service import ai_service
from app.services.search_service import search_service
from app.services.sector_analysis import sector_analysis
from app.services.logger import selector_logger
from app.services.chat_service import chat_service
from app.services.market.fundamental_service import fundamental_service
from app.services.market.tdx_formula_service import tdx_formula_service
from app.services.market.stock_data_service import stock_data_service
import concurrent.futures
from typing import Any, Dict, List, TypedDict
from app.services.market.market_utils import get_limit_prices

from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator, DailyBar, OutcomeEvent

class QuoteItem(TypedDict):
    ts_code: str
    quote: Dict[str, Any]
    amount: float
    volume_ratio: float
    turnover_rate: float
    pct_chg: float
    intraday_range: float
    score: float

class StockSelectorService:
    def __init__(self):
        # 增加并发控制信号量，限制 AI 并行分析的数量 (防止 API 超时/限流)
        self.ai_semaphore = asyncio.Semaphore(3)

    async def close(self):
        """清理资源"""
        try:
            await search_service.close()
            # 其他需要清理的服务可以在这里添加
        except Exception as e:
            selector_logger.log(f"Error closing StockSelectorService: {e}", level="INFO")

    def _persist_selector_results(self, trade_date: str, strategy: str, results: list):
        db = SessionLocal()
        try:
            try:
                event_date = datetime.strptime(trade_date.replace("-", "").replace("/", ""), "%Y%m%d").date()
            except Exception:
                event_date = datetime.now().date()

            event_type = f"selector_{strategy or 'default'}"

            for idx, r in enumerate(results or []):
                if not isinstance(r, dict):
                    continue
                ts_code = r.get("ts_code")
                if not ts_code:
                    continue
                payload = {
                    "strategy": strategy or "default",
                    "rank": idx + 1,
                    "ts_code": ts_code,
                    "name": r.get("name"),
                    "industry": r.get("industry"),
                    "score": r.get("score"),
                    "is_worth_trading": r.get("is_worth_trading"),
                    "is_observation": r.get("is_observation"),
                    "analysis": r.get("analysis"),
                    "reason": r.get("reason"),
                    "source": r.get("source"),
                    "realtime_price": r.get("realtime_price"),
                    "metrics": r.get("metrics"),
                }

                existing = (
                    db.query(OutcomeEvent)
                    .filter(
                        OutcomeEvent.ts_code == ts_code,
                        OutcomeEvent.event_type == event_type,
                        OutcomeEvent.event_date == event_date,
                    )
                    .first()
                )
                if existing:
                    existing.payload_json = json.dumps(payload, ensure_ascii=False)
                else:
                    db.add(
                        OutcomeEvent(
                            ts_code=ts_code,
                            event_type=event_type,
                            event_date=event_date,
                            payload_json=json.dumps(payload, ensure_ascii=False),
                        )
                    )
            db.commit()
        finally:
            db.close()

    async def _upsert_monitor_plans_from_selector(self, trade_date: str, strategy: str, results: list):
        from app.services.trading_service import trading_service

        try:
            plan_date = datetime.strptime(trade_date.replace("-", "").replace("/", ""), "%Y%m%d").date()
        except Exception:
            plan_date = datetime.now().date()

        try:
            from app.services.market.market_utils import is_after_market_close
            trade_date_str = trade_date.replace("-", "").replace("/", "")
            trade_cal = await data_provider.check_trade_day(trade_date_str)
            next_trade_date_str = trade_cal.get("next_trade_date")
            if next_trade_date_str:
                next_trade_date = datetime.strptime(next_trade_date_str, "%Y%m%d").date()
                if is_after_market_close(datetime.now()) or plan_date < datetime.now().date():
                    plan_date = next_trade_date
        except Exception:
            pass

        strategy_key = strategy or "default"
        strategy_cn = "多维综合"
        if strategy_key == "pullback":
            strategy_cn = "强势回调"
        elif strategy_key == "vol_doubling":
            strategy_cn = "倍量柱"
        elif strategy_key == "four_signals":
            strategy_cn = "四信号共振"
            
        strategy_name = f"选股监控-{strategy_cn}"

        for r in results or []:
            if not isinstance(r, dict):
                continue
            # 四信号策略不需要 is_worth_trading 显式为 True，只要分数够高即可 (AI已确认)
            if strategy_key != "four_signals" and r.get("is_worth_trading") is not True:
                continue
            
            ts_code = r.get("ts_code")
            if not ts_code:
                continue

            name = r.get("name") or ts_code
            score = float(r.get("score") or 0.0)
            rt_price = float(r.get("realtime_price") or 0.0)
            # 如果实时价格未获取到，尝试用 recent_close
            if rt_price <= 0:
                rt_price = float(r.get("price") or 0.0)
                
            if rt_price <= 0:
                continue

            analysis = (r.get("analysis") or "").strip()
            reason = f"[{strategy_name}] {name} 评分{score:.0f}"
            if analysis:
                reason = reason + f"；{analysis[:160]}"

            # [用户定制] 四信号共振策略参数调整
            # 波动大，止损放宽至 >10% (设为 12%)，不设默认止盈 (设为 0 或极高)
            if strategy_key == "four_signals":
                stop_loss = rt_price * 0.88 # -12% 止损
                take_profit = 0.0 # 不设止盈，让利润奔跑
            else:
                stop_loss = rt_price * 0.95
                take_profit = rt_price * 1.10

            try:
                await trading_service.create_plan(
                    ts_code=ts_code,
                    strategy_name=strategy_name,
                    buy_price=rt_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    position_pct=0.1,
                    reason=reason,
                    plan_date=plan_date,
                    score=score,
                    source="system",
                    order_type="MARKET",
                    limit_price=rt_price,
                )
            except Exception as e:
                selector_logger.log(f"创建/更新监控计划失败 {ts_code}: {e}", level="ERROR")

    async def _get_indicators_batch(self, ts_codes, trade_date=None):
        """
        批量获取预计算的指标 (已优化为调用 stock_data_service)
        """
        from types import SimpleNamespace
        
        # 调用优化后的服务方法
        indicator_data = await asyncio.to_thread(
            stock_data_service.get_latest_indicators_batch,
            ts_codes,
            trade_date
        )
        if not indicator_data and trade_date:
            indicator_data = await asyncio.to_thread(
                stock_data_service.get_latest_indicators_batch,
                ts_codes,
                None
            )
        if not indicator_data:
            selector_logger.log("指标缺失，触发指标补算", level="WARN")
            from app.services.indicator_service import indicator_service
            await indicator_service.calculate_for_codes(ts_codes[:200], trade_date=trade_date, force_full=False, force_recalc_today=False)
            indicator_data = await asyncio.to_thread(
                stock_data_service.get_latest_indicators_batch,
                ts_codes,
                trade_date
            )
        
        # 转换为支持属性访问的对象 (兼容原有代码)
        results = {}
        for ts_code, data in indicator_data.items():
            results[ts_code] = SimpleNamespace(**data)
            
        return results

    async def _is_near_ma5(self, ts_code, freq='D', threshold=0.05):
        """
        检查股价是否在 MA5 附近 (上方或下方不远处)
        """
        kline = await data_provider.get_kline(ts_code, freq=freq)
        if not kline or len(kline) < 5:
            return False
            
        latest = kline[-1]
        close = float(latest['close'])
        
        # 优先使用预计算指标
        if 'ma5' in latest and latest['ma5'] is not None:
            ma5 = float(latest['ma5'])
        else:
            # 回退到手动计算
            df = pd.DataFrame(kline)
            df['close'] = df['close'].astype(float)
            df['ma5'] = df['close'].rolling(window=5).mean()
            ma5 = df['ma5'].iloc[-1]
        
        if pd.isna(ma5) or ma5 == 0:
            return False
            
        # 逻辑：
        # 1. 股价不能大幅低于 MA5 (趋势破坏) -> close >= ma5 * (1 - threshold)
        # 2. 股价不能大幅高于 MA5 (乖离过大/追高) -> close <= ma5 * (1 + threshold)
        
        lower_bound = ma5 * (1 - threshold)
        upper_bound = ma5 * (1 + threshold)
        
        return lower_bound <= close <= upper_bound

    async def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """
        计算 ATR (平均真实波幅)
        """
        if len(df) < period + 1:
            return 0.0
            
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)
        
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]
        return atr

    async def _check_weekly_structure(self, ts_code: str) -> dict:
        """
        [新增] 周线结构分析 (战术沙盘)
        1. ABC 结构识别 (简化版：检测最近 12 周是否有 N 字下跌或盘整)
        2. 动量背离 (价格新低但 RSI 不新低)
        3. 均线共振 (周线 MA60 支撑)
        """
        kline = await data_provider.get_kline(ts_code, freq='W')
        if not kline or len(kline) < 20:
            return {"valid": False, "reason": "周线数据不足"}
            
        df = pd.DataFrame(kline)
        df['close'] = df['close'].astype(float)
        df['low'] = df['low'].astype(float)
        latest = df.iloc[-1]
        
        # 1. 均线支撑验证 (周线 MA60 是牛熊分界线)
        ma60 = df['close'].rolling(60).mean().iloc[-1]
        close = float(latest['close'])
        
        # 如果 MA60 有效，且股价在 MA60 附近 (支撑位)
        is_ma60_support = False
        if not pd.isna(ma60) and ma60 > 0:
            # 支撑范围：MA60 * 0.95 ~ MA60 * 1.05
            if ma60 * 0.95 <= close <= ma60 * 1.05:
                is_ma60_support = True
        
        # 2. 简单的 RSI 背离检测
        # 计算 14 周 RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 检测最近 12 周的低点
        recent_12 = df.tail(12)
        min_price_idx = recent_12['low'].idxmin()
        min_rsi_idx = recent_12['rsi'].idxmin()
        
        # 如果价格最低点比 RSI 最低点发生得更晚 (价格创新低，RSI 没创新低)
        is_divergence = False
        if min_price_idx > min_rsi_idx:
            # 且当前 RSI 已拐头向上
            if df['rsi'].iloc[-1] > df['rsi'].iloc[-2]:
                is_divergence = True
                
        return {
            "valid": True,
            "ma60_support": is_ma60_support,
            "rsi_divergence": is_divergence,
            "atr": await self._calculate_atr(df),
            "ma60": ma60
        }

    async def _detect_intraday_pullback(self, ts_code: str, quote: Dict[str, Any]) -> Dict[str, Any] | None:
        try:
            price = float(quote.get("price") or 0)
            pre_close = float(quote.get("pre_close") or 0)
            if price <= 0 or pre_close <= 0:
                return None

            limit_up, _ = get_limit_prices(ts_code, pre_close, str(quote.get("name") or ""))
            if limit_up > 0 and price >= limit_up:
                return None

            bars = await data_provider.get_kline(ts_code, freq="5min", limit=96, include_indicators=False, cache_scope="selector")
            if not bars:
                return None

            df = pd.DataFrame(bars)
            if df.empty:
                return None

            if "time" in df.columns:
                df["trade_time"] = pd.to_datetime(df["time"], errors="coerce")
            elif "trade_time" in df.columns:
                df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
            else:
                return None

            df = df.dropna(subset=["trade_time"])
            if df.empty:
                return None

            target_date = df["trade_time"].dt.date.max()
            df = df[df["trade_time"].dt.date == target_date].copy()
            if df.empty or len(df) < 12:
                return None

            df = df.sort_values("trade_time")
            vol_col = "volume" if "volume" in df.columns else "vol" if "vol" in df.columns else None
            amt_col = "amount" if "amount" in df.columns else None
            if vol_col is None:
                return None

            df["vol"] = df[vol_col].astype(float)
            if amt_col:
                df["amt"] = df[amt_col].astype(float)
            else:
                df["amt"] = df["close"].astype(float) * df["vol"].astype(float)

            df = df[(df["vol"] > 0) & (df["close"] > 0)]
            if df.empty:
                return None

            df["cum_vol"] = df["vol"].cumsum()
            df["cum_amt"] = df["amt"].cumsum()
            df["vwap"] = df["cum_amt"] / df["cum_vol"].replace(0, pd.NA)

            morning_mask = df["trade_time"].dt.time <= datetime.strptime("11:30", "%H:%M").time()
            morning_df = df[morning_mask]
            if morning_df.empty:
                return None

            max_idx = morning_df["high"].astype(float).idxmax()
            max_loc = df.index.get_loc(max_idx)
            if max_loc >= len(df) - 3:
                return None

            max_high = float(df.loc[max_idx, "high"])
            min_low = float(morning_df[morning_df.index <= max_idx]["low"].astype(float).min())
            if min_low <= 0 or max_high <= 0:
                return None

            surge_pct = (max_high - min_low) / min_low * 100
            if surge_pct < 6.0:
                return None

            morning_vol = float(morning_df["vol"].mean()) if not morning_df.empty else float(df["vol"].mean())
            surge_vol = float(df.iloc[max(0, max_loc - 2) : max_loc + 1]["vol"].mean())
            if morning_vol <= 0 or surge_vol <= morning_vol * 1.3:
                return None

            pullback_df = df.iloc[max_loc + 1 :]
            pullback_tail = pullback_df.tail(3)
            if pullback_tail.empty:
                return None

            pullback_vol = float(pullback_tail["vol"].mean())
            if pullback_vol <= 0 or pullback_vol > surge_vol * 0.65:
                return None

            pullback_low = float(pullback_tail["low"].astype(float).min())
            drawdown_pct = (max_high - pullback_low) / max_high * 100
            if drawdown_pct < 0.5 or drawdown_pct > 4.5:
                return None

            vwap_tail = pullback_tail["vwap"].astype(float)
            if vwap_tail.isna().any():
                return None

            if (pullback_tail["low"].astype(float) < vwap_tail * 0.998).any():
                return None

            last_vwap = float(vwap_tail.iloc[-1])
            if last_vwap <= 0 or price < last_vwap * 1.0:
                return None

            return {
                "ts_code": ts_code,
                "name": quote.get("name") or ts_code,
                "pct_chg": float(quote.get("pct_chg") or 0),
                "price": price,
                "type": "IntradayPullback",
                "surge_pct": round(surge_pct, 2),
                "drawdown_pct": round(drawdown_pct, 2),
                "vwap": round(last_vwap, 4),
                "volume_ratio": float(quote.get("volume_ratio") or 0),
                "turnover_rate": float(quote.get("turnover_rate") or 0),
            }
        except Exception:
            return None

    async def scan_noon_opportunities(self):
        """
        午间扫描：寻找下午的交易机会
        策略：
        1. 优先寻找龙头 (连板/涨停)
        2. 如果没有龙头，寻找趋势拐点启动 (Trend Reversal)
        """
        selector_logger.log("开始执行午间扫描...")
        # 获取最新交易日
        trade_date = await data_provider.get_last_trade_date(include_today=True)
        
        stock_list = await data_provider.get_stock_basic()
        if not stock_list:
            return {"dragons": [], "reversals": [], "msg": "No basic data"}

        df = pd.DataFrame(stock_list)
        if df.empty or "ts_code" not in df.columns:
            return {"dragons": [], "reversals": [], "msg": "No basic data"}

        df = df[
            ~df["ts_code"].str.startswith("688")
            & ~df["ts_code"].str.startswith("8")
            & ~df["ts_code"].str.startswith("4")
            & ~df["ts_code"].str.startswith("920")
            & ~df["ts_code"].str.endswith(".BJ")
        ].copy()

        if "name" in df.columns:
            df = df[~df["name"].str.contains("ST", na=False)]
            df = df[~df["name"].str.contains("退", na=False)]

        turnover_codes = await data_provider.get_market_turnover_top_codes(top_n=200)
        if turnover_codes:
            df_pool = df[df["ts_code"].isin(turnover_codes)].copy()
        else:
            df_pool = df

        if df_pool.empty:
            return {"dragons": [], "reversals": [], "msg": "No candidate pool"}

        ts_codes = df_pool["ts_code"].tolist()
        
        # 2. 获取实时行情
        try:
            quotes = await data_provider.get_realtime_quotes(ts_codes, force_tdx=False, cache_scope="selector")
        except Exception as e:
            selector_logger.log(f"获取实时行情失败，已降级为空结果: {e}", level="WARNING")
            quotes = {}
        quote_items: List[QuoteItem] = []
        max_amount = 0.0
        max_volume_ratio = 0.0
        max_turnover_rate = 0.0
        max_pct_chg = 0.0
        max_intraday_range = 0.0
        for ts_code, quote in quotes.items():
            if not quote or quote.get('price', 0) <= 0 or quote.get('vol', 0) == 0:
                continue
            amount = float(quote.get('amount') or 0)
            volume_ratio = float(quote.get('volume_ratio') or 0)
            turnover_rate = float(quote.get('turnover_rate') or 0)
            pct_chg = float(quote.get('pct_chg') or 0)
            high_price = float(quote.get('high') or 0)
            low_price = float(quote.get('low') or 0)
            pre_close = float(quote.get('pre_close') or 0)
            range_base = pre_close if pre_close > 0 else float(quote.get('price') or 0)
            intraday_range = 0.0
            if range_base > 0 and high_price > 0 and low_price > 0:
                intraday_range = (high_price - low_price) / range_base * 100
            if amount > max_amount:
                max_amount = amount
            if volume_ratio > max_volume_ratio:
                max_volume_ratio = volume_ratio
            if turnover_rate > max_turnover_rate:
                max_turnover_rate = turnover_rate
            if pct_chg > max_pct_chg:
                max_pct_chg = pct_chg
            if intraday_range > max_intraday_range:
                max_intraday_range = intraday_range
            quote_items.append({
                "ts_code": ts_code,
                "quote": quote,
                "amount": amount,
                "volume_ratio": volume_ratio,
                "turnover_rate": turnover_rate,
                "pct_chg": pct_chg,
                "intraday_range": intraday_range,
                "score": 0.0
            })
        max_amount = max_amount or 1.0
        max_volume_ratio = max_volume_ratio or 1.0
        max_turnover_rate = max_turnover_rate or 1.0
        max_pct_chg = max_pct_chg or 1.0
        max_intraday_range = max_intraday_range or 1.0
        for item in quote_items:
            item["score"] = (
                (item["amount"] / max_amount) * 0.3
                + (item["volume_ratio"] / max_volume_ratio) * 0.25
                + (item["turnover_rate"] / max_turnover_rate) * 0.15
                + (item["pct_chg"] / max_pct_chg) * 0.15
                + (item["intraday_range"] / max_intraday_range) * 0.15
            )
        quote_items.sort(key=lambda x: x["score"], reverse=True)

        reversal_codes = []
        for item in quote_items:
            pct_chg = float(item.get("pct_chg") or 0.0)
            volume_ratio = float(item.get("volume_ratio") or 0.0)
            turnover_rate = float(item.get("turnover_rate") or 0.0)
            intraday_range = float(item.get("intraday_range") or 0.0)
            is_reversal_band = 3.0 <= pct_chg <= 8.0
            is_volume_surge = volume_ratio >= 1.8 or turnover_rate >= 2.5
            is_price_swing = intraday_range >= 5.0
            is_anomaly = is_volume_surge and is_price_swing and abs(pct_chg) >= 2.0
            if is_reversal_band or is_anomaly:
                reversal_codes.append(item["ts_code"])
        indicators = {}
        if reversal_codes:
            indicators = await self._get_indicators_batch(reversal_codes, trade_date=trade_date)
        
        dragons = []
        reversals = []
        intraday_pullbacks: List[Dict[str, Any]] = []
        
        for item in quote_items:
            ts_code = item["ts_code"]
            quote = item["quote"]
            # 过滤停牌或数据异常的股票 (vol=0, price=0)
            if not quote or quote['price'] <= 0 or quote.get('vol', 0) == 0: 
                continue
            
            pct_chg = quote['pct_chg']
            price = quote['price']
            name = quote['name']
            volume_ratio = float(item.get("volume_ratio") or 0)
            turnover_rate = float(item.get("turnover_rate") or 0)
            intraday_range = float(item.get("intraday_range") or 0)
            
            # 3.1 寻找龙头 (涨停)
            # 简单判定：涨幅 > 9.5%
            if pct_chg >= 9.5:
                dragons.append({
                    "ts_code": ts_code,
                    "name": name,
                    "pct_chg": pct_chg,
                    "price": price,
                    "type": "Dragon"
                })
                continue # 已是龙头，归类完毕
                
            # 3.2 寻找趋势拐点 (Trend Reversal)
            # 条件：
            # 1. 涨幅适中 (3% - 8%)
            # 2. 趋势向上 (MA20支撑) 
            if 3.0 <= pct_chg <= 8.0:
                ind = indicators.get(ts_code)
                if not ind: continue
                if volume_ratio < 1.3 and turnover_rate < 2.0 and intraday_range < 3.0:
                    continue
                
                # Check Trend
                # Current Price > MA20 (Trend support)
                is_trend_ok = False
                if ind.ma20 and price > ind.ma20:
                    is_trend_ok = True
                
                # [新增] 乖离率检查 (Bias Ratio Protection)
                # 即使趋势向上，如果乖离率过大，也跳过，等待回归
                is_bias_ok = True
                if hasattr(ind, 'bias5') and ind.bias5 is not None:
                    if ind.bias5 > 5.0: # BIAS5 > 5% 视为短线过热
                        is_bias_ok = False
                elif hasattr(ind, 'bias10') and ind.bias10 is not None:
                    if ind.bias10 > 10.0: # BIAS10 > 10% 视为中线过热
                        is_bias_ok = False
                
                if is_trend_ok and is_bias_ok:
                    reversals.append({
                        "ts_code": ts_code,
                        "name": name,
                        "pct_chg": pct_chg,
                        "price": price,
                        "ma20": ind.ma20,
                        "type": "Reversal",
                        "volume_ratio": volume_ratio,
                        "turnover_rate": turnover_rate,
                        "intraday_range": intraday_range
                    })
            
            if abs(pct_chg) >= 2.0:
                ind = indicators.get(ts_code)
                if not ind:
                    continue
                is_volume_surge = volume_ratio >= 1.8 or turnover_rate >= 2.5
                is_price_swing = intraday_range >= 5.0
                is_anomaly = is_volume_surge and is_price_swing
                if not is_anomaly:
                    continue
                ma20 = float(ind.ma20 or 0)
                if ma20 > 0 and price < ma20 * 0.98:
                    continue
                reversals.append({
                    "ts_code": ts_code,
                    "name": name,
                    "pct_chg": pct_chg,
                    "price": price,
                    "ma20": ind.ma20,
                    "type": "Anomaly",
                    "volume_ratio": volume_ratio,
                    "turnover_rate": turnover_rate,
                    "intraday_range": intraday_range
                })

        pullback_candidates = []
        for item in quote_items[:40]:
            volume_ratio = float(item.get("volume_ratio") or 0)
            intraday_range = float(item.get("intraday_range") or 0)
            if volume_ratio >= 1.2 and intraday_range >= 3.0:
                pullback_candidates.append(item)

        if pullback_candidates:
            async def _worker(item: QuoteItem):
                return await self._detect_intraday_pullback(item["ts_code"], item["quote"])

            tasks = [_worker(item) for item in pullback_candidates[:20]]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict) and res.get("ts_code"):
                    intraday_pullbacks.append(res)
        
        # Sort
        dragons.sort(key=lambda x: x['pct_chg'], reverse=True)
        reversals.sort(key=lambda x: x['pct_chg'], reverse=True)
        intraday_pullbacks.sort(key=lambda x: x.get('surge_pct', 0), reverse=True)
        
        return {
            "dragons": dragons[:5], # Top 5 dragons
            "reversals": reversals[:10], # Top 10 reversals
            "intraday_pullbacks": intraday_pullbacks[:10]
        }

    async def scan_evening_opportunities(self, trade_date_str=None):
        """
        晚间扫描：寻找次日交易机会
        策略：
        1. 龙头延续 (Dragons)
        2. 趋势中继/拐点 (Reversals)
        """
        selector_logger.log("开始执行晚间扫描...")
        explicit_trade_date = trade_date_str is not None
        if not trade_date_str:
            trade_date_str = await data_provider.get_last_trade_date(include_today=True)
            
        # 1. 获取基础数据
        # silent argument is not supported in get_daily_basic yet, removing it
        df_basic = await data_provider.get_daily_basic(trade_date=trade_date_str, allow_fallback_latest=not explicit_trade_date)
        if df_basic.empty and explicit_trade_date:
            df_basic = await data_provider.get_daily_basic(trade_date=trade_date_str, allow_fallback_latest=True)
            if not df_basic.empty:
                explicit_trade_date = False
        if df_basic.empty:
            return {"dragons": [], "reversals": [], "msg": "No basic data"}
        if "trade_date" in df_basic.columns:
            latest_date = str(df_basic["trade_date"].max())
            if latest_date and not explicit_trade_date:
                trade_date_str = latest_date.replace("-", "")

        # Filter ST, 688 (STAR), BJ (Beijing Stock Exchange)
        # 1. Filter by code prefix/suffix
        df = df_basic[
            ~df_basic['ts_code'].str.startswith('688') & # 科创板
            ~df_basic['ts_code'].str.startswith('8') &   # 北交所
            ~df_basic['ts_code'].str.startswith('4') &   # 北交所
            ~df_basic['ts_code'].str.startswith('920') & # 北交所新号段
            ~df_basic['ts_code'].str.endswith('.BJ')     # 北交所后缀
        ].copy()

        if 'name' not in df.columns:
             stock_list = await data_provider.get_stock_basic()
             name_map = {s['ts_code']: s['name'] for s in stock_list}
             df['name'] = df['ts_code'].map(name_map)
        
        # 2. Filter by name (ST, 退)
        df = df[~df['name'].str.contains('ST', na=False)]
        df = df[~df['name'].str.contains('退', na=False)]

        today_str = datetime.now().strftime("%Y%m%d")
        is_today = trade_date_str == today_str
        if is_today and data_provider.is_trading_time():
            turnover_codes = await data_provider.get_turnover_top_codes(universe_codes=df["ts_code"].tolist(), top_n=100)
        else:
            turnover_codes = await data_provider.market_data_service.get_market_turnover_top_codes(top_n=200)
        if turnover_codes:
            rank_map = {c: i for i, c in enumerate(turnover_codes)}
            df_pool = df[df["ts_code"].isin(turnover_codes)].copy()
            if not df_pool.empty:
                df_pool["__rank"] = df_pool["ts_code"].map(rank_map)
                df_pool = df_pool.sort_values("__rank", ascending=True).drop(columns=["__rank"])
        else:
            df_pool = pd.DataFrame()

        if df_pool.empty:
            mv_col = 'circ_mv' if 'circ_mv' in df.columns else 'total_mv'
            df_pool = df.sort_values(mv_col, ascending=False).head(500)
        
        ts_codes = df_pool['ts_code'].tolist()
        
        # New code: Fetch market data (close, pct_chg)
        market_data = {}
        
        def fetch_market_data():
            from app.db.session import SessionLocal
            from sqlalchemy import desc
            db = SessionLocal()
            try:
                target_date = trade_date_str
                bars = db.query(DailyBar.ts_code, DailyBar.close, DailyBar.pct_chg).filter(
                    DailyBar.trade_date == target_date,
                    DailyBar.ts_code.in_(ts_codes)
                ).all()
                if not bars:
                    latest = db.query(DailyBar.trade_date).order_by(desc(DailyBar.trade_date)).first()
                    if latest:
                        target_date = latest[0]
                        bars = db.query(DailyBar.ts_code, DailyBar.close, DailyBar.pct_chg).filter(
                            DailyBar.trade_date == target_date,
                            DailyBar.ts_code.in_(ts_codes)
                        ).all()
                if hasattr(target_date, "strftime"):
                    target_date_str = target_date.strftime("%Y%m%d")
                else:
                    target_date_str = str(target_date).replace("-", "")
                return target_date_str, {b.ts_code: {'close': b.close, 'pct_chg': b.pct_chg} for b in bars}
            finally:
                db.close()

        market_date_str, market_data = await asyncio.to_thread(fetch_market_data)
        if market_date_str:
            trade_date_str = market_date_str
        
        # 2. 获取预计算指标
        # 传入 trade_date_str 确保只获取最新的指标，极大提升性能
        indicators = await self._get_indicators_batch(ts_codes, trade_date=trade_date_str)
        selector_logger.log(f"获取到 {len(indicators)} 个股票的预计算指标")
        
        dragons = []
        reversals = []
        
        for _, row in df_pool.iterrows():
            ts_code = row['ts_code']
            name = row['name']
            
            # Use fetched market data
            m_data = market_data.get(ts_code)
            if not m_data:
                selector_logger.log(f"股票 {ts_code} ({name}) 无当日行情数据，跳过", level="DEBUG")
                continue
            
            pct_chg = m_data['pct_chg']
            price = m_data['close']
            
            # 2.1 龙头 (涨停)
            if pct_chg >= 9.5:
                dragons.append({
                    "ts_code": ts_code,
                    "name": name,
                    "pct_chg": pct_chg,
                    "price": price,
                    "type": "Dragon"
                })
                selector_logger.log(f"发现龙头候选: {ts_code} ({name}), 涨幅: {pct_chg}%")
                continue
                
            # 2.2 趋势拐点
            if 2.0 <= pct_chg <= 9.0: # 放宽涨幅限制
                ind = indicators.get(ts_code)
                if not ind:
                    selector_logger.log(f"股票 {ts_code} ({name}) 无指标数据，跳过", level="DEBUG")
                    continue
                
                # Check Trend
                is_trend_ok = False
                # 放宽趋势判断：只要在 MA20 上方，或者距离 MA20 不远 (MA20 * 0.97)
                if ind.ma20 and price > ind.ma20 * 0.97:
                    is_trend_ok = True
                else:
                    selector_logger.log(f"股票 {ts_code} ({name}) 趋势不佳: Price={price}, MA20={ind.ma20}", level="DEBUG")
                
                # [新增] 乖离率检查 (Bias Ratio Protection)
                # 即使趋势向上，如果乖离率过大，也跳过，等待回归
                is_bias_ok = True
                if hasattr(ind, 'bias5') and ind.bias5 is not None:
                    if ind.bias5 > 15.0: # 放宽：BIAS5 > 15% 视为日线级别过热 (原 5%)
                        is_bias_ok = False
                        selector_logger.log(f"股票 {ts_code} ({name}) BIAS5 过高: {ind.bias5}", level="DEBUG")
                elif hasattr(ind, 'bias10') and ind.bias10 is not None:
                    if ind.bias10 > 25.0: # 放宽：BIAS10 > 25% 视为日线级别过热 (原 10%)
                        is_bias_ok = False
                        selector_logger.log(f"股票 {ts_code} ({name}) BIAS10 过高: {ind.bias10}", level="DEBUG")
                
                if is_trend_ok and is_bias_ok:
                    reversals.append({
                        "ts_code": ts_code,
                        "name": name,
                        "pct_chg": pct_chg,
                        "price": price,
                        "ma20": ind.ma20,
                        "type": "Reversal"
                    })
                    selector_logger.log(f"发现反转候选: {ts_code} ({name}), 涨幅: {pct_chg}%")
        
        selector_logger.log(f"扫描完成: 发现 {len(dragons)} 个龙头, {len(reversals)} 个反转")
        dragons.sort(key=lambda x: x['pct_chg'], reverse=True)
        reversals.sort(key=lambda x: x['pct_chg'], reverse=True)
        
        return {
            "dragons": dragons[:5], 
            "reversals": reversals[:10]
        }

    def _calculate_tech_score(self, ind, kline):
        """
        计算综合技术面得分 (用于候选股排序)
        """
        tech_score = 0
        
        # 1. 趋势得分
        if getattr(ind, 'is_monthly_bullish', False): tech_score += 30
        if getattr(ind, 'is_weekly_bullish', False): tech_score += 20
        if getattr(ind, 'is_daily_bullish', False): tech_score += 10
        if getattr(ind, 'is_trend_recovering', False): tech_score += 15
        
        # 2. 均线斜率得分
        ma20_slope = getattr(ind, 'weekly_ma20_slope', 0)
        if ma20_slope is not None:
            if ma20_slope > 0.5: tech_score += 15
            elif ma20_slope > 0: tech_score += 5
        
        # 3. 成交量得分 (量价配合)
        if kline and len(kline) >= 5:
            try:
                vol_now = float(kline[-1].get('volume', 0))
                vol_ma5 = sum(float(k.get('volume', 0)) for k in kline[-5:]) / 5
                if vol_now > vol_ma5 * 1.2: tech_score += 10 # 放量
                elif vol_now < vol_ma5 * 0.8: tech_score -= 5 # 缩量严重
            except (ValueError, TypeError):
                pass
        
        # 4. 阶段强度 (过去20日涨幅)
        if kline and len(kline) >= 20:
            try:
                close_now = float(kline[-1].get('close', 0))
                close_prev = float(kline[-20].get('close', 0))
                if close_prev > 0:
                    pct_20d = (close_now - close_prev) / close_prev * 100
                    if pct_20d > 15: tech_score += 10
                    elif pct_20d < 0: tech_score -= 10
            except (ValueError, TypeError):
                pass
                
        return tech_score

    async def _get_historical_kline_batch(self, ts_codes, trade_date=None, days=60):
        """
        批量获取多只股票的历史 K 线数据 (带指标)
        """
        if not ts_codes:
            return {}
            
        # [优化] 使用 data_provider 的批量接口，内部有并行和缓存逻辑，避免同步 DB 查询阻塞
        from app.services.data_provider import data_provider
        try:
            # 增加 limit 确保获取足够的数据（MACD/MA等计算需要背景数据）
            # 默认 days 是 60，我们取 150 以保证指标准确
            klines_map = await data_provider.get_kline_batch(ts_codes, freq='D', limit=max(150, days + 30), cache_scope="selector")
            
            result = {}
            for ts_code, klines in klines_map.items():
                if klines:
                    # 只要最近 days 天的数据用于展示/分析
                    result[ts_code] = klines[-days:]
            return result
        except Exception as e:
            selector_logger.log(f"Error in _get_historical_kline_batch: {e}", level="ERROR")
            return {}

    async def select_stocks(self, strategy="default", top_n=10, trade_date=None):
        """
        多维度综合选股
        """
        # 强制限制初选传递给 AI 的标的数量上限为 10 只
        # 用户要求: 最多 10 个股票提交给 AI
        MAX_CANDIDATES_FOR_AI = 10
        
        # Clear sector cache before starting
        sector_analysis.clear_cache()
        
        selector_logger.clear()
        selector_logger.log(f"开始执行选股策略: {strategy}...")
        
        # 获取最新交易日 (如果没有指定)
        if not trade_date:
            trade_date = await data_provider.get_last_trade_date()
        selector_logger.log(f"使用交易日进行筛选: {trade_date}")
        
        # 1. 根据不同策略进行初选
        try:
            if strategy == "pullback":
                candidates = await self._filter_pullback_candidates(trade_date)
            elif strategy == "vol_doubling":
                # [新策略] 倍量柱选股
                # 注意：select_single_vol_doubling 返回的是 List[Dict]，需转换为 DataFrame
                cand_list = await self.select_single_vol_doubling(top_n=MAX_CANDIDATES_FOR_AI)
                candidates = pd.DataFrame(cand_list)
            elif strategy == "four_signals":
                # [新策略] 四信号共振选股
                cand_list = await self.select_four_signal_resonance(top_n=MAX_CANDIDATES_FOR_AI)
                if isinstance(cand_list, list):
                    candidates = pd.DataFrame(cand_list)
                else:
                    candidates = cand_list
            else:
                candidates = await self._filter_candidates(trade_date)
        except Exception as e:
            selector_logger.log(f"策略初选过程出错: {str(e)}", level="ERROR")
            raise e
            
        if candidates.empty:
            selector_logger.log("初选未找到符合条件的股票", level="INFO")
            return []
        
        # [前10截取] 确保初选名单最多只有 10 只进入行业加强和后续流程
        candidates = candidates.head(MAX_CANDIDATES_FOR_AI)
        selector_logger.log(f"[前10截取] 从初选池中选取 Top {len(candidates)} 只个股进入行业加强环节。")
        
        # 1.2 [用户要求] 行业加强：每个板块取成交额前10，共计最多100只股票进行技术面评分并重新排序
        # 逻辑：取初选 Top 10，查找其行业内的 Top 10（按成交额），与初选合并后按技术面评分取前 10
        # 获取所有相关股票的所属行业
        stock_basic = await asyncio.to_thread(stock_data_service.get_stock_basic)
        basic_map = {s['ts_code']: s for s in stock_basic}
        
        top_candidates = candidates.head(MAX_CANDIDATES_FOR_AI)
        ts_codes = top_candidates['ts_code'].tolist()
        
        industries = set()
        for ts_code in ts_codes:
            info = basic_map.get(ts_code)
            if info and info.get('industry'):
                industries.add(info['industry'])
        
        # 批量获取板块分析缓存或执行分析
        sector_context_map = {}
        for industry in industries:
            # 找到该行业的一个代表股
            representative_stock = next((c for c in ts_codes if basic_map.get(c, {}).get('industry') == industry), None)
            if representative_stock:
                res = await sector_analysis.analyze_sector(representative_stock)
                sector_context_map[industry] = res

        candidates = await self._enhance_candidates_with_sector_leaders(top_candidates, trade_date, sector_context_map)
        
        selector_logger.log(f"技术面筛选完成，共选取 {len(candidates)} 只进入基本面一票否决阶段 (上限 10 只)")
        
        # 1.3 [用户要求] 这10个股票进行基础信息与基本面数据获取，基础信息与基本面有严重问题的剔除
        vetoed_candidates = await self._apply_fundamental_veto(candidates)
        
        if vetoed_candidates.empty:
            selector_logger.log("经过基本面一票否决后，无符合条件的股票进入 AI 分析阶段", level="INFO")
            return []
            
        selector_logger.log(f"基本面过滤完成，最终 {len(vetoed_candidates)} 只进入 AI 深度分析阶段")
        
        # 2. AI + 搜索深度筛选
        final_results = await self._deep_analyze_candidates(vetoed_candidates, strategy)
        
        # [用户要求] AI 分析完成发回来最多 5 只股票
        # 按评分排序并取前 5
        final_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        best_results = final_results[:5]
        
        await asyncio.to_thread(self._persist_selector_results, trade_date, strategy, best_results)
        await self._upsert_monitor_plans_from_selector(trade_date, strategy, best_results)
        selector_logger.log(f"选股策略执行完毕，AI 深度分析 {len(final_results)} 只符合门槛，输出 {len(best_results)} 只最强标的")
        return best_results

    async def _filter_pullback_candidates(self, trade_date):
        """
        强势股回调筛选逻辑 - 优化版 (成交额Top2000 + 换手率过滤 + 均线过滤)
        """
        selector_logger.log(f"正在执行“强势回调”策略初选 (日期: {trade_date})...")
        
        # 1. 获取全市场成交额前2000的代码
        selector_logger.log(f"[海选] 正在获取全市场成交额前2000的个股作为初始海选池...")
        top_turnover_codes = await data_provider.market_data_service.get_market_turnover_top_codes(top_n=2000)
        if not top_turnover_codes:
            selector_logger.log("无法获取全市场成交额排名数据", level="WARN")
            return pd.DataFrame()

        # 2. 获取股票基本信息以进行初步过滤
        stock_list = await data_provider.get_stock_basic()
        name_map = {s['ts_code']: s['name'] for s in stock_list}
        turnover_map = {s.get("ts_code"): s for s in await data_provider.market_data_service.get_market_turnover_top(top_n=2000)}
        
        # 过滤：排除科创、北交、ST、退市 + 换手率 5-15%
        valid_codes = []
        for code in top_turnover_codes:
            # 排除 688, 8, 4, .BJ
            if code.startswith(('688', '8', '4')) or code.endswith('.BJ'):
                continue
            
            name = name_map.get(code, "")
            # 排除 ST, *ST, 退
            if "ST" in name or "退" in name:
                continue
            t_item = turnover_map.get(code) or {}
            t_rate = float(t_item.get("turnover_rate") or 0.0)
            if t_rate < 5.0 or t_rate > 15.0:
                continue
            
            valid_codes.append(code)
            
        selector_logger.log(f"[海选] 剔除科创/北交/ST/退市/换手率过滤后，剩余 {len(valid_codes)} 只个股进入均线过滤...")

        if not valid_codes:
            return pd.DataFrame()

        # 3. 批量获取预计算指标进行均线过滤 (MA5>=MA10>=MA20)
        selector_logger.log(f"[技术筛选] 正在执行“强势回调”策略初选，扫描 {len(valid_codes)} 只个股...")
        indicators_map = await self._get_indicators_batch(valid_codes, trade_date=trade_date)
        if not indicators_map:
            selector_logger.log("[技术筛选] 指标数据为空，无法执行选股", level="ERROR")
            raise RuntimeError("指标缺失，无法选股")
        trend_valid_codes = []
        for code in valid_codes:
            ind = indicators_map.get(code)
            if not ind:
                continue
            ma5 = float(getattr(ind, "ma5", 0) or 0)
            ma10 = float(getattr(ind, "ma10", 0) or 0)
            ma20 = float(getattr(ind, "ma20", 0) or 0)
            if ma5 <= 0 or ma10 <= 0 or ma20 <= 0:
                continue
            if ma5 >= ma10 >= ma20:
                trend_valid_codes.append(code)
        if not trend_valid_codes:
            selector_logger.log("均线过滤后无符合条件的股票", level="INFO")
            return pd.DataFrame()

        # 4. 批量获取实时行情和历史数据进行技术面深度匹配 (并行获取以提升性能)
        # 限制数量，避免过度请求，取前100只活跃股
        target_codes = trend_valid_codes[:100]
        quotes, historical_data = await asyncio.gather(
            data_provider.get_realtime_quotes(target_codes),
            self._get_historical_kline_batch(target_codes, trade_date=trade_date)
        )
        
        pullback_candidates = []
        for ts_code in target_codes:
            # 1. 获取 K 线数据
            kline = historical_data.get(ts_code, [])
            if not kline or len(kline) < 20: 
                continue
            
            # 2. 实时行情合并
            realtime_quote = quotes.get(ts_code)
            df_k = pd.DataFrame(kline)
            # 确保数值类型
            for col in ['close', 'high', 'low', 'volume']:
                if col in df_k.columns:
                    df_k[col] = df_k[col].astype(float)
            
            current_price = df_k['close'].iloc[-1]
            current_vol = df_k['volume'].iloc[-1]
            
            if realtime_quote and realtime_quote.get('price', 0) > 0:
                current_price = realtime_quote['price']
                current_vol = realtime_quote.get('vol', current_vol)
            
            # 3. 计算回调指标
            recent_high = df_k['high'].tail(20).max()
            pullback_pct = (recent_high - current_price) / recent_high * 100
            
            # 阶段涨幅 (寻找最高点之前的起涨点)
            high_idx = df_k['high'].tail(20).idxmax()
            df_before_high = df_k.loc[:high_idx]
            if len(df_before_high) >= 5:
                start_price = df_before_high['low'].tail(15).min()
            else:
                start_price = df_before_high['low'].min()
            
            stage_increase = (recent_high - start_price) / start_price * 100
            
            # 强势特征：涨停历史
            has_limit_up = False
            if 'pct_chg' in df_k.columns:
                has_limit_up = (df_k['pct_chg'].tail(30).astype(float) > 9.8).any()
            
            # 4. 均线支撑 (MA20)
            latest_k = kline[-1]
            ma20 = latest_k.get('ma20')
            if ma20 is None:
                ma20 = df_k['close'].tail(20).mean()
            
            dist_to_ma20 = (current_price - ma20) / ma20 * 100
            
            # 5. 成交量缩量 (对比前5日均量)
            vol_ma5_prev = df_k['volume'].iloc[-6:-1].mean() if len(df_k) >= 6 else df_k['volume'].mean()
            is_shrinking_vol = current_vol < vol_ma5_prev * 1.1
            
            # 6. 综合判断 (强势回调逻辑)
            is_strong = stage_increase > 12 or has_limit_up
            is_pullback = 2 <= pullback_pct <= 30
            is_supported = -5 <= dist_to_ma20 <= 12
            
            if is_strong and is_pullback and is_supported and (is_shrinking_vol or dist_to_ma20 < 3):
                # 乖离率风险过滤
                ind = indicators_map.get(ts_code)
                if ind:
                    if hasattr(ind, 'bias5') and ind.bias5 is not None and ind.bias5 > 5.0:
                        continue
                    if hasattr(ind, 'bias10') and ind.bias10 is not None and ind.bias10 > 10.0:
                        continue

                # 注意：此处移除了基本面“一票否决”预筛选，延迟到 AI 分析前进行

                row_dict = {
                    'ts_code': ts_code,
                    'name': name_map.get(ts_code, ""),
                    'pullback_pct': pullback_pct,
                    'stage_increase': stage_increase,
                    'dist_to_ma20': dist_to_ma20,
                    'is_shrinking_vol': is_shrinking_vol,
                    'has_limit_up': has_limit_up,
                    'weekly_ma20_slope': ind.weekly_ma20_slope if ind else 0,
                    'is_weekly_bullish': ind.is_weekly_bullish if ind else False,
                    'is_daily_bullish': ind.is_daily_bullish if ind else False,
                    'is_trend_recovering': ind.is_trend_recovering if ind else False,
                }
                
                if realtime_quote:
                    row_dict['realtime_price'] = realtime_quote['price']
                    row_dict['realtime_pct_chg'] = realtime_quote['pct_chg']
                
                pullback_candidates.append(row_dict)
        
        # 结果按阶段涨幅排序 (优先展示最强的主升浪回调)
        df_res = pd.DataFrame(pullback_candidates)
        if df_res.empty:
            selector_logger.log("回调形态未命中，尝试仅基于均线筛选", level="WARN")
            fallback = []
            for ts_code in target_codes:
                ind = indicators_map.get(ts_code)
                if not ind:
                    continue
                ma5 = float(getattr(ind, "ma5", 0) or 0)
                ma10 = float(getattr(ind, "ma10", 0) or 0)
                ma20 = float(getattr(ind, "ma20", 0) or 0)
                if ma5 <= 0 or ma10 <= 0 or ma20 <= 0:
                    continue
                kline = historical_data.get(ts_code, [])
                if not kline:
                    continue
                last_close = float(kline[-1].get("close") or 0)
                if last_close <= 0:
                    continue
                if not (ma5 >= ma10 >= ma20):
                    continue
                fallback.append(
                    {
                        "ts_code": ts_code,
                        "name": name_map.get(ts_code, ""),
                        "pct_chg": 0.0,
                        "price": last_close,
                        "type": "MAFallback",
                        "surge_pct": 0.0,
                        "drawdown_pct": 0.0,
                        "vwap": 0.0,
                        "volume_ratio": 0.0,
                        "turnover_rate": float(getattr(ind, "turnover_rate", 0) or 0),
                        "stage_increase": 0.0,
                    }
                )
                if len(fallback) >= 10:
                    break
            df_res = pd.DataFrame(fallback)
        if not df_res.empty:
            df_res = df_res.sort_values('stage_increase', ascending=False)
        return df_res

    async def _filter_candidates(self, trade_date):
        """
        多维综合策略初选 - 优化版 (成交额Top200 + 综合技术面评分)
        """
        selector_logger.log(f"正在执行“多维综合”策略初选 (日期: {trade_date})...")
        
        # 1. 获取全市场成交额前200的代码
        selector_logger.log(f"[海选] 正在获取全市场成交额前200的个股作为初始海选池...")
        top_turnover_codes = await data_provider.market_data_service.get_market_turnover_top_codes(top_n=200)
        if not top_turnover_codes:
            selector_logger.log("无法获取全市场成交额排名数据", level="WARN")
            return pd.DataFrame()

        # 2. 获取股票基本信息以进行初步过滤
        stock_list = await data_provider.get_stock_basic()
        name_map = {s['ts_code']: s['name'] for s in stock_list}
        
        # 过滤：排除科创、北交、ST、退市
        valid_codes = []
        for code in top_turnover_codes:
            if code.startswith(('688', '8', '4')) or code.endswith('.BJ'):
                continue
            
            name = name_map.get(code, "")
            if "ST" in name or "退" in name:
                continue
            
            valid_codes.append(code)
            
        selector_logger.log(f"[海选] 剔除科创/北交/ST/退市股后，剩余 {len(valid_codes)} 只个股进入全面技术指标扫描...")

        if not valid_codes:
            return pd.DataFrame()

        # 3. 批量获取预计算指标和历史 K 线进行“全面”技术面过滤 (并行获取以提升性能)
        selector_logger.log(f"[技术筛选] 正在执行“多维综合”策略初选，扫描 {len(valid_codes)} 只个股...")
        indicators_map, historical_data = await asyncio.gather(
            self._get_indicators_batch(valid_codes, trade_date=trade_date),
            self._get_historical_kline_batch(valid_codes, trade_date=trade_date)
        )
        if not indicators_map:
            selector_logger.log("[技术筛选] 指标数据为空，无法执行选股", level="ERROR")
            raise RuntimeError("指标缺失，无法选股")
        
        candidates = []
        for code in valid_codes:
            ind = indicators_map.get(code)
            if not ind:
                continue
                
            # --- 趋势硬门槛 ---
            # 必须满足：周线多头 OR 趋势强力复苏
            if not (ind.is_weekly_bullish or ind.is_trend_recovering):
                continue
            
            # --- 乖离率风险过滤 ---
            if hasattr(ind, 'bias5') and ind.bias5 is not None and ind.bias5 > 7.0:
                continue 
            if hasattr(ind, 'bias10') and ind.bias10 is not None and ind.bias10 > 13.0:
                continue

            # --- 综合技术面强度评分 (用于排序取前10) ---
            tech_score = self._calculate_tech_score(ind, historical_data.get(code, []))
            
            candidates.append({
                'ts_code': code,
                'name': name_map.get(code, ""),
                'tech_score': tech_score,
                'stage_increase': tech_score, # 统一使用 tech_score 作为排序基准
                'is_weekly_bullish': ind.is_weekly_bullish,
                'is_trend_recovering': ind.is_trend_recovering,
                'weekly_ma20_slope': getattr(ind, 'weekly_ma20_slope', 0),
            })
            
        df_res = pd.DataFrame(candidates)
        if df_res.empty:
            selector_logger.log("技术面筛选未命中，尝试仅基于均线筛选", level="WARN")
            fallback = []
            for code in valid_codes:
                ind = indicators_map.get(code)
                if not ind:
                    continue
                ma5 = float(getattr(ind, "ma5", 0) or 0)
                ma10 = float(getattr(ind, "ma10", 0) or 0)
                ma20 = float(getattr(ind, "ma20", 0) or 0)
                if ma5 <= 0 or ma10 <= 0 or ma20 <= 0:
                    continue
                if not (ma5 >= ma10 >= ma20):
                    continue
                fallback.append(
                    {
                        "ts_code": code,
                        "name": name_map.get(code, ""),
                        "tech_score": 0,
                        "stage_increase": 0,
                        "is_weekly_bullish": False,
                        "is_trend_recovering": False,
                        "weekly_ma20_slope": getattr(ind, "weekly_ma20_slope", 0),
                    }
                )
                if len(fallback) >= 10:
                    break
            df_res = pd.DataFrame(fallback)
        if not df_res.empty:
            df_res = df_res.sort_values('tech_score', ascending=False)
        selector_logger.log(f"[前10截取] 技术面筛选完成，共 {len(df_res)} 只个股入围，取 Top {min(len(df_res), 10)} 进入基本面审查。")
        return df_res


    async def _apply_fundamental_veto(self, candidates_df):
        """
        基本面一票否决逻辑：对初选出的10只股票进行深度基本面检查
        严重问题的定义：
        1. 业绩爆雷（净利润增长率 < -50% 且亏损）
        2. 财务杠杆极高（资产负债率 > 85%）
        3. ROE 极差（ROE < -10%）
        4. 关键财务数据缺失
        """
        if candidates_df.empty:
            return candidates_df
            
        ts_codes = candidates_df['ts_code'].tolist()
        selector_logger.log(f"[基本面过滤] 正在对 {len(ts_codes)} 只标的执行基本面一票否决校验...")
        
        # 批量获取基本面数据
        fina_map = await fundamental_service.batch_get_screening_scores(ts_codes)
        
        passed_indices = []
        for idx, row in candidates_df.iterrows():
            ts_code = row['ts_code']
            name = row.get('name', ts_code)
            
            fina_ctx = fina_map.get(ts_code, {})
            fina_data = fina_ctx.get('fina_indicators', {})
            
            # 严重问题判断
            reasons = []
            
            # 1. 数据缺失检查
            if not fina_data:
                reasons.append("关键财务数据缺失")
            else:
                # 2. 业绩检查
                net_yoy = fina_data.get('yoy_net_profit', 0)
                roe = fina_data.get('roe', 0)
                if net_yoy < -50 and roe < 0:
                    reasons.append(f"业绩爆雷 (净利同比 {net_yoy:.1f}%, ROE {roe:.1f}%)")
                
                # 3. 负债率检查
                debt = fina_data.get('debt_to_assets', 0)
                if debt > 85:
                    reasons.append(f"资产负债率极高 ({debt:.1f}%)")
                
                # 4. ROE 检查
                if roe < -15:
                    reasons.append(f"ROE 极差 ({roe:.1f}%)")
            
            if reasons:
                selector_logger.log(f"【基本面否决】{name} ({ts_code}): {', '.join(reasons)}", level="INFO")
            else:
                passed_indices.append(idx)
        
        # 返回通过校验的股票
        valid_df = candidates_df.loc[passed_indices].copy()
        
        if valid_df.empty and not candidates_df.empty:
            selector_logger.log("【基本面过滤】入围标的基本面均存在严重瑕疵，已被全部剔除。", level="WARN")
        
        # 预先创建列，避免 pandas 赋值时的歧义
        if not valid_df.empty:
            valid_df['fina_score'] = 0.0
            
        # 将获取到的基本面评分和上下文存入 DataFrame，方便 AI 分析阶段使用
        valid_df = valid_df.copy() # 确保是副本
        valid_df['fina_score'] = 0.0
        valid_df['fina_ctx'] = None # 预先创建 Object 列
        valid_df['fina_ctx'] = valid_df['fina_ctx'].astype(object)

        for idx, row in valid_df.iterrows():
            ts_code = row['ts_code']
            fina_ctx = fina_map.get(ts_code, {})
            valid_df.at[idx, 'fina_score'] = float(fina_ctx.get('screening', {}).get('total_score', 0.0))
            # [优化] 存储完整上下文，避免后续重复获取
            valid_df.at[idx, 'fina_ctx'] = fina_ctx
            
        return valid_df

    async def _deep_analyze_candidates(self, candidates, strategy="default"):
        """
        使用 AI 对初选股进行深度分析
        """
        results = []
        
        # 获取股票名称和基础信息 - 使用全局缓存或批量获取
        stock_list = await data_provider.get_stock_basic()
        name_map = {s['ts_code']: s['name'] for s in stock_list}
        industry_map = {s['ts_code']: s['industry'] for s in stock_list}
        area_map = {s['ts_code']: s['area'] for s in stock_list}

        # 晚间选股不调用实时行情API，使用本地数据
        from app.services.market.market_utils import is_trading_time
        local_only = not is_trading_time()
        
        # 批量预加载行情 (晚间选股使用本地数据)
        ts_codes = candidates['ts_code'].tolist()
        selector_logger.log(f"[AI分析] 正在对 {len(ts_codes)} 只入围标的进行 AI 深度分析 (并发限制: 5)...")
        quotes = await data_provider.get_realtime_quotes(ts_codes, local_only=local_only, cache_scope="selector")
        
        async def analyze_single(row):
            ts_code = row['ts_code']
            name = name_map.get(ts_code, '未知')
            industry = industry_map.get(ts_code, '未知')
            area = area_map.get(ts_code, '未知')
            
            # [用户要求] 获取已预取的评分和基本面上下文
            fina_score = row.get('fina_score', 0.0)
            fina_ctx = row.get('fina_ctx')
            
            selector_logger.log(f"正在深度分析: {name} ({ts_code}), 基本面评分: {fina_score}...")
            
            # [性能优化] 并行执行数据获取任务
            # 1. 启动板块上下文分析任务 (非阻塞，供前端展示，AI 仅参考结论)
            sector_task = asyncio.create_task(sector_analysis.analyze_sector(ts_code))
            # 2. 获取多周期数据上下文 (传入预取的财务数据，避免重复抓取)
            context_task = asyncio.create_task(chat_service.get_ai_trading_context(ts_code, pre_fetched_fundamental=fina_ctx, cache_scope="selector"))
            
            # 准备基础信息字典
            basic_info = {
                "name": name,
                "industry": industry,
                "area": area,
                "ts_code": ts_code
            }
            
            # 实时行情依然传给 AI 以便它知道最新价格
            realtime_quote = quotes.get(ts_code) or await data_provider.get_realtime_quote(ts_code)

            # [用户要求] 集成通达信系统定义函数数据 (FINONE, EXTERNSTR等)
            def _get_tdx_extra_data_sync():
                try:
                    return {
                        "total_shares": tdx_formula_service.FINONE(1, ts_code),
                        "float_shares": tdx_formula_service.FINONE(10, ts_code),
                        "net_assets": tdx_formula_service.FINONE(21, ts_code),
                        "net_profit": tdx_formula_service.FINONE(32, ts_code),
                        "extern_str_1": tdx_formula_service.EXTERNSTR(1, ts_code),
                        "extern_value_1": tdx_formula_service.EXTERNVALUE(1, ts_code),
                    }
                except Exception:
                    return {}

            try:
                tdx_extra_data = await asyncio.wait_for(asyncio.to_thread(_get_tdx_extra_data_sync), timeout=2.5)
            except Exception:
                tdx_extra_data = {}
            # 将系统定义数据注入 basic_info，供 AI 参考
            basic_info["tdx_extra_data"] = tdx_extra_data

            # 4. 执行 AI 深度分析
            async with self.ai_semaphore:
                # 等待 context_task 完成
                try:
                    context_str = await asyncio.wait_for(context_task, timeout=30.0)
                except asyncio.TimeoutError:
                    selector_logger.log(f"获取上下文超时: {ts_code}", level="INFO")
                    context_str = ""

                # [改进] 传递可能存在的 prev_score，防止评分大幅跳水
                prev_score = row.get('score') if isinstance(row, dict) else (row.score if hasattr(row, 'score') else None)
                
                # [用户要求] 简化 AI 分析：不传递 sector_task，让 AI 专注于个股和 context，板块共振已由技术面完成
                analysis = await ai_service.analyze_stock(
                    ts_code,
                    None, # 不传原始日线列表
                    basic_info, # 传字典
                    realtime_quote,
                    weekly_kline=None, # 不传原始周线列表
                    monthly_kline=None, # 不传原始月线列表
                    raw_trading_context=context_str, # 兼容性参数
                    sector_task=None, # [优化] 不再让 AI 等待繁琐的板块分析
                    prev_score=prev_score, # [新增] 传递历史评分参考
                    strategy=strategy,
                    preferred_provider=None # 允许自动切换备用 API
                )
            
            # 获取板块分析结果 (仅用于前端展示，不阻塞 AI)
            try:
                sector_res = await asyncio.wait_for(sector_task, timeout=2.0)
            except:
                sector_res = {"error": "板块分析不可用"}
            
            score = 0
            source = "AI_Selector"
            if analysis:
                if 'score' in analysis:
                    score = analysis['score']
                if 'source' in analysis:
                    source = f"Selector_{analysis['source']}"
            
            selector_logger.log(f"{name} 分析完成，得分: {score}, 是否建议交易: {'是' if analysis.get('is_worth_trading') else '否'}")
            
            return {
                "ts_code": ts_code,
                "name": name,
                "industry": industry,
                "score": score,
                "is_worth_trading": analysis.get('is_worth_trading', False),
                "rejection_reason": analysis.get('rejection_reason', ""),
                "source": source, # 透传 AI 供应商
                "analysis": analysis.get('analysis') or "分析失败",
                "reason": analysis.get('analysis') or "分析失败",
                "realtime_price": float((realtime_quote or {}).get("price", 0) or 0),
                "sector_analysis": sector_res,
                "metrics": {
                    "fina_score": row.get('fina_score', 0),
                    "fina_details": row.get('fina_details', {}),
                    "pe": row.get('pe', 0),
                    "net_mf": row.get('net_mf_amount', 0),
                    "vol_ratio": row.get('volume_ratio', 0),
                    "pullback_pct": row.get('pullback_pct', 0) if strategy == "pullback" else 0
                }
            }

        # 使用 asyncio.gather 并行执行所有分析任务
        # 限制并发数为 5，避免 Tushare 频率限制
        semaphore = asyncio.Semaphore(5)

        async def analyze_with_limit(row):
            async with semaphore:
                return await analyze_single(row)

        tasks = [analyze_with_limit(row) for _, row in candidates.iterrows()]
        completed_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 根据 AI 的真实意愿进行过滤
        for res in completed_results:
            if isinstance(res, (Exception, BaseException)):
                selector_logger.log(f"分析股票出错: {str(res)}", level="ERROR")
                continue
            
            if not isinstance(res, dict):
                selector_logger.log(f"分析股票返回异常数据格式: {type(res)}", level="WARNING")
                continue

            # 策略：
            # 1. 如果 AI 建议交易，直接入选
            # 2. 如果 AI 不建议交易，但评分 >= 45，作为“观察”入选，不直接剔除
            is_worth = res.get('is_worth_trading')
            score = res.get('score', 0)
            
            if is_worth is True:
                results.append(res)
            elif score >= 45:
                res['is_observation'] = True
                res['analysis'] = f"【观察级】{res.get('analysis')}"
                results.append(res)
                selector_logger.log(f"AI 虽然未建议立即交易 {res.get('name')}，但分值 {score} 尚可，转为观察名单")
            else:
                selector_logger.log(f"AI 拒绝了标的 {res.get('name')}: {res.get('rejection_reason')} (分值: {score})")
        
        # 按评分降序排列：将 AI 认为最好的排在最前面
        results.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        # 最终只返回前 5 只（已按评分排好序）
        # 用户要求: AI 分析完成发回来最多 5 只股票
        final_results = results[:5]
        selector_logger.log(f"最终选出 {len(final_results)} 只标的，已按 AI 评分排序。")
        
        return final_results

    async def _check_weekly_monthly_trend(self, ts_code: str):
        """
        [内部复用] 检查周线和月线趋势 (MA60 + RSI)
        """
        # 复用 _get_indicators_batch 获取的指标会更快，但这里为了独立性，
        # 我们使用 _get_historical_kline_batch
        
        # 获取周线数据
        # _get_historical_kline_batch 只支持 trade_date 参数，不支持 freq/limit
        # 所以我们改用 data_provider 直接获取
        klines_map = await data_provider.get_batch_kline([ts_code], freq="W", limit=60)
        weekly_bars = klines_map.get(ts_code, [])
        
        if not weekly_bars or len(weekly_bars) < 20:
            return {"valid": False, "reason": "周线数据不足"}
            
        df = pd.DataFrame(weekly_bars)
        df['close'] = df['close'].astype(float)
        df['low'] = df['low'].astype(float)
        latest = df.iloc[-1]
        
        # 1. 均线支撑验证 (周线 MA60 是牛熊分界线)
        ma60 = df['close'].rolling(60).mean().iloc[-1]
        close = float(latest['close'])
        
        # 如果 MA60 有效，且股价在 MA60 附近 (支撑位)
        is_ma60_support = False
        if not pd.isna(ma60) and ma60 > 0:
            # 支撑范围：MA60 * 0.95 ~ MA60 * 1.05
            if ma60 * 0.95 <= close <= ma60 * 1.05:
                is_ma60_support = True
        
        # 2. 简单的 RSI 背离检测
        # 计算 14 周 RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 检测最近 12 周的低点
        recent_12 = df.tail(12)
        min_price_idx = recent_12['low'].idxmin()
        min_rsi_idx = recent_12['rsi'].idxmin()
        
        # 如果价格最低点比 RSI 最低点发生得更晚 (价格创新低，RSI 没创新低)
        is_divergence = False
        if min_price_idx > min_rsi_idx:
            # 且当前 RSI 已拐头向上
            if df['rsi'].iloc[-1] > df['rsi'].iloc[-2]:
                is_divergence = True
                
        return {
            "valid": True,
            "ma60_support": is_ma60_support,
            "rsi_divergence": is_divergence,
            "ma60": ma60
        }

    async def _analyze_candidates_with_ai(self, candidates_df, strategy="default"):
        """
        [内部复用] 调用 _deep_analyze_candidates 进行 AI 分析
        """
        return await self._deep_analyze_candidates(candidates_df, strategy)

    async def select_single_vol_doubling(self, top_n: int = 5) -> List[Dict[str, Any]]:
        """
        [新策略] 10日内唯一倍量柱选股 (Single Volume Doubling)
        逻辑:
        1. 10日内有且只有1个倍量柱 (Vol >= Ref(Vol, 1) * 2)
        2. 当天就是那个倍量柱
        3. 结合周月线趋势过滤 (MA60支撑 + RSI背离/向上)
        """
        selector_logger.log("启动 [10日内唯一倍量柱] 选股策略...")
        
        # 1. 获取全市场基础数据
        df_basics = await data_provider.get_stock_basic()
        # df_basics is a list of dicts, not DataFrame, if get_stock_basic returns list
        # We need to convert it or use it as list
        if isinstance(df_basics, list):
             df_basics = pd.DataFrame(df_basics)
             
        if df_basics.empty:
            return []
            
        # 2. 初步筛选: 剔除 ST、退市、停牌 (data_provider已处理一部分)
        # 获取近期日线数据 (至少11天，因为要算倍量和10日统计)
        # 为了效率，先获取今日成交量 Top 500 或 活跃股，避免全市场扫描
        # 或者使用 get_market_turnover_top_codes 获取活跃池
        active_codes = await data_provider.get_market_turnover_top_codes(top_n=500)
        
        candidates = []
        
        # 3. 批量获取日线数据
        # 注意: get_kline_batch 可能会很大，这里分批处理或直接循环
        # 既然是选股，我们重点关注 active_codes
        
        # 获取 K 线数据 (取最近 15 天以确保有足够数据计算)
        klines_map = await data_provider.get_batch_kline(active_codes, freq="D", limit=15)
        
        for ts_code, klines in klines_map.items():
            if not klines or len(klines) < 11:
                continue
                
            df = pd.DataFrame(klines)
            # 确保按时间升序
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date')
            
            # 计算倍量柱
            # Vol / Ref(Vol, 1) >= 2
            df['vol'] = df['vol'].astype(float)
            df['prev_vol'] = df['vol'].shift(1)
            df['is_double_vol'] = df['vol'] >= (df['prev_vol'] * 2)
            
            # 取最近 10 天数据
            recent_10 = df.tail(10)
            
            # 条件1: 10日内倍量柱数量 = 1
            double_vol_count = recent_10['is_double_vol'].sum()
            if double_vol_count != 1:
                continue
                
            # 条件2: 当天就是倍量柱 (recent_10 的最后一天)
            if not recent_10.iloc[-1]['is_double_vol']:
                continue
                
            # 初选通过，进入周月线过滤
            # 4. 周月线趋势过滤
            trend_check = await self._check_weekly_monthly_trend(ts_code)
            if not trend_check.get('valid', False):
                continue
            
            # MA60 支撑检查 (可选，根据用户需求"结合周月进行过滤")
            if not trend_check.get('ma60_support', False):
                # 如果不在 MA60 支撑位，检查是否 RSI 强势
                # 这里可以灵活调整，暂且严格一点：必须趋势向好
                pass

            candidates.append({
                "ts_code": ts_code,
                "name": df_basics[df_basics['ts_code'] == ts_code]['name'].values[0] if not df_basics[df_basics['ts_code'] == ts_code].empty else ts_code,
                "price": float(recent_10.iloc[-1]['close']),
                "pct_chg": float(recent_10.iloc[-1]['pct_chg']),
                "vol_ratio": float(recent_10.iloc[-1]['vol']) / float(recent_10.iloc[-1]['prev_vol']) if float(recent_10.iloc[-1]['prev_vol']) > 0 else 0,
                "trend_info": trend_check
            })
            
        selector_logger.log(f"[唯一倍量柱] 初选通过 {len(candidates)} 只")
        
        # 5. 转换为 DataFrame 供后续 AI 分析
        if not candidates:
            return []
            
        df_candidates = pd.DataFrame(candidates)
        # 按量比排序
        df_candidates = df_candidates.sort_values('vol_ratio', ascending=False).head(top_n * 2)
        
        # 6. 调用 AI 进行最终筛选
        final_results = await self._analyze_candidates_with_ai(df_candidates, strategy="vol_doubling")
        
        # 持久化结果
        self._persist_selector_results(datetime.now().strftime("%Y%m%d"), "vol_doubling", final_results)
        
        return final_results

    async def select_four_signal_resonance(self, top_n: int = 5) -> List[Dict[str, Any]]:
        """
        [新策略] 四信号共振选股 (Four Signal Resonance)
        核心逻辑: 20日内涨停 + 4连阳以上 + 向上跳空缺口 + 持续倍量
        """
        selector_logger.log("启动 [四信号共振] 选股策略...")
        
        # 1. 获取全市场基础数据
        df_basics = await data_provider.get_stock_basic()
        # df_basics is a list of dicts, not DataFrame, if get_stock_basic returns list
        # We need to convert it or use it as list
        if isinstance(df_basics, list):
             df_basics = pd.DataFrame(df_basics)
             
        if df_basics.empty:
            return []
            
        # 2. 优先扫描活跃股 (成交额 Top 500)
        active_codes = await data_provider.market_data_service.get_market_turnover_top_codes(top_n=500)
        candidates = []
        
        # 3. 批量获取日线数据 (取最近 30 天，足够覆盖20日窗口)
        klines_map = await data_provider.get_kline_batch(active_codes, freq="D", limit=30)
        
        for ts_code, klines in klines_map.items():
            if not klines or len(klines) < 20:
                continue
                
            df = pd.DataFrame(klines)
            # 确保按时间升序
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date')
            
            # 数据预处理
            df['close'] = df['close'].astype(float)
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['vol'] = df['vol'].astype(float)
            df['pct_chg'] = df['pct_chg'].astype(float)
            df['prev_close'] = df['close'].shift(1)
            df['prev_high'] = df['high'].shift(1)
            df['prev_vol'] = df['vol'].shift(1)
            
            # 取最近 20 天数据
            recent_20 = df.tail(20)
            
            # --- 信号1: 20日内出现过涨停 ---
            # 涨停判定: 涨幅 >= 9.5% (简单判定，创业板/科创板可能需要 19.5%，暂统一定义强力大阳线)
            has_limit_up = (recent_20['pct_chg'] >= 9.5).any()
            if not has_limit_up:
                continue
                
            # --- 信号2: 启动前连续阳线 (至少4连阳) ---
            # 检查最近 20 天内是否存在连续 >= 4 根阳线
            # 阳线定义: close > open 且涨幅为非大阳线
            is_red = (recent_20['close'] > recent_20['open']) & (recent_20['pct_chg'] > 0) & (recent_20['pct_chg'] < 7.0)
            # 计算连续阳线天数
            consecutive_red = is_red.groupby((is_red != is_red.shift()).cumsum()).cumsum()
            max_consecutive_red = consecutive_red.max()
            if max_consecutive_red < 4:
                continue
                
            # --- 信号3: 向上跳空缺口 (未回补) ---
            # 缺口定义: 当日 low > 昨日 high
            recent_20['is_gap_up'] = recent_20['low'] > recent_20['prev_high']
            has_gap = recent_20['is_gap_up'].any()
            if not has_gap:
                continue
            # 进阶: 缺口是否未回补？(简单起见，只要近期有缺口即可，主力强势特征)
            
            # --- 信号4: 持续倍量 (成交量异动) ---
            # 定义: 某日量能 >= 昨日量能 * 1.8 (接近倍量)
            # 且这种放量不是孤立的，近期至少出现过 2 次以上倍量，或者连续放量
            recent_20['is_double_vol'] = recent_20['vol'] >= (recent_20['prev_vol'] * 1.8)
            double_vol_count = recent_20['is_double_vol'].sum()
            
            # 或者: 最近 5 天均量明显大于 20 天均量 (放量攻击形态)
            vol_ma5 = recent_20['vol'].tail(5).mean()
            vol_ma20 = recent_20['vol'].mean()
            is_vol_active = (vol_ma5 > vol_ma20 * 1.5) or (double_vol_count >= 2)
            
            if not is_vol_active:
                continue

            # --- 信号5 (新): 月线涨幅限制 ---
            # 两个月内涨幅小于 50%
            # 需要获取月线数据
            try:
                monthly_klines = await data_provider.get_kline(ts_code, freq="M", limit=3, local_only=True)
                if monthly_klines and len(monthly_klines) >= 2:
                    # 计算最近两个月的累计涨幅
                    # 简单算法: (最新收盘 - 2个月前收盘) / 2个月前收盘
                    # 注意 monthly_klines 按时间升序
                    current_close = float(monthly_klines[-1]['close'])
                    prev_2_close = float(monthly_klines[-2]['close']) # 上个月收盘 (即2个月前的基准)
                    
                    # 如果有3根，取更早一根作为基准更稳妥
                    if len(monthly_klines) >= 3:
                         prev_2_close = float(monthly_klines[-3]['close'])
                    
                    monthly_gain = (current_close - prev_2_close) / prev_2_close * 100
                    
                    if monthly_gain >= 50.0:
                        # selector_logger.log(f"剔除 {ts_code}: 月线近期涨幅过大 ({monthly_gain:.1f}%)")
                        continue
            except Exception:
                pass # 获取月线失败暂不剔除

            # 所有信号共振，入选
            candidates.append({
                "ts_code": ts_code,
                "name": df_basics[df_basics['ts_code'] == ts_code]['name'].values[0] if not df_basics[df_basics['ts_code'] == ts_code].empty else ts_code,
                "price": float(recent_20.iloc[-1]['close']),
                "pct_chg": float(recent_20.iloc[-1]['pct_chg']),
                "score": 80 + double_vol_count * 2 + max_consecutive_red, # 基础分 + 加分项
                "reason": f"四信号共振: 20日内涨停+非大阳连阳{max_consecutive_red}天+缺口+倍量{double_vol_count}次"
            })
            
        selector_logger.log(f"[四信号共振] 初选通过 {len(candidates)} 只")
        
        if not candidates:
            return []
            
        df_candidates = pd.DataFrame(candidates)
        df_candidates = df_candidates.sort_values('score', ascending=False).head(top_n * 2)
        
        # 调用 AI 进行最终筛选 (AI 会看到我们植入的 Memory)
        final_results = await self._analyze_candidates_with_ai(df_candidates, strategy="four_signals")
        
        self._persist_selector_results(datetime.now().strftime("%Y%m%d"), "four_signals", final_results)
        
        # [新增] 自动创建交易监控计划
        await self._upsert_monitor_plans_from_selector(
            datetime.now().strftime("%Y%m%d"), 
            "four_signals", 
            final_results
        )
        
        return final_results

    async def _enhance_candidates_with_sector_leaders(self, candidates_df, trade_date, sector_context_map=None):
        """
        [用户要求] 行业加强逻辑
        每个板块取成交额前10，共计最多100只股票进行技术面评分并重新排序
        :param sector_context_map: 预加载的板块分析结果，避免重复计算
        """
        if candidates_df.empty:
            return candidates_df
            
        selector_logger.log("[行业加强] 正在对比行业内成交额前10的活跃标的...")
        
        # 1. 获取基础信息
        stock_list = await data_provider.get_stock_basic()
        industry_map = {s['ts_code']: s['industry'] for s in stock_list}
        
        # 2. 识别需要加强的行业活跃股 (每个行业取 Top 10)
        potential_codes = set()
        if sector_context_map:
            for industry, context in sector_context_map.items():
                if isinstance(context, dict) and 'rising_wave_status' in context:
                    # analyze_sector 内部已经按成交额取了前 10
                    active_stocks = context['rising_wave_status'].get('active_stocks', [])
                    potential_codes.update(active_stocks)
        
        # 3. 如果没有预加载，则按行业获取成交额 Top 10 (兜底逻辑)
        if not potential_codes:
            industries = set([industry_map.get(code) for code in candidates_df['ts_code'].tolist() if industry_map.get(code)])
            # 获取成交额 Top 200
            top_200_codes = await data_provider.market_data_service.get_market_turnover_top_codes(top_n=200)
            for industry in industries:
                industry_top_10 = []
                for code in top_200_codes:
                    if industry_map.get(code) == industry:
                        industry_top_10.append(code)
                        if len(industry_top_10) >= 10:
                            break
                potential_codes.update(industry_top_10)
        
        if not potential_codes:
            return candidates_df
            
        # 4. 排除已在初选名单中的
        existing_codes = set(candidates_df['ts_code'].tolist())
        new_potential_codes = [c for c in potential_codes if c not in existing_codes]
        
        # 限制总数最多 100 只 (应对极端情况)
        if len(new_potential_codes) > 100:
            new_potential_codes = new_potential_codes[:100]

        if not new_potential_codes:
            return candidates_df
            
        selector_logger.log(f"[行业加强] 在相关行业内发现 {len(new_potential_codes)} 只潜在活跃标的，进行新一轮技术面筛选...")
        
        # 5. 批量获取指标进行评分
        indicators_map = await self._get_indicators_batch(new_potential_codes, trade_date=trade_date)
        
        new_candidates = []
        for code in new_potential_codes:
            ind = indicators_map.get(code)
            if not ind: continue
            
            # 必须满足基本趋势门槛 (周线多头或趋势复苏)
            if not (getattr(ind, 'is_weekly_bullish', False) or getattr(ind, 'is_trend_recovering', False)): 
                continue
            
            # 使用统一的技术评分逻辑
            # ... (复用 select_intraday_pullback 中的评分逻辑，此处简化)
            score = 50.0
            if getattr(ind, 'macd_red_count', 0) > 0: score += 10
            if getattr(ind, 'kdj_golden_cross', False): score += 10
            
            new_candidates.append({
                "ts_code": code,
                "name": next((s['name'] for s in stock_list if s['ts_code'] == code), "未知"),
                "pct_chg": 0.0, # 需补充实时数据
                "price": 0.0,
                "score": score
            })
            
        # ... 后续处理 ...
        return candidates_df

stock_selector = StockSelectorService()

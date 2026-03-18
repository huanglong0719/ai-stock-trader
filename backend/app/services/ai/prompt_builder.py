import pandas as pd
import numpy as np
import re
import math
from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from app.services.logger import logger
from app.services.learning_service import learning_service
from app.services.data_provider import data_provider

class PromptBuilder:
    async def get_trading_status(self) -> str:
        """
        获取当前 A 股交易状态 (精准版，包含节假日判断)
        """
        now = datetime.now()
        date_str = now.strftime('%Y%m%d')
        
        # 1. 检查是否为交易日 (包含节假日判断)
        try:
            trade_cal = await data_provider.check_trade_day(date_str)
            if not trade_cal.get('is_open', False):
                next_day = trade_cal.get('next_trade_date', '未知')
                return f"休市 ({trade_cal.get('reason', '非交易日')}) - 下一交易日: {next_day}"
            
            # 如果是交易日，也要获取下一交易日
            next_day = trade_cal.get('next_trade_date', '未知')
            
        except Exception as e:
            # 降级处理：仅判断周末
            next_day = "未知"
            if now.weekday() >= 5:
                return "休市 (周末)"
            
        current_time = now.strftime("%H:%M")
        status_suffix = f" (下一交易日: {next_day})"
        
        if "09:15" <= current_time < "09:25":
            return "盘前集合竞价" + status_suffix
        elif "09:25" <= current_time < "09:30":
            return "盘前休整" + status_suffix
        elif "09:30" <= current_time <= "11:30":
            return "盘中交易 (上午)" + status_suffix
        elif "11:30" < current_time < "13:00":
            return "午间休市" + status_suffix
        elif "13:00" <= current_time <= "15:00":
            return "盘中交易 (下午)" + status_suffix
        elif "15:00" < current_time <= "15:30":
            return "盘后固定价格交易/清算" + status_suffix
        elif current_time > "15:30":
            return "已收盘" + status_suffix
        else:
            return "盘前等待" + status_suffix

    def _detect_macd_divergence(self, df: pd.DataFrame) -> str:
        """
        检测 MACD 背离特征 (底背离/顶背离)
        """
        if len(df) < 30:
            return ""
        
        recent_df = df.tail(20)
        
        # 1. 底背离检测
        p1 = df.iloc[-20:-10]['low'].min()
        p2 = df.iloc[-10:]['low'].min()
        
        macd_col = None
        if 'macd_diff' in df.columns:
            macd_col = 'macd_diff'
        elif 'macd_dif' in df.columns:
            macd_col = 'macd_dif'
        elif 'macd_hist' in df.columns:
            macd_col = 'macd_hist'
        else:
            return ""

        m1 = df.iloc[-20:-10][macd_col].min()
        m2 = df.iloc[-10:][macd_col].min()
        
        if p2 < p1 and m2 > m1 and m2 < 0:
            return "★发现底背离结构(潜在底部)"
            
        # 2. 顶背离检测
        p1_h = df.iloc[-20:-10]['high'].max()
        p2_h = df.iloc[-10:]['high'].max()
        
        m1_h = df.iloc[-20:-10][macd_col].max()
        m2_h = df.iloc[-10:][macd_col].max()
        
        if p2_h > p1_h and m2_h < m1_h and m2_h > 0:
            return "★发现顶背离结构(警惕回调)"
            
        return ""

    def _detect_strong_stock_patterns(self, df: pd.DataFrame) -> list:
        """
        检测强势股特征 (涨停、连板、反包)
        """
        if len(df) < 5:
            return []
            
        patterns = []
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        closes = df['close'].values
        pct_changes = (closes[1:] - closes[:-1]) / closes[:-1] * 100
        
        latest_pct_chg = pct_changes[-1]
        if latest_pct_chg > 9.5:
            patterns.append("★今日涨停")
            
        is_limit = pct_changes > 9.5
        limit_count = 0
        for i in range(len(is_limit)-1, -1, -1):
            if is_limit[i]:
                limit_count += 1
            else:
                break
        
        if limit_count >= 2:
            patterns.append(f"★{limit_count}连板(强势龙头)")
            
        if prev['close'] < prev['open'] and latest['close'] > prev['high'] and latest['close'] > latest['open']:
            patterns.append("★反包形态(转强信号)")
            
        if "★今日涨停" in patterns and latest['volume'] < prev['volume'] * 0.8:
            patterns.append("★缩量涨停(筹码高度锁定)")
            
        return patterns

    def _detect_level_testing(self, df: pd.DataFrame, level: float, type: str = "support") -> str:
        """
        检测关键价位的反复试探
        """
        if level <= 0 or len(df) < 10:
            return ""
        
        threshold = level * 0.005
        recent_bars = df.tail(10).copy()
        
        if type == "support":
            mask = (abs(recent_bars['low'] - level) <= threshold) | \
                   ((recent_bars['low'] <= level) & (level <= recent_bars['high']))
        else:
            mask = (abs(recent_bars['high'] - level) <= threshold) | \
                   ((recent_bars['low'] <= level) & (level <= recent_bars['high']))
            
        touch_count = mask.sum()
                    
        if touch_count >= 3:
            is_broken = False
            latest_close = df.iloc[-1]['close']
            if type == "support" and latest_close < level * 0.998:
                is_broken = True
            elif type == "resistance" and latest_close > level * 1.002:
                is_broken = True

            if type == "support":
                status = "【已跌破】" if is_broken else "反复试探"
                return f"★关键支撑位({level:.2f}){status}，警惕“久守必失”风险"
            else:
                status = "【已突破】" if is_broken else "反复试探"
                return f"★关键压力位({level:.2f}){status}，关注向上空间"
        
        return ""

    def _format_kline_raw_data(self, df: pd.DataFrame, period_name: str, limit: int = 20) -> str:
        """
        格式化原始K线数据 (CSV Compact Format)
        Date,O,H,L,C,V,TR,M5,M20
        """
        if df is None or df.empty:
            return ""
            
        # 取最近 limit 条
        recent = df.tail(limit).copy()
        
        # 尝试转换日期格式
        date_col = None
        for col in ['trade_date', 'date', 'time']:
            if col in recent.columns:
                date_col = col
                break
        
        # 使用 CSV 格式头
        lines = [f"【{period_name}最近{limit}根K线(CSV)】\nDt,O,H,L,C,V,TR,M5,M20"]
        
        for _, row in recent.iterrows():
            try:
                # 日期压缩: 2023-10-01 -> 231001
                raw_date = str(row.get(date_col, ''))[:10]
                d_str = raw_date.replace('-', '').replace('/', '')[2:]
                
                o = round(float(row.get('open', 0)), 2)
                h = round(float(row.get('high', 0)), 2)
                l = round(float(row.get('low', 0)), 2)
                c = round(float(row.get('close', 0)), 2)
                # 成交量保留整数
                v = int(float(row.get('volume', 0)))
                # 换手率
                tr = round(float(row.get('turnover_rate', 0)), 1)
                
                # 均线
                ma5 = round(float(row.get('ma5', 0) or c), 2)
                ma20 = round(float(row.get('ma20', 0) or c), 2)
                
                # CSV 行
                line = f"{d_str},{o},{h},{l},{c},{v},{tr},{ma5},{ma20}"
                lines.append(line)
            except Exception:
                continue
                
        return "\n".join(lines) + "\n"

    def _get_historical_context(self, df: pd.DataFrame, period_name: str, basic_info: dict = None, quote: dict = None) -> str:
        """
        计算长周期历史背景特征 (精简版)
        """
        if df is None or df.empty:
            return ""
        
        latest = df.iloc[-1]
        context = f"【{period_name}背景】"
        if period_name != "日线":
             return ""

        summary = []
        
        # 1. 价格位置
        hist_high = df['high'].max()
        hist_low = df['low'].min()
        curr_price = latest['close']
        range_pos = (curr_price - hist_low) / (hist_high - hist_low) * 100 if hist_high > hist_low else 50
        summary.append(f"位置: {range_pos:.0f}% (H:{hist_high:.2f}/L:{hist_low:.2f})")
        
        # 2. 均线状态
        ma20 = latest.get('ma20', 0)
        if ma20 > 0:
            trend = "多头" if curr_price > ma20 else "空头"
            summary.append(f"MA20: {trend}")

        # 3. 成交量状态
        vol_multiple = latest['volume'] / df['volume'].mean() if df['volume'].mean() > 0 else 1.0
        if vol_multiple > 2.0: summary.append("放量")
        elif vol_multiple < 0.6: summary.append("缩量")
        
        return context + " " + " | ".join(summary)

    def cleanup_ai_content(self, content: str) -> str:
        """
        清理 AI 输出中的复读、乱码或无意义符号串
        """
        if not content:
            return ""
        
        # 1. 移除常见的 AI 复读模式
        content = re.sub(r'([^\d\w\s])\1{2,}', r'\1', content)
        content = re.sub(r'([\u4e00-\u9fa5])\1{2,}', r'\1', content)
        
        # 2. 移除词组级别的重复
        content = re.sub(r'([\u4e00-\u9fa5]{2,4})\1', r'\1', content)
        
        # 3. 移除异常符号串
        content = re.sub(r'[\*\-,，。\.]{3,}', lambda m: m.group(0)[0], content)
        
        # 4. 移除特定的乱码字符
        content = content.replace('\ufffd', '')
        
        # 4.1 移除特定的声明文字
        disclaimers = [
            '(不含交易账户)', '（不含交易账户）',
            '投资有风险，入市需谨慎', '仅供参考，不构成投资建议',
            '作为AI模型', '作为AI助手', 'AI分析结论',
            'DeepSeek-V3', 'DeepSeek-V3.1', 'NVIDIA', 'terminus',
            '风险提示：', '风险提示', '分析结论：', '免责声明：', '免责声明',
            '总结来说，', '综上所述，', '以上分析仅供参考',
            '【风险提示】', '【免责声明】'
        ]
        for d in disclaimers:
            content = content.replace(d, '')
        
        # 移除 Markdown 头部或尾部常见的 "分析报告" 标题
        content = re.sub(r'^#+ (分析报告|个股点评|复盘报告).*$', '', content, flags=re.MULTILINE)
        
        # 5. 逐行清理
        lines = content.split('\n')
        clean_lines = []
        for line in lines:
            line = line.strip()
            if not line:
                clean_lines.append("")
                continue
                
            if len(line) > 5 and re.match(r'^[\s\W_!@#$%^&*()+\-=\[\]{};\':"\\|,.<>\/?]+$', line):
                continue
            
            line = re.sub(r'(.{3,10})\1', r'\1', line)
            clean_lines.append(line)
        
        content = '\n'.join(clean_lines)
        
        # 6. 截断结尾处明显的幻觉
        last_period = content.rfind('。')
        if last_period != -1 and last_period < len(content) - 1:
            suffix = content[last_period+1:].strip()
            if (len(suffix) > 5 and len(set(suffix)) < 3) or re.search(r'[\*\-]{3,}', suffix):
                content = content[:last_period+1]
                
        return content.strip()

    def get_concise_analysis_rules(self) -> str:
        return """
【核心分析准则 (Core Rules)】
1. **三位一体分析**: 必须同时结合日线趋势(主方向)、周/月线背景(大级别确认)和 30min/5min 走势(介入时机)。
2. **激进顺势**: 只要大级别趋势强势且量价配合良好，允许在上涨趋势中的回调下跌主动买入，追求波段利润。
3. **量价验证**: 回调末端缩量更优；突破放量是确认信号，但若已显著拉高则需谨慎追价。强势骑线阳（开盘在均线下方、盘中突破并站稳均线）允许主动追涨介入。
4. **强势判定**: 日线 MA5 角度参考 18.55°，允许 ±5° 误差；低于该区间视为不够强势。
5. **进攻优先**: 强势结构中优先寻找进攻点，宁可承担小幅回撤，也不轻易错过主升段。
6. **数据依赖**: 所有判断必须基于提供的 K 线数据和指标，严禁臆测。

【当前可获取的独立数据维度】
- 实时行情：当前价、涨跌幅、成交量、量比、换手率、分时均价、盘口买卖五档、涨停/跌停价。
- 日K线：最近30根K线数据 (CSV格式)。
- 周K线：最近20根K线数据 (CSV格式)。
- 月K线：最近15根K线数据 (CSV格式)。
- 30分钟K线：最近16根K线数据 (CSV格式，覆盖约8小时)。
- 5分钟K线：最近48根K线数据 (CSV格式，覆盖完整一天)。
- 基本面数据：五步基本面筛选结果。
- 历史统计：5年区间高低点、当前价相对位置。
"""

    def get_core_analysis_rules(self) -> str:
        """
        [已废弃] 请使用 get_concise_analysis_rules
        """
        return self.get_concise_analysis_rules()

    def _calc_ma5_angle(self, df: pd.DataFrame) -> Optional[float]:
        if df is None or df.empty:
            return None
        if "ma5" in df.columns:
            ma5_series = df["ma5"]
        elif "close" in df.columns:
            ma5_series = df["close"].rolling(window=5).mean()
        else:
            return None
        ma5_series = ma5_series.dropna()
        if len(ma5_series) < 5:
            return None
        ma5_recent = ma5_series.tail(5).tolist()
        ma_start = ma5_recent[0]
        ma_end = ma5_recent[-1]
        days = len(ma5_recent) - 1
        if days <= 0:
            return None
        slope = (ma_end - ma_start) / days
        return math.degrees(math.atan(slope))

    def get_auto_decision_rules(self) -> str:
        return """
【自动决策硬性规则】
1. 本次为系统自动执行分析，必须输出确定性决策，不允许使用“建议/仅供参考/可能/倾向/或许”等措辞。
2. reason 字段必须是决策口吻，严禁出现“建议”字样。
3. 成交规则：买入仅当现价低于挂单价才可成交，卖出仅当现价高于挂单价才可成交，相等时不得成交。
4. 强势趋势中回调下跌必须优先判断为买点；上涨中出现放量加速或情绪高亢时，允许主动卖出锁定利润。
5. 如果标的是看好对象但未到买点，必须保持监控与持续跟踪观察，不得因为当次未买入就从监控列表剔除；若已明确走弱则移出监控。跟踪观察不等于挂单成交。
6. 【重要】若【实时市场大势】中包含重大宏观利空或突发黑天鹅事件，请务必在 reason 中体现避险逻辑，并酌情降低仓位或执行卖出/观望。
7. 对“选股监控-四信号”策略，若出现大幅低开回调且不构成卖出信号，应输出加仓买入决策并给出买入价。
"""

    def build_analysis_prompt(
        self,
        stock_info: Dict,
        kline_data: Dict[str, pd.DataFrame],
        indicators: Dict[str, Any],
        market_sentiment: Dict,
        news: List[Dict],
        fundamental: Dict,
        user_query: str = ""
    ) -> str:
        """
        构建 AI 分析提示词 (Prompt)
        """
        ts_code = stock_info.get('ts_code', 'Unknown')
        name = stock_info.get('name', 'Unknown')
        industry = stock_info.get('industry', 'Unknown')
        
        # 1. 基础信息段
        base_info = f"""
股票: {ts_code} ({name})
行业: {industry}
当前价格: {stock_info.get('price', 0)}
涨跌幅: {stock_info.get('pct_chg', 0)}%
换手率: {stock_info.get('turnover_rate', 0)}%
量比: {stock_info.get('vol_ratio', 0)}
"""
        
        # 2. 多周期 K 线数据段
        # [Fix] 显式检查并格式化周线/月线/分钟线数据，确保 AI 能看到
        kline_section = ""
        
        # 日线 (必须有)
        df_daily = kline_data.get('D')
        if df_daily is not None and not df_daily.empty:
            # 增加最近 5 日的详细数据展示，方便 AI 细看
            recent_daily = df_daily.tail(5).to_csv(index=False)
            kline_section += f"\n【日线数据 (最近 30 天摘要)】\n{df_daily.tail(30).to_string(index=False)}\n"
        else:
            kline_section += "\n【日线数据】缺失\n"
            
        # 周线
        df_weekly = kline_data.get('W')
        if df_weekly is not None and not df_weekly.empty:
            kline_section += f"\n【周线数据 (最近 20 周)】\n{df_weekly.tail(20).to_string(index=False)}\n"
        else:
            kline_section += "\n【周线数据】缺失 (可能数据不足或未同步)\n"
            
        # 月线
        df_monthly = kline_data.get('M')
        if df_monthly is not None and not df_monthly.empty:
            kline_section += f"\n【月线数据 (最近 15 月)】\n{df_monthly.tail(15).to_string(index=False)}\n"
        else:
            kline_section += "\n【月线数据】缺失\n"
            
        # 分钟线 (30min)
        df_30m = kline_data.get('30min')
        if df_30m is not None and not df_30m.empty:
            kline_section += f"\n【30分钟线 (最近 16 根)】\n{df_30m.tail(16).to_string(index=False)}\n"
        else:
            kline_section += "\n【30分钟线】缺失\n"
            
        return f"{base_info}\n{kline_section}"

    async def generate_analysis_prompt(self, symbol: str, df: pd.DataFrame, basic_info: dict = None, search_info: str = "", realtime_quote: dict = None, df_w: pd.DataFrame = None, df_m: pd.DataFrame = None, df_30m: pd.DataFrame = None, df_5m: pd.DataFrame = None, sector_info: dict = None, raw_trading_context: Optional[str] = None, prev_score: Optional[int] = None, strategy: str = "default") -> str:
        """
        生成给 AI 的提示词 (增强版：注入原始数据并强制三位一体分析)
        """
        # [核心修正] 如果有 prev_score，说明该股近期在选股系统中表现优异，需注入给 AI 保持一致性
        prev_score_context = ""
        if prev_score is not None:
            if prev_score >= 80:
                prev_score_context = f"\n**【重要参考】该标的在智能选股系统中得分为 {prev_score} 分，说明其量价策略、基本面、资金面均处于极佳状态。本次分析应优先基于此背景，寻找长线逻辑，而非被短期的日线波动误导。**\n"
            else:
                prev_score_context = f"\n**【重要参考】该标的在选股系统中得分为 {prev_score} 分。**\n"

        # 1. 核心分析准则 (使用极简版以节省 Token)
        core_rules = self.get_concise_analysis_rules()
        
        # 2. 交易状态
        trading_status = await self.get_trading_status()
        
        ma5_angle = self._calc_ma5_angle(df)
        if ma5_angle is None:
            ma5_angle_context = "日线 MA5 角度: 无法计算"
        else:
            ma5_angle_context = f"日线 MA5 角度: {ma5_angle:.2f}° (强势参考 18.55°±5°)"

        # 3. 提取周期背景 (Token 优化策略)
        # 月线/周线: 仅提供 CSV 原始数据，不再提供冗长的文字描述，AI 需自行根据 CSV 判断趋势
        monthly_context = self._format_kline_raw_data(df_m, "月线", limit=15)
        weekly_context = self._format_kline_raw_data(df_w, "周线", limit=20)
        
        # 日线: 提供 CSV + 详细上下文 (保留详细特征以供精确判定，如涨停/连板/支撑压力)
        daily_raw = self._format_kline_raw_data(df, "日线", limit=30)
        daily_context = self._get_historical_context(df, "日线", basic_info, realtime_quote) if df is not None and not df.empty else ""
        
        # 短周期分时背景 (30min/5min)
        df_30m_slice = df_30m.tail(16) if df_30m is not None else None
        min_30_context = self._format_kline_raw_data(df_30m_slice, "30分钟线", limit=16) if df_30m_slice is not None and not df_30m_slice.empty else ""
        
        df_5m_slice = df_5m.tail(48) if df_5m is not None else None
        min_5_context = self._format_kline_raw_data(df_5m_slice, "5分钟线", limit=48) if df_5m_slice is not None and not df_5m_slice.empty else ""
        
        # [新增] 关键K线提取与支撑压力分析
        key_k_lines_context = ""
        try:
            from app.services.indicators.technical_indicators import technical_indicators
            # 优先使用日线数据提取关键K线
            kl_df = df if df is not None and not df.empty else None
            if kl_df is not None:
                key_lines = technical_indicators.get_key_k_lines(kl_df)
                if key_lines:
                    key_k_lines_context = "\n【关键K线分析 (Key K-Lines & Support/Resistance)】\n"
                    key_k_lines_context += "说明：基于近期成交量异动与大K线提取，作为短期核心支撑压力参考。\n"
                    for k in key_lines:
                        key_k_lines_context += f"- {k['date']}: {k['type']} (涨幅{k['pct_chg']}%, 量比{k['vol_ratio']}) -> 支撑: {k['support']}, 压力: {k['resistance']}\n"
        except Exception as e:
            logger.error(f"Error getting key k-lines: {e}")

        # 4. 基本面五步筛选结果 (仅用于退市风险过滤)
        fundamental_analysis = ""
        if basic_info:
            fundamental_analysis = f"""
【基本面五步筛选结果(仅作退市风险过滤)】
1. 行业地位: {basic_info.get('industry', '未知')} | {basic_info.get('concept', '未知')}
2. 财务安全: ROE {basic_info.get('roe', 0):.2f}% | 资产负债率 {basic_info.get('debt_to_assets', 0):.2f}%
3. 成长性: 营收增长 {basic_info.get('rev_growth', 0):.2f}% | 净利增长 {basic_info.get('net_profit_growth', 0):.2f}%
4. 估值水平: PE(TTM) {basic_info.get('pe_ttm', 0):.2f} | PB {basic_info.get('pb', 0):.2f}
5. 机构动向: 机构持股变动 {basic_info.get('inst_change', 0):.2f}% | 近期调研 {basic_info.get('research_count', 0)} 次
"""

        # 5. 板块背景
        sector_context = ""
        if sector_info:
            sector_context = f"""
【所属板块表现 - {sector_info.get('sector_name', '未知')}】
- 板块涨跌幅: {sector_info.get('sector_pct_chg', 0):.2f}%
- 板块内个股强度排名: {sector_info.get('rank', '未知')}
- 板块领涨股: {', '.join(sector_info.get('leaders', []))}
- 板块趋势判定: {sector_info.get('trend', '震荡')}
"""

        # [新增] 注入长期记忆与反思
        market_temp = None
        if daily_context:
            # 尝试从 daily_context 中提取温度或通过 realtime_quote (这里简化处理，暂不深入解析字符串)
            pass
        
        memories = await learning_service.get_reflection_memories(strategy, market_temperature=market_temp)
        successful_patterns = await learning_service.get_successful_pattern_context(strategy)
        failed_patterns = await learning_service.get_failed_pattern_context(strategy)

        # 6. 策略特定逻辑
        strategy_note = ""
        if strategy == "sell":
            strategy_note = """
**【卖出决策特别提醒】**：
- 除非发现明确的、多周期共振的走弱信号，否则对于处于月线/周线级别上升趋势的标的，应保持耐心。
 - 重点参考“板块未弱不卖”和“独立走强不卖”原则。
"""

        def _fmt_price(v):
            try:
                return f"{float(v):.2f}"
            except Exception:
                return "N/A"

        def _fmt_pct(v):
            try:
                return f"{float(v):.2f}%"
            except Exception:
                return "N/A"

        if realtime_quote:
            rt = realtime_quote
            realtime_summary = (
                f"开盘:{_fmt_price(rt.get('open'))} "
                f"最高:{_fmt_price(rt.get('high'))} "
                f"最低:{_fmt_price(rt.get('low'))} "
                f"现价:{_fmt_price(rt.get('price'))} "
                f"昨收:{_fmt_price(rt.get('pre_close'))} "
                f"涨跌幅:{_fmt_pct(rt.get('pct_chg'))} "
                f"量比:{_fmt_price(rt.get('vol_ratio'))} "
                f"换手:{_fmt_pct(rt.get('turnover_rate'))}"
            )
        else:
            realtime_summary = "参考 K 线最后一条数据"

        prompt = f"""
你是一位顶级的 A 股职业交易员和量化分析专家。请根据以下提供的多维度数据，对股票 **{symbol}** 进行深度透视分析。

{core_rules}
{prev_score_context}
{strategy_note}

【当前市场与标的信息】
- 标的代码: {symbol}
- 交易状态: {trading_status}
- 实时行情: {realtime_summary}
- {ma5_angle_context}

{fundamental_analysis}
{sector_context}
【关键K线分析 (Key K-Lines & Support/Resistance)】
{key_k_lines_context}

【个股多周期 K 线数据】
{monthly_context}
{weekly_context}
{daily_raw}
{daily_context}
{min_30_context}
{min_5_context}

{memories}
{successful_patterns}
{failed_patterns}

【外部搜索与新闻情报】
{search_info if search_info else "无最新新闻情报"}

【原始交易上下文 (选股策略详情)】
{raw_trading_context if raw_trading_context else "无特定选股策略上下文"}

【任务要求】
1. **关键K线定位**: 分析【关键K线分析】部分，确认当前价格相对于关键支撑/压力的位置，判定运行方向。
2. **趋势阶段定义**: 明确判定当前处于大级别趋势的哪个阶段（起点、中段、末端）。
3. **量价灵魂解读**: 重点分析"缩量攀升"的本质（是主力锁仓还是动能衰竭？）、MACD 波段峰值的比较。
4. **分时走势判定**: 结合 30min 和 5min 数据，判定短期洗盘是否结束，寻找介入点。
5. **决策建议**: 
   - 给出 **0-100** 的综合评分（80+ 为极佳，60-80 为持有，60 以下需警惕）。
   - 给出明确的操作建议：BUY (买入), HOLD (持有), REDUCE (减仓), SELL (清仓), WAIT (观望)。
   - 设定明确的止损位和目标位。

## Output Format (Strict JSON):
请严格按照以下 JSON 格式输出，不要包含任何 Markdown 标记或其他文字（JSON 必须合法）。
**严禁输出任何 <think> 标签或思维链内容，只输出最终的 JSON 结果。**
{{
    "is_worth_trading": true/false,
    "score": 0-100,
    "rejection_reason": "如果不值得交易，简述拒绝理由",
    "full_report": "# 1. 核心结论与操作建议\\n\\n**综合评分：[实际评分值]**\\n\\n**一句话定性**：[简明定性]\\n\\n**【实战建议】**：\\n- **操作方向**：[BUY/HOLD/REDUCE/SELL/WAIT]\\n- **建议价位**：[具体数值]\\n- **止损保护**：[具体数值]\\n- **第一目标**：[具体数值]\\n\\n# 2. 主力意图深度解密\\n\\n## 核心逻辑\\n- **趋势定调**：[月/周线级别趋势判断，限50字]\\n- **形态确认**：[日线关键形态与支撑压力，限50字]\\n\\n## 资金与量价\\n- **量能特征**：[分析缩量/放量背后的资金意图]\\n- **筹码状态**：[判断获利盘与套牢盘情况]\\n\\n# 3. 分时博弈 (30min/5min)\\n\\n- **洗盘/出货判定**：[结合分时图判断]\\n- **精准介入点**：[具体的盘中信号]\\n\\n# 4. 风险提示\\n\\n- [列出1-2个核心风险点]"
}}

**格式强制要求**：
1. `full_report` 字段的内容必须是标准的 Markdown 格式。
2. **标题必须使用 Markdown 一级标题 (`#`) 或二级标题 (`##`)。**
3. **内容必须高度精炼，严禁堆砌长难句，多用短句和列表项 (`-`)。**
4. **标题与正文之间必须有空行 (`\\n\\n`)。**
5. **所有输出内容必须严格使用简体中文。**
6. **报告总字数严格控制在 800 字以内，务必言简意赅。**
"""
        return prompt


prompt_builder = PromptBuilder()

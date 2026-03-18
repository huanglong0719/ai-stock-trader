import pandas as pd
import numpy as np
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from app.services.data_provider import data_provider
from app.services.ai_service import ai_service
from app.services.logger import selector_logger
from app.services.market.stock_data_service import stock_data_service
from app.services.market.market_utils import is_trading_time
from app.services.market.tdx_fundamental_service import tdx_fundamental_service

class SectorAnalysisService:
    def __init__(self):
        self._cache = {} # {industry: {'rising_wave': ..., 'timestamp': ...}}
        self._locks = {} # {industry: asyncio.Lock}

    def clear_cache(self):
        self._cache = {}
        self._locks = {}
        selector_logger.log("Sector Analysis Cache cleared.")

    async def analyze_sector(self, target_ts_code: str):
        """
        对目标股票进行板块内横向对比分析
        """
        # 1. 获取目标股票基础信息 (使用本地数据)
        stock_basic = await asyncio.to_thread(stock_data_service.get_stock_basic)
        target_info = next((s for s in stock_basic if s['ts_code'] == target_ts_code), None)
        if not target_info or not target_info.get('industry'):
            return {"error": "未找到股票或所属行业信息"}

        industry = target_info['industry']
        
        # 获取或创建行业锁，确保同一时间只有一个任务在计算该行业的板块共振
        if industry not in self._locks:
            self._locks[industry] = asyncio.Lock()
            
        async with self._locks[industry]:
            selector_logger.log(f"开始对 {target_info['name']} 进行板块分析 (行业: {industry})...")

            # 1.1 获取板块宏观数据 (使用本地数据，避免调用Tushare)
            trade_date = await data_provider.get_last_trade_date()
            industry_data_list = await asyncio.to_thread(stock_data_service.get_industry_data_local, trade_date)
            
            # 排序获取行业排名
            sorted_industries = sorted(industry_data_list, key=lambda x: x['avg_pct_chg'], reverse=True)
            industry_rank = next((i for i, item in enumerate(sorted_industries) if item['industry'] == industry), -1) + 1
            target_industry_summary = next((i for i in industry_data_list if i['industry'] == industry), None)
            
            if target_industry_summary:
                target_industry_summary['rank'] = industry_rank
                target_industry_summary['total_industries'] = len(industry_data_list)
                target_industry_summary['is_realtime'] = False

            # 2. 获取同板块所有股票
            industry_peers = [s for s in stock_basic if s.get('industry') == industry]
            peer_codes = [s['ts_code'] for s in industry_peers]
            
            if len(peer_codes) < 3:
                return {"error": "板块个股数量过少，无法进行有效横向对比"}

            # 3. 检查缓存 (TTL: 15分钟)
            now = datetime.now().timestamp()
            cached_data = self._cache.get(industry)
            if cached_data and (now - cached_data.get('timestamp', 0) < 900):
                rising_wave_info = cached_data['rising_wave']
                selector_logger.log(f"使用行业 {industry} 的缓存分析结果")
            else:
                # 3.1 限制板块共振检查的股票数量 (取成交额前 10)
                # [优化] 仅查询该行业个股的日线数据，不再加载全市场数据
                trade_date = await data_provider.get_last_trade_date()
                df_daily_industry = await asyncio.to_thread(stock_data_service.get_daily_basic_local, trade_date, peer_codes)
                
                if df_daily_industry:
                    df_daily_industry = pd.DataFrame(df_daily_industry)
                    # [用户要求] 仅分析成交额前 10 的个股作为板块风向标
                    # 用户要求: 行业板块分析里面的股票也必须按照成交额排序来分析，不是市值大的
                    top_peers = df_daily_industry.sort_values('amount', ascending=False).head(10)['ts_code'].tolist()
                    # 确保目标股也在分析列表中
                    if target_ts_code not in top_peers:
                        top_peers.append(target_ts_code)
                    
                    selector_logger.log(f"行业 {industry} 共有 {len(peer_codes)} 只个股，选取成交额前 {len(top_peers)} 只进行共振分析...")
                    rising_wave_info = await self._check_main_rising_wave(top_peers)
                else:
                    # 回退：如果没获取到市值数据，最多分析前 30 个
                    rising_wave_info = await self._check_main_rising_wave(peer_codes[:30])
                
                self._cache[industry] = {
                    'rising_wave': rising_wave_info,
                    'timestamp': now
                }
            
            # 4. 优质标的多维度对比 (这里内部已经有限制 compare_pool)
            comparison_results = await self._compare_stocks(peer_codes, target_ts_code, industry)
            
            # 5. 生成操作策略建议
            trading_status = await ai_service.get_trading_status()
            strategy_recommendation = self._generate_strategy_recommendation(rising_wave_info, comparison_results, target_ts_code, trading_status)

            return {
                "industry": industry,
                "industry_summary": target_industry_summary,
                "peer_count": len(peer_codes),
                "rising_wave_status": rising_wave_info,
                "comparison": comparison_results,
                "strategy": strategy_recommendation
            }

    async def _check_main_rising_wave(self, peer_codes: list):
        """
        检查板块内是否有3只及以上个股同时出现主升浪启动特征 (优化版: 使用预计算指标)
        """
        selector_logger.log(f"正在分析板块共振情况 (共 {len(peer_codes)} 只个股)...")
        
        # 1. 批量获取预计算指标
        indicator_map = await asyncio.to_thread(stock_data_service.get_latest_indicators_batch, peer_codes)
        
        active_stocks = []
        for ts_code in peer_codes:
            ind = indicator_map.get(ts_code)
            if not ind:
                continue
                
            # 使用预计算的标志位进行判定
            # 日线多头: Close > MA20/MA60, MA5>10>20, MACD>0
            # 周线多头: 周MA5>10>20, MACD>0
            # 月线多头: 月MA5>10>20, MACD>0
            
            is_daily = ind.get('is_daily_bullish', False)
            is_weekly = ind.get('is_weekly_bullish', False)
            is_monthly = ind.get('is_monthly_bullish', False)
            
            # 成交量判定 (额外保留，因为标志位里不含成交量要求)
            vol_ma5 = ind.get('vol_ma5')
            vol_ma10 = ind.get('vol_ma10')
            volume = ind.get('volume', 0)
            
            vol_ok = False
            if vol_ma5 and vol_ma10:
                # 放量或量能维持在 5日均量 60% 以上
                vol_ok = (vol_ma5 > vol_ma10 and volume > vol_ma5 * 0.6)
            
            # 三周期共振 + 成交量配合
            # 活跃定义: 日线趋势向上 且 (周线多头 或 周线复苏)
            if is_daily and (is_weekly or ind.get('is_trend_recovering', False)):
                active_stocks.append(ts_code)
            else:
                # 记录不活跃原因以便调试
                reasons = []
                if not is_daily: reasons.append("日线非多头")
                if not (is_weekly or ind.get('is_trend_recovering', False)): reasons.append("周线趋势弱")
                # selector_logger.log(f"  - {ts_code} 不活跃: {', '.join(reasons)}", level="DEBUG")
                pass

        is_sector_active = len(active_stocks) >= 3
        selector_logger.log(f"板块共振分析完成: {len(active_stocks)} 只个股活跃, 共振状态: {is_sector_active}")
        
        return {
            "is_sector_active": is_sector_active,
            "active_stock_count": len(active_stocks),
            "active_stocks": active_stocks
        }

    async def _compare_stocks(self, peer_codes: list, target_ts_code: str, industry: str):
        """
        多维度对比板块内标的 - 优化版 (使用本地数据，不调用Tushare)
        """
        selector_logger.log(f"正在执行板块标的横向对比...")
        
        trade_date = await data_provider.get_last_trade_date()
        selector_logger.log(f"使用交易日: {trade_date}, 对比股票数: {len(peer_codes)}")
        
        # 1. 仅获取行业内个股的日线基础数据 (本地)
        df_daily_industry = await asyncio.to_thread(stock_data_service.get_daily_basic_local, trade_date, peer_codes)
        if not df_daily_industry:
             selector_logger.log(f"错误: 无法获取交易日 {trade_date} 的行业 {industry} 日线基础数据", level="ERROR")
             return {"error": "无法获取行业对比数据"}
        
        df_daily_industry = pd.DataFrame(df_daily_industry)
        selector_logger.log(f"获取到行业 {industry} 日线数据: {len(df_daily_industry)} 条记录")

        # 2. 筛选对比池：成交额前 5 + 目标股
        # 用户要求: 行业板块分析里面的股票也必须按照成交额排序来分析，不是市值大的
        sorted_peers = df_daily_industry.sort_values('amount', ascending=False).head(5)
        peer_sample = sorted_peers['ts_code'].tolist()
        compare_pool = list(set([target_ts_code] + peer_sample))
        compare_pool_str = ",".join(compare_pool)
        
        selector_logger.log(f"对比池: {compare_pool}")
        
        avg_pe = df_daily_industry['pe'].mean()
        avg_pb = df_daily_industry['pb'].mean()
        
        all_basic = await asyncio.to_thread(stock_data_service.get_stock_basic)
        basic_map = {s['ts_code']: s for s in all_basic}

        # 3. 批量获取数据 (使用通达信本地数据，不调用Tushare)
        # 3.1 财务指标 (使用通达信批量获取)
        tdx_fina_map = tdx_fundamental_service.batch_get_fundamental_data(compare_pool)
        fina_map = {}
        for code in compare_pool:
            if code in tdx_fina_map:
                tdx_data = tdx_fina_map[code]
                fina_map[code] = {
                    'roe': tdx_data.get('roe', 0),
                    'yoy_net_profit': tdx_data.get('yoy_net_profit', 0),
                    'debt_to_assets': tdx_data.get('debt_to_assets', 0)
                }

        # 3.2 资金流向 (简化版，不调用Tushare)
        mf_map: dict[str, pd.DataFrame] = {}
        # 注释掉资金流向获取，避免调用Tushare
        # start_dt = (datetime.now() - timedelta(days=15)).strftime('%Y%m%d')
        # end_dt = datetime.now().strftime('%Y%m%d')
        # df_mf = await data_provider.get_moneyflow(ts_code=compare_pool_str, start_date=start_dt, end_date=end_dt)
        # if not df_mf.empty:
        #     df_mf = df_mf.sort_values('trade_date', ascending=False)
        #     for code in compare_pool:
        #         mf_map[code] = df_mf[df_mf['ts_code'] == code]

        # 3.3 预计算指标 (替代原来的 K 线批量获取和手动计算)
        indicator_map = await asyncio.to_thread(stock_data_service.get_latest_indicators_batch, compare_pool)
        
        # 3.4 日线基础数据
        daily_map = {}
        for code in compare_pool:
            row = df_daily_industry[df_daily_industry['ts_code'] == code]
            if not row.empty:
                daily_map[code] = row.iloc[0].to_dict()

        # 3.5 实时行情 (跳过，避免调用Tushare，使用日线数据即可)
        realtime_map: dict[str, dict] = {}
        # if is_trading_time():
        #     realtime_map = await data_provider.get_realtime_quotes(compare_pool)

        # 4. 内存计算指标
        comparison_data = []
        for ts_code in compare_pool:
            try:
                # 4.1 基本面 (使用通达信数据)
                fina = fina_map.get(ts_code, {})
                roe = float(fina.get('roe') or 0)
                profit_growth = float(fina.get('yoy_net_profit') or 0)
                debt_to_assets = float(fina.get('debt_to_assets') or 0)
                
                # 4.2 资金面
                mf_df = mf_map.get(ts_code, pd.DataFrame())
                net_mf_5d = 0.0
                large_order_ratio = 0.0
                if not mf_df.empty:
                    net_mf_5d = float(mf_df.head(5)['net_mf_amount'].sum())
                    latest_mf = mf_df.iloc[0]
                    total_mf = sum([abs(float(latest_mf.get(k) or 0)) for k in ['buy_sm_amount', 'sell_sm_amount', 'buy_md_amount', 'sell_md_amount', 'buy_lg_amount', 'sell_lg_amount', 'buy_elg_amount', 'sell_elg_amount']])
                    if total_mf > 0:
                        large_order_ratio = (abs(float(latest_mf.get('buy_lg_amount') or 0)) + abs(float(latest_mf.get('buy_elg_amount') or 0))) / total_mf * 100

                # 4.3 行情与预计算指标
                daily = daily_map.get(ts_code, {})
                quote = realtime_map.get(ts_code, {})
                ind = indicator_map.get(ts_code, {})
                
                pct_chg = float(quote.get('pct_chg') or ind.get('pct_chg') or daily.get('pct_chg') or 0)
                turnover_rate = float(quote.get('turnover_rate') or daily.get('turnover_rate') or 0)
                pe = float(daily.get('pe') or 0)
                pb = float(daily.get('pb') or 0)
                vol_ratio = float(daily.get('volume_ratio') or 0)
                
                # 量价配合度 (使用预计算指标简化)
                vol_price_score = 60 # 默认及格
                if ind:
                    ma5 = ind.get('ma5')
                    ma10 = ind.get('ma10')
                    vol_ma5 = ind.get('vol_ma5')
                    vol_ma10 = ind.get('vol_ma10')
                    
                    if pct_chg > 0 and vol_ma5 and vol_ma10 and vol_ma5 > vol_ma10:
                        vol_price_score = 85
                    elif pct_chg > 0:
                        vol_price_score = 70
                    elif pct_chg < 0 and vol_ma5 and vol_ma10 and vol_ma5 > vol_ma10:
                        vol_price_score = 30 # 放量下跌
                
                # 筹码集中度 (使用 MA5/MA10 关系简化)
                chip_concentration = 50
                if ind and ind.get('ma5') and ind.get('ma10'):
                    chip_concentration = 70 if ind['ma5'] > ind['ma10'] else 40
                
                stock_name = basic_map.get(ts_code, {}).get('name', ts_code)
                
                comparison_data.append({
                    "ts_code": ts_code,
                    "name": stock_name,
                    "roe": round(roe, 2),
                    "profit_growth": round(profit_growth, 2),
                    "debt_to_assets": round(debt_to_assets, 2),
                    "net_mf_5d": round(float(net_mf_5d), 2),
                    "large_order_ratio": round(large_order_ratio, 2),
                    "pe": round(pe, 2),
                    "pb": round(pb, 2),
                    "vol_ratio": round(vol_ratio, 2),
                    "turnover_rate": round(turnover_rate, 2),
                    "pct_chg": round(pct_chg, 2),
                    "vol_price_score": vol_price_score,
                    "chip_concentration": chip_concentration,
                    "is_daily_bullish": ind.get('is_daily_bullish', False),
                    "is_trend_recovering": ind.get('is_trend_recovering', False)
                })
            except Exception as e:
                selector_logger.log(f"处理 {ts_code} 数据出错: {e}", level="ERROR")
                continue

        df_comp = pd.DataFrame(comparison_data)
        sector_avg_pct_chg = df_comp['pct_chg'].mean() if not df_comp.empty else 0
        
        for item in comparison_data:
            item['pe_premium'] = round((item['pe'] / avg_pe - 1) * 100, 2) if avg_pe > 0 else 0
            item['pb_premium'] = round((item['pb'] / avg_pb - 1) * 100, 2) if avg_pb > 0 else 0
            item['breakout_strength'] = round(item['pct_chg'] - sector_avg_pct_chg, 2)

        return {
            "avg_pe": round(avg_pe, 2),
            "avg_pb": round(avg_pb, 2),
            "sector_avg_pct_chg": round(sector_avg_pct_chg, 2),
            "stock_data": comparison_data
        }

    def _generate_strategy_recommendation(self, rising_wave, comparison, target_ts_code, trading_status):
        """
        生成基于对比的策略建议
        """
        if 'error' in comparison:
            return f"无法生成策略建议: {comparison['error']}"
        
        if 'stock_data' not in comparison:
            return "数据不足，无法生成建议。"
        
        is_active = rising_wave['is_sector_active']
        active_count = rising_wave['active_stock_count']
        active_stocks = rising_wave['active_stocks']
        
        df_comp = pd.DataFrame(comparison['stock_data'])
        if df_comp.empty:
            return "数据不足，无法生成建议。"
            
        target_data = df_comp[df_comp['ts_code'] == target_ts_code].iloc[0]
        
        # 寻找板块龙头 (涨幅最高且量价配合好)
        leader = df_comp.sort_values(['pct_chg', 'vol_price_score'], ascending=False).iloc[0]
        
        # 针对交易时间调整建议措辞
        time_prefix = ""
        is_market_closed = "已收盘" in trading_status or "休市" in trading_status
        if is_market_closed:
            time_prefix = "【盘后研判】"
            action_verb = "明日开盘关注"
        else:
            time_prefix = "【实时操作】"
            action_verb = "分时择机介入"

        recommendation = f"{time_prefix}(当前状态: {trading_status})\n"
        
        if is_active:
            # 板块集体启动
            recommendation += f"【板块效应】板块出现集体启动迹象 (共 {active_count} 只个股满足主升浪条件)。\n"
            recommendation += f"优先策略：\n"
            recommendation += f"1. 关注板块率先突破的龙头股 (当前显示为: {leader['name']})；\n"
            recommendation += f"2. 选择量价配合最理想的标的 (当前目标股评分: {target_data['vol_price_score']})；\n"
            
            if target_ts_code == leader['ts_code']:
                recommendation += f"\n建议：目标股 {target_data['name']} 是板块领涨龙头，突破强度高 ({target_data['breakout_strength']:.2f}%)，量价配合理想，建议{action_verb}作为首选标的。"
            elif target_ts_code in active_stocks:
                recommendation += f"\n建议：目标股 {target_data['name']} 已进入主升浪启动池，属于板块强势梯队，建议{action_verb}并持有。"
            else:
                recommendation += f"\n建议：目标股 {target_data['name']} 尚未触发板块共振，估值溢价率为 {target_data['pe_premium']}%，若板块持续走强，可关注其补涨机会。"
        else:
            # 板块未形成合力
            recommendation += "【独立走势】板块尚未形成集体合力，个股走势相对独立。\n"
            recommendation += "优先策略：选择技术形态优于板块 90% 个股、有催化剂、资金介入深的标的。\n"
            
            is_strong_tech = target_data['breakout_strength'] > 2
            is_strong_fund = target_data['large_order_ratio'] > 30
            
            if is_strong_tech and is_strong_fund:
                recommendation += f"\n建议：目标股 {target_data['name']} 具备独立走强特征。突破强度 {target_data['breakout_strength']:.2f}%，大单占比 {target_data['large_order_ratio']}%，建议{action_verb}。"
            elif is_strong_tech:
                recommendation += f"\n建议：目标股 {target_data['name']} 技术形态强于板块，但主力资金介入程度一般 ({target_data['large_order_ratio']}%)，需警惕冲高回落，建议{action_verb}并观察量能配合。"
            else:
                recommendation += f"\n建议：目标股 {target_data['name']} 走势与板块基本同步，暂无明显独立走强特征，建议{action_verb if not is_market_closed else '暂时'}观望。"

        return recommendation

sector_analysis = SectorAnalysisService()

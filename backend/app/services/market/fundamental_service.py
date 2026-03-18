import pandas as pd
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
from sqlalchemy import desc

from app.db.session import SessionLocal
from app.models.stock_models import FinaIndicator, DailyBasic, FinaScreeningResult
from app.services.market.tdx_fundamental_service import tdx_fundamental_service
from app.services.logger import logger
import json

class FundamentalService:
    def __init__(self):
        self._cache = {}
        self._cache_duration = 3600 * 24  # 基本面数据变化慢，缓存 24 小时

    async def get_fundamental_context(self, ts_code: str) -> Dict[str, Any]:
        """
        获取 5 步基本面筛选所需的上下文数据
        """
        # 检查内存缓存
        if ts_code in self._cache:
            data, ts = self._cache[ts_code]
            if datetime.now().timestamp() - ts < self._cache_duration:
                return data

        # [优化] 优先从通达信本地 DBF 获取，毫秒级响应
        try:
            tdx_data = await asyncio.wait_for(
                asyncio.to_thread(tdx_fundamental_service.get_fundamental_data, ts_code),
                timeout=2.5
            )
        except Exception:
            tdx_data = None
        if tdx_data:
            roe = tdx_data.get('roe', 0)
            debt = tdx_data.get('debt_to_assets', 0)
            yoy_profit = tdx_data.get('yoy_net_profit', 0)
            
            # 简单估算 PE/PB (如果需要实时性，可以从行情获取，这里先从DBF取缓存值)
            pe = tdx_data.get('pe_ratio', 0)
            pb = tdx_data.get('pb_ratio', 0)
            
            fina_indicators = {
                "end_date": tdx_data['end_date'].strftime('%Y%m%d'),
                "roe": roe,
                "yoy_net_profit": yoy_profit,
                "yoy_revenue": tdx_data.get('yoy_revenue', 0),
                "debt_to_assets": debt,
                "source": "tdx"
            }
            
            # 执行评分逻辑
            screening = self._run_five_step_screening(ts_code, fina_indicators, pe, pb)
            
            res = {
                "fina_indicators": fina_indicators,
                "business_profile": {}, 
                "forecast": {},
                "valuation": {
                    "pe": pe,
                    "pb": pb,
                    "total_mv": (tdx_data.get('total_shares', 0) * pe / 10000.0) if pe > 0 else 0 # 粗略估算
                },
                "screening": screening
            }
            self._cache[ts_code] = (res, datetime.now().timestamp())
            return res

        # 检查数据库缓存
        def _load_cached_result():
            db = SessionLocal()
            try:
                cached_result = db.query(FinaScreeningResult).filter(
                    FinaScreeningResult.ts_code == ts_code
                ).order_by(FinaScreeningResult.end_date.desc()).first()
                if cached_result:
                    cached_end_date = cached_result.end_date
                    days_old = (datetime.now().date() - cached_end_date).days
                    if days_old <= 30:
                        try:
                            json_str = str(cached_result.screening_json)
                            return json.loads(json_str)
                        except Exception:
                            return None
                return None
            finally:
                db.close()

        try:
            cached_payload = await asyncio.wait_for(asyncio.to_thread(_load_cached_result), timeout=2.5)
        except Exception:
            cached_payload = None
        if cached_payload:
            return cached_payload

        # 并行获取各项数据（简化版，只获取财务指标）
        tasks = [
            self.get_latest_fina_indicators(ts_code),
        ]
        fina_data, = await asyncio.gather(*tasks)
        
        # 2. 获取每日基础指标 (PE, PB)
        from app.services.market.market_data_service import market_data_service
        daily_basic = await market_data_service.get_daily_basic(ts_codes=[ts_code])
        stock_basic = daily_basic[daily_basic['ts_code'] == ts_code]
        
        pe = float(stock_basic['pe'].iloc[0]) if not stock_basic.empty and stock_basic['pe'].iloc[0] else 0.0
        pb = float(stock_basic['pb'].iloc[0]) if not stock_basic.empty and stock_basic['pb'].iloc[0] else 0.0
        total_mv = float(stock_basic['total_mv'].iloc[0]) if not stock_basic.empty and stock_basic['total_mv'].iloc[0] else 0.0

        # 3. 执行 5 步筛选逻辑
        screening = self._run_five_step_screening(ts_code, fina_data, pe, pb)

        res = {
            "fina_indicators": fina_data,
            "business_profile": {},  # 简化，不调用Tushare
            "forecast": {},  # 简化，不调用Tushare
            "valuation": {
                "pe": pe,
                "pb": pb,
                "total_mv": total_mv / 10000.0 if total_mv else 0.0  # 亿元
            },
            "screening": screening
        }
        
        # 写入内存缓存
        self._cache[ts_code] = (res, datetime.now().timestamp())
        
        # 写入数据库缓存
        def _write_cache():
            db = SessionLocal()
            try:
                end_date = fina_data.get('end_date') if fina_data else datetime.now().date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, '%Y%m%d').date()
                existing = db.query(FinaScreeningResult).filter(
                    FinaScreeningResult.ts_code == ts_code,
                    FinaScreeningResult.end_date == end_date
                ).first()
                if existing:
                    existing.screening_json = json.dumps(res, ensure_ascii=False)
                    existing.total_score = screening.get('total_score', 0)
                    existing.updated_at = datetime.now()
                else:
                    end_date_val = end_date
                    if not end_date_val:
                        end_date_val = datetime.now().date()
                    if isinstance(end_date_val, str):
                        end_date_val = datetime.strptime(end_date_val, '%Y%m%d').date()
                    result = FinaScreeningResult()
                    result.ts_code = ts_code
                    result.end_date = end_date_val
                    result.screening_json = json.dumps(res, ensure_ascii=False)
                    result.total_score = screening.get('total_score', 0)
                    db.add(result)
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to cache screening result for {ts_code}: {e}")
                db.rollback()
            finally:
                db.close()

        try:
            await asyncio.wait_for(asyncio.to_thread(_write_cache), timeout=3.0)
        except Exception:
            pass
        
        return res

    async def get_performance_forecast(self, ts_code: str) -> Dict[str, Any]:
        """获取最新的业绩预告 (不调用 Tushare)"""
        # 目前本地数据不包含详细预告，返回空
        return {}

    async def get_business_profile(self, ts_code: str) -> Dict[str, Any]:
        """获取上市公司基本业务画像 (不调用 Tushare)"""
        try:
            from app.services.market.tdx_formula_service import tdx_formula_service
            # 尝试从通达信 EXTERNSTR 获取本地资讯 (1=主营业务/核心题材)
            # 不同版本的通达信 EXTERNSTR ID 不同，这里尝试获取核心题材
            val = tdx_formula_service.EXTERNSTR(2, ts_code)
            
            profile: Dict[str, Any] = {
                "main_business": val or "暂无本地主营业务描述",
                "business_scope": "",
                "main_products": []
            }
            
            # 如果核心题材很长，尝试提取前 200 字
            if val and len(val) > 200:
                profile["main_business"] = val[:200] + "..."
            
            return profile
        except Exception as e:
            logger.info(f"获取本地业务画像失败 ({ts_code}): {e}")
            return {"main_business": "暂无数据", "business_scope": "", "main_products": []}

    async def get_screening_score(self, ts_code: str) -> float:
        """
        快速获取基本面评分 (用于初选过滤)
        """
        ctx = await self.get_fundamental_context(ts_code)
        return ctx.get("screening", {}).get("total_score", 0.0)
    
    async def batch_get_screening_scores(self, ts_codes: list) -> Dict[str, Dict[str, Any]]:
        """
        批量获取基本面评分（优化版，使用通达信批量获取）
        """
        # 1. 批量从通达信获取财务数据
        tdx_data_map = tdx_fundamental_service.batch_get_fundamental_data(ts_codes)
        
        # 2. 获取 PE/PB 数据 (从本地数据库)
        from app.services.market.market_data_service import market_data_service
        daily_basic_df = await market_data_service.get_daily_basic(ts_codes=ts_codes)
        daily_basic_map = {}
        if not daily_basic_df.empty:
            for _, row in daily_basic_df.iterrows():
                daily_basic_map[row['ts_code']] = row

        results = {}
        for ts_code in ts_codes:
            # 优先使用通达信数据
            if ts_code in tdx_data_map:
                tdx_data = tdx_data_map[ts_code]
                db_basic = daily_basic_map.get(ts_code, {})
                
                # 获取 PE/PB
                pe = float(db_basic.get('pe', 0)) if db_basic.get('pe') else tdx_data.get('pe_ratio', 0)
                pb = float(db_basic.get('pb', 0)) if db_basic.get('pb') else tdx_data.get('pb_ratio', 0)
                total_mv = float(db_basic.get('total_mv', 0)) if db_basic.get('total_mv') else 0
                
                fina_indicators = {
                    "end_date": tdx_data['end_date'].strftime('%Y%m%d'),
                    "roe": tdx_data.get('roe', 0),
                    "yoy_net_profit": tdx_data.get('yoy_net_profit', 0),
                    "yoy_revenue": tdx_data.get('yoy_revenue', 0),
                    "debt_to_assets": tdx_data.get('debt_to_assets', 0),
                    "source": "tdx"
                }
                
                # 使用统一的 5 步筛选逻辑
                screening_result = self._run_five_step_screening(ts_code, fina_indicators, pe, pb)
                
                results[ts_code] = {
                    "screening": screening_result,
                    "fina_indicators": fina_indicators,
                    "valuation": {
                        "pe": pe,
                        "pb": pb,
                        "total_mv": total_mv / 10000.0 if total_mv else 0.0
                    }
                }
            else:
                # 如果通达信没有数据，使用原有方法 (可能涉及数据库或网络)
                ctx = await self.get_fundamental_context(ts_code)
                results[ts_code] = ctx
        
        return results

    async def get_latest_fina_indicators(self, ts_code: str) -> Dict[str, Any]:
        """获取最新的财务指标，仅从通达信本地文件获取"""
        
        # 从通达信本地文件获取
        try:
            tdx_data = await asyncio.wait_for(
                asyncio.to_thread(tdx_fundamental_service.get_fundamental_data, ts_code),
                timeout=2.5
            )
        except Exception:
            tdx_data = None
        if tdx_data:
            logger.info(f"从通达信获取财务数据: {ts_code}")
            return {
                "end_date": tdx_data['end_date'].strftime('%Y%m%d'),
                "roe": tdx_data['roe'],
                "netprofit_margin": 0.0,  # 备选
                "grossprofit_margin": 0.0,  # 备选
                "yoy_net_profit": tdx_data.get('yoy_net_profit', 0.0),
                "yoy_revenue": tdx_data.get('yoy_revenue', 0.0),
                "debt_to_assets": tdx_data.get('debt_to_assets', 0.0),
                "op_cashflow": 0.0,  # 备选
                "source": "tdx"
            }
        
        # 通达信没有数据，返回空字典
        logger.warning(f"通达信中未找到股票 {ts_code} 的财务数据")
        return {}

    def _run_five_step_screening(self, ts_code: str, fina: Dict, pe: float, pb: float) -> Dict[str, Any]:
        """
        执行 5 步基本面筛选逻辑
        """
        results: Dict[str, Any] = {
            "step1_safety": {"passed": False, "score": 0, "details": []},
            "step2_profitability": {"passed": False, "score": 0, "details": []},
            "step3_business": {"passed": True, "score": 80, "details": ["需结合 AI 深度分析"]}, # 主要是定性
            "step4_growth": {"passed": False, "score": 0, "details": []},
            "step5_valuation": {"passed": False, "score": 0, "details": []},
            "total_score": 0,
            "conclusion": ""
        }

        if not fina:
            results["conclusion"] = "暂无财务数据，无法进行基本面筛选"
            return results

        # Step 1: 财务安全 (排除地雷)
        # 1. 负债率 < 50%
        debt = fina.get('debt_to_assets', 100)
        if debt < 50:
            results["step1_safety"]["details"].append(f"资产负债率 {debt:.2f}% (优秀)")
            results["step1_safety"]["score"] += 40
        elif debt < 70:
            results["step1_safety"]["details"].append(f"资产负债率 {debt:.2f}% (合格)")
            results["step1_safety"]["score"] += 20
        else:
            results["step1_safety"]["details"].append(f"资产负债率 {debt:.2f}% (偏高)")

        # 2. 经营现金流
        cash = fina.get('op_cashflow', 0)
        if cash > 0:
            results["step1_safety"]["details"].append(f"经营现金流净额 {cash:.2f}亿 (正向)")
            results["step1_safety"]["score"] += 40
        else:
            results["step1_safety"]["details"].append(f"经营现金流净额 {cash:.2f}亿 (负向，需警惕)")

        # 3. PB/PE 基础门槛
        if pe > 0 and pe < 50: results["step1_safety"]["score"] += 10
        if pb > 0 and pb < 8: results["step1_safety"]["score"] += 10

        results["step1_safety"]["passed"] = results["step1_safety"]["score"] >= 60

        # Step 2: 盈利能力 (评估生意好坏)
        # 1. 毛利率 > 20%
        gross = fina.get('grossprofit_margin', 0)
        if gross > 35:
            results["step2_profitability"]["details"].append(f"毛利率 {gross:.2f}% (极强)")
            results["step2_profitability"]["score"] += 50
        elif gross > 20:
            results["step2_profitability"]["details"].append(f"毛利率 {gross:.2f}% (良好)")
            results["step2_profitability"]["score"] += 30
        else:
            results["step2_profitability"]["details"].append(f"毛利率 {gross:.2f}% (平庸)")

        # 2. ROE > 15%
        roe = fina.get('roe', 0)
        if roe > 15:
            results["step2_profitability"]["details"].append(f"ROE {roe:.2f}% (优秀)")
            results["step2_profitability"]["score"] += 50
        elif roe > 10:
            results["step2_profitability"]["details"].append(f"ROE {roe:.2f}% (合格)")
            results["step2_profitability"]["score"] += 30
        else:
            results["step2_profitability"]["details"].append(f"ROE {roe:.2f}% (偏低)")

        results["step2_profitability"]["passed"] = results["step2_profitability"]["score"] >= 60

        # Step 4: 成长性 (判断未来潜力)
        # 1. 营收增长 > 15%
        rev_yoy = fina.get('yoy_revenue', 0)
        if rev_yoy > 25:
            results["step4_growth"]["details"].append(f"营收增长 {rev_yoy:.2f}% (高成长)")
            results["step4_growth"]["score"] += 50
        elif rev_yoy > 15:
            results["step4_growth"]["details"].append(f"营收增长 {rev_yoy:.2f}% (稳健成长)")
            results["step4_growth"]["score"] += 30
        else:
            results["step4_growth"]["details"].append(f"营收增长 {rev_yoy:.2f}% (成长缓慢)")

        # 2. 净利增长 > 15%
        net_yoy = fina.get('yoy_net_profit', 0)
        if net_yoy > 25:
            results["step4_growth"]["details"].append(f"净利增长 {net_yoy:.2f}% (爆发)")
            results["step4_growth"]["score"] += 50
        elif net_yoy > 15:
            results["step4_growth"]["details"].append(f"净利增长 {net_yoy:.2f}% (健康)")
            results["step4_growth"]["score"] += 30
        else:
            results["step4_growth"]["details"].append(f"净利增长 {net_yoy:.2f}% (低速)")

        results["step4_growth"]["passed"] = results["step4_growth"]["score"] >= 60

        # Step 5: 综合估值
        if pe > 0 and pe < 20:
            results["step5_valuation"]["details"].append(f"PE {pe:.2f} (估值偏低)")
            results["step5_valuation"]["score"] = 90
        elif pe < 40:
            results["step5_valuation"]["details"].append(f"PE {pe:.2f} (估值中等)")
            results["step5_valuation"]["score"] = 60
        else:
            results["step5_valuation"]["details"].append(f"PE {pe:.2f} (估值偏高)")
            results["step5_valuation"]["score"] = 30
        
        results["step5_valuation"]["passed"] = results["step5_valuation"]["score"] >= 60

        # 计算总分
        results["total_score"] = (
            results["step1_safety"]["score"] * 0.2 +
            results["step2_profitability"]["score"] * 0.2 +
            results["step3_business"]["score"] * 0.2 +
            results["step4_growth"]["score"] * 0.2 +
            results["step5_valuation"]["score"] * 0.2
        )

        if results["total_score"] >= 80:
            results["conclusion"] = "基本面非常扎实，具备长期投资价值"
        elif results["total_score"] >= 60:
            results["conclusion"] = "基本面良好，建议结合技术面寻找介入点"
        else:
            results["conclusion"] = "基本面存在瑕疵，需谨慎对待"

        return results

# 全局单例
fundamental_service = FundamentalService()

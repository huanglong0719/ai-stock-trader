import httpx
import json
from app.core.config import settings

from app.services.logger import selector_logger

from datetime import datetime

class SearchService:
    def __init__(self):
        self.api_key = settings.SEARCH_API_KEY
        self.engine = settings.SEARCH_ENGINE
        self._client = None
        self._search_cache = {} # {query: (timestamp, content)}
        self._is_available = True
        self._last_failure_time = 0.0

    async def get_client(self):
        if self._client is None or self._client.is_closed:
            # [性能优化] 减少超时时间，Serper 响应通常很快
            self._client = httpx.AsyncClient(timeout=5.0)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def search_stock_info(self, symbol: str, name: str) -> str:
        """
        在东方财富、同花顺等平台搜索股票相关概念和实时资讯
        """
        # [性能优化] 增加本地数据 fallback 机制
        def get_local_fallback():
            from app.services.market.tdx_formula_service import tdx_formula_service
            # 尝试从通达信 EXTERNSTR 获取本地资讯 (1=机构评级, 2=核心题材, 3=主力动态)
            parts = []
            for i in range(1, 4):
                val = tdx_formula_service.EXTERNSTR(i, symbol)
                if val and val.strip():
                    parts.append(f"本地系统数据#{i}: {val.strip()}")
            
            if parts:
                return "\n".join(parts)
            return "暂无本地资讯储备。"

        # [性能优化] 如果服务已知不可用，直接返回本地 fallback
        if not self._is_available:
            import time
            # 5分钟后尝试恢复
            if time.time() - self._last_failure_time > 300:
                self._is_available = True
            else:
                return f"搜索服务暂时不可用（熔断中），已自动回退到本地资讯：\n{get_local_fallback()}"

        selector_logger.log(f"正在全网深度挖掘隐形题材与资讯: {name} ({symbol})...")
        
        if not self.api_key:
            return f"未配置搜索 API，已回退到本地资讯：\n{get_local_fallback()}"

        # 深度挖掘策略：构建核心查询
        query = f"{name} {symbol} 业绩预测 行业地位 核心题材 研报"
        
        try:
            if self.engine == "serper":
                result = await self._search_serper(query)
                if "搜索服务返回错误" in result or "搜索暂不可用" in result or "未找到相关实时资讯" in result:
                     return f"{result}\n本地资讯补充：\n{get_local_fallback()}"
                return result
            else:
                return f"暂不支持该搜索引擎，已回退到本地资讯：\n{get_local_fallback()}"
        except Exception as e:
            return f"搜索出错: {str(e)}，已回退到本地资讯：\n{get_local_fallback()}"

    async def search_market_news(self) -> str:
        """
        搜索今日A股市场核心资讯与复盘要点
        """
        if not self._is_available:
            return "搜索服务暂时不可用，已跳过。"

        selector_logger.log(f"正在全网搜索今日市场核心资讯与复盘要点...")
        
        if not self.api_key:
            return "未配置搜索 API，无法获取实时全网资讯。"

        # 构建市场级查询
        query = "A股今日复盘 国际国内大事 宏观经济数据 政策变动 市场核心热点 连板龙头 龙虎榜机构动向 晚间突发利好"
        
        try:
            if self.engine == "serper":
                return await self._search_serper(query)
            else:
                return "暂不支持该搜索引擎。"
        except Exception as e:
            return f"搜索出错: {str(e)}"

    async def _search_serper(self, query: str) -> str:
        # 1. 检查缓存 (30分钟有效)
        import time
        now_ts = time.time()
        if query in self._search_cache:
            ts, content = self._search_cache[query]
            if now_ts - ts < 1800: # 30 mins
                return content

        url = "https://google.serper.dev/search"
        payload = {
            "q": query,
            "gl": "cn",
            "hl": "zh-cn"
        }
        headers = {
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json'
        }

        try:
            client = await self.get_client()
            response = await client.post(url, headers=headers, json=payload)
            
            if response.status_code != 200:
                # [性能优化] 针对额度耗尽或授权错误，触发熔断
                if response.status_code in [400, 403, 429]:
                    self._is_available = False
                    self._last_failure_time = now_ts
                    # [改进] 提升日志级别为 ERROR，便于监控报警
                    selector_logger.log(f"Serper API 额度耗尽或不可用 (状态码: {response.status_code})，已触发熔断并回退到本地资讯。", level="ERROR")
                else:
                    selector_logger.warning(f"Serper API 返回非200状态码: {response.status_code}")
                return "搜索服务返回错误，已跳过全网资讯。"
                
            results = response.json()
            
            # 提取摘要信息
            snippets = []
            if "organic" in results:
                for item in results["organic"][:5]: # 取前5条
                    title = str(item.get('title', ''))
                    snippet = str(item.get('snippet', ''))
                    # [改进] 过滤无关广告与垃圾信息
                    if "广告" in title or "推广" in title or len(snippet) < 10:
                        continue
                    snippets.append(f"- {title}: {snippet}")
            
            content = "\n".join(snippets) if snippets else "未找到相关实时资讯。"
            
            # 写入缓存
            self._search_cache[query] = (now_ts, content)
            return content
        except Exception as e:
            selector_logger.warning(f"Serper search failed: {e}")
            # 网络超时等异常也记录失败时间
            self._last_failure_time = now_ts
            return "搜索暂不可用，已跳过全网资讯补充。"

search_service = SearchService()

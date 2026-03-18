import requests
import json
import time as sys_time
import pandas as pd
import asyncio
import aiohttp
import concurrent.futures
from datetime import datetime
from typing import Dict, Optional, Any, List, Union
from app.core.config import settings
from app.services.logger import logger

class TushareClient:
    def __init__(self):
        self.token = settings.TUSHARE_TOKEN
        self.base_url = "https://api.tushare.pro"
        self.session = requests.Session()
        # 频率限制相关
        self.max_calls = settings.TUSHARE_MAX_CALLS_PER_MINUTE
        self.call_history = []  # 记录最近一分钟的调用时间戳
        
        # 强制禁用 Session 代理
        self.session.trust_env = False
        self.session.proxies = {"http": None, "https": None}
        # 增加连接池大小
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        # 异步队列相关
        self._queue = None # asyncio.Queue
        self._worker_task = None
        self._async_session = None
        self._lock = asyncio.Lock() # 用于同步 call_history 的访问

    def _ensure_async_init(self):
        """确保异步组件初始化，仅在需要时调用"""
        try:
            loop = asyncio.get_running_loop()
            if self._queue is None:
                self._queue = asyncio.Queue()
                self._worker_task = loop.create_task(self._async_worker())
                logger.info("Tushare 异步请求队列与 Worker 已启动")
        except RuntimeError:
            # 不在异步循环中，不启动异步组件
            pass

    async def _get_async_session(self):
        if self._async_session is None or self._async_session.closed:
            # 增加连接池限制，降低连接超时风险，并将总超时增加到 90 秒
            self._async_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False, limit=30, use_dns_cache=True),
                timeout=aiohttp.ClientTimeout(total=90, connect=30)
            )
        return self._async_session

    async def _wait_for_rate_limit_async(self):
        """
        异步滑动窗口限流逻辑
        """
        async with self._lock:
            while True:
                now = sys_time.time()
                # 清理超过 60 秒的历史记录
                self.call_history = [t for t in self.call_history if now - t < 60]
                
                if len(self.call_history) < self.max_calls:
                    # 还有额度，记录并退出
                    self.call_history.append(now)
                    return
                
                # 计算需要等待的时间
                wait_time = self.call_history[0] + 60 - now
                if wait_time > 0:
                    logger.info(f"触发 Tushare 异步频率预警 (限制 {self.max_calls}次/分)，等待 {wait_time:.1f} 秒...")
                    # 释放锁，让出 CPU，等待后再试
                    pass
                else:
                    # 时间窗口已过，继续循环清理
                    continue
            
            # 这里不会直接到达，逻辑在 while 循环中处理
        
        # 为了释放锁，我们需要在 while 循环外 sleep
        if wait_time > 0:
            await asyncio.sleep(wait_time + 0.1)
            await self._wait_for_rate_limit_async() # 递归调用再次检查

    async def _async_worker(self):
        """
        异步 Worker：从队列中获取请求并按限流规则执行
        """
        queue = self._queue
        if queue is None:
            return

        while True:
            item = await queue.get()
            future, api_name, params, fields, silent = item
            try:
                # 1. 等待限流
                now = sys_time.time()
                self.call_history = [t for t in self.call_history if now - t < 60]
                if len(self.call_history) >= self.max_calls:
                    wait_time = self.call_history[0] + 60 - now
                    if wait_time > 0:
                        await asyncio.sleep(wait_time + 0.1)
                
                self.call_history.append(sys_time.time())

                # 2. 执行请求
                df = await self._execute_async_request(api_name, params, fields, silent)
                future.set_result(df)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)
            finally:
                queue.task_done()

    async def _execute_async_request(self, api_name, params, fields, silent) -> pd.DataFrame:
        """执行实际的 HTTP 请求"""
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params or {},
            "fields": fields
        }
        
        session = await self._get_async_session()
        max_retries = 5 # 增加重试次数
        for attempt in range(max_retries):
            try:
                async with session.post(self.base_url, json=payload) as res:
                    if res.status == 200:
                        result = await res.json()
                        if result['code'] == 0:
                            data = result['data']
                            df = pd.DataFrame(data['items'], columns=data['fields'])
                            return df.where(pd.notnull(df), None)
                        else:
                            msg = result.get('msg', '')
                            if "每分钟最多访问" in msg:
                                wait_time = 60.1 + (attempt * 10) # 阶梯式等待
                                logger.info(f"Tushare 频率限制 (API反馈): {msg}，等待 {wait_time:.1f} 秒...")
                                await asyncio.sleep(wait_time)
                                continue
                            if not silent:
                                logger.info(f"Tushare API 错误: {msg}")
                            return pd.DataFrame()
                    else:
                        if not silent:
                            logger.info(f"Tushare HTTP 错误: {res.status}")
                        if 500 <= res.status < 600 or res.status == 429:
                            await asyncio.sleep(3 * (attempt + 1)) # 增加指数避让
                            continue
                        return pd.DataFrame()
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1) # 增加重试等待时间
                    logger.info(f"Tushare 异步请求重试 ({attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(wait_time)
                else:
                    if not silent:
                        logger.info(f"Tushare 异步请求异常: {e}")
                    return pd.DataFrame()
        return pd.DataFrame()

    async def async_query(self, api_name, params=None, fields="", silent=False) -> pd.DataFrame:
        """
        异步查询：将请求加入队列并等待结果
        """
        self._ensure_async_init()
        
        if self._queue is not None:
            # 通过队列处理
            future = asyncio.get_running_loop().create_future()
            await self._queue.put((future, api_name, params, fields, silent))
            return await future
        else:
            # 回退到直接执行 (例如在测试中没有运行循环)
            return await self._execute_async_request(api_name, params, fields, silent)

    def _wait_for_rate_limit(self):
        """
        简单的滑动窗口限流逻辑
        """
        now = sys_time.time()
        # 清理超过 60 秒的历史记录
        self.call_history = [t for t in self.call_history if now - t < 60]
        
        if len(self.call_history) >= self.max_calls:
            # 计算需要等待的时间：最早的一次调用时间 + 60s - 当前时间
            wait_time = self.call_history[0] + 60 - now
            if wait_time > 0:
                logger.info(f"触发 Tushare 频率预警 (限制 {self.max_calls}次/分)，主动等待 {wait_time:.1f} 秒...")
                sys_time.sleep(wait_time + 0.1)  # 多等 0.1s 确保窗口滑动过去
            
            # 等待后再清理一次
            now = sys_time.time()
            self.call_history = [t for t in self.call_history if now - t < 60]

        # 记录本次调用
        self.call_history.append(sys_time.time())

    def query(self, api_name, params=None, fields="", silent=False) -> pd.DataFrame:
        """
        直接通过 HTTPS POST 请求 Tushare Pro 接口
        """
        # 执行限流等待
        self._wait_for_rate_limit()
        
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params or {},
            "fields": fields
        }
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # 增加超时时间到 90 秒
                res = self.session.post(self.base_url, data=json.dumps(payload), timeout=90)
                if res.status_code == 200:
                    result = res.json()
                    if result['code'] == 0:
                        data = result['data']
                        df = pd.DataFrame(data['items'], columns=data['fields'])
                        return df.where(pd.notnull(df), None)
                    else:
                        msg = result.get('msg', '')
                        if "每分钟最多访问" in msg:
                            if not silent:
                                logger.warning(f"Tushare 频率限制: {msg}，等待 60 秒...")
                            # 既然已经报错了，说明本地记录不准，重置一下
                            self.call_history = [sys_time.time()] * self.max_calls
                            sys_time.sleep(60.1)
                            continue
                        if not silent:
                            logger.error(f"Tushare API 错误: {msg}")
                        return pd.DataFrame()
                else:
                    if not silent:
                        logger.error(f"Tushare HTTP 错误: {res.status_code}")
                    if 500 <= res.status_code < 600 or res.status_code == 429:
                        sys_time.sleep(3 * (attempt + 1))
                        continue
                    return pd.DataFrame()
                    
            except (requests.exceptions.RequestException, Exception) as e:
                try: self.session.close()
                except: pass
                self.session = requests.Session()
                self.session.trust_env = False
                self.session.proxies = {"http": None, "https": None}
                
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    if not silent:
                        logger.warning(f"Tushare 请求失败 ({type(e).__name__}: {e})，{wait_time}s 后重试 ({attempt+1}/{max_retries})...")
                    sys_time.sleep(wait_time)
                else:
                    if not silent:
                        logger.error(f"Tushare 请求异常（已重试 {max_retries} 次）: {e}")
                    return pd.DataFrame()
        return pd.DataFrame()

    async def async_get_realtime_quotes(self, ts_codes: List[str], local_only: bool = False) -> Dict[str, Dict]:
        """
        批量获取实时行情 (Snapshot) via Sina API (异步版)
        :param local_only: 仅使用本地缓存或数据库，不调用网络 API
        """
        if not ts_codes:
            return {}
        
        # 如果 local_only 为 True，返回空字典（由上层从本地获取数据）
        if local_only:
            return {}
            
        batch_size = 80
        batches = [ts_codes[i:i + batch_size] for i in range(0, len(ts_codes), batch_size)]
        
        async def fetch_batch_async(batch_codes):
            batch_results = {}
            sina_codes = []
            for code in batch_codes:
                if code.endswith('.SH'):
                    sina_codes.append('sh' + code.replace('.SH', ''))
                elif code.endswith('.SZ'):
                    sina_codes.append('sz' + code.replace('.SZ', ''))
                elif code.endswith('.BJ'):
                    sina_codes.append('bj' + code.replace('.BJ', ''))
                else:
                    sina_codes.append(code.replace('.', '').lower())
            
            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            headers = {
                "Referer": "https://finance.sina.com.cn/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            session = await self._get_async_session()
            for attempt in range(3):
                try:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            try:
                                text = content.decode('gbk')
                            except UnicodeDecodeError:
                                text = content.decode('utf-8', errors='ignore')
                            
                            lines = text.split(';')
                            for line in lines:
                                if '="' not in line: continue
                                
                                var_part, val_part = line.split('="')
                                sina_code = var_part.split('_str_')[-1]
                                val_str = val_part.replace('"', '')
                                
                                fields = val_str.split(',')
                                if len(fields) < 32: continue

                                ts_code = ""
                                if sina_code.startswith('sh'): ts_code = sina_code[2:] + '.SH'
                                elif sina_code.startswith('sz'): ts_code = sina_code[2:] + '.SZ'
                                elif sina_code.startswith('bj'): ts_code = sina_code[2:] + '.BJ'
                                
                                if not ts_code: continue

                                try:
                                    price = float(fields[3])
                                    pre_close = float(fields[2])
                                    if price <= 0 and pre_close > 0:
                                        price = pre_close
                                        
                                    real_date = fields[30]
                                    real_time = fields[31]
                                    
                                    results_item = {
                                        "name": fields[0],
                                        "symbol": ts_code,
                                        "price": price,
                                        "pre_close": pre_close,
                                        "open": float(fields[1]),
                                        "high": float(fields[4]),
                                        "low": float(fields[5]),
                                        "vol": float(fields[8]) / 100,
                                        "amount": float(fields[9]) / 100000000, 
                                        "time": f"{real_date} {real_time}",
                                        "bid_ask": {
                                           "b1_p": float(fields[11]), "b1_v": float(fields[10])/100,
                                           "s1_p": float(fields[21]), "s1_v": float(fields[20])/100,
                                        }
                                    }
                                    results_item['change'] = results_item['price'] - results_item['pre_close']
                                    results_item['pct_chg'] = (results_item['change'] / results_item['pre_close']) * 100 if results_item['pre_close'] > 0 else 0
                                    
                                    batch_results[ts_code] = results_item
                                except:
                                    continue
                            break
                        else:
                            if attempt < 2: await asyncio.sleep(0.5)
                except Exception:
                    if attempt < 2: await asyncio.sleep(0.5)
            return batch_results

        all_results = {}
        # 限制并发数，防止 Sina 封禁或网络拥塞
        semaphore = asyncio.Semaphore(5)
        
        async def sem_fetch_batch(batch):
            async with semaphore:
                return await fetch_batch_async(batch)

        tasks = [sem_fetch_batch(batch) for batch in batches]
        batch_outputs = await asyncio.gather(*tasks)
        for out in batch_outputs:
            all_results.update(out)
        return all_results

    def get_realtime_quotes(self, ts_codes: List[str], local_only: bool = False) -> Dict[str, Dict]:
        """
        批量获取实时行情 (Snapshot) via Sina API
        :param local_only: 仅使用本地缓存或数据库，不调用网络 API
        """
        if not ts_codes:
            return {}
        
        # 如果 local_only 为 True，返回空字典（由上层从本地获取数据）
        if local_only:
            return {}
            
        # 分批处理，每批 80 个 (Sina URL 长度限制)
        batch_size = 80
        batches = [ts_codes[i:i + batch_size] for i in range(0, len(ts_codes), batch_size)]
        results = {}

        def fetch_batch(batch_codes):
            batch_results = {}
            # 转换为 sina 格式: 000001.SZ -> sz000001
            sina_codes = []
            for code in batch_codes:
                if code.endswith('.SH'):
                    sina_codes.append('sh' + code.replace('.SH', ''))
                elif code.endswith('.SZ'):
                    sina_codes.append('sz' + code.replace('.SZ', ''))
                elif code.endswith('.BJ'):
                    sina_codes.append('bj' + code.replace('.BJ', ''))
                else:
                    # 默认以此逻辑尝试
                    sina_codes.append(code.replace('.', '').lower())
            
            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            headers = {
                "Referer": "https://finance.sina.com.cn/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            # 增加重试逻辑 (最多 3 次)
            for attempt in range(3):
                try:
                    resp = self.session.get(url, headers=headers, timeout=5)
                    if resp.status_code == 200:
                        # 显式指定 GBK 解码，新浪 API 常用编码
                        try:
                            text = resp.content.decode('gbk')
                        except UnicodeDecodeError:
                            text = resp.text
                        
                        lines = text.split(';')
                        for line in lines:
                            if '="' not in line: continue
                            
                            # 解析: var hq_str_sz000001="平安银行,..."
                            var_part, val_part = line.split('="')
                            sina_code = var_part.split('_str_')[-1]
                            val_str = val_part.replace('"', '')
                            
                            fields = val_str.split(',')
                            if len(fields) < 32: continue

                            # 还原 ts_code
                            ts_code = ""
                            if sina_code.startswith('sh'): ts_code = sina_code[2:] + '.SH'
                            elif sina_code.startswith('sz'): ts_code = sina_code[2:] + '.SZ'
                            elif sina_code.startswith('bj'): ts_code = sina_code[2:] + '.BJ'
                            
                            try:
                                price = float(fields[3])
                                pre_close = float(fields[2])
                                
                                if price <= 0 and pre_close > 0:
                                    price = pre_close
                                    
                                # 新浪接口 fields[30] 是日期 (yyyy-mm-dd), fields[31] 是时间 (hh:mm:ss)
                                real_date = fields[30]
                                real_time = fields[31]
                                
                                results_item = {
                                    "name": fields[0],
                                    "symbol": ts_code,
                                    "price": price,
                                    "pre_close": pre_close,
                                    "open": float(fields[1]),
                                    "high": float(fields[4]),
                                    "low": float(fields[5]),
                                    "vol": float(fields[8]) / 100, # 手
                                        "amount": float(fields[9]) / 100000000, 
                                        "time": f"{real_date} {real_time}",
                                    "bid_ask": {
                                        # 简化，只取买1卖1
                                       "b1_p": float(fields[11]), "b1_v": float(fields[10])/100,
                                       "s1_p": float(fields[21]), "s1_v": float(fields[20])/100,
                                    }
                                }
                                
                                # Calculate change
                                results_item['change'] = results_item['price'] - results_item['pre_close']
                                results_item['pct_chg'] = (results_item['change'] / results_item['pre_close']) * 100 if results_item['pre_close'] > 0 else 0
                                
                                batch_results[ts_code] = results_item
                            except:
                                continue
                        # 成功获取，退出重试
                        break
                    else:
                        logger.warning(f"Sina API returned status {resp.status_code} on attempt {attempt+1}")
                except Exception as e:
                    if attempt == 2: # 最后一次尝试
                        logger.error(f"Fetch Sina batch failed after 3 attempts: {e}")
                    else:
                        sys_time.sleep(0.5) # 等待后重试
            
            return batch_results

        # 限制并发数，Sina 接口容易封禁
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_batch, batch) for batch in batches]
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.update(future.result())
                except:
                    pass
        
        return results

    def get_stock_basic(self):
        """获取A股列表，包含指数"""
        return self.query('stock_basic', params={'exchange': '', 'list_status': 'L'}, 
                         fields='ts_code,symbol,name,area,industry,market,list_date')

    async def async_get_stock_basic(self):
        """获取A股列表 (异步)"""
        return await self.async_query('stock_basic', params={'exchange': '', 'list_status': 'L'}, 
                                     fields='ts_code,symbol,name,area,industry,market,list_date')

    def get_daily_basic(self, trade_date: Optional[str] = None, ts_code: Optional[str] = None) -> pd.DataFrame:
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if ts_code: params['ts_code'] = ts_code
        return self.query('daily_basic', params=params, 
                         fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps,dv_ratio,circ_mv,total_mv')

    async def async_get_daily_basic(self, trade_date: Optional[str] = None, ts_code: Optional[str] = None) -> pd.DataFrame:
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if ts_code: params['ts_code'] = ts_code
        return await self.async_query('daily_basic', params=params, 
                                     fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps,dv_ratio,circ_mv,total_mv')

    def get_moneyflow(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None, silent: bool = False):
        """
        获取个股资金流向 (支持时间范围查询)
        """
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if ts_code: params['ts_code'] = ts_code
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        
        return self.query('moneyflow', params=params,
                         fields='ts_code,trade_date,buy_sm_amount,sell_sm_amount,buy_md_amount,sell_md_amount,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount',
                         silent=silent)

    async def async_get_moneyflow(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None, silent: bool = False):
        """
        获取个股资金流向 (异步)
        """
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if ts_code: params['ts_code'] = ts_code
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        
        return await self.async_query('moneyflow', params=params,
                                     fields='ts_code,trade_date,buy_sm_amount,sell_sm_amount,buy_md_amount,sell_md_amount,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount',
                                     silent=silent)

    def get_fina_indicator(self, ts_code: str, period: str = None):
        """
        获取财务指标 (ROE, 净利率, 负债率等)
        """
        params = {'ts_code': ts_code}
        if period: params['period'] = period
        
        # 增加 yoy_revenue (tr_yoy) 和 debt_to_assets
        return self.query('fina_indicator', params=params,
                         fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,netprofit_yoy,tr_yoy,debt_to_assets')

    async def async_get_fina_indicator(self, ts_code: str, period: str = None):
        """
        获取财务指标 (异步)
        """
        params = {'ts_code': ts_code}
        if period: params['period'] = period
        
        return await self.async_query('fina_indicator', params=params,
                                     fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,netprofit_yoy,tr_yoy,debt_to_assets')

    def get_cashflow(self, ts_code: str, period: str = None):
        """
        获取现金流量表 (经营性现金流净额)
        """
        params = {'ts_code': ts_code}
        if period: params['period'] = period
        
        return self.query('cashflow', params=params,
                         fields='ts_code,end_date,n_cashflow_act')

    async def async_get_cashflow(self, ts_code: str, period: str = None):
        """
        获取现金流量表 (异步)
        """
        params = {'ts_code': ts_code}
        if period: params['period'] = period
        
        return await self.async_query('cashflow', params=params,
                                     fields='ts_code,end_date,n_cashflow_act')

    def stock_company(self, ts_code: str, fields: str = ""):
        """
        获取上市公司基本信息
        """
        return self.query('stock_company', params={'ts_code': ts_code}, fields=fields)

    async def async_stock_company(self, ts_code: str, fields: str = ""):
        """
        获取上市公司基本信息 (异步)
        """
        return await self.async_query('stock_company', params={'ts_code': ts_code}, fields=fields)

    def fina_mainbz(self, ts_code: str, type: str = "P"):
        """
        获取主营业务构成
        """
        return self.query('fina_mainbz', params={'ts_code': ts_code, 'type': type})

    async def async_fina_mainbz(self, ts_code: str, type: str = "P"):
        """
        获取主营业务构成 (异步)
        """
        return await self.async_query('fina_mainbz', params={'ts_code': ts_code, 'type': type})

    def forecast(self, ts_code: str, limit: int = 1):
        """
        获取业绩预告
        """
        return self.query('forecast', params={'ts_code': ts_code, 'limit': limit},
                         fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,summary')

    async def async_forecast(self, ts_code: str, limit: int = 1):
        """
        获取业绩预告 (异步)
        """
        return await self.async_query('forecast', params={'ts_code': ts_code, 'limit': limit},
                                     fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,summary')

    async def close(self):
        """关闭资源"""
        if self._async_session and not self._async_session.closed:
            await self._async_session.close()
            logger.info("Tushare ClientSession closed.")
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            logger.info("Tushare Worker task cancelled.")
        if self.session:
            self.session.close()

tushare_client = TushareClient()

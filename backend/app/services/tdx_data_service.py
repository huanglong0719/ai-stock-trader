import pandas as pd
import atexit
import redis
import json
import logging
import socket
from typing import Any, Iterable, Dict, List
from datetime import datetime, timedelta
import threading
import time
from queue import Queue, Empty
from pytdx.hq import TdxHq_API
from app.core.config import settings
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar
from sqlalchemy import desc

logger = logging.getLogger(__name__)

class TdxDataService:
    def __init__(self):
        self.api = TdxHq_API()
        self.redis_client = None
        self._redis_last_check_ts = 0.0
        self._redis_last_warn_ts = 0.0
        self._redis_check_interval_sec = 60.0
        self._redis_warn_interval_sec = 300.0
        self._mem_cache = {}
        self._mem_cache_expire_ts = {}
        self._mem_cache_lock = threading.Lock()
        self._api_lock = threading.RLock()
        self._last_connect_fail_ts = 0.0
        self._connect_fail_cooldown_sec = 2.0
        self._last_connect_fail_log_ts = 0.0
        self._connect_fail_log_interval_sec = 30.0
        self._socket_timeout_sec = 1.5
        self.best_ip = None
        self.valid_ips = []
        self._last_best_ip_scan_ts = 0.0
        self._best_ip_scan_interval_sec = 60.0
        self._ip_rr_index = 0  # Round Robin index
        self.ip_list = [
            {'ip': '218.6.170.47', 'port': 7709},
            {'ip': '180.153.18.170', 'port': 7709},
            {'ip': '115.238.90.165', 'port': 7709},
            {'ip': '115.238.56.198', 'port': 7709},
            {'ip': 'sztdx.gtjas.com', 'port': 7709},
            {'ip': '60.191.117.167', 'port': 7709},
            {'ip': '218.75.126.9', 'port': 7709},
            {'ip': 'jstdx.gtjas.com', 'port': 7709},
            {'ip': '60.12.136.250', 'port': 7709}
        ]
        
        # Connection Pool settings
        self._pool_max_size = 48
        self._pool: Queue[TdxHq_API] = Queue(maxsize=self._pool_max_size)
        self._pool_current_size = 0
        self._pool_lock = threading.Lock()
        self._pool_wait_timeout_sec = 15.0
        
        self._scan_lock = threading.Lock()
        
        self._xdxr_cache = {}
        self._xdxr_cache_lock = threading.Lock()
        
        atexit.register(self._disconnect_safe)

    def _get_redis_client(self):
        now_ts = datetime.now().timestamp()
        if self.redis_client is not None:
            return self.redis_client

        if now_ts - self._redis_last_check_ts < self._redis_check_interval_sec:
            return None

        self._redis_last_check_ts = now_ts
        try:
            client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=0.4,
                socket_timeout=0.8
            )
            client.ping()
            self.redis_client = client
            logger.info("Redis connected for minute cache.")
            return self.redis_client
        except Exception as e:
            if now_ts - self._redis_last_warn_ts >= self._redis_warn_interval_sec:
                self._redis_last_warn_ts = now_ts
                logger.info(f"Redis unavailable, using in-process cache (fallback): {e}")
            return None

    def _create_api_connection(self):
        """创建一个新的 API 连接 (不放入池，直接返回)"""
        api = TdxHq_API()
        
        # 1. Ensure we have valid IPs
        if not self.valid_ips:
            self._scan_best_ip(force=True)
            
        if not self.valid_ips:
            raise RuntimeError("No available TDX server")
            
        # 2. Pick next IP (Round Robin)
        with self._pool_lock:
            idx = self._ip_rr_index % len(self.valid_ips)
            self._ip_rr_index += 1
            target_ip = self.valid_ips[idx]
            
        # 3. Try to connect
        try:
            socket.setdefaulttimeout(self._socket_timeout_sec)
            if api.connect(target_ip['ip'], target_ip['port'], time_out=self._socket_timeout_sec):
                return api
        except Exception:
            pass
            
        # 4. If failed, force re-scan and try one more time
        logger.warning(f"Connection to {target_ip['ip']} failed, re-scanning...")
        self._scan_best_ip(force=True)
        
        if not self.valid_ips:
             # Cleanup just in case
            try:
                api.disconnect()
            except:
                pass
            raise RuntimeError("No available TDX server after re-scan")
        
        # Try again with best IP (most reliable)
        target_ip = self.valid_ips[0]
        try:
            if api.connect(target_ip['ip'], target_ip['port'], time_out=self._socket_timeout_sec):
                return api
        except Exception as e:
            try:
                api.disconnect()
            except:
                pass
            raise e
            
        raise RuntimeError(f"Failed to connect to {target_ip['ip']}")

    def _acquire_api(self):
        """从池中获取一个可用连接"""
        try:
            # 1. Try to get from pool
            api = self._pool.get_nowait()
            return api
        except Empty:
            # 2. If pool is empty, check if we can create more
            with self._pool_lock:
                if self._pool_current_size < self._pool_max_size:
                    self._pool_current_size += 1
                    create_new = True
                else:
                    create_new = False
            
            if create_new:
                try:
                    return self._create_api_connection()
                except Exception as e:
                    with self._pool_lock:
                        self._pool_current_size -= 1
                    logger.warning(f"Failed to create new TDX connection: {e}")
                    # Fallback to wait for existing
                    pass
            
            # 3. Wait for an existing one
            try:
                return self._pool.get(timeout=self._pool_wait_timeout_sec)
            except Empty:
                raise RuntimeError("Timeout waiting for TDX connection from pool")

    def _release_api(self, api):
        """归还连接到池中"""
        try:
            self._pool.put_nowait(api)
        except Exception:
            # Should not happen if logic is correct
            self._destroy_api(api)

    def _destroy_api(self, api):
        """销毁连接"""
        try:
            self._force_close_api(api)
        finally:
            with self._pool_lock:
                self._pool_current_size -= 1

    def _scan_best_ip(self, force: bool = False):
        with self._scan_lock:
            now_ts = time.time()
            # If recently scanned (within 10s), skip even if force=True
            if self.best_ip and now_ts - self._last_best_ip_scan_ts < 10.0:
                return self.best_ip
                
            if not force and self.best_ip and now_ts - self._last_best_ip_scan_ts < self._best_ip_scan_interval_sec:
                return self.best_ip
                
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(self._socket_timeout_sec)
            api = TdxHq_API()
            
            valid_results: list[tuple[float, dict[str, Any]]] = []
            
            try:
                # Try all IPs
                for ip_info in self.ip_list:
                    # Skip if we already have enough valid IPs (e.g. 5) and we are just refreshing?
                    # No, we want the best ones.
                    
                    start = time.perf_counter()
                    success = False
                    try:
                        # Use a shorter timeout for scanning to be fast
                        scan_timeout = 0.8
                        if api.connect(ip_info['ip'], ip_info['port'], time_out=scan_timeout):
                            # Verify with a small request
                            data = api.get_security_bars(0, 1, "600000", 0, 1)
                            if data:
                                cost = time.perf_counter() - start
                                valid_results.append((cost, ip_info))
                                success = True
                        
                        # Always close after check
                        self._force_close_api(api)
                    except Exception:
                        self._force_close_api(api)
            finally:
                self._force_close_api(api)
                socket.setdefaulttimeout(old_timeout)
                
            if valid_results:
                valid_results.sort(key=lambda x: x[0])
                top_n = valid_results[:5]
                self.valid_ips = [ip for _, ip in top_n]
                self.best_ip = self.valid_ips[0]
                
                self._last_best_ip_scan_ts = now_ts
                logger.info(f"TDX servers scanned. Found {len(valid_results)} valid. Top 3: {[ip['ip'] for _, ip in top_n[:3]]}")
            else:
                logger.warning("No valid TDX servers found during scan!")
                
            return self.best_ip

    def connect(self):
        """连接最快的 TDX 服务器"""
        now_ts = datetime.now().timestamp()
        if now_ts - self._last_connect_fail_ts < self._connect_fail_cooldown_sec:
            return False

        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self._socket_timeout_sec)
        try:

            with self._api_lock:
                try:
                    data = self.api.get_security_bars(0, 1, "600000", 0, 1)
                    if data:
                        return True
                    self._disconnect_safe()
                except Exception:
                    try:
                        self.api.disconnect()
                    except Exception:
                        pass

            if not self.best_ip or now_ts - self._last_best_ip_scan_ts >= self._best_ip_scan_interval_sec:
                self._scan_best_ip()

            if self.best_ip:
                with self._api_lock:
                    try:
                        if self.api.connect(self.best_ip['ip'], self.best_ip['port'], time_out=self._socket_timeout_sec):
                            data = self.api.get_security_bars(0, 1, "600000", 0, 1)
                            if data:
                                logger.info(f"Connected to TDX server: {self.best_ip['ip']}")
                                return True
                        else:
                            self._disconnect_safe()
                    except Exception:
                        self._disconnect_safe()

            self._scan_best_ip(force=True)
            if self.best_ip:
                with self._api_lock:
                    try:
                        if self.api.connect(self.best_ip['ip'], self.best_ip['port'], time_out=self._socket_timeout_sec):
                            data = self.api.get_security_bars(0, 1, "600000", 0, 1)
                            if data:
                                logger.info(f"Connected to TDX server: {self.best_ip['ip']}")
                                return True
                        else:
                            self._disconnect_safe()
                    except Exception:
                        self._disconnect_safe()

            # Limit attempts to avoid timeout
            max_try = 12
            tried_count = 0
            for ip_info in self.ip_list:
                if tried_count >= max_try:
                    break
                tried_count += 1
                
                with self._api_lock:
                    try:
                        if self.api.connect(ip_info['ip'], ip_info['port'], time_out=self._socket_timeout_sec):
                            data = self.api.get_security_bars(0, 1, "600000", 0, 1)
                            if data:
                                self.best_ip = ip_info
                                logger.info(f"Connected to TDX server: {ip_info['ip']}")
                                return True
                        else:
                            self._disconnect_safe()
                    except Exception:
                        self._disconnect_safe()
                        continue

            self._last_connect_fail_ts = now_ts
            if now_ts - self._last_connect_fail_log_ts >= self._connect_fail_log_interval_sec:
                self._last_connect_fail_log_ts = now_ts
                logger.info("Failed to connect to any TDX server")
            self._disconnect_safe()
            return False
        finally:
            socket.setdefaulttimeout(old_timeout)

    def _disconnect_safe(self):
        try:
            with self._api_lock:
                self._force_close_api(self.api)
        except Exception:
            pass

    def _force_close_api(self, api):
        try:
            api.disconnect()
        except Exception:
            pass
        try:
            client = getattr(api, "client", None)
            if client:
                try:
                    client.close()
                finally:
                    api.client = None
        except Exception:
            pass

    def _get_market_code(self, symbol: str):
        """
        转换股票代码为 TDX 市场代码
        0: 深圳 (sz), 1: 上海 (sh), 2: 北京 (bj)
        """
        s = str(symbol or "").upper()
        if s.startswith('880'):
            return 1, s[:6]
            
        if s.endswith('.SH') or s.startswith('6'):
            return 1, s[:6]
        if s == '000001.SH':
            return 1, '000001'
        
        if s.endswith('.BJ') or s.startswith(('8', '4', '92', '43', '83', '87')):
            return 2, s[:6]
        
        if s.endswith('.SZ'):
            return 0, s[:6]
            
        return 0, s[:6]

    def _get_tdx_category(self, freq: str):
        """
        频率映射
        0: 5min, 1: 15min, 2: 30min, 3: 60min, 4: Daily, 5: Weekly, 6: Monthly
        """
        if freq == '5min':
            return 0
        elif freq == '15min':
            return 1
        elif freq == '30min':
            return 2
        elif freq == '60min':
            return 3
        elif freq == 'D':
            return 4
        elif freq == 'W':
            return 5
        elif freq == 'M':
            return 6
        return 4 # 默认日线

    def _is_index_code(self, market: int, code: str) -> bool:
        c = str(code or "")
        if market == 0 and c.startswith("399"):
            return True
        if market == 1 and (c.startswith("000") or c.startswith("880")):
            return True
        return False

    def get_xdxr_info(self, symbol: str, api=None) -> pd.DataFrame:
        """
        获取除权除息信息 (带缓存)
        """
        market, code = self._get_market_code(symbol)
        cache_key = f"{market}_{code}"
        
        with self._xdxr_cache_lock:
            if cache_key in self._xdxr_cache:
                return self._xdxr_cache[cache_key]

        need_release = False
        if api is None:
            # 如果没有传入 api，需要自己获取
            try:
                api = self._acquire_api()
                need_release = True
            except Exception as e:
                logger.warning(f"Failed to acquire API for XDXR info: {e}")
                return pd.DataFrame()
            
        try:
            # 使用 api 获取 xdxr
            # 注意：get_xdxr_info 可能会因为网络问题失败
            data = None
            try:
                data = api.get_xdxr_info(market, code)
            except Exception:
                pass
                
            if not data:
                # 即使没获取到，也缓存一个空的 DataFrame 防止重复查询？
                # 不，网络失败不应该缓存空值，应该重试。
                # 但如果是该股票确实没有 XDXR，应该缓存空值。
                # 很难区分是网络失败还是没有数据。
                # pytdx 如果返回 None 或 [] 表示没有数据。如果是异常表示失败。
                return pd.DataFrame()
                
            df = api.to_df(data)
            with self._xdxr_cache_lock:
                self._xdxr_cache[cache_key] = df
            return df
        except Exception as e:
            logger.warning(f"Failed to get XDXR info for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            if need_release and api:
                self._release_api(api)

    def _calc_adjust_factor(self, df_bars: pd.DataFrame, xdxr_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算并应用复权因子 (前复权)
        """
        if df_bars.empty or xdxr_df.empty:
            return df_bars
            
        # Ensure df_bars is sorted by time
        df_bars = df_bars.sort_values('trade_time').reset_index(drop=True)
        
        # Prepare XDXR
        # Filter category = 1 (除权除息)
        if 'category' not in xdxr_df.columns:
            return df_bars
            
        xdxr = xdxr_df[xdxr_df['category'] == 1].copy()
        if xdxr.empty:
            return df_bars
            
        # Parse XDXR dates
        try:
            xdxr['date'] = pd.to_datetime(xdxr[['year', 'month', 'day']])
        except Exception:
            return df_bars
        
        # Loop through XDXR records
        # 必须按时间倒序处理吗？其实只要遍历每个除权日，把该日之前的所有数据都乘上因子即可。
        # 乘法是可交换的，顺序不重要。
        for _, row in xdxr.iterrows():
            ex_date = row['date']
            
            # Find the last bar BEFORE ex_date
            # mask: trade_time < ex_date
            mask_before = df_bars['trade_time'] < ex_date
            if not mask_before.any():
                continue
                
            # The last bar before ex_date is the one used for calculation
            last_idx = df_bars[mask_before].index[-1]
            pre_close = df_bars.loc[last_idx, 'close']
            
            # Extract factors
            fenhong = (row['fenhong'] if row['fenhong'] is not None else 0) / 10.0
            songzhu = (row['songzhuangu'] if row['songzhuangu'] is not None else 0) / 10.0
            peigu = (row['peigu'] if row['peigu'] is not None else 0) / 10.0
            peigujia = (row['peigujia'] if row['peigujia'] is not None else 0)
            
            # Calculate adjusted close for T-1
            # Formula: (P - fenhong + peigu * peigujia) / (1 + songzhu + peigu)
            numerator = pre_close - fenhong + peigu * peigujia
            denominator = 1 + songzhu + peigu
            
            if denominator == 0:
                continue
                
            adj_close = numerator / denominator
            if pre_close == 0:
                continue
                
            k = adj_close / pre_close
            
            # Apply factor k to all bars BEFORE ex_date
            # Note: We update 'adj_factor'
            # Be careful with multiple adjustments: they multiply
            df_bars.loc[mask_before, 'adj_factor'] *= k
            
        return df_bars

    def fetch_bars(self, symbol: str, freq: str, count: int = 800, start: int = 0):
        """
        获取 K 线数据 (支持 D/W/M 及分钟线)
        使用连接池支持高并发
        """
        market, code = self._get_market_code(symbol)
        category = self._get_tdx_category(freq)
        
        # 确保 best_ip 已初始化
        if not self.best_ip:
            self.connect()

        for attempt in range(3):
            api = None
            released = False
            try:
                api = self._acquire_api()
                
                # Check if api is still connected (simple check)
                # Note: pytdx doesn't have a cheap is_connected check other than trying
                # We assume it's good, if not exception will be caught
                
                old_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(self._socket_timeout_sec)
                try:
                    total_count = int(count or 0)
                    if total_count <= 0:
                        total_count = 800
                    page_size = 800
                    start_offset = int(start or 0)
                    pages: List[pd.DataFrame] = []
                    xdxr_df = pd.DataFrame()
                    is_index = self._is_index_code(market, code)
                    for page_start in range(start_offset, start_offset + total_count, page_size):
                        page_count = min(page_size, start_offset + total_count - page_start)
                        if is_index:
                            page_data = api.get_index_bars(category, market, code, page_start, page_count)
                        else:
                            page_data = api.get_security_bars(category, market, code, page_start, page_count)
                            if xdxr_df.empty:
                                xdxr_df = self.get_xdxr_info(symbol, api=api)
                        if not page_data:
                            if not pages:
                                logger.warning(f"TDX fetch_bars returned empty for {symbol} freq={freq} cat={category} m={market} c={code}")
                                return pd.DataFrame()
                            break
                        df_page = api.to_df(page_data)
                        if df_page is None or df_page.empty:
                            if not pages:
                                logger.warning(f"TDX to_df returned empty for {symbol}")
                                return pd.DataFrame()
                            break
                        pages.append(df_page)
                    if not pages:
                        logger.warning(f"TDX fetch_bars returned empty for {symbol} freq={freq} cat={category} m={market} c={code}")
                        return pd.DataFrame()
                    df = pd.concat(pages, ignore_index=True)
                finally:
                    socket.setdefaulttimeout(old_timeout)
                
                self._release_api(api)
                released = True
                
                if df is None or df.empty:
                    logger.warning(f"TDX to_df returned empty for {symbol}")
                    return pd.DataFrame()

                # 统一字段名
                rename_map = {
                    'datetime': 'trade_time',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'vol',
                    'amount': 'amount'
                }
                # 有些频率返回的是 'year', 'month', 'day' 等字段，pytdx to_df 会自动合并到 datetime
                df = df.rename(columns=rename_map)
                
                # 统一成交额单位为亿元 (TDX 返回的是元)
                if 'amount' in df.columns:
                    df['amount'] = df['amount'] / 100000000.0
                
                df['ts_code'] = symbol
                # 对于日线及以上，trade_time 通常是 YYYY-MM-DD 格式
                # 对于分钟线，是 YYYY-MM-DD HH:MM 格式
                df['trade_time'] = pd.to_datetime(df['trade_time'], errors='coerce')
                df = df.dropna(subset=['trade_time'])
                
                # 补全 adj_factor 字段 (默认为 1.0)
                if 'adj_factor' not in df.columns:
                    df['adj_factor'] = 1.0
                
                # 计算并应用前复权
                if not xdxr_df.empty:
                    try:
                        df = self._calc_adjust_factor(df, xdxr_df)
                        # Don't apply factor to price here. MarketDataService handles QFQ.
                        # We only need the calculated adj_factor column.
                        # for col in ['open', 'high', 'low', 'close']:
                        #     if col in df.columns:
                        #         df[col] = df[col] * df['adj_factor']
                    except Exception as e:
                        logger.warning(f"Failed to apply adjust factor for {symbol}: {e}")

                # 标准化分钟线时间 (Start Time -> End Time)
                if freq in ['5min', '30min']:
                    df = self._standardize_minute_time(df, freq)
                    
                return df
            except Exception as e:
                if api and not released:
                    self._destroy_api(api)
                
                if attempt == 2:
                    logger.info(f"Fetch bars failed for {symbol} ({freq}): {e}")
                else:
                    import time as _time
                    _time.sleep(0.4 * (2 ** attempt))
                    # Trigger IP scan if repeated failures
                    if attempt == 1:
                        self._scan_best_ip(force=True)
            
        return pd.DataFrame()

    def _standardize_minute_time(self, df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """
        标准化分钟线时间 (解决 TDX 返回 Start Time 而非 End Time 的问题)
        """
        if df.empty or freq not in ['5min', '30min']:
            return df

        # Copy to avoid SettingWithCopy warning if it's a slice
        df = df.copy()
        
        # Debug Log
         # logger.info(f"Standardize check: rows={len(df)} freq={freq} head={df['trade_time'].head(1)}")

         # Extract times for check
        times = set(df['trade_time'].dt.strftime('%H:%M').unique())
        
        has_0930 = '09:30' in times
        has_1300 = '13:00' in times
        has_1130 = '11:30' in times
        has_1500 = '15:00' in times
        
        # Logic: Shift if Start exists but End does not
        # This implies the timestamps are "Start Time"
        need_shift = (has_0930 or has_1300) and (not has_1130) and (not has_1500)
        
        if need_shift:
            offset_minutes = 5 if freq == '5min' else 30
            
            # Apply shift
            df['trade_time'] = df['trade_time'] + timedelta(minutes=offset_minutes)
            
            # Revert future data (for intraday safety)
            # Allow 1 min buffer for clock skew
            now = datetime.now()
            cutoff = now + timedelta(minutes=1)
            
            mask = df['trade_time'] > cutoff
            if mask.any():
                # Revert logic is tricky. If we shift 15:00 -> 15:30 (future), we should NOT revert to 15:00.
                # We should drop it? No, 15:00 start time means 15:30 end time.
                # But today is 2026-03-09 12:15.
                # If we have 11:30 bar (Start Time) -> Shift to 12:00. It is valid (past).
                # If we have 13:00 bar (Start Time) -> Shift to 13:30. It is FUTURE.
                # We should DROP future bars, or revert them?
                # If we revert, we say "This bar ends at 13:00". But it actually started at 13:00.
                # It's better to keep it as is (Start Time) or drop it?
                # Actually, for intraday, if we are at 13:10, and we get 13:00 bar (Start), it means 13:00-13:30.
                # This bar is INCOMPLETE.
                # We should probably keep it as is, or mark it.
                # But my code does:
                # df.loc[mask, 'trade_time'] -= timedelta(minutes=offset_minutes)
                # It reverts the shift. So it stays as "Start Time".
                df.loc[mask, 'trade_time'] -= timedelta(minutes=offset_minutes)

        minutes_total = df['trade_time'].dt.hour * 60 + df['trade_time'].dt.minute
        in_morning = (minutes_total >= 570) & (minutes_total <= 690)
        in_afternoon = (minutes_total >= 780) & (minutes_total <= 900)
        df = df[in_morning | in_afternoon]
        return df

    def fetch_minute_bars(self, symbol: str, freq: str, count: int = 800, start: int = 0):
        """
        获取分钟 K 线 (仅支持 5min, 30min)
        已迁移到 fetch_bars
        """
        return self.fetch_bars(symbol, freq, count, start)

    def fetch_realtime_quotes(self, symbols: list):
        """获取实时盘口 (用于合成/补全)"""
        if not self.connect():
            # If connect fails, it logs internally and returns False.
            # We should return empty list to trigger fallback immediately.
            return []
            
        # TDX 一次最多 80 个
        results = []
        batch_size = 80
        try:
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i+batch_size]
                req_list = []
                for s in batch:
                    m, c = self._get_market_code(s)
                    req_list.append((m, c))

                data = None
                for attempt in range(2):
                    try:
                        if not self.connect():
                            raise RuntimeError("TDX not connected")
                        with self._api_lock:
                            old_timeout = socket.getdefaulttimeout()
                            socket.setdefaulttimeout(self._socket_timeout_sec)
                            try:
                                data = self.api.get_security_quotes(req_list)
                            finally:
                                socket.setdefaulttimeout(old_timeout)
                        if data:
                            break
                    except Exception as e:
                        self._disconnect_safe()
                        if attempt == 0:
                            self._scan_best_ip(force=True)
                        logger.warning(f"Fetch quotes batch failed: {e}")

                if data:
                    results.extend(data)
        finally:
            self._disconnect_safe()
        
        return results

    def fetch_ticks(self, symbol: str, count: int = 2000):
        """
        获取分笔成交数据 (Tick) 并通过 Redis 缓存实现增量抓取
        :param symbol: 股票代码
        :param count: 获取条数
        """
        import json
        from app.core.redis import redis_client
        
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        cache_key = f"ticks:{symbol}:{today_str}"
        
        # 1. 尝试从 Redis 获取已缓存的数据
        cached_data = redis_client.get(cache_key) if redis_client else None
        all_ticks = []
        if cached_data:
            import asyncio
            if asyncio.iscoroutine(cached_data):
                cached_data = None
            elif isinstance(cached_data, (bytes, bytearray)):
                cached_data = cached_data.decode("utf-8", errors="ignore")
            if isinstance(cached_data, str):
                all_ticks = json.loads(cached_data)
        
        # 2. 检查并清理旧缓存 (确保不会命中往日的缓存)
        if redis_client:
            try:
                # 查找该股票的所有分笔缓存键 (ticks:SYMBOL:*)
                all_keys_raw = redis_client.keys(f"ticks:{symbol}:*")
                import asyncio
                all_keys: list[Any] = []
                if isinstance(all_keys_raw, list):
                    all_keys = all_keys_raw
                elif asyncio.iscoroutine(all_keys_raw):
                    all_keys = []
                elif isinstance(all_keys_raw, Iterable):
                    all_keys = list(all_keys_raw)
                if all_keys:
                    for key in all_keys:
                        if key != cache_key:
                            redis_client.delete(key)
                            logger.info(f"Deleted old tick cache: {key}")
            except Exception as e:
                logger.info(f"Error cleaning old tick cache for {symbol}: {e}")
        
        market, code = self._get_market_code(symbol)
        new_ticks = []
        
        for attempt in range(3):
            try:
                if not self.connect():
                    raise RuntimeError("TDX not connected")
                
                with self._api_lock:
                    # 获取当日最新分笔
                    old_timeout = socket.getdefaulttimeout()
                    socket.setdefaulttimeout(self._socket_timeout_sec)
                    try:
                        data = self.api.get_transaction_data(market, code, 0, count)
                    finally:
                        socket.setdefaulttimeout(old_timeout)
                
                if data:
                    new_ticks = data
                    break
            except Exception as e:
                self._disconnect_safe()
                if attempt == 2:
                    logger.info(f"Fetch ticks failed for {symbol}: {e}")
                else:
                    import time as _time
                    _time.sleep(0.2 * (attempt + 1))
            finally:
                self._disconnect_safe()
        
        if not new_ticks and not all_ticks:
            return pd.DataFrame()
        
        # 3. 合并新旧数据并去重
        if new_ticks:
            combined = all_ticks + new_ticks
            df_combined = pd.DataFrame(combined)
            if 'time' in df_combined.columns:
                # 去重：利用 time + price + vol + num (分笔唯一标识)
                df_combined = df_combined.drop_duplicates(subset=['time', 'price', 'vol', 'num'], keep='first')
                # 按时间排序
                df_combined = df_combined.sort_values(by='time', ascending=True)
                
                # 更新缓存
                updated_list = df_combined.to_dict('records')
                
                # 设置过期时间：次日开盘前 (早晨 09:00:00)
                # 这样可以确保新的一天开始前，旧缓存被清理，迎接新缓存
                tomorrow = now + timedelta(days=1)
                expiry_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 9, 0, 0)
                ttl = int((expiry_time - now).total_seconds())
                if ttl < 0: # 如果现在已经超过 9 点了，则设为明天的 9 点
                    expiry_time += timedelta(days=1)
                    ttl = int((expiry_time - now).total_seconds())
                
                if redis_client:
                    redis_client.set(cache_key, json.dumps(updated_list), ex=ttl)
                
                df_combined['trade_time'] = pd.to_datetime(today_str + ' ' + df_combined['time'])
                df_combined['ts_code'] = symbol
                return df_combined
        
        if all_ticks:
            df_cached = pd.DataFrame(all_ticks)
            df_cached['trade_time'] = pd.to_datetime(today_str + ' ' + df_cached['time'])
            df_cached['ts_code'] = symbol
            return df_cached
            
        return pd.DataFrame()

    def compute_intraday_bias_at_extremes(self, symbol: str, ma_window: int = 5) -> Dict[str, Any]:
        """
        计算当日最高价/最低价形成时刻的乖离率 (基于分笔成交)
        bias_high = (high_price - ma_at_high) / ma_at_high * 100
        bias_low = (low_price - ma_at_low) / ma_at_low * 100
        """
        try:
            df_ticks = self.fetch_ticks(symbol)
        except Exception as e:
            logger.info(f"Fetch ticks failed for bias calc {symbol}: {e}")
            return {}
        if df_ticks is None or df_ticks.empty:
            return {}
        if 'trade_time' not in df_ticks.columns or 'price' not in df_ticks.columns:
            return {}

        df = df_ticks.copy()
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df[df['price'] > 0]
        if df.empty:
            return {}
        df = df.sort_values('trade_time', ascending=True)
        df['minute'] = df['trade_time'].dt.floor('min')

        minute_df = (
            df.groupby('minute', as_index=False)
            .agg(high=('price', 'max'), low=('price', 'min'), close=('price', 'last'))
        )
        if len(minute_df) < ma_window:
            return {}

        minute_df['ma_close'] = minute_df['close'].rolling(window=ma_window).mean()

        high_idx = df['price'].idxmax()
        high_price = float(df.loc[high_idx, 'price'])
        high_time = df.loc[high_idx, 'trade_time']
        high_minute = high_time.floor('min')
        ma_at_high = minute_df.loc[minute_df['minute'] == high_minute, 'ma_close']
        ma_at_high_val = float(ma_at_high.iloc[0]) if not ma_at_high.empty else None

        low_idx = df['price'].idxmin()
        low_price = float(df.loc[low_idx, 'price'])
        low_time = df.loc[low_idx, 'trade_time']
        low_minute = low_time.floor('min')
        ma_at_low = minute_df.loc[minute_df['minute'] == low_minute, 'ma_close']
        ma_at_low_val = float(ma_at_low.iloc[0]) if not ma_at_low.empty else None

        bias_high = None
        if ma_at_high_val and ma_at_high_val > 0:
            bias_high = (high_price - ma_at_high_val) / ma_at_high_val * 100
        bias_low = None
        if ma_at_low_val and ma_at_low_val > 0:
            bias_low = (low_price - ma_at_low_val) / ma_at_low_val * 100

        return {
            "ts_code": symbol,
            "high_price": high_price,
            "high_time": high_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(high_time, 'strftime') else str(high_time),
            "ma_at_high": ma_at_high_val,
            "bias_high": bias_high,
            "low_price": low_price,
            "low_time": low_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(low_time, 'strftime') else str(low_time),
            "ma_at_low": ma_at_low_val,
            "bias_low": bias_low,
        }

    def calculate_qfq(self, df: pd.DataFrame, symbol: str):
        """
        计算前复权 (QFQ)
        需要从数据库读取该股票最新的 adj_factor
        """
        if df.empty:
            return df

        db = SessionLocal()
        try:
            # 获取最近的复权因子记录
            # 注意：分钟线是当天的，所以应该取最近的一个交易日的因子
            # 如果当天有因子记录（比如收盘后），取当天的；如果是盘中，取昨天的
            # 这里简化逻辑：取数据库中该股票最新的 adj_factor
            last_record = db.query(DailyBar).filter(
                DailyBar.ts_code == symbol
            ).order_by(desc(DailyBar.trade_date)).first()
            
            if not last_record or not last_record.adj_factor:
                # 如果没有因子，假设为 1 (不复权)
                logger.warning(f"No adj_factor found for {symbol}, using raw data")
                return df
                
            # 最新因子 (假设当前已经是最新)
            # 严谨的 QFQ: 历史价格 * (历史因子 / 最新因子)
            # 但 TDX 返回的是 Raw Data。
            # 如果我们认为 fetch 的是“当前”数据，那么它的因子就是“最新因子”。
            # 等等，TDX get_security_bars 返回的是**不复权**的历史数据。
            # 我们需要对应每一天的因子。
            
            # 对于最近 3 个月的分钟线，跨度较大，不能只用一个因子。
            # 必须获取这段时间内的所有日线因子，并 merge 到分钟线上。
            
            # 1. 获取分钟线的时间范围
            min_day = df['trade_time'].dt.date.min()
            max_day = df['trade_time'].dt.date.max()
            
            # 2. 查询范围内的日线因子
            factors = db.query(DailyBar.trade_date, DailyBar.adj_factor).filter(
                DailyBar.ts_code == symbol,
                DailyBar.trade_date >= min_day,
                DailyBar.trade_date <= max_day
            ).all()
            
            if not factors:
                 return df
            
            # 5. 获取最新因子 (用于 QFQ 基准)
            latest_factor = last_record.adj_factor
            factor_map = {d: (float(f) if f is not None else None) for d, f in factors}
            date_list = df['trade_time'].dt.date.tolist()
            adj_list = []
            for d in date_list:
                f = factor_map.get(d)
                adj_list.append(f if f else float(latest_factor))
            df['adj_factor'] = adj_list
            
            # 6. 计算 QFQ
            # qfq = raw * (adj / latest)
            scale = df['adj_factor'] / latest_factor
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col] * scale
            
            return df
        except Exception as e:
            logger.error(f"QFQ calc failed for {symbol}: {e}")
            return df
        finally:
            db.close()

    def save_to_redis(self, df: pd.DataFrame, symbol: str, freq: str):
        """
        保存分钟数据到 Redis
        Key: MARKET:MIN:{freq}:{symbol}
        Value: List of JSON strings
        """
        if df.empty or freq not in {"5min", "30min"}:
            return
            
        key = f"MARKET:MIN:{freq}:{symbol}"
        expire_ts = datetime.now().timestamp() + 86400
        df_copy = df.copy()
        df_copy['trade_time'] = df_copy['trade_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        records = df_copy.to_dict('records')

        client = self._get_redis_client()
        if client is None:
            with self._mem_cache_lock:
                self._mem_cache[key] = records
                self._mem_cache_expire_ts[key] = expire_ts
            return

        try:
            pipeline = client.pipeline()
            
            # 全量替换：先删后加 (简单粗暴但有效，适合即时查询)
            # 或者使用 RPush
            pipeline.delete(key)
            for r in records:
                pipeline.rpush(key, json.dumps(r))
            
            # 设置过期时间 (例如 24 小时，保证第二天盘前清理或更新)
            pipeline.expire(key, 86400)
            pipeline.execute()
        except Exception as e:
            now_ts = datetime.now().timestamp()
            if now_ts - self._redis_last_warn_ts >= self._redis_warn_interval_sec:
                self._redis_last_warn_ts = now_ts
                logger.warning(f"Redis save failed, using in-process cache: {e}")
            with self._mem_cache_lock:
                self._mem_cache[key] = records
                self._mem_cache_expire_ts[key] = expire_ts

    def get_from_redis(self, symbol: str, freq: str) -> pd.DataFrame:
        """从 Redis 读取分钟数据"""
        key = f"MARKET:MIN:{freq}:{symbol}"
        now_ts = datetime.now().timestamp()

        client = self._get_redis_client()
        if client is None:
            with self._mem_cache_lock:
                exp = self._mem_cache_expire_ts.get(key)
                if exp and exp < now_ts:
                    self._mem_cache.pop(key, None)
                    self._mem_cache_expire_ts.pop(key, None)
                    return pd.DataFrame()
                data = self._mem_cache.get(key)
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
            if not df.empty:
                df['trade_time'] = pd.to_datetime(df['trade_time'])
            return df

        try:
            raw_list = client.lrange(key, 0, -1)
            if not raw_list:
                return pd.DataFrame()
            data = [json.loads(x) for x in raw_list]
            df = pd.DataFrame(data)
            if not df.empty:
                df['trade_time'] = pd.to_datetime(df['trade_time'])
            return df
        except Exception as e:
            if now_ts - self._redis_last_warn_ts >= self._redis_warn_interval_sec:
                self._redis_last_warn_ts = now_ts
                logger.warning(f"Redis get failed, using in-process cache: {e}")
            return pd.DataFrame()

tdx_service = TdxDataService()

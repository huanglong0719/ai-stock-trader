import pandas as pd
import numpy as np
import ta
import os
import pickle
import threading
from typing import List, Dict, Optional, Any
from app.services.logger import logger

class TechnicalIndicators:
    def __init__(self):
        # 内存缓存
        self._cache = {}
        self._lock = threading.Lock()
        self.cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "data", "cache")
        self.cache_file = os.path.join(self.cache_dir, "indicators_cache.pkl")
        self._load_cache()

    def _load_cache(self):
        """从磁盘加载缓存"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'rb') as f:
                    with self._lock:
                        self._cache = pickle.load(f)
                # logger.info(f"Loaded {len(self._cache)} indicators from persistent cache.")
        except Exception as e:
            logger.error(f"Error loading indicators cache: {e}")
            with self._lock:
                self._cache = {}

    def _save_cache(self):
        """保存缓存到磁盘"""
        try:
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)
            
            # 使用副本进行保存，避免在 pickle 过程中字典被修改
            with self._lock:
                # 限制保存的数量，避免文件过大
                if len(self._cache) > 5000:
                    keys = list(self._cache.keys())
                    # 优先保留 _FULL 结尾的核心缓存
                    full_keys = [k for k in keys if k.endswith('_FULL')]
                    other_keys = [k for k in keys if not k.endswith('_FULL')]
                    
                    if len(other_keys) > 1000:
                        for k in other_keys[:1000]:
                            self._cache.pop(k, None)
                    elif len(keys) > 5000: # 如果全是 FULL 键且超限，则清理最早的
                        for k in keys[:500]:
                            self._cache.pop(k, None)
                cache_copy = self._cache.copy()
            
            with open(self.cache_file, 'wb') as f:
                pickle.dump(cache_copy, f)
        except Exception as e:
            logger.error(f"Error saving indicators cache: {e}")

    def get_last_date(self, symbol: str, freq: str) -> Optional[str]:
        """获取指定标的和频率的缓存最后日期"""
        full_cache_key = f"{symbol}_{freq}_FULL"
        with self._lock:
            if full_cache_key in self._cache:
                df = self._cache[full_cache_key]
                if not df.empty and 'time' in df.columns:
                    # 返回 yyyymmdd 格式
                    last_time = df['time'].iloc[-1]
                    if isinstance(last_time, str):
                        return last_time.replace("-", "").replace("/", "")
                    elif hasattr(last_time, 'strftime'):
                        return last_time.strftime('%Y%m%d')
        return None

    def calculate(self, kline_data: List[Dict[str, Any]], cache_key: Optional[str] = None) -> pd.DataFrame:
        if not kline_data:
            return pd.DataFrame()

        # 1. 自动识别 symbol 和 freq
        symbol, freq = None, None
        if cache_key:
            parts = cache_key.rsplit("_", 3)
            if len(parts) >= 2:
                symbol, freq = parts[0], parts[1]
        
        if not symbol and len(kline_data) > 0:
            symbol = kline_data[0].get('ts_code') or kline_data[0].get('symbol')

        # 2. 准备数据进行对比 (在锁外完成)
        request_times = [d['time'] for d in kline_data]
        full_cache_key = f"{symbol}_{freq}_FULL" if symbol and freq else None
        
        # 3. 尝试从该股票的最全缓存中截取或追加
        with self._lock:
            cached_df = self._cache.get(full_cache_key)
            
        if cached_df is not None:
            full_times_set = set(cached_df['time'].values)
            
            # 情况 A: 请求的数据全部在缓存中 -> 直接截取
            if all(t in full_times_set for t in request_times):
                res_df = cached_df[cached_df['time'].isin(request_times)].copy()
                if cache_key:
                    with self._lock:
                        self._cache[cache_key] = res_df
                return res_df
            
            # 情况 B: 请求的数据是缓存的后续 (增量) -> 合并并重新计算
            last_cached_time = cached_df['time'].iloc[-1]
            new_data = [d for d in kline_data if d['time'] > last_cached_time]
            if new_data and len(new_data) < 200: # 增加增量阈值到 200 条
                new_df = pd.DataFrame(new_data)
                # 过滤掉全为空的列以避免 FutureWarning
                valid_dfs = [df for df in [cached_df, new_df] if not df.empty]
                if valid_dfs:
                    combined_df = pd.concat(valid_dfs, ignore_index=True).drop_duplicates('time')
                else:
                    combined_df = pd.DataFrame()
                
                if not combined_df.empty:
                    combined_df = combined_df.sort_values('time')
                    df = self._calculate_indicators(combined_df)
                
                with self._lock:
                    self._cache[full_cache_key] = df
                    if cache_key:
                        self._cache[cache_key] = df[df['time'].isin(request_times)].copy()
                
                return df[df['time'].isin(request_times)].copy()

        # 4. 兜底方案：全量计算
        df = pd.DataFrame(kline_data)
        df = df.sort_values('time')
        df = self._calculate_indicators(df)
        
        # 5. 存入缓存
        if symbol and freq:
            with self._lock:
                if len(df) >= 30:
                    self._cache[full_cache_key] = df
                if cache_key:
                    # 这里的缓存淘汰逻辑保持不变
                    if len(self._cache) > 5000:
                        keys = list(self._cache.keys())
                        for k in keys[:500]:
                            self._cache.pop(k, None)
                    self._cache[cache_key] = df
                    
        return df

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """核心指标计算逻辑 (原 calculate 的核心部分)"""
        if df.empty: return df
        
        # 兼容性处理: 如果有 volume 而没有 vol，则复制一份
        if 'volume' in df.columns and 'vol' not in df.columns:
            df['vol'] = df['volume']
        elif 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']
            
        # 确保数据类型
        for col in ['open', 'high', 'low', 'close', 'vol', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 1. 基础均线
        df['ma5'] = ta.trend.sma_indicator(df['close'], window=5)
        df['ma10'] = ta.trend.sma_indicator(df['close'], window=10)
        df['ma20'] = ta.trend.sma_indicator(df['close'], window=20)
        df['ma60'] = ta.trend.sma_indicator(df['close'], window=60)
        # 使用 vol 列计算成交量均线
        if 'vol' in df.columns:
            df['vol_ma5'] = ta.trend.sma_indicator(df['vol'], window=5)
            df['vol_ma10'] = ta.trend.sma_indicator(df['vol'], window=10)
            df['vol_ma20'] = ta.trend.sma_indicator(df['vol'], window=20)
        else:
            df['vol_ma5'] = 0.0
            df['vol_ma10'] = 0.0
            df['vol_ma20'] = 0.0
        
        # 2. 趋势斜率 (以MA20为例)
        if len(df) >= 5:
            df['ma20_slope'] = (df['ma20'] - df['ma20'].shift(4)) / 4
        else:
            df['ma20_slope'] = 0.0
            
        # 3. MACD
        macd = ta.trend.MACD(df['close'])
        df['macd_dif'] = macd.macd()
        df['macd_dea'] = macd.macd_signal()
        df['macd_hist'] = macd.macd_diff() * 2
        df['macd_diff'] = df['macd_dif']
        df['macd_signal'] = df['macd_dea']
        df['macd'] = df['macd_hist']
        
        # 4. 换手率 (Turnover Rate) - 如果有 vol 和 float_share 可以计算，但通常从数据源直接获取
        # 这里假设 df 中已经包含了 'turnover_rate' 列 (Tushare 数据源自带)
        # 如果没有，尝试用 vol 和流通股本估算 (暂略，依赖数据源)
        if 'turnover_rate' not in df.columns:
             df['turnover_rate'] = 0.0
        
        # 5. 乖离率 (BIAS) - [增强版] 严格按照"最高点发生时刻均线"计算
        # 用户需求：最高点出来那一刻就认为是当天的收盘价，这样就同平时计算均线是一样方法
        # 算法：
        # MA5_at_High = (Close[t-1] + Close[t-2] + Close[t-3] + Close[t-4] + High[t]) / 5
        # BIAS5_High = (High[t] - MA5_at_High) / MA5_at_High * 100
        # 同理计算 MA10, MA20
        
        # 1. 计算 Close 的累计和 (用于快速计算前 N-1 天的总和)
        # rolling().sum() 会包含当前行，我们需要 shift(1) 来获取前 N-1 行
        close_sum = df['close'].rolling(window=1).sum() # 只是为了对齐索引，实际需要 rolling(N-1)
        
        for n in [5, 10, 20]:
            # 前 n-1 天的收盘价之和
            # sum_prev_n_minus_1 = rolling(window=n-1).sum().shift(1)
            # 注意：如果数据不足 n 天，rolling 会产生 NaN
            sum_prev = df['close'].rolling(window=n-1).sum().shift(1).fillna(0)
            
            # 构造"假设当前收盘价=最高价"时的 N 日均线总和
            # sum_at_high = sum_prev + high
            # 但对于前 n-1 天数据不足的情况（例如第 1 天），rolling sum 是 NaN
            # 我们需要处理初期数据：
            # 第 1 天：MA5_at_high = High / 1
            # 第 2 天：MA5_at_high = (Close[0] + High[1]) / 2
            # ...
            # 为了简化向量化计算，我们使用 rolling(min_periods=1) 但这会比较复杂
            # 这里采用标准算法，对于前 n 天数据不足的设为 NaN 或 0
            
            ma_n_at_high = (sum_prev + df['high']) / n
            ma_n_at_low = (sum_prev + df['low']) / n
            
            # 修正前 n-1 行的数据 (因为 shift(1) 导致前 n-1 行数据不完整)
            # 实际上，标准的 MA 定义在前 n-1 天是无效的。
            # 如果非要计算，可以使用 expanding().mean() 的变体，但用户强调"同平时计算均线一样方法"
            # 平时计算 MA5 在前 4 天是没有值的。所以这里保持 NaN 即可。
            
            df[f'bias{n}_high'] = (df['high'] - ma_n_at_high) / ma_n_at_high * 100
            df[f'bias{n}_low'] = (df['low'] - ma_n_at_low) / ma_n_at_low * 100

        # 保留原 bias 字段名为 close bias 以兼容旧逻辑，但推荐使用 high/low
        df['bias5'] = (df['close'] - df['ma5']) / df['ma5'] * 100
        df['bias10'] = (df['close'] - df['ma10']) / df['ma10'] * 100
        df['bias20'] = (df['close'] - df['ma20']) / df['ma20'] * 100

        # 6. 均线纠缠度 (MA Squeeze) - 均线越靠近，纠缠度越高
        ma_list = [df['ma5'], df['ma10'], df['ma20']]
        ma_max = pd.concat(ma_list, axis=1).max(axis=1)
        ma_min = pd.concat(ma_list, axis=1).min(axis=1)
        df['ma_squeeze'] = (ma_max - ma_min) / df['ma20'] * 100

        # 7. 趋势判定
        df['is_bullish'] = (df['ma5'] > df['ma10']) & (df['ma10'] > df['ma20'])
        df['is_bullish'] = df['is_bullish'].astype(int)
        
        # 8. 最终清理
        df = df.replace([np.inf, -np.inf], np.nan)
        return df

    def detect_top_divergence(self, df: pd.DataFrame, window: int = 100) -> bool:
        """
        检测顶背离 (MACD 顶背离) - 严谨波段比较版
        逻辑：当前价格波段创新高，但 MACD 能量柱(红柱)或 DIF 指标的波峰未能同步创新高
        """
        try:
            if len(df) < window:
                return False
            
            # 确保有 MACD 指标
            if 'macd_dif' not in df.columns:
                df = self._calculate_indicators(df.copy())
            
            recent = df.tail(window).copy()
            
            # 1. 识别“红柱波段” (macd_hist > 0)
            recent['is_red'] = recent['macd_hist'] > 0
            # 标记波段 ID (当 is_red 状态变化时，ID + 1)
            recent['wave_id'] = (recent['is_red'] != recent['is_red'].shift()).cumsum()
            
            # 只分析红柱波段
            red_waves = recent[recent['is_red']].copy()
            if red_waves.empty:
                return False
            
            # 按 wave_id 分组，找到每波的特征值
            wave_stats = []
            for wid, group in red_waves.groupby('wave_id'):
                if len(group) < 2: continue # 过滤极小噪点
                wave_stats.append({
                    'macd_hist': group['macd_hist'].max(),
                    'macd_dif': group['macd_dif'].max(),
                    'high': group['high'].max(),
                    'end_time': group['time'].iloc[-1]
                })
            
            if len(wave_stats) < 2:
                return False
                
            curr_wave = wave_stats[-1]
            prev_wave = wave_stats[-2] # 仅与紧邻的前一波对比
            
            # 如果当前波段还在“增长”中（最后一根柱子不是最大值），背离尚未最终形成
            # 但如果当前红柱已经开始缩短，说明波峰已过
            current_bar = recent.iloc[-1]
            is_shrinking = current_bar['macd_hist'] < recent['macd_hist'].iloc[-2]
            
            # 核心判定逻辑：
            # A. 价格创新高 (当前波最高价 > 紧邻前一波的最高价)
            # B. 指标未创新高 (当前波最大红柱 < 紧邻前一波红柱 OR 当前波最大 DIF < 紧邻前一波 DIF)
            
            # 1. 基于 MACD 柱状图的背离
            hist_div = (curr_wave['high'] > prev_wave['high'] * 1.002) and \
                       (curr_wave['macd_hist'] < prev_wave['macd_hist'] * 0.98)
            
            # 2. 基于 DIF 线的背离
            dif_div = (curr_wave['high'] > prev_wave['high'] * 1.002) and \
                      (curr_wave['macd_dif'] < prev_wave['macd_dif'] * 0.98)
            
            if (hist_div or dif_div) and is_shrinking:
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error detecting top divergence: {e}")
            return False

    def detect_trend_acceleration(self, df: pd.DataFrame, window: int = 20) -> bool:
        """
        检测趋势加速上涨 (Trend Acceleration)
        逻辑：
        1. 价格突破：收盘价创近期新高 (20日)
        2. 放量确认：当日成交量 > 5日均量的 1.5 倍
        3. 均线发散：均线从纠缠状态 (ma_squeeze < 3%) 开始向上发散
        4. 位置安全：价格距离 60 日最低价涨幅不超过 20% (确保是起点而非末端)
        5. MACD 健康：MACD 金叉或红柱增长
        """
        try:
            if len(df) < 60:
                return False
            
            recent = df.tail(window)
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            # 1. 价格突破近期高点
            price_breakout = current['close'] >= recent['close'].max() * 0.99
            
            # 2. 放量确认
            volume_surge = False
            if 'vol' in current and 'vol_ma5' in current:
                volume_surge = current['vol'] > current['vol_ma5'] * 1.5
            
            # 3. 均线纠缠后发散
            # 观察过去 5 天是否有过纠缠状态
            had_squeeze = df['ma_squeeze'].iloc[-10:-2].min() < 3.0
            ma_expanding = current['ma5'] > prev['ma5'] and current['ma5'] > current['ma10']
            
            # 4. 位置安全性
            low_60 = df['low'].iloc[-60:].min()
            position_safe = current['close'] < low_60 * 1.25
            
            # 5. MACD 金叉或红柱增长
            macd_healthy = current['macd_dif'] > current['macd_dea'] or current['macd_hist'] > prev['macd_hist']
            
            if price_breakout and volume_surge and had_squeeze and ma_expanding and position_safe and macd_healthy:
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error detecting trend acceleration: {e}")
            return False

    def detect_consolidation(self, df: pd.DataFrame, window: int = 20) -> bool:
        """
        检测横盘整理阶段 (Consolidation)
        逻辑：
        1. 价格波动小：最高价和最低价差值 < 5%
        2. 均线纠缠：ma_squeeze < 3%
        3. 成交量萎缩：成交量 < 5日均量的 1.2 倍
        """
        try:
            if len(df) < window:
                return False
            
            recent = df.tail(window)
            current = df.iloc[-1]
            
            # 1. 价格波动小
            high_low_ratio = (recent['high'].max() - recent['low'].min()) / recent['low'].min()
            price_stable = high_low_ratio < 0.05
            
            # 2. 均线纠缠
            ma_squeeze = current['ma_squeeze'] < 3.0 if 'ma_squeeze' in current else False
            
            # 3. 成交量萎缩
            volume_shrink = False
            if 'vol' in current and 'vol_ma5' in current:
                volume_shrink = current['vol'] < current['vol_ma5'] * 1.2
            
            if price_stable and ma_squeeze and volume_shrink:
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error detecting consolidation: {e}")
            return False

    def detect_trend_start(self, df: pd.DataFrame, window: int = 20) -> bool:
        """
        检测趋势起点 (Trend Start) - 实战增强版
        逻辑：
        1. 价格突破：收盘价创近期新高 (20日)
        2. 放量确认：当日成交量 > 5日均量的 1.5 倍
        3. 均线发散：均线从纠缠状态 (ma_squeeze < 3%) 开始向上发散
        4. 位置安全：价格距离 60 日最低价涨幅不超过 20% (确保是起点而非末端)
        """
        try:
            if len(df) < 60:
                return False
            
            recent = df.tail(window)
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            # 1. 价格突破近期高点
            price_breakout = current['close'] >= recent['close'].max() * 0.99
            
            # 2. 放量确认
            volume_surge = False
            if 'vol' in current and 'vol_ma5' in current:
                volume_surge = current['vol'] > current['vol_ma5'] * 1.5
            
            # 3. 均线纠缠后发散
            # 观察过去 5 天是否有过纠缠状态
            had_squeeze = df['ma_squeeze'].iloc[-10:-2].min() < 3.0
            ma_expanding = current['ma5'] > prev['ma5'] and current['ma5'] > current['ma10']
            
            # 4. 位置安全性
            low_60 = df['low'].iloc[-60:].min()
            position_safe = current['close'] < low_60 * 1.25
            
            # 5. MACD 金叉或红柱增长
            macd_healthy = current['macd_dif'] > current['macd_dea'] or current['macd_hist'] > prev['macd_hist']
            
            if price_breakout and volume_surge and had_squeeze and ma_expanding and position_safe and macd_healthy:
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error detecting trend start: {e}")
            return False

    def detect_bottom_divergence(self, df: pd.DataFrame, window: int = 100) -> bool:
        """
        检测底背离 (MACD 底背离) - 严谨波段比较版
        逻辑：价格波段创新低，但 MACD 能量柱(绿柱)或 DIF 指标的波谷未能同步创新低
        """
        try:
            if len(df) < window:
                return False
            
            # 确保有 MACD 指标
            if 'macd_dif' not in df.columns:
                df = self._calculate_indicators(df.copy())
            
            recent = df.tail(window).copy()
            
            # 1. 识别“绿柱波段” (macd_hist < 0)
            recent['is_green'] = recent['macd_hist'] < 0
            # 标记波段 ID
            recent['wave_id'] = (recent['is_green'] != recent['is_green'].shift()).cumsum()
            
            # 只分析绿柱波段
            green_waves = recent[recent['is_green']].copy()
            if green_waves.empty:
                return False
            
            # 按 wave_id 分组，找到每波的特征值
            wave_stats = []
            for wid, group in green_waves.groupby('wave_id'):
                if len(group) < 2: continue # 过滤噪点
                wave_stats.append({
                    'macd_hist': group['macd_hist'].min(), # 负数最小值 = 绝对值最大
                    'macd_dif': group['macd_dif'].min(),
                    'low': group['low'].min(),
                    'end_time': group['time'].iloc[-1]
                })
            
            if len(wave_stats) < 2:
                return False
                
            curr_wave = wave_stats[-1]
            prev_wave = wave_stats[-2] # 仅与紧邻的前一波对比
            
            # 如果当前波段还在“下探”中（最后一根柱子不是最小值），底背离尚未最终形成
            # 但如果当前绿柱已经开始缩短（数值回升），说明波谷已过
            current_bar = recent.iloc[-1]
            is_recovering = current_bar['macd_hist'] > recent['macd_hist'].iloc[-2]
            
            # 核心判定逻辑：
            # A. 价格创新低 (当前波最低价 < 紧邻前一波的最低价)
            # B. 指标未创新低 (当前波最大绿柱绝对值更小，即数值更大；或者 DIF 数值更大)
            
            # 1. 基于 MACD 柱状图的底背离
            hist_div = (curr_wave['low'] < prev_wave['low'] * 0.998) and \
                       (curr_wave['macd_hist'] > prev_wave['macd_hist'] + 0.01)
            
            # 2. 基于 DIF 线的底背离
            dif_div = (curr_wave['low'] < prev_wave['low'] * 0.998) and \
                      (curr_wave['macd_dif'] > prev_wave['macd_dif'] + 0.01)
            
            if (hist_div or dif_div) and is_recovering:
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error detecting bottom divergence: {e}")
            return False

    def detect_platform_breakout(self, df: pd.DataFrame, window: int = 60) -> bool:
        """
        检测平台突破 (Platform Breakout)
        逻辑：股价放量突破过去 N 根 K 线的最高收盘价或最高点，通常代表大级别趋势启动
        """
        try:
            if len(df) < window + 5:
                return False
            
            # 1. 识别过去 window 期的最高点 (不含当前)
            history = df.iloc[-(window+1):-1]
            platform_high = history['high'].max()
            
            # 2. 当前突破信号
            current = df.iloc[-1]
            
            # 突破收盘价
            is_breakout = current['close'] > platform_high
            
            # 3. 量能确认 (成交量 > 过去 20 日均量的 1.5 倍)
            avg_vol = df['volume'].iloc[-20:-1].mean()
            is_volume_confirmed = current['volume'] > avg_vol * 1.5
            
            if is_breakout and is_volume_confirmed:
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error detecting platform breakout: {e}")
            return False

    def get_key_k_lines(self, df: pd.DataFrame, window: int = 60) -> List[Dict[str, Any]]:
        """
        识别关键 K 线 (Key K-Lines)
        定义：成交量明显异动 (Volume > 1.8 * MA5) 且 价格波动明显 (涨跌幅 > 3% 或 振幅 > 5%)
        用途：作为短期支撑/压力位
        """
        if df.empty or len(df) < 5:
            return []
            
        try:
            # 截取最近 window 天
            recent = df.tail(window).copy()
            
            # 计算 MA5 成交量
            recent['vol_ma5'] = recent['volume'].rolling(window=5).mean()
            
            key_lines = []
            for i in range(5, len(recent)):
                curr = recent.iloc[i]
                prev_vol_ma = recent['vol_ma5'].iloc[i-1] if recent['vol_ma5'].iloc[i-1] > 0 else curr['volume']
                
                # 判定条件
                # 1. 量能异动: 大于 1.8 倍 5日均量
                is_vol_surge = curr['volume'] > prev_vol_ma * 1.8
                
                # 2. 价格异动: 涨跌幅绝对值 > 3% 或 振幅 > 5%
                pct_chg = curr.get('pct_chg', 0)
                if 'pct_chg' not in curr and 'close' in curr and 'open' in curr:
                    # 尝试估算涨跌幅 (如果没有 pct_chg 字段)
                    prev_close = recent['close'].iloc[i-1]
                    pct_chg = (curr['close'] - prev_close) / prev_close * 100
                    
                amplitude = (curr['high'] - curr['low']) / curr['low'] * 100
                
                is_price_surge = abs(pct_chg) > 3.0 or amplitude > 5.0
                
                if is_vol_surge and is_price_surge:
                    k_type = "大阳线" if pct_chg > 0 else "大阴线"
                    if pct_chg > 9.5: k_type = "涨停板"
                    elif pct_chg < -9.5: k_type = "跌停板"
                    elif amplitude > 8.0 and abs(pct_chg) < 2.0: k_type = "长腿十字星/剧烈震荡"
                    
                    # 确定关键价位
                    # 阳线：收盘价为强支撑，开盘价为弱支撑
                    # 阴线：开盘价为强压力，收盘价为弱压力
                    support = curr['low']
                    resistance = curr['high']
                    
                    # 实体部分更重要
                    entity_top = max(curr['open'], curr['close'])
                    entity_bottom = min(curr['open'], curr['close'])
                    
                    key_lines.append({
                        "date": curr['time'] if 'time' in curr else curr.name, # 假设索引或 time 列
                        "type": k_type,
                        "pct_chg": round(pct_chg, 2),
                        "vol_ratio": round(curr['volume'] / prev_vol_ma, 1),
                        "support": round(entity_bottom, 2),   # 实体下沿
                        "resistance": round(entity_top, 2),   # 实体上沿
                        "high": round(curr['high'], 2),
                        "low": round(curr['low'], 2)
                    })
            
            # 只保留最近的 5 根关键 K 线，越近越重要
            return key_lines[-5:]
            
        except Exception as e:
            logger.error(f"Error identifying key k-lines: {e}")
            return []

    def save_cache(self):
        """手动触发保存缓存（公开接口）"""
        self._save_cache()

# Global instance
technical_indicators = TechnicalIndicators()

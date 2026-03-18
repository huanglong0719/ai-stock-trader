"""
通达信财务数据读取服务
从通达信本地数据文件读取财务数据，避免频繁调用Tushare API
"""
import os
import logging
from typing import Dict, Optional, Any, Tuple, List
from datetime import datetime
from dbfread import DBF

logger = logging.getLogger(__name__)


class TdxFundamentalService:
    """通达信财务数据服务"""
    
    def __init__(self, tdx_path: str = r"D:\tdxkxgzhb"):
        self.tdx_path = tdx_path
        self.base_dbf_path = os.path.join(tdx_path, "T0002", "hq_cache", "base.dbf")
        self._cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._cache_duration = 3600 * 24  # 缓存24小时
        self._data_map: Optional[Dict[str, Dict[str, Any]]] = None  # 全局数据映射
        self._data_map_loaded = False  # 数据是否已加载
        self._last_mtime = 0.0  # 文件最后修改时间
        
    def _load_base_dbf(self) -> Dict[str, Dict[str, Any]]:
        """加载base.dbf文件（带指纹校验和只加载一次优化）"""
        if not os.path.exists(self.base_dbf_path):
            logger.warning(f"通达信财务数据文件不存在: {self.base_dbf_path}")
            return {}
            
        current_mtime = os.path.getmtime(self.base_dbf_path)
        
        # 如果已加载且文件未变化，直接返回缓存
        if self._data_map_loaded and self._data_map is not None and current_mtime <= self._last_mtime:
            return self._data_map
        
        try:
            import time
            start_time = time.time()
            logger.info(f"[财务数据] 开始加载通达信财务文件: {self.base_dbf_path}")
            
            # 使用 DBF 的 load=True 提高读取速度，虽然 list(table) 也能达到类似效果
            table = DBF(self.base_dbf_path, encoding='gbk', load=True)
            records = list(table)
            
            # 按股票代码建立索引
            data_map: Dict[str, Dict[str, Any]] = {}
            for record in records:
                ts_code = record.get('GPDM', '')
                if ts_code:
                    data_map[ts_code] = record
            
            self._data_map = data_map
            self._data_map_loaded = True
            self._last_mtime = current_mtime
            
            elapsed = (time.time() - start_time) * 1000
            logger.info(f"[财务数据] 成功加载通达信财务数据，共 {len(records)} 条记录，耗时: {elapsed:.2f}ms")
            return data_map
        except Exception as e:
            logger.error(f"[财务数据] 加载通达信财务数据失败: {e}")
            self._data_map_loaded = True
            return {}
    
    def get_fundamental_data(self, ts_code: str) -> Optional[Dict[str, Any]]:
        """
        从通达信获取财务数据
        
        Args:
            ts_code: 股票代码，如 '000001.SZ' 或 '000001'
            
        Returns:
            财务数据字典，包含：
            - ts_code: 股票代码
            - end_date: 数据更新日期
            - total_assets: 总资产
            - net_assets: 净资产
            - total_revenue: 营业收入
            - net_profit: 净利润
            - total_shares: 总股本
            - float_shares: 流通股
            - pe_ratio: 市盈率
            - pb_ratio: 市净率
            - roe: 净资产收益率
        """
        # 标准化股票代码
        ts_code = ts_code.replace('.SZ', '').replace('.SH', '')
        
        # 检查缓存
        if ts_code in self._cache:
            data, timestamp = self._cache[ts_code]
            if datetime.now().timestamp() - timestamp < self._cache_duration:
                return data
        
        # 加载数据（只加载一次）
        data_map = self._load_base_dbf()
        
        if not data_map:
            return None
        
        # 获取股票数据
        record = data_map.get(ts_code)
        if not record:
            return None
        
        # 解析数据
        try:
            gxrq = record.get('GXRQ', 0)
            if isinstance(gxrq, int) and gxrq > 0:
                end_date = datetime.strptime(str(gxrq), '%Y%m%d').date()
            else:
                end_date = datetime.now().date()
            
            total_assets = record.get('ZZC', 0) or 0  # 总资产
            net_assets = record.get('JZC', 0) or 0  # 净资产
            total_revenue = record.get('ZYSY', 0) or 0  # 营业收入
            net_profit = record.get('JLY', 0) or 0  # 净利润
            total_shares = record.get('ZGB', 0) or 0  # 总股本（万股）
            float_shares = record.get('LTAG', 0) or 0  # 流通A股（万股）
            
            # 计算资产负债率
            debt_to_assets = record.get('FZL', 0)
            if (not debt_to_assets or debt_to_assets == 0) and total_assets > 0:
                debt_to_assets = ((total_assets - net_assets) / total_assets) * 100
            
            # 计算ROE（净资产收益率）
            roe = record.get('ROE', 0)
            if (not roe or roe == 0) and net_assets > 0:
                roe = (net_profit / net_assets) * 100
            
            # 计算同比（DBF中通常不直接提供同比，尝试寻找字段）
            # 某些版本的 base.dbf 可能有 JLY_TB (净利同比) 等字段
            yoy_net_profit = record.get('JLY_TB', 0) or 0
            yoy_revenue = record.get('ZYSY_TB', 0) or 0
            
            # 计算每股收益
            eps = record.get('MGSY', 0)
            if not eps and total_shares > 0:
                eps = net_profit / (total_shares * 10000)  # 转换为元
            
            # 计算每股净资产
            bps = 0.0
            if total_shares > 0:
                bps = net_assets / (total_shares * 10000)
            
            result = {
                'ts_code': ts_code,
                'end_date': end_date,
                'total_assets': total_assets,  # 元
                'net_assets': net_assets,  # 元
                'total_revenue': total_revenue,  # 元
                'net_profit': net_profit,  # 元
                'total_shares': total_shares * 10000,  # 转换为股
                'float_shares': float_shares * 10000,  # 转换为股
                'roe': roe,  # %
                'debt_to_assets': debt_to_assets, # %
                'yoy_net_profit': yoy_net_profit, # %
                'yoy_revenue': yoy_revenue, # %
                'eps': eps,  # 元/股
                'bps': bps,  # 元/股
                'source': 'tdx'
            }
            
            # 缓存结果
            self._cache[ts_code] = (result, datetime.now().timestamp())
            
            return result
            
        except Exception as e:
            logger.error(f"解析通达信财务数据失败 {ts_code}: {e}")
            return None
    
    def batch_get_fundamental_data(self, ts_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        批量获取财务数据（优化版，只加载一次DBF文件）
        
        Args:
            ts_codes: 股票代码列表
            
        Returns:
            股票代码到财务数据的映射字典
        """
        # 一次性加载所有数据
        data_map = self._load_base_dbf()
        
        if not data_map:
            return {}
        
        results: Dict[str, Dict[str, Any]] = {}
        now_ts = datetime.now().timestamp()
        
        for ts_code in ts_codes:
            # 标准化股票代码
            code_key = ts_code.replace('.SZ', '').replace('.SH', '')
            
            # 1. 检查解析后的内存缓存
            if code_key in self._cache:
                data, timestamp = self._cache[code_key]
                if now_ts - timestamp < self._cache_duration:
                    results[ts_code] = data
                    continue
            
            # 2. 从原始数据映射中获取并解析
            record = data_map.get(code_key)
            if record:
                # 解析数据
                try:
                    # 提前解析常用字段，避免重复调用 get()
                    gxrq = record.get('GXRQ', 0)
                    if isinstance(gxrq, int) and gxrq > 0:
                        try:
                            end_date = datetime.strptime(str(gxrq), '%Y%m%d').date()
                        except:
                            end_date = datetime.now().date()
                    else:
                        end_date = datetime.now().date()
                    
                    total_assets = record.get('ZZC', 0) or 0
                    net_assets = record.get('JZC', 0) or 0
                    total_revenue = record.get('ZYSY', 0) or 0
                    net_profit = record.get('JLY', 0) or 0
                    total_shares = record.get('ZGB', 0) or 0
                    float_shares = record.get('LTAG', 0) or 0
                    
                    # 计算资产负债率
                    debt_to_assets = record.get('FZL', 0)
                    if (not debt_to_assets or debt_to_assets == 0) and total_assets > 0:
                        debt_to_assets = ((total_assets - net_assets) / total_assets) * 100
                    
                    # 计算ROE
                    roe = record.get('ROE', 0)
                    if (not roe or roe == 0) and net_assets > 0:
                        roe = (net_profit / net_assets) * 100
                    
                    yoy_net_profit = record.get('JLY_TB', 0) or 0
                    yoy_revenue = record.get('ZYSY_TB', 0) or 0
                    
                    eps = record.get('MGSY', 0)
                    if not eps and total_shares > 0:
                        eps = net_profit / (total_shares * 10000)
                    
                    bps = record.get('MGJZC', 0)
                    if not bps and total_shares > 0:
                        bps = net_assets / (total_shares * 10000)
                    
                    result = {
                        'ts_code': ts_code,
                        'end_date': end_date,
                        'total_assets': total_assets,
                        'net_assets': net_assets,
                        'total_revenue': total_revenue,
                        'net_profit': net_profit,
                        'total_shares': total_shares * 10000,
                        'float_shares': float_shares * 10000,
                        'roe': roe,
                        'debt_to_assets': debt_to_assets,
                        'yoy_net_profit': yoy_net_profit,
                        'yoy_revenue': yoy_revenue,
                        'eps': eps,
                        'bps': bps,
                        'source': 'tdx'
                    }
                    
                    # 写入缓存
                    self._cache[code_key] = (result, now_ts)
                    results[ts_code] = result
                    
                except Exception as e:
                    logger.error(f"解析通达信财务数据失败 {ts_code}: {e}")
        
        return results
    
    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        if hasattr(self, '_data_map'):
            del self._data_map


# 全局实例
tdx_fundamental_service = TdxFundamentalService()

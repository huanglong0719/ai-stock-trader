"""
通达信公式函数服务
实现类似 EXTERNSTR, EXTERNVALUE, SIGNALS_SYS 以及 FINVALUE.FINONE 等功能
"""
import os
import logging
from typing import Dict, Optional, Any, Union, Tuple
from dbfread import DBF

logger = logging.getLogger(__name__)

class TdxFormulaService:
    def __init__(self, tdx_path: str = r"D:\tdxkxgzhb"):
        self.tdx_path = tdx_path
        self.base_dbf_path = os.path.join(tdx_path, "T0002", "hq_cache", "base.dbf")
        self.extern_sys_path = os.path.join(tdx_path, "T0002", "signals", "extern_sys.txt")
        
        self._base_data: Dict[str, Dict[str, Any]] = {}
        self._extern_data: Dict[str, Dict[str, Tuple[str, float]]] = {}  # {ts_code: {type: (str_val, num_val)}}
        self._loaded = False

    def _load_data(self):
        """加载所有相关数据"""
        if self._loaded:
            return
            
        # 1. 加载 base.dbf
        if os.path.exists(self.base_dbf_path):
            try:
                table = DBF(self.base_dbf_path, encoding='gbk')
                for record in table:
                    ts_code = record.get('GPDM', '')
                    if ts_code:
                        self._base_data[ts_code] = record
            except Exception as e:
                logger.error(f"加载 base.dbf 失败: {e}")

        # 2. 加载 extern_sys.txt
        if os.path.exists(self.extern_sys_path):
            try:
                with open(self.extern_sys_path, 'r', encoding='gbk', errors='ignore') as f:
                    for line in f:
                        # 格式: Index|StockCode|Type|StringValue|NumValue
                        parts = line.strip().split('|')
                        if len(parts) >= 5:
                            # 处理第一部分 1→0 这种格式
                            code_part = parts[1]
                            data_type = parts[2]
                            str_val = parts[3]
                            try:
                                num_val = float(parts[4])
                            except:
                                num_val = 0.0
                            
                            if code_part not in self._extern_data:
                                self._extern_data[code_part] = {}
                            self._extern_data[code_part][data_type] = (str_val, num_val)
            except Exception as e:
                logger.error(f"加载 extern_sys.txt 失败: {e}")
                
        self._loaded = True

    def _normalize_code(self, ts_code: str) -> str:
        """标准化股票代码为 6 位数字"""
        return ts_code.split('.')[0][-6:]

    def FINONE(self, id_or_field: Union[int, str], ts_code: str) -> Any:
        """
        获取财务数据 (对应 FINVALUE.FINONE)
        """
        self._load_data()
        code = self._normalize_code(ts_code)
        record = self._base_data.get(code, {})
        
        # ID 到字段名的映射 (基于通达信标准 FINANCE 函数)
        id_map = {
            1: 'ZGB',    # 总股本
            10: 'LTAG',  # 流通A股
            13: 'ZZC',   # 总资产
            15: 'GDZC',  # 固定资产
            21: 'JZC',   # 净资产
            22: 'ZYSY',  # 营业收入
            32: 'JLY',   # 净利润
            35: 'MGSY',  # 每股收益
            37: 'MGJZC', # 每股净资产
            44: 'ROE',   # ROE (如果DBF中有)
        }
        
        field: Optional[str]
        if isinstance(id_or_field, int):
            field = id_map.get(id_or_field)
        else:
            field = str(id_or_field) if id_or_field else None
            
        if not field:
            return 0
            
        return record.get(field, 0)

    def EXTERNSTR(self, data_type: Union[int, str], ts_code: str) -> str:
        """
        获取外部字符串数据
        """
        self._load_data()
        code = self._normalize_code(ts_code)
        data_type = str(data_type)
        
        stock_data = self._extern_data.get(code, {})
        return stock_data.get(data_type, ("", 0.0))[0]

    def EXTERNVALUE(self, data_type: Union[int, str], ts_code: str) -> float:
        """
        获取外部数值数据
        """
        self._load_data()
        code = self._normalize_code(ts_code)
        data_type = str(data_type)
        
        stock_data = self._extern_data.get(code, {})
        return stock_data.get(data_type, ("", 0.0))[1]

    def GPJYONE(self, id_or_field: Any, ts_code: str) -> float:
        """
        获取股票交易数据 (占位实现)
        """
        return 0.0

    def SCJYONE(self, id_or_field: Any, ts_code: str) -> float:
        """
        获取市场交易数据 (占位实现)
        """
        return 0.0

# 全局实例
tdx_formula_service = TdxFormulaService()

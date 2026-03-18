from datetime import datetime, time, timedelta

def is_trading_time() -> bool:
    """
    判断当前是否为交易时间 (周一至周五, 9:15-11:30, 13:00-15:00)
    """
    now = datetime.now()
    # 1. 周末不交易
    if now.weekday() >= 5:
        return False
    
    current_time = now.time()
    # 2. 上午交易时段
    morning_start = time(9, 15)
    morning_end = time(11, 35) # 稍微多留一点时间
    if morning_start <= current_time <= morning_end:
        return True
        
    # 3. 下午交易时段
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 1) # 15:00 收盘，多留 1 分钟处理最后一笔
    if afternoon_start <= current_time <= afternoon_end:
        return True
        
    return False

def is_after_market_close(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return True
    return now.time() > time(15, 1)

def normalize_date(d_str: str) -> str:
    """
    标准化日期格式为 YYYYMMDD
    接受: '2023-10-27', '2023-10-27 15:00:00', '2023/10/27'
    """
    if not d_str: return ""
    return d_str.split(' ')[0].replace('-', '').replace('/', '')

def normalize_datetime(dt_str: str) -> str:
    """
    标准化日期时间格式为 YYYYMMDDHHMMSS，用于高精度比较
    """
    if not dt_str: return ""
    # 去除所有非数字字符
    import re
    return re.sub(r'\D', '', dt_str)

def get_limit_prices(ts_code: str, pre_close: float, name: str = "") -> tuple:
    """
    计算涨跌停价格 (基于昨收价和代码规则)
    :return: (limit_up, limit_down)
    """
    if not pre_close or pre_close <= 0:
        return 0.0, 0.0
        
    ratio = 0.1  # 默认 10%
    if name and 'ST' in name:
        ratio = 0.05
    elif ts_code.startswith('688') or ts_code.startswith('30'):
        ratio = 0.2
    elif ts_code.endswith('.BJ') or ts_code.startswith('8') or ts_code.startswith('4') or ts_code.startswith('43') or ts_code.startswith('83') or ts_code.startswith('87') or ts_code.startswith('92'):
        ratio = 0.3
        
    # A股涨跌停价格计算：昨收价 * (1 ± 比例)，四舍五入保留两位小数
    # 特殊处理：有些品种（如北交所）规则可能略有差异，但主流是 round(pre_close * (1+ratio), 2)
    limit_up = round(pre_close * (1 + ratio) + 0.0001, 2)
    limit_down = round(pre_close * (1 - ratio) + 0.0001, 2)
    
    return limit_up, limit_down

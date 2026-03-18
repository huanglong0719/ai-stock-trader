import os
import struct
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

class TdxVipdocService:
    def __init__(self, vipdoc_root: str):
        root = os.path.abspath(str(vipdoc_root or "").strip())
        base = root
        base_name = os.path.basename(root).lower()
        if base_name in {"sh", "sz"}:
            parent = os.path.dirname(root)
            if os.path.basename(parent).lower() == "vipdoc":
                base = parent
        elif base_name in {"lday", "fzline", "minline", "eday"}:
            parent = os.path.dirname(root)
            if os.path.basename(parent).lower() in {"sh", "sz"}:
                grand = os.path.dirname(parent)
                if os.path.basename(grand).lower() == "vipdoc":
                    base = grand
        self.vipdoc_root = base

    def _get_vipdoc_dir(self) -> str:
        if os.path.basename(self.vipdoc_root).lower() == "vipdoc":
            return self.vipdoc_root
        return os.path.join(self.vipdoc_root, "vipdoc")

    def _get_lc5_path(self, ts_code: str) -> Optional[str]:
        code = str(ts_code or "").strip()
        if len(code) < 8 or "." not in code:
            return None
        code6, suffix = code[:6], code.split(".")[-1].upper()
        if suffix not in {"SZ", "SH"}:
            return None
        market_dir = "sz" if suffix == "SZ" else "sh"
        prefix = "sz" if suffix == "SZ" else "sh"
        base_dir = self._get_vipdoc_dir()
        path = os.path.join(base_dir, market_dir, "fzline", f"{prefix}{code6}.lc5")
        return path

    def _get_day_path(self, ts_code: str) -> Optional[str]:
        code = str(ts_code or "").strip()
        if len(code) < 8 or "." not in code:
            return None
        code6, suffix = code[:6], code.split(".")[-1].upper()
        if suffix not in {"SZ", "SH"}:
            return None
        market_dir = "sz" if suffix == "SZ" else "sh"
        prefix = "sz" if suffix == "SZ" else "sh"
        base_dir = self._get_vipdoc_dir()
        path = os.path.join(base_dir, market_dir, "lday", f"{prefix}{code6}.day")
        return path

    @staticmethod
    def _decode_trade_time(date_num: int, minutes_of_day: int) -> Optional[datetime]:
        try:
            year = int(date_num // 2048) + 2004
            md = int(date_num % 2048)
            month = int(md // 100)
            day = int(md % 100)
            hour = int(minutes_of_day // 60)
            minute = int(minutes_of_day % 60)
            return datetime(year, month, day, hour, minute, 0)
        except Exception:
            return None

    def read_5min_bars(self, ts_code: str, limit: int = 1000) -> pd.DataFrame:
        path = self._get_lc5_path(ts_code)
        if not path:
            return pd.DataFrame()
            
        if not os.path.isfile(path):
            return pd.DataFrame()

        rec_size = 32
        try:
            file_size = os.path.getsize(path)
            if file_size < rec_size:
                return pd.DataFrame()

            # [优化] 只读取最后 limit 条数据，极大提升加载速度
            read_size = limit * rec_size
            offset = max(0, file_size - read_size)
            if offset > 0:
                offset = offset - (offset % rec_size)
            
            with open(path, "rb") as f:
                if offset > 0:
                    f.seek(offset)
                data = f.read()
        except Exception as e:
            logger.warning(f"Failed to read local TDX file {path}: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        remainder = len(data) % rec_size
        if remainder != 0:
            data = data[remainder:]
            if not data:
                return pd.DataFrame()

        fmt = "<HHfffffII"
        times, opens, highs, lows, closes, amounts, vols = [], [], [], [], [], [], []

        # 使用 struct.unpack_from 提高效率
        for i in range(0, len(data) - rec_size + 1, rec_size):
            try:
                date_num, mins, o, h, l, c, amt, vol, _ = struct.unpack_from(fmt, data, i)
                dt = self._decode_trade_time(date_num, mins)
                if dt:
                    times.append(dt)
                    opens.append(float(o))
                    highs.append(float(h))
                    lows.append(float(l))
                    closes.append(float(c))
                    amounts.append(float(amt))
                    vols.append(float(vol))
            except:
                continue

        if not times:
            return pd.DataFrame()

        df = pd.DataFrame(
            {
                "trade_time": pd.to_datetime(times),
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "amount": amounts,
                "vol": vols,
            }
        )
        df = df.dropna(subset=["trade_time"]).drop_duplicates(subset=["trade_time"]).sort_values("trade_time")
        return df

    def read_day_bars(self, ts_code: str, limit: int = 1200) -> pd.DataFrame:
        path = self._get_day_path(ts_code)
        if not path:
            return pd.DataFrame()

        if not os.path.isfile(path):
            return pd.DataFrame()

        rec_size = 32
        try:
            file_size = os.path.getsize(path)
            if file_size < rec_size:
                return pd.DataFrame()
            read_size = limit * rec_size
            offset = max(0, file_size - read_size)
            if offset > 0:
                offset = offset - (offset % rec_size)
            with open(path, "rb") as f:
                if offset > 0:
                    f.seek(offset)
                data = f.read()
        except Exception as e:
            logger.warning(f"Failed to read local TDX file {path}: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        remainder = len(data) % rec_size
        if remainder != 0:
            data = data[remainder:]
            if not data:
                return pd.DataFrame()

        fmt = "<IIIIIfII"
        times, opens, highs, lows, closes, amounts, vols = [], [], [], [], [], [], []
        for i in range(0, len(data) - rec_size + 1, rec_size):
            try:
                date_num, o, h, l, c, amt, vol, _ = struct.unpack_from(fmt, data, i)
                if not date_num:
                    continue
                dt = datetime.strptime(str(int(date_num)), "%Y%m%d")
                times.append(dt)
                opens.append(float(o) / 100.0)
                highs.append(float(h) / 100.0)
                lows.append(float(l) / 100.0)
                closes.append(float(c) / 100.0)
                amounts.append(float(amt) / 100000000.0)
                vols.append(float(vol))
            except Exception:
                continue

        if not times:
            return pd.DataFrame()

        df = pd.DataFrame(
            {
                "trade_time": pd.to_datetime(times),
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "amount": amounts,
                "vol": vols,
            }
        )
        df = df.dropna(subset=["trade_time"]).drop_duplicates(subset=["trade_time"]).sort_values("trade_time")
        return df

    @staticmethod
    def aggregate_30min_from_5min(df_5m: pd.DataFrame) -> pd.DataFrame:
        if df_5m is None or df_5m.empty:
            return pd.DataFrame()

        df = df_5m.copy()
        df["trade_time"] = pd.to_datetime(df["trade_time"])
        df = df.dropna(subset=["trade_time"]).sort_values("trade_time")
        if df.empty:
            return pd.DataFrame()

        # 核心修复：更准确的分钟线对齐逻辑
        # TDX 的 5 分钟线时间戳是 bar 的结束时间 (e.g. 09:35, 09:40)
        # 30 分钟线也应该是结束时间 (e.g. 10:00, 10:30)
        # 09:35, 40, 45, 50, 55, 10:00 -> 归为 10:00
        # 10:05, ..., 10:30 -> 归为 10:30
        
        # 算法：
        # 1. 计算每个时间点是当天的第几分钟
        # 2. 向上取整到最近的 30 分钟倍数
        # 3. 如果是 11:30/15:00 这种休市边界，要特殊处理 (通常不用，因为整点已经涵盖)
        
        # 注意：Python 的 dt.minute 是 0-59，而 bar time 是结束时间，可能是 60 (显示为下一小时00分)
        # 比如 10:00 的 minute 是 0。
        
        # 转换逻辑：
        # 将时间减去 1 分钟，然后除以 30 向下取整，再加 1，再乘以 30，得到结束时间的偏移量
        # (HH*60 + MM - 1) // 30 + 1 -> 30min block index
        
        minutes_total = df["trade_time"].dt.hour * 60 + df["trade_time"].dt.minute
        # 早上 09:30 开盘，第一个 bar 是 09:35。
        # 09:35 -> (575-1)//30 + 1 = 19.13 -> 20.  20*30 = 600 min = 10:00. 正确。
        # 10:00 -> (600-1)//30 + 1 = 20. 20*30 = 600 min = 10:00. 正确。
        # 10:05 -> (605-1)//30 + 1 = 21. 21*30 = 630 min = 10:30. 正确。
        
        bucket_mins = ((minutes_total - 1) // 30 + 1) * 30
        
        # 构造新的时间戳
        # 基础日期（年月日）
        base_dates = df["trade_time"].dt.normalize()
        
        # 加上分钟偏移量
        bucket_times = base_dates + pd.to_timedelta(bucket_mins, unit='m')
        
        df["bucket_time"] = bucket_times

        agg = (
            df.groupby("bucket_time", sort=True)
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                vol=("vol", "sum"),
                amount=("amount", "sum"),
            )
            .reset_index()
            .rename(columns={"bucket_time": "trade_time"})
        )
        agg = agg.dropna(subset=["trade_time"]).drop_duplicates(subset=["trade_time"]).sort_values("trade_time")
        return agg


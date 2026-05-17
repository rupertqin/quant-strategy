"""
Dashboard 数据访问层 - 统一路径和简单函数

实时数据统一从 intraday parquet 读取，不再依赖 JSON。
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime
import sys

BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from DataHub.config import INTRADAY_DIR


def _get_intraday_parquet_path(asset_type: str = 'stock') -> Optional[Path]:
    """获取当天的 intraday parquet 文件路径（严格当天，不回退到历史文件）"""
    today_str = datetime.now().strftime('%Y%m%d')
    filepath = INTRADAY_DIR / asset_type / f"{today_str}.parquet"
    if filepath.exists():
        return filepath
    return None


def get_todays_realtime_file(asset_type: str = None) -> Optional[str]:
    """获取当天最新的实时数据文件路径（返回 parquet 路径）

    Args:
        asset_type: 'stock'|'etf'|'index'|None，None表示依次尝试 stock/etf/index
    """
    if asset_type:
        path = _get_intraday_parquet_path(asset_type)
        return str(path) if path else None

    for at in ['stock', 'etf', 'index']:
        path = _get_intraday_parquet_path(at)
        if path:
            return str(path)
    return None


# 别名保持兼容
find_todays_realtime_file = get_todays_realtime_file


def load_realtime_data(filepath: str = None) -> pd.DataFrame:
    """加载实时数据为 DataFrame（从 parquet）"""
    if filepath is None:
        filepath = get_todays_realtime_file()

    if not filepath or not Path(filepath).exists():
        return pd.DataFrame()

    df = pd.read_parquet(filepath)

    # 取每个 symbol 的最新快照（按 timestamp）
    if 'timestamp' in df.columns and 'symbol' in df.columns:
        df = df.sort_values('timestamp').groupby('symbol').tail(1).reset_index(drop=True)

    # 确保 symbol 格式统一
    if 'symbol' in df.columns:
        def format_symbol(code):
            code_str = str(code).strip()
            if '.' in code_str:
                return code_str
            if code_str.startswith('6') or code_str.startswith('500') or code_str.startswith('501'):
                return f"{code_str}.SH"
            if code_str.startswith('51') or code_str.startswith('52') or code_str.startswith('53') or code_str.startswith('56') or code_str.startswith('58') or code_str.startswith('59'):
                return f"{code_str}.SH"
            if code_str.startswith('0') or code_str.startswith('3') or code_str.startswith('159') or code_str.startswith('169'):
                return f"{code_str}.SZ"
            if code_str.startswith('4') or code_str.startswith('8'):
                return f"{code_str}.BJ"
            return code_str
        df['symbol'] = df['symbol'].apply(format_symbol)

    return df


def _fmt_ts(ts) -> str:
    """格式化 timestamp 为显示字符串"""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    if isinstance(ts, str):
        return ts
    try:
        return ts.strftime('%Y-%m-%d %H:%M')
    except Exception:
        return str(ts)


def get_realtime_price_time() -> Optional[str]:
    """获取实时数据的最新时间（用于显示）"""
    filepath = get_todays_realtime_file()
    if not filepath:
        return None

    try:
        df = pd.read_parquet(filepath)
        if 'timestamp' in df.columns and not df.empty:
            return _fmt_ts(df['timestamp'].max())
    except Exception:
        pass
    return None


def has_realtime_data() -> bool:
    """检查是否有当天实时数据"""
    return get_todays_realtime_file() is not None


def _rt_val(realtime: pd.Series, key: str, fallback_key: str = 'close') -> float:
    """安全读取实时数据字段，None/NaN 时回退"""
    import pandas as pd
    val = realtime.get(key)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        val = realtime.get(fallback_key, 0)
    return float(val)


def merge_realtime_to_history(hist_df: pd.DataFrame, realtime: pd.Series, adjust: str = "qfq") -> pd.DataFrame:
    """
    将实时数据合并到历史K线（内存中）

    冷热数据时间线规则：
    - 热数据日期 <= 冷数据最后日期：丢弃热数据（冷数据已归档或更新）
    - 热数据日期 > 冷数据最后日期：追加新行（前复权转换后）

    Args:
        hist_df: 历史日线数据（默认前复权）
        realtime: 实时行情Series（通常不复权）
        adjust: 历史数据复权方式，默认 "qfq"

    Returns:
        合并后的DataFrame
    """
    from datetime import datetime

    if hist_df.empty:
        return hist_df

    # 确保 trade_date 列是 datetime 类型
    hist_df['trade_date'] = pd.to_datetime(hist_df['trade_date'])

    # 从实时数据提取实际日期（优先 timestamp，其次 trade_date/date）
    rt_date = None
    ts = realtime.get('timestamp')
    if ts is not None and not pd.isna(ts):
        rt_date = pd.to_datetime(ts).date()
    else:
        for key in ('trade_date', 'date'):
            val = realtime.get(key)
            if val is not None and not pd.isna(val):
                rt_date = pd.to_datetime(val).date()
                break
    if rt_date is None:
        rt_date = datetime.now().date()

    # 冷数据最后一天日期
    last_date = hist_df['trade_date'].iloc[-1]
    if isinstance(last_date, pd.Timestamp):
        last_date = last_date.date()

    # 热数据不比冷数据新（同一天或更旧），不合并
    if rt_date <= last_date:
        return hist_df

    close = _rt_val(realtime, 'close')
    change_pct = _rt_val(realtime, 'change_pct', 'close')
    open_price = _rt_val(realtime, 'open', 'close')
    high = _rt_val(realtime, 'high', 'close')
    low = _rt_val(realtime, 'low', 'close')

    # 前复权转换：实时数据通常为不复权价格，需转换到与历史数据一致的价格体系
    if adjust == "qfq":
        try:
            prev_close = float(hist_df.iloc[-1]['close'])
            if prev_close > 0 and close > 0:
                qfq_close = prev_close * (1 + change_pct / 100)
                ratio = qfq_close / close
                open_price = open_price * ratio
                high = high * ratio
                low = low * ratio
                close = qfq_close
        except Exception:
            pass  # 转换失败时保持原始价格

    # 追加新行（热数据比冷数据新）
    new_row = pd.DataFrame([{
        'trade_date': pd.Timestamp(rt_date),
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'volume': _rt_val(realtime, 'volume', 'close'),
        'amount': _rt_val(realtime, 'amount', 'close'),
        'change_pct': change_pct,
        'symbol': realtime.get('symbol', '')
    }])
    hist_df = pd.concat([hist_df, new_row], ignore_index=True)

    return hist_df

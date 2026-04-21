"""
Dashboard 数据访问层 - 统一路径和简单函数
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime

# 统一路径常量
BASE_DIR = Path(__file__).parent.parent.parent
REALTIME_DIR = BASE_DIR / "storage" / "raw" / "realtime"


def get_todays_realtime_file(asset_type: str = None) -> Optional[str]:
    """获取当天最新的实时数据文件路径
    
    Args:
        asset_type: 'stock'|'etf'|None，None表示获取任意类型最新文件
    
    返回最新数据文件（包括盘中和盘后）
    """
    today = datetime.now().strftime('%Y%m%d')

    if not REALTIME_DIR.exists():
        return None

    # 根据资产类型确定文件前缀
    if asset_type == 'etf':
        patterns = [f"etf_realtime_{today}_*.json"]
    elif asset_type == 'stock':
        patterns = [f"realtime_{today}_*.json"]
    else:
        # 获取所有类型
        patterns = [f"realtime_{today}_*.json", f"etf_realtime_{today}_*.json"]

    # 获取今天的所有文件
    today_files = []
    for pattern in patterns:
        for f in REALTIME_DIR.glob(pattern):
            try:
                with open(f, 'r', encoding='utf-8') as fp:
                    data = json.load(fp)
                fetch_time = data.get('fetch_time', '')
                if fetch_time:
                    today_files.append((f, fetch_time))
            except:
                continue

    if not today_files:
        return None

    # 返回最新的数据文件（按时间排序）
    return str(sorted(today_files, key=lambda x: x[1], reverse=True)[0][0])


# 别名保持兼容
find_todays_realtime_file = get_todays_realtime_file


def load_realtime_data(filepath: str = None) -> pd.DataFrame:
    """加载实时数据为DataFrame

    实时数据文件由 realtime_service.py 生成，列名已经是英文
    """
    if filepath is None:
        filepath = get_todays_realtime_file()

    if not filepath or not Path(filepath).exists():
        return pd.DataFrame()

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 实时数据文件结构: {"fetch_time": "...", "data": [...]}
    stocks = data.get('data', [])
    df = pd.DataFrame(stocks)

    # 确保symbol格式统一（添加后缀 .SH/.SZ）
    if 'symbol' in df.columns:
        def format_symbol(code):
            code_str = str(code).strip()
            if '.' in code_str:
                return code_str
            # 沪市：6开头股票、500/501/510-519/520/530/560-563/588/589 ETF
            if code_str.startswith('6') or code_str.startswith('500') or code_str.startswith('501'):
                return f"{code_str}.SH"
            if code_str.startswith('51') or code_str.startswith('52') or code_str.startswith('53') or code_str.startswith('56') or code_str.startswith('58') or code_str.startswith('59'):
                return f"{code_str}.SH"
            # 深市：0/3开头股票、159/169 ETF
            if code_str.startswith('0') or code_str.startswith('3') or code_str.startswith('159') or code_str.startswith('169'):
                return f"{code_str}.SZ"
            # 北交所
            if code_str.startswith('4') or code_str.startswith('8'):
                return f"{code_str}.BJ"
            return code_str
        df['symbol'] = df['symbol'].apply(format_symbol)

    return df


def get_realtime_price_time() -> Optional[str]:
    """获取实时数据的时间（用于显示）"""
    filepath = get_todays_realtime_file()
    if not filepath:
        return None
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        fetch_time = data.get('fetch_time', '')
        if fetch_time and len(fetch_time) >= 13:
            return f"{fetch_time[:4]}-{fetch_time[4:6]}-{fetch_time[6:8]} {fetch_time[9:11]}:{fetch_time[11:13]}"
    except:
        pass
    return None


def has_realtime_data() -> bool:
    """检查是否有当天实时数据"""
    return get_todays_realtime_file() is not None


def get_latest_realtime_data(force_fetch: bool = False, full_format: bool = False, asset_type: str = None) -> tuple[pd.DataFrame, str]:
    """
    获取最新实时数据（统一入口）
    
    Args:
        force_fetch: 是否强制获取最新数据（True=总是fetch，False=优先用缓存）
        full_format: 时间格式（True=YYYY-MM-DD HH:MM，False=HH:MM）
        asset_type: 'stock'|'etf'|None，None表示获取任意类型最新文件
        
    Returns:
        (DataFrame, fetch_time_str)
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    def format_time(fetch_time: str) -> str:
        """格式化时间字符串"""
        if not fetch_time or len(fetch_time) < 15:
            return ""
        # fetch_time格式: YYYYMMDD_HHMMSS
        date_part = f"{fetch_time[:4]}-{fetch_time[4:6]}-{fetch_time[6:8]}"
        time_part = f"{fetch_time[9:11]}:{fetch_time[11:13]}"
        if full_format:
            return f"{date_part} {time_part}"
        return time_part
    
    # 根据模式决定获取方式
    if force_fetch:
        # 强制获取最新（用于信号扫描）
        try:
            from DataHub.services.realtime_service import get_realtime_service
            rt_service = get_realtime_service()
            rt_file = rt_service.fetch_and_save()
            df = load_realtime_data(rt_file)
            
            # 提取时间
            match = __import__('re').search(r'(?:etf_)?realtime_(\d{8})_(\d{6})\.json', Path(rt_file).name)
            if match:
                time_str = f"{match.group(2)[:2]}:{match.group(2)[2:4]}"
                if full_format:
                    date_part = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:8]}"
                    return df, f"{date_part} {time_str}"
                return df, time_str
            
            return df, ""
        except Exception:
            # 获取失败，回退到已有数据
            pass
    
    # 使用已有最新数据（用于图表展示）
    rt_file = get_todays_realtime_file(asset_type=asset_type)
    if rt_file:
        df = load_realtime_data(rt_file)

        # 从文件内容读取fetch_time
        try:
            with open(rt_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            fetch_time = data.get('fetch_time', '')
            return df, format_time(fetch_time)
        except:
            pass

        return df, ""

    return pd.DataFrame(), ""


def merge_realtime_to_history(hist_df: pd.DataFrame, realtime: pd.Series) -> pd.DataFrame:
    """
    将实时数据合并到历史K线（内存中）

    Args:
        hist_df: 历史日线数据
        realtime: 实时行情Series

    Returns:
        合并后的DataFrame
    """
    from datetime import datetime

    # 确保 trade_date 列是 datetime 类型
    hist_df['trade_date'] = pd.to_datetime(hist_df['trade_date'])

    today = datetime.now()
    today_date = today.date()

    # 获取最后一天日期
    last_date = hist_df['trade_date'].iloc[-1]
    if isinstance(last_date, pd.Timestamp):
        last_date = last_date.date()

    # 如果历史数据已有今天数据，更新它；否则追加新行
    if not hist_df.empty and last_date == today_date:
        idx = hist_df.index[-1]
        hist_df.loc[idx, 'close'] = float(realtime['close'])
        hist_df.loc[idx, 'high'] = max(float(hist_df.loc[idx, 'high']), float(realtime.get('high', realtime['close'])))
        hist_df.loc[idx, 'low'] = min(float(hist_df.loc[idx, 'low']), float(realtime.get('low', realtime['close'])))
        hist_df.loc[idx, 'volume'] = float(realtime['volume'])
        hist_df.loc[idx, 'amount'] = float(realtime.get('amount', 0))
        hist_df.loc[idx, 'change_pct'] = float(realtime['change_pct'])
    else:
        new_row = pd.DataFrame([{
            'trade_date': today,
            'open': float(realtime.get('open', realtime['close'])),
            'high': float(realtime.get('high', realtime['close'])),
            'low': float(realtime.get('low', realtime['close'])),
            'close': float(realtime['close']),
            'volume': float(realtime['volume']),
            'amount': float(realtime.get('amount', 0)),
            'change_pct': float(realtime['change_pct']),
            'symbol': realtime.get('symbol', '')
        }])
        hist_df = pd.concat([hist_df, new_row], ignore_index=True)

    return hist_df

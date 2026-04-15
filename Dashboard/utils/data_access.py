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


def get_todays_realtime_file() -> Optional[str]:
    """获取当天最新的实时数据文件路径（盘后数据优先）"""
    today = datetime.now().strftime('%Y%m%d')
    
    if not REALTIME_DIR.exists():
        return None
    
    # 获取今天的所有文件
    today_files = []
    for f in REALTIME_DIR.glob(f"realtime_{today}_*.json"):
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
            fetch_time = data.get('fetch_time', '')
            if fetch_time:
                hour = int(fetch_time[9:11]) if len(fetch_time) >= 11 else 0
                today_files.append((f, fetch_time, hour))
        except:
            continue
    
    if not today_files:
        return None
    
    # 盘后数据(>=15点)优先，然后按时间最新
    post_market = [f for f in today_files if f[2] >= 15]
    if post_market:
        return str(sorted(post_market, key=lambda x: x[1], reverse=True)[0][0])
    
    return str(sorted(today_files, key=lambda x: x[1], reverse=True)[0][0])


# 别名保持兼容
find_todays_realtime_file = get_todays_realtime_file


def load_realtime_data(filepath: str = None) -> pd.DataFrame:
    """加载实时数据为DataFrame"""
    if filepath is None:
        filepath = get_todays_realtime_file()
    
    if not filepath or not Path(filepath).exists():
        return pd.DataFrame()
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 实时数据文件结构: {"fetch_time": "...", "data": [...]}
    stocks = data.get('data', [])
    df = pd.DataFrame(stocks)
    
    # 列名映射（实时数据用中文，转为英文）
    column_map = {
        '最高': 'high',
        '最低': 'low',
        '昨收': 'prev_close',
        '涨跌额': 'change',
        '买入': 'bid',
        '卖出': 'ask',
        '时间戳': 'timestamp',
        '代码': 'code'
    }
    df = df.rename(columns=column_map)
    
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


def get_latest_realtime_data(force_fetch: bool = False, full_format: bool = False) -> tuple[pd.DataFrame, str]:
    """
    获取最新实时数据（统一入口）
    
    Args:
        force_fetch: 是否强制获取最新数据（True=总是fetch，False=优先用缓存）
        full_format: 时间格式（True=YYYY-MM-DD HH:MM，False=HH:MM）
        
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
            match = __import__('re').search(r'realtime_(\d{8})_(\d{6})\.json', Path(rt_file).name)
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
    rt_file = get_todays_realtime_file()
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

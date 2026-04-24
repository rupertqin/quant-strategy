"""
股票价格复权工具模块

用于将不复权价格转换为前复权价格
支持股票和ETF
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def is_etf(symbol: str) -> bool:
    """
    判断是否为ETF代码

    Args:
        symbol: 代码，如 '510300.SH'

    Returns:
        bool: True表示ETF
    """
    if not symbol:
        return False
    code = symbol.replace('.SH', '').replace('.SZ', '')
    # ETF代码通常以5开头（沪市）或1/15开头（深市）
    return code.startswith(('5', '15', '16', '18'))


def load_adjust_factor(symbol: str, base_dir: Path = None) -> Optional[pd.DataFrame]:
    """
    加载复权因子数据（自动判断股票或ETF）

    Args:
        symbol: 股票/ETF代码，如 '600519.SH' 或 '510300.SH'
        base_dir: 存储根目录

    Returns:
        DataFrame with columns: trade_date, adjust_factor
    """
    if base_dir is None:
        from DataHub.config import get_storage_path
        base_dir = get_storage_path()

    # 自动判断股票或ETF
    if is_etf(symbol):
        factor_path = base_dir / "raw" / "etf" / "adjust_factor" / f"{symbol}.parquet"
    else:
        factor_path = base_dir / "raw" / "stocks" / "adjust_factor" / f"{symbol}.parquet"

    if not factor_path.exists():
        logger.debug(f"复权因子文件不存在: {factor_path}")
        return None

    try:
        df = pd.read_parquet(factor_path)
        if df.empty:
            logger.debug(f"复权因子文件为空: {symbol}")
            return None

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df
    except Exception as e:
        logger.error(f"加载复权因子失败 {symbol}: {e}")
        return None


def convert_to_qfq(
    price_df: pd.DataFrame,
    factor_df: Optional[pd.DataFrame] = None,
    symbol: str = None
) -> pd.DataFrame:
    """
    将不复权价格转换为前复权价格
    
    公式: 前复权价格 = 不复权价格 × (当天复权因子 / 最新复权因子)
    
    Args:
        price_df: 价格数据DataFrame，必须包含 trade_date, open, high, low, close 列
        factor_df: 复权因子DataFrame，包含 trade_date, adjust_factor 列
        symbol: 股票代码（用于加载复权因子，如果factor_df为None）
        
    Returns:
        DataFrame with qfq_ prefixed columns added or replaced
    """
    df = price_df.copy()
    
    # 确保日期格式正确
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    # 加载复权因子
    if factor_df is None and symbol is not None:
        factor_df = load_adjust_factor(symbol)
    
    if factor_df is None or factor_df.empty:
        # 没有复权因子文件表示该股票从未分红送股，无需复权
        logger.debug(f"{symbol} 无复权因子（从未分红），使用原始价格")
        return df
    
    # 合并价格数据和复权因子
    merged = pd.merge(df, factor_df, on='trade_date', how='left')
    
    # 向前填充缺失的复权因子
    merged['adjust_factor'] = merged['adjust_factor'].ffill()
    
    # 获取最新复权因子（作为前复权基准）
    latest_factor = merged['adjust_factor'].iloc[-1]
    
    if pd.isna(latest_factor) or latest_factor == 0:
        logger.warning("最新复权因子无效，返回原始价格")
        return df
    
    # 计算复权比例
    merged['adjust_ratio'] = merged['adjust_factor'] / latest_factor
    
    # 转换价格列（开、高、低、收）
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        if col in merged.columns:
            merged[f'{col}_qfq'] = merged[col] * merged['adjust_ratio']
    
    # 成交量复权（可选，保持成交额不变）
    if 'volume' in merged.columns:
        merged['volume_qfq'] = merged['volume'] / merged['adjust_ratio']
    
    # 用前复权价格替换原始价格（保持兼容性）
    for col in price_cols:
        if f'{col}_qfq' in merged.columns:
            merged[col] = merged[f'{col}_qfq']
    
    if 'volume_qfq' in merged.columns:
        merged['volume'] = merged['volume_qfq']
    
    # 删除中间列
    merged = merged.drop(columns=['adjust_factor', 'adjust_ratio'], errors='ignore')
    
    return merged


def get_latest_price_qfq(symbol: str, base_dir: Path = None) -> Optional[float]:
    """
    获取股票/ETF最新前复权价格

    Args:
        symbol: 股票/ETF代码
        base_dir: 存储根目录

    Returns:
        最新前复权收盘价，如果失败返回None
    """
    if base_dir is None:
        from DataHub.config import get_storage_path
        base_dir = get_storage_path()

    # 自动判断股票或ETF
    if is_etf(symbol):
        price_path = base_dir / "raw" / "etf" / "price" / f"{symbol}.parquet"
    else:
        price_path = base_dir / "raw" / "stocks" / "price" / f"{symbol}.parquet"

    if not price_path.exists():
        return None

    try:
        df = pd.read_parquet(price_path)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date')

        # 转换为前复权
        df_qfq = convert_to_qfq(df, symbol=symbol)

        if not df_qfq.empty and 'close' in df_qfq.columns:
            return float(df_qfq['close'].iloc[-1])
    except Exception as e:
        logger.error(f"获取最新价格失败 {symbol}: {e}")

    return None


def get_price_at_date_qfq(
    symbol: str,
    date: str,
    base_dir: Path = None
) -> Optional[dict]:
    """
    获取指定日期的前复权价格

    Args:
        symbol: 股票/ETF代码
        date: 日期，格式 'YYYY-MM-DD'
        base_dir: 存储根目录

    Returns:
        dict with keys: open, high, low, close, volume
    """
    if base_dir is None:
        from DataHub.config import get_storage_path
        base_dir = get_storage_path()

    # 自动判断股票或ETF
    if is_etf(symbol):
        price_path = base_dir / "raw" / "etf" / "price" / f"{symbol}.parquet"
    else:
        price_path = base_dir / "raw" / "stocks" / "price" / f"{symbol}.parquet"

    if not price_path.exists():
        return None

    try:
        df = pd.read_parquet(price_path)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date')

        # 转换为前复权
        df_qfq = convert_to_qfq(df, symbol=symbol)

        # 查找指定日期
        target_date = pd.to_datetime(date)
        day_data = df_qfq[df_qfq['trade_date'] == target_date]

        if not day_data.empty:
            row = day_data.iloc[0]
            return {
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row.get('volume', 0))
            }
    except Exception as e:
        logger.error(f"获取日期价格失败 {symbol} {date}: {e}")

    return None

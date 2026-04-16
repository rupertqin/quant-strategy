"""
数据读取接口 - 底层封装

提供统一的股票数据读取接口，默认返回前复权数据
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import logging

from DataHub.config import RAW_PRICES_DIR

logger = logging.getLogger(__name__)


def load_stock_prices(
    symbol: str,
    start_date: str = None,
    end_date: str = None,
    adjust: str = "qfq",
    base_dir: Path = None
) -> pd.DataFrame:
    """
    加载股票历史价格数据（默认前复权）

    Args:
        symbol: 股票代码，如 '600519.SH'
        start_date: 开始日期 'YYYY-MM-DD'，None表示从最早开始
        end_date: 结束日期 'YYYY-MM-DD'，None表示到最新
        adjust: 复权方式 - "qfq"(前复权)/None(不复权)，默认前复权
        base_dir: 存储根目录，默认使用项目 storage 目录

    Returns:
        DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
    """
    if base_dir is None:
        base_dir = RAW_PRICES_DIR

    price_path = base_dir / f"{symbol}.parquet"

    if not price_path.exists():
        logger.warning(f"价格数据不存在: {price_path}")
        return pd.DataFrame()

    try:
        # 加载原始价格
        df = pd.read_parquet(price_path)
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        # 日期过滤
        if start_date:
            df = df[df['trade_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['trade_date'] <= pd.to_datetime(end_date)]

        # 前复权转换（默认）
        if adjust == "qfq":
            from Dashboard.utils.adjustment import convert_to_qfq
            df = convert_to_qfq(df, symbol=symbol)
            if df is None or df.empty:
                logger.warning(f"前复权转换失败: {symbol}")
                return pd.DataFrame()

        # 过滤无效数据
        df = df[df['volume'] > 0]
        df = df[df['close'] > 0]

        # 按日期排序
        df = df.sort_values('trade_date').reset_index(drop=True)

        return df

    except Exception as e:
        logger.error(f"加载股票数据失败 {symbol}: {e}")
        return pd.DataFrame()


def load_stock_prices_raw(
    symbol: str,
    start_date: str = None,
    end_date: str = None,
    base_dir: Path = None
) -> pd.DataFrame:
    """
    加载股票历史价格数据（不复权）

    用于需要原始价格的场景（如真实成交计算）
    """
    return load_stock_prices(symbol, start_date, end_date, adjust=None, base_dir=base_dir)


def load_stock_latest_price(
    symbol: str,
    adjust: str = "qfq",
    base_dir: Path = None
) -> Optional[float]:
    """
    获取股票最新收盘价（默认前复权）

    Returns:
        最新收盘价，失败返回 None
    """
    df = load_stock_prices(symbol, adjust=adjust, base_dir=base_dir)
    if not df.empty and 'close' in df.columns:
        return float(df['close'].iloc[-1])
    return None


def load_stock_latest_date(
    symbol: str,
    base_dir: Path = None
) -> Optional[str]:
    """
    获取股票最新数据日期

    Returns:
        最新日期字符串 'YYYY-MM-DD'，失败返回 None
    """
    if base_dir is None:
        base_dir = RAW_PRICES_DIR

    price_path = base_dir / f"{symbol}.parquet"

    if not price_path.exists():
        return None

    try:
        # 只读取 trade_date 列的最后一行（高效）
        df = pd.read_parquet(price_path, columns=['trade_date'])
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            latest_date = df['trade_date'].max()
            return latest_date.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"获取最新日期失败 {symbol}: {e}")

    return None


def load_stock_price_at_date(
    symbol: str,
    date: str,
    adjust: str = "qfq",
    base_dir: Path = None
) -> Optional[dict]:
    """
    获取指定日期的股票价格（默认前复权）

    Args:
        symbol: 股票代码
        date: 日期 'YYYY-MM-DD'
        adjust: 复权方式
        base_dir: 存储根目录

    Returns:
        dict with keys: open, high, low, close, volume, 失败返回 None
    """
    df = load_stock_prices(symbol, start_date=date, end_date=date, adjust=adjust, base_dir=base_dir)
    if not df.empty:
        row = df.iloc[0]
        return {
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row.get('volume', 0))
        }
    return None


def load_multiple_stocks(
    symbols: list,
    start_date: str = None,
    end_date: str = None,
    adjust: str = "qfq",
    base_dir: Path = None
) -> dict:
    """
    批量加载多只股票数据

    Args:
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        adjust: 复权方式
        base_dir: 存储根目录

    Returns:
        dict: {symbol: DataFrame}
    """
    result = {}
    for symbol in symbols:
        df = load_stock_prices(symbol, start_date, end_date, adjust, base_dir)
        if not df.empty:
            result[symbol] = df
    return result

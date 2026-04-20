"""
数据读取接口 - 底层封装

提供统一的股票/ETF数据读取接口，默认返回前复权数据
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import logging

from DataHub.config import RAW_PRICE_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR

logger = logging.getLogger(__name__)


def is_etf(symbol: str) -> bool:
    """
    判断代码是否为ETF
    
    ETF代码规则:
    - 沪市ETF: 以 51, 52, 56, 58, 59 开头，后缀 .SH
    - 深市ETF: 以 15, 16 开头，后缀 .SZ
    """
    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
    
    # 沪市ETF
    if symbol.endswith('.SH'):
        return code.startswith(('51', '52', '56', '58', '59'))
    
    # 深市ETF
    if symbol.endswith('.SZ'):
        return code.startswith(('15', '16'))
    
    return False


def is_index(symbol: str) -> bool:
    """
    判断代码是否为指数
    
    指数代码规则:
    - 上海指数: 000001(上证指数), 000300(沪深300) 等，以000开头
    - 深圳指数: 399001(深证成指), 399006(创业板指) 等，以399开头
    
    从 official_indices.csv 读取指数代码列表
    """
    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
    
    if not code.isdigit():
        return False
    
    return code in _get_index_codes()


# 缓存指数代码集合
_index_codes_cache = None


def _get_index_codes():
    """从 official_indices.csv 读取所有指数代码"""
    global _index_codes_cache
    
    if _index_codes_cache is not None:
        return _index_codes_cache
    
    _index_codes_cache = set()
    csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'
    
    if csv_path.exists():
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get('code', '').strip()
                    if code:
                        _index_codes_cache.add(code)
        except Exception:
            pass
    
    return _index_codes_cache


def get_symbol_data_path(symbol: str) -> Path:
    """获取代码对应的数据文件路径（自动判断股票/ETF/指数）"""
    if is_etf(symbol):
        return RAW_ETF_PRICE_DIR / f"{symbol}.parquet"
    elif is_index(symbol):
        return RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
    else:
        return RAW_PRICE_DIR / f"{symbol}.parquet"


def load_stock_prices(
    symbol: str,
    start_date: str = None,
    end_date: str = None,
    adjust: str = "qfq",
    base_dir: Path = None
) -> pd.DataFrame:
    """
    加载股票/ETF历史价格数据（默认前复权）

    自动识别股票或ETF，从对应目录加载数据

    Args:
        symbol: 股票代码，如 '600519.SH' 或 '510300.SH'
        start_date: 开始日期 'YYYY-MM-DD'，None表示从最早开始
        end_date: 结束日期 'YYYY-MM-DD'，None表示到最新
        adjust: 复权方式 - "qfq"(前复权)/None(不复权)，默认前复权
        base_dir: 存储根目录（已废弃，保留参数兼容性）

    Returns:
        DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
    """
    # 自动判断股票/ETF并获取路径
    price_path = get_symbol_data_path(symbol)

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

        # 复权处理
        if adjust == "qfq":
            df = _apply_qfq_adjustment(df, symbol)

        return df

    except Exception as e:
        logger.error(f"加载价格数据失败 {symbol}: {e}")
        return pd.DataFrame()


def load_stock_prices_raw(
    symbol: str,
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    """
    加载股票/ETF原始价格数据（不复权）

    Args:
        symbol: 股票代码，如 '600519.SH'
        start_date: 开始日期 'YYYY-MM-DD'，None表示从最早开始
        end_date: 结束日期 'YYYY-MM-DD'，None表示到最新

    Returns:
        DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
    """
    return load_stock_prices(symbol, start_date, end_date, adjust=None)


def _apply_qfq_adjustment(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    应用前复权调整

    Args:
        df: 原始价格数据
        symbol: 股票代码

    Returns:
        前复权后的价格数据
    """
    if df.empty:
        return df

    # 获取复权因子数据路径
    adj_path = _get_adjust_factor_path(symbol)

    if not adj_path or not adj_path.exists():
        # 如果没有复权因子数据，直接返回原始价格
        return df

    try:
        # 加载复权因子
        adj_df = pd.read_parquet(adj_path)
        adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date'])

        # 合并价格数据和复权因子
        df = df.merge(adj_df[['trade_date', 'adj_factor']], on='trade_date', how='left')

        # 使用最新的复权因子作为基准
        if 'adj_factor' in df.columns and not df['adj_factor'].isna().all():
            latest_factor = df['adj_factor'].iloc[-1]

            # 计算前复权价格
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = df[col] * df['adj_factor'] / latest_factor

            # 删除临时列
            df = df.drop(columns=['adj_factor'])

        return df

    except Exception as e:
        logger.warning(f"应用复权失败 {symbol}: {e}")
        return df


def _get_adjust_factor_path(symbol: str) -> Optional[Path]:
    """获取复权因子数据文件路径"""
    from DataHub.config import RAW_ADJUST_FACTOR_DIR

    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')

    # 根据资产类型确定路径
    if is_etf(symbol):
        # ETF通常不需要复权
        return None
    elif is_index(symbol):
        # 指数不需要复权
        return None
    else:
        return RAW_ADJUST_FACTOR_DIR / f"{code}.parquet"


def get_index_codes_from_csv() -> list:
    """
    从 official_indices.csv 读取所有指数代码列表

    Returns:
        指数代码列表，格式如 ['000001.SH', '399001.SZ', ...]
    """
    codes = []
    csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'

    if csv_path.exists():
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get('symbol', '').strip()
                    if symbol:
                        codes.append(symbol)
        except Exception as e:
            logger.warning(f"读取指数列表失败: {e}")

    return codes


def get_index_name_mapper() -> dict:
    """
    获取指数代码到名称的映射

    Returns:
        dict: {symbol: name}
    """
    mapper = {}
    csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'

    if csv_path.exists():
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get('symbol', '').strip()
                    name = row.get('name', '').strip()
                    if symbol and name:
                        mapper[symbol] = name
        except Exception as e:
            logger.warning(f"读取指数名称映射失败: {e}")

    return mapper

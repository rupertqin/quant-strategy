"""
数据读取接口 - 底层封装

提供统一的股票/ETF数据读取接口，默认返回前复权数据
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import logging

from DataHub.config import RAW_PRICE_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR, RAW_INDEX_INTRADAY_DIR

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
    判断代码是否为指数（结合后缀判断）
    
    指数代码规则:
    - 上海指数: 000001.SH(上证指数), 000300.SH(沪深300) 等
    - 深圳指数: 399001.SZ(深证成指), 399006.SZ(创业板指) 等
    
    注意：代码重名问题！
    - 000001.SH = 上证指数（指数）
    - 000001.SZ = 平安银行（股票）
    
    因此必须结合后缀判断，不能只看6位代码。
    
    硬编码规则（无需查表）：
    - 以 399 开头 + .SZ 后缀 = 深证指数
    
    查表确认（official_indices.csv）：
    - 以 000 开头 + .SH 后缀 = 可能是上证指数
    """
    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
    
    if not code.isdigit():
        return False
    
    # 硬编码规则：399xxx.SZ 一定是深证指数（深证成指、创业板指等）
    if symbol.endswith('.SZ') and code.startswith('399'):
        return True
    
    # 其他情况查表确认（如 000001.SH 上证指数、000300.SH 沪深300等）
    return code in _get_index_codes()


# 缓存指数代码集合
_index_codes_cache = None


def clear_index_cache():
    """清除指数代码缓存（用于调试或文件更新后）"""
    global _index_codes_cache
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


def load_stock_latest_price(symbol: str) -> Optional[float]:
    """
    获取股票最新收盘价

    Args:
        symbol: 股票代码，如 '600519.SH'

    Returns:
        最新收盘价，如果数据不存在返回 None
    """
    df = load_stock_prices(symbol)
    if df.empty:
        return None
    return df['close'].iloc[-1]


def load_stock_latest_date(symbol: str) -> Optional[str]:
    """
    获取股票最新数据日期

    Args:
        symbol: 股票代码，如 '600519.SH'

    Returns:
        最新日期字符串 'YYYY-MM-DD'，如果数据不存在返回 None
    """
    df = load_stock_prices(symbol)
    if df.empty:
        return None
    latest_date = df['trade_date'].iloc[-1]
    if isinstance(latest_date, pd.Timestamp):
        return latest_date.strftime('%Y-%m-%d')
    return str(latest_date).split()[0]


def load_index_intraday(symbol: str, date: str = None) -> pd.DataFrame:
    """
    加载指数分时数据（1分钟线）

    优先从本地存储读取，如果不存在则返回空 DataFrame
    调用方可以使用 DataHub.core.data_client.UnifiedDataClient.get_index_intraday 获取实时数据并保存

    Args:
        symbol: 指数代码，如 '000001.SH'
        date: 日期 'YYYYMMDD'，None 表示最新日期

    Returns:
        DataFrame with columns: time, open, high, low, close, volume, amount, symbol
    """
    # 构建文件路径
    if date:
        file_path = RAW_INDEX_INTRADAY_DIR / f"{symbol}_{date}.parquet"
    else:
        # 查找最新的文件
        pattern = f"{symbol}_*.parquet"
        files = sorted(RAW_INDEX_INTRADAY_DIR.glob(pattern), reverse=True)
        if not files:
            logger.debug(f"指数分时数据不存在: {symbol}")
            return pd.DataFrame()
        file_path = files[0]

    if not file_path.exists():
        logger.debug(f"指数分时数据不存在: {file_path}")
        return pd.DataFrame()

    try:
        df = pd.read_parquet(file_path)
        logger.debug(f"加载指数分时数据成功: {file_path}, {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"加载指数分时数据失败 {file_path}: {e}")
        return pd.DataFrame()


def save_index_intraday(df: pd.DataFrame, symbol: str, date: str) -> bool:
    """
    保存指数分时数据到本地存储

    Args:
        df: 分时数据 DataFrame
        symbol: 指数代码，如 '000001.SH'
        date: 日期 'YYYYMMDD'

    Returns:
        是否保存成功
    """
    if df is None or df.empty:
        logger.warning(f"没有数据可保存: {symbol} {date}")
        return False

    try:
        file_path = RAW_INDEX_INTRADAY_DIR / f"{symbol}_{date}.parquet"
        df.to_parquet(file_path, index=False)
        logger.info(f"保存指数分时数据成功: {file_path}, {len(df)} 条")
        return True
    except Exception as e:
        logger.error(f"保存指数分时数据失败 {symbol} {date}: {e}")
        return False

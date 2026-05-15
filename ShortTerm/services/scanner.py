"""
涨停板扫描器
每日收盘后扫描全市场涨停板，分析板块热度
使用 DataHub 统一数据管理
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
import logging

from DataHub.core.data_client import UnifiedDataClient
from .market_regime import MarketRegime

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


def get_trading_date(dt: datetime = None) -> str:
    """
    获取当前交易日日期字符串 (YYYYMMDD)

    A股交易时间规则：
    - 交易日：周一到周五（节假日除外）
    - 交易时间：9:30-11:30, 13:00-15:00（盘前竞价从9:20开始）
    - 16:00收盘后（考虑港股）到第二天9:20盘前，算作前一个交易日

    Args:
        dt: 指定时间，默认为当前时间

    Returns:
        交易日日期字符串，格式 'YYYYMMDD'
    """
    if dt is None:
        dt = datetime.now()

    # A股盘前竞价开始时间 9:20，在此之前算作前一个交易日
    market_open_time = dt.replace(hour=9, minute=20, second=0, microsecond=0)

    # 如果当前时间在当天9:20之前，算作前一个交易日
    if dt < market_open_time:
        dt = dt - timedelta(days=1)

    # 处理周末：如果结果是周六或周日，回退到周五
    while dt.weekday() >= 5:  # 5=周六, 6=周日
        dt = dt - timedelta(days=1)

    return dt.strftime('%Y%m%d')


def get_trading_date_str(dt: datetime = None) -> str:
    """
    获取当前交易日日期字符串 (YYYY-MM-DD)
    """
    if dt is None:
        dt = datetime.now()

    # A股盘前竞价开始时间 9:20，在此之前算作前一个交易日
    market_open_time = dt.replace(hour=9, minute=20, second=0, microsecond=0)

    if dt < market_open_time:
        dt = dt - timedelta(days=1)

    while dt.weekday() >= 5:
        dt = dt - timedelta(days=1)

    return dt.strftime('%Y-%m-%d')


def get_data_close_time(dt: datetime = None) -> tuple[str, str]:
    """
    根据当前运行时间，确定数据收盘时间和状态

    A股交易时间规则：
    - 盘前: 00:00-09:30 → 数据时间为上一交易日 15:00
    - 盘中: 09:30-15:00 → 数据时间为当前时间（实时）
    - 盘后: 15:00-24:00 → 数据时间为当天 15:00
    - 周末/节假日：回退到最近一个交易日 15:00

    优先使用API获取交易日历，失败时使用本地计算

    Args:
        dt: 指定时间，默认为当前时间

    Returns:
        (数据时间字符串, 状态描述)
    """
    if dt is None:
        dt = datetime.now()

    # 定义交易时间边界
    market_open_am = dt.replace(hour=9, minute=30, second=0, microsecond=0)   # 上午开盘
    market_close_noon = dt.replace(hour=11, minute=30, second=0, microsecond=0)  # 上午收盘
    market_open_pm = dt.replace(hour=13, minute=0, second=0, microsecond=0)   # 下午开盘
    market_close = dt.replace(hour=15, minute=0, second=0, microsecond=0)     # 下午收盘

    # 尝试使用API获取交易日历
    def get_last_trading_date_from_api(check_dt: datetime) -> datetime | None:
        """从API获取最近交易日"""
        try:
            import akshare as ak
            # 获取最近7天的交易日历
            end_date = check_dt.strftime('%Y-%m-%d')
            start_date = (check_dt - timedelta(days=7)).strftime('%Y-%m-%d')
            df = ak.tool_trading_date()
            mask = (df["calendarDate"] >= start_date) & (df["calendarDate"] <= end_date)
            trading_dates = df[mask]["calendarDate"].tolist()
            if trading_dates:
                # 返回最近一个交易日
                last_date = trading_dates[-1]
                return datetime.strptime(last_date, '%Y-%m-%d')
        except Exception:
            pass
        return None

    # 判断当前是否为交易时段
    is_trading_hours = (market_open_am <= dt < market_close) and dt.weekday() < 5

    # 盘前：使用上一交易日收盘时间
    if dt < market_open_am:
        # 优先使用API
        last_trading = get_last_trading_date_from_api(dt - timedelta(days=1))
        if last_trading:
            close_time = last_trading.replace(hour=15, minute=0, second=0)
            return close_time.strftime('%Y-%m-%d %H:%M:%S'), "盘前（API）"
        # 回退：本地计算
        prev_trading_day = dt - timedelta(days=1)
        while prev_trading_day.weekday() >= 5:  # 跳过周末
            prev_trading_day = prev_trading_day - timedelta(days=1)
        close_time = prev_trading_day.replace(hour=15, minute=0, second=0)
        return close_time.strftime('%Y-%m-%d %H:%M:%S'), "盘前（本地）"

    # 如果是周末，回退到最近交易日
    if dt.weekday() >= 5:  # 5=周六, 6=周日
        last_trading = get_last_trading_date_from_api(dt)
        if last_trading:
            close_time = last_trading.replace(hour=15, minute=0, second=0)
            return close_time.strftime('%Y-%m-%d %H:%M:%S'), "周末（API）"
        # 回退：本地计算（周五）
        days_back = dt.weekday() - 4  # 周六=1, 周日=2
        friday = dt - timedelta(days=days_back)
        close_time = friday.replace(hour=15, minute=0, second=0)
        return close_time.strftime('%Y-%m-%d %H:%M:%S'), "周末（本地）"

    # 盘后：使用当天收盘时间，但先验证当天是否为交易日
    if dt >= market_close:
        # 检查今天是否为交易日
        today_str = dt.strftime('%Y-%m-%d')
        try:
            import akshare as ak
            df = ak.tool_trading_date()
            if today_str in df["calendarDate"].values:
                close_time = dt.replace(hour=15, minute=0, second=0)
                return close_time.strftime('%Y-%m-%d %H:%M:%S'), "盘后收盘（API）"
            else:
                # 今天不是交易日，找上一个交易日
                last_trading = get_last_trading_date_from_api(dt)
                if last_trading:
                    close_time = last_trading.replace(hour=15, minute=0, second=0)
                    return close_time.strftime('%Y-%m-%d %H:%M:%S'), "节假日（API）"
        except Exception:
            pass
        # 回退：假设今天是交易日
        close_time = dt.replace(hour=15, minute=0, second=0)
        return close_time.strftime('%Y-%m-%d %H:%M:%S'), "收盘"

    # 午间休市
    if market_close_noon <= dt < market_open_pm:
        close_time = dt.replace(hour=11, minute=30, second=0)
        return close_time.strftime('%Y-%m-%d %H:%M:%S'), "午间休市"

    # 盘中：使用当前时间
    close_time = dt.replace(hour=15, minute=0, second=0)
    return dt.strftime('%Y-%m-%d %H:%M:%S'), "盘中实时"


class LimitUpScanner:
    """涨停板扫描器"""

    def __init__(self, config_path: str = None, use_datahub: bool = True):
        from DataHub.config import get_storage_path
        # 自动查找 config.yaml
        if config_path is None:
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            config_path = os.path.join(parent_dir, "config.yaml")

        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.base_dir = os.path.dirname(config_path)
        self.cache_dir = self.config['cache']['dir']
        if not os.path.isabs(self.cache_dir):
            self.cache_dir = str(get_storage_path(self.cache_dir))
        os.makedirs(self.cache_dir, exist_ok=True)

        self.data_client = UnifiedDataClient()

        # 市场状态判断（宏观+技术）
        self.market_regime = MarketRegime(config_path)

        # 主要指数代码映射（从 official_indices.csv 读取）
        from lib.utils.stock_code import StockCodeUtil
        self.CORE_INDICES = StockCodeUtil.get_core_indices()
        if not self.CORE_INDICES:
            raise RuntimeError("无法读取 official_indices.csv，请确保文件存在且格式正确")

        logger.info("LimitUpScanner initialized")

    def _load_config(self, path: str) -> dict:
        import yaml
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {path}, using default config")
            return {
                'cache': {'dir': 'cache'},
                'event_params': {'min_zt_count': 3},
                'output': {'signals_file': 'signals.json', 'history_file': 'history.csv'}
            }

    def get_today_zt_pool(self, date: str = None) -> pd.DataFrame:
        """
        获取某日涨停板数据

        Args:
            date: 日期，格式 'YYYYMMDD'，默认当前交易日（考虑A股开盘时间）

        Returns:
            涨停板 DataFrame
        """
        if date is None:
            date = get_trading_date()

        # 优先从本地缓存获取
        cache_file = os.path.join(self.cache_dir, f"zt_pool_{date}.csv")

        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            if not df.empty:
                return df

        # 从网络获取
        try:
            df = self.data_client.get_zt_pool(date)

            if not df.empty:
                df.to_csv(cache_file, index=False, encoding='utf-8-sig')

            return df
        except Exception as e:
            print(f"获取 {date} 涨停数据失败: {e}")
            return pd.DataFrame()

    def get_industry_index_change(self, date: str = None) -> pd.DataFrame:
        """获取行业指数涨跌幅"""
        if date is None:
            date = get_trading_date()

        try:
            return self.data_client.get_industry_list()
        except Exception as e:
            logger.warning(f"获取行业指数失败: {e}")
            return pd.DataFrame()

    def calculate_sector_heat(self, df_zt: pd.DataFrame) -> pd.DataFrame:
        """
        计算板块热度（涨停家数）

        Args:
            df_zt: 涨停板数据，需包含 '所属行业' 列

        Returns:
            每个板块的涨停家数
        """
        if df_zt.empty or '所属行业' not in df_zt.columns:
            return pd.DataFrame()

        heat = df_zt.groupby('所属行业').size().reset_index(name='limit_up_count')
        heat['date'] = get_trading_date()

        return heat

    def get_industry_list(self) -> pd.DataFrame:
        """获取同花顺行业分类列表"""
        try:
            return self.data_client.get_industry_list()
        except Exception as e:
            logger.warning(f"获取行业列表失败: {e}")
            return pd.DataFrame()

    def analyze_sector_performance(self, sector: str, days: int = 5) -> dict:
        """
        分析某板块近期表现

        Args:
            sector: 板块名称
            days: 回看天数

        Returns:
            绩效字典
        """
        try:
            df = self.data_client.get_industry_cons(sector)

            if df.empty:
                return {}

            df = df.tail(days + 1)
            if len(df) < 2:
                return {}

            change = (df['close'].iloc[-1] / df['close'].iloc[0] - 1)
            avg_change = df['pct_chg'].mean() if 'pct_chg' in df.columns else 0

            return {
                'sector': sector,
                'period_return': change,
                'avg_daily_change': avg_change,
                'volatility': df['pct_chg'].std() if 'pct_chg' in df.columns else 0
            }
        except Exception as e:
            logger.warning(f"分析板块 {sector} 表现失败: {e}")
            return {}

    def _calculate_market_breadth_from_local(self, date_str: str = None) -> dict:
        """
        从本地股价数据计算涨跌家数

        Args:
            date_str: 日期 'YYYYMMDD'，None表示最新日期

        Returns:
            {'up': int, 'down': int, 'flat': int, 'total': int, 'up_ratio': float, 'breadth_score': float}
        """
        from DataHub.config import RAW_PRICE_DIR

        if date_str is None:
            date_str = get_trading_date()

        up_count = down_count = flat_count = 0

        try:
            price_files = list(RAW_PRICE_DIR.glob("*.parquet"))
            target_date = pd.to_datetime(date_str).date()

            for file_path in price_files:
                try:
                    df = pd.read_parquet(file_path)
                    if df.empty or 'trade_date' not in df.columns or 'change_pct' not in df.columns:
                        continue

                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    day_data = df[df['trade_date'] == target_date]

                    if day_data.empty:
                        continue

                    change_pct = day_data.iloc[0]['change_pct']
                    if pd.isna(change_pct):
                        continue

                    if change_pct > 0:
                        up_count += 1
                    elif change_pct < 0:
                        down_count += 1
                    else:
                        flat_count += 1
                except Exception:
                    continue

            total = up_count + down_count + flat_count
            up_ratio = up_count / total if total > 0 else 0.5
            breadth_score = (up_ratio - 0.5) * 200  # 范围 -100 到 100

            return {
                'up': up_count,
                'down': down_count,
                'flat': flat_count,
                'total': total,
                'up_ratio': up_ratio,
                'breadth_score': breadth_score
            }
        except Exception as e:
            logger.warning(f"从本地数据计算涨跌家数失败: {e}")
            return {'up': 0, 'down': 0, 'flat': 0, 'total': 0, 'up_ratio': 0.5, 'breadth_score': 0}

    def _is_realtime_data_fresh(self, date: str = None, benchmark_symbol: str = '000001.SZ') -> bool:
        """
        判断实时数据是否比冷数据更新

        通过比较 realtime parquet 和冷数据中同一只基准股票的最新日期来判断。
        只有当 realtime 数据的日期 > 冷数据日期时，才认为真正使用了实时数据。

        Args:
            date: 日期 YYYYMMDD，默认今天
            benchmark_symbol: 基准股票代码，默认 000001.SZ

        Returns:
            True: realtime 数据比冷数据新（真正使用了实时数据）
            False: realtime 数据不新于冷数据（未使用实时数据或数据已过时）
        """
        from DataHub.config import RAW_PRICE_DIR
        from DataHub.services.realtime_service import RealtimeDataService
        from datetime import datetime

        today = date if date else datetime.now().strftime('%Y%m%d')

        # 1. 获取冷数据中基准股票的最新日期
        cold_latest_date = None
        cold_file = RAW_PRICE_DIR / f"{benchmark_symbol}.parquet"
        if cold_file.exists():
            try:
                df_cold = pd.read_parquet(cold_file)
                if not df_cold.empty and 'trade_date' in df_cold.columns:
                    cold_latest_date = pd.to_datetime(df_cold['trade_date']).max().date()
            except Exception as e:
                logger.debug(f"读取冷数据最新日期失败: {e}")

        # 2. 获取 realtime 数据中基准股票的最新日期
        rt_latest_date = None
        try:
            rt_df = RealtimeDataService().load_intraday_parquet(
                date_str=today, asset_type='stock', latest_snapshot=True
            )
            if rt_df is not None and not rt_df.empty:
                rt_row = rt_df[rt_df['symbol'] == benchmark_symbol]
                if not rt_row.empty and 'timestamp' in rt_row.columns:
                    rt_timestamp = pd.to_datetime(rt_row['timestamp'].iloc[0])
                    rt_latest_date = rt_timestamp.date()
        except Exception as e:
            logger.debug(f"读取实时数据最新日期失败: {e}")

        # 3. 比较日期
        if rt_latest_date is None:
            print(f"  ⏱️ 实时数据新鲜度检查: 无实时数据")
            return False
        if cold_latest_date is None:
            print(f"  ⏱️ 实时数据新鲜度检查: 无冷数据，默认使用实时数据")
            return True

        is_fresh = rt_latest_date > cold_latest_date
        print(f"  ⏱️ 实时数据新鲜度检查: 冷数据{cold_latest_date} vs 实时数据{rt_latest_date} -> {'新鲜' if is_fresh else '不新鲜'}")
        return is_fresh

    def _calculate_market_breadth_from_realtime(self, date: str = None) -> dict:
        """
        从分钟级实时 parquet 计算涨跌家数（每个 symbol 取最新快照）

        Args:
            date: 日期 YYYYMMDD，默认今天

        Returns:
            {'up': int, 'down': int, 'flat': int, 'total': int, 'up_ratio': float, 'breadth_score': float}
        """
        try:
            from DataHub.services.realtime_service import RealtimeDataService
            from datetime import datetime

            today = date if date else datetime.now().strftime('%Y%m%d')
            service = RealtimeDataService()

            df = service.load_intraday_parquet(
                date_str=today, asset_type='stock', latest_snapshot=True
            )
            if df is None or df.empty:
                print(f"    ⚠️ 未找到今日实时数据 parquet")
                return {'up': 0, 'down': 0, 'flat': 0, 'total': 0, 'up_ratio': 0.5, 'breadth_score': 0}

            print(f"    从实时 parquet 计算涨跌家数: {len(df)} 只")

            up_count = down_count = flat_count = 0
            for _, row in df.iterrows():
                change_pct = row.get('change_pct')
                if change_pct is None or pd.isna(change_pct):
                    continue
                if change_pct > 0:
                    up_count += 1
                elif change_pct < 0:
                    down_count += 1
                else:
                    flat_count += 1

            total = up_count + down_count + flat_count
            up_ratio = up_count / total if total > 0 else 0.5
            breadth_score = (up_ratio - 0.5) * 200  # 范围 -100 到 100

            print(f"    计算结果: 涨{up_count}/跌{down_count}/平{flat_count}, 总计{total}")

            return {
                'up': up_count,
                'down': down_count,
                'flat': flat_count,
                'total': total,
                'up_ratio': up_ratio,
                'breadth_score': breadth_score
            }
        except Exception as e:
            print(f"    ⚠️ 从实时 parquet 计算涨跌家数失败: {e}")
            logger.debug(f"从实时 parquet 计算涨跌家数失败: {e}")
            return {'up': 0, 'down': 0, 'flat': 0, 'total': 0, 'up_ratio': 0.5, 'breadth_score': 0}

    def _get_limit_threshold(self, symbol: str, stock_name: str = None) -> dict:
        """
        根据股票代码和名称获取涨跌幅限制阈值

        支持多种代码格式:
        - '300001.SZ', '688001.SH' (带后缀)
        - 'sz300001', 'sh688001', 'bj920001' (新浪格式带前缀)
        - '300001', '688001' (纯数字)

        Args:
            symbol: 股票代码
            stock_name: 股票名称，用于判断ST股

        Returns:
            {'up': 涨停阈值, 'down': 跌停阈值, 'type': 板块类型}
        """
        # 处理新浪格式代码 (sz300001, sh688001, bj920001)
        if symbol.lower().startswith(('sz', 'sh', 'bj')):
            code = symbol[2:]  # 去掉前缀
        else:
            # 提取纯数字代码 (去掉.SZ/.SH后缀)
            code = symbol.split('.')[0] if '.' in symbol else symbol

        # 判断是否为ST股（通过名称）
        is_st = stock_name and ('ST' in stock_name or '*ST' in stock_name)
        if is_st:
            return {'up': 4.95, 'down': -4.95, 'type': 'ST'}

        # 创业板 (300/301开头): 20%涨跌幅
        if code.startswith('300') or code.startswith('301'):
            return {'up': 19.8, 'down': -19.8, 'type': '创业板'}

        # 科创板 (688/689开头): 20%涨跌幅
        if code.startswith('688') or code.startswith('689'):
            return {'up': 19.8, 'down': -19.8, 'type': '科创板'}

        # 北交所 (8/43/83/87/88/92开头): 30%涨跌幅
        if (code.startswith('8') or code.startswith('43') or
            code.startswith('83') or code.startswith('87') or
            code.startswith('88') or code.startswith('92')):
            return {'up': 29.7, 'down': -29.7, 'type': '北交所'}

        # 主板/中小板 (00/60/68开头): 10%涨跌幅
        return {'up': 9.9, 'down': -9.9, 'type': '主板'}

    def _calculate_zt_dt_from_local(self, date_str: str = None) -> tuple:
        """
        从本地股价数据计算涨停跌停数量（区分不同板块的涨跌幅限制）

        Args:
            date_str: 日期 'YYYYMMDD'，None表示最新日期

        Returns:
            (zt_count, dt_count)
        """
        from DataHub.config import RAW_PRICE_DIR

        if date_str is None:
            date_str = get_trading_date()

        zt_count = dt_count = 0
        zt_breakdown = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}
        dt_breakdown = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}
        processed_count = 0

        try:
            price_files = list(RAW_PRICE_DIR.glob("*.parquet"))
            target_date = pd.to_datetime(date_str).date()

            print(f"    扫描 {len(price_files)} 只股票数据，日期: {target_date}")

            for file_path in price_files:
                try:
                    # 从文件名提取股票代码
                    symbol = file_path.stem

                    df = pd.read_parquet(file_path)
                    if df.empty or 'trade_date' not in df.columns or 'change_pct' not in df.columns:
                        continue

                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    day_data = df[df['trade_date'] == target_date]

                    if day_data.empty:
                        continue

                    change_pct = day_data.iloc[0]['change_pct']
                    if pd.isna(change_pct):
                        continue

                    # 获取股票名称（用于判断ST股）
                    stock_name = day_data.iloc[0].get('name', '') if 'name' in day_data.columns else None

                    # 根据股票代码和名称获取对应的涨跌幅阈值
                    threshold = self._get_limit_threshold(symbol, stock_name)
                    stock_type = threshold['type']

                    processed_count += 1

                    # 使用对应板块的阈值判断
                    if change_pct >= threshold['up']:
                        zt_count += 1
                        zt_breakdown[stock_type] = zt_breakdown.get(stock_type, 0) + 1
                    elif change_pct <= threshold['down']:
                        dt_count += 1
                        dt_breakdown[stock_type] = dt_breakdown.get(stock_type, 0) + 1
                except Exception:
                    continue

            # 打印分类统计
            print(f"    处理股票数: {processed_count}")
            print(f"    涨停分类: 主板{zt_breakdown['主板']}/创业板{zt_breakdown['创业板']}/科创板{zt_breakdown['科创板']}/北交所{zt_breakdown['北交所']}/ST{zt_breakdown['ST']}")
            print(f"    跌停分类: 主板{dt_breakdown['主板']}/创业板{dt_breakdown['创业板']}/科创板{dt_breakdown['科创板']}/北交所{dt_breakdown['北交所']}/ST{dt_breakdown['ST']}")

            return zt_count, dt_count
        except Exception as e:
            logger.warning(f"从本地数据计算涨跌停失败: {e}")
            return 0, 0

    def _calculate_zt_dt_from_realtime(self, date: str = None) -> tuple:
        """
        从分钟级实时 parquet 计算涨跌停数量（每个 symbol 取最新快照）

        Args:
            date: 日期 YYYYMMDD，默认今天

        Returns:
            (zt_count, dt_count, source, stats)
        """
        try:
            from DataHub.services.realtime_service import RealtimeDataService
            from datetime import datetime

            today = date if date else datetime.now().strftime('%Y%m%d')
            service = RealtimeDataService()

            df = service.load_intraday_parquet(
                date_str=today, asset_type='stock', latest_snapshot=True
            )
            if df is None or df.empty:
                print(f"    ⚠️ 未找到今日实时数据 parquet")
                return 0, 0, "无本地数据", {}

            print(f"    从实时 parquet 计算涨跌停: {len(df)} 只")

            zt_count = 0
            dt_count = 0
            zt_breakdown = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}
            dt_breakdown = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}

            for _, stock in df.iterrows():
                try:
                    symbol = stock.get('symbol', '')
                    change_pct = stock.get('change_pct')
                    name = stock.get('name', '')

                    if change_pct is None or pd.isna(change_pct):
                        continue

                    threshold = self._get_limit_threshold(symbol, name)
                    stock_type = threshold['type']

                    if change_pct >= threshold['up']:
                        zt_count += 1
                        zt_breakdown[stock_type] = zt_breakdown.get(stock_type, 0) + 1
                    elif change_pct <= threshold['down']:
                        dt_count += 1
                        dt_breakdown[stock_type] = dt_breakdown.get(stock_type, 0) + 1
                except Exception:
                    continue

            stats = {
                'zt_breakdown': zt_breakdown,
                'dt_breakdown': dt_breakdown,
                'total': len(df)
            }

            print(f"    计算结果: 涨停{zt_count}/跌停{dt_count}, 总计{len(df)}")
            return zt_count, dt_count, f"实时 parquet({today})", stats

        except Exception as e:
            print(f"    ⚠️ 从实时 parquet 计算涨跌停失败: {e}")
            logger.debug(f"从实时 parquet 计算涨跌停失败: {e}")
            return 0, 0, "失败", {}

    def _calculate_index_performance_from_local(self) -> dict:
        """
        从本地指数数据文件计算主要指数表现（包含道氏理论和波浪理论分析）

        Returns:
            {指数名称: {'change': float, 'close': float, 'trend': str, 'dow_theory': {...}, 'elliott_wave': {...}}}
        """
        from DataHub.config import RAW_INDEX_PRICE_DIR

        result = {}

        try:
            for symbol, name in self.CORE_INDICES.items():
                try:
                    file_path = RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
                    if not file_path.exists():
                        continue

                    df = pd.read_parquet(file_path)
                    if df.empty or 'trade_date' not in df.columns or 'change_pct' not in df.columns or 'close' not in df.columns:
                        continue

                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    df = df.sort_values('trade_date')

                    latest = df.iloc[-1]
                    change_pct = latest.get('change_pct', 0)
                    close = latest.get('close', 0)

                    # 判断趋势
                    if len(df) >= 20:
                        ma20 = df['close'].rolling(20).mean().iloc[-1]
                        trend = 'UP' if close > ma20 else 'DOWN'
                    else:
                        trend = 'NEUTRAL'

                    # 准备技术分析用的DataFrame（需要标准列名）
                    tech_df = df.rename(columns={
                        'trade_date': 'date',
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume'
                    }).copy()

                    # 确保date列为字符串格式
                    tech_df['date'] = tech_df['date'].astype(str)

                    # === 三周期分析：日线 / 周线 / 月线 ===
                    # 日线
                    dow_daily = self.market_regime._dow_theory_analysis(tech_df)
                    elliott_daily = self.market_regime._elliott_wave_analysis(tech_df)

                    # 周线
                    df_weekly = self.market_regime.resample_ohlcv(tech_df, 'weekly')
                    dow_weekly = self.market_regime._dow_theory_analysis(df_weekly) if not df_weekly.empty else {'primary_trend': 'UNKNOWN', 'note': '数据不足'}
                    elliott_weekly = self.market_regime._elliott_wave_analysis(df_weekly) if not df_weekly.empty else {'current_phase': 'unknown', 'note': '数据不足'}

                    # 月线
                    df_monthly = self.market_regime.resample_ohlcv(tech_df, 'monthly')
                    dow_monthly = self.market_regime._dow_theory_analysis(df_monthly) if not df_monthly.empty else {'primary_trend': 'UNKNOWN', 'note': '数据不足'}
                    elliott_monthly = self.market_regime._elliott_wave_analysis(df_monthly) if not df_monthly.empty else {'current_phase': 'unknown', 'note': '数据不足'}

                    result[name] = {
                        'change': change_pct if not pd.isna(change_pct) else 0,
                        'close': close if not pd.isna(close) else 0,
                        'trend': trend,
                        'analysis': {
                            'daily': {
                                'dow_theory': dow_daily,
                                'elliott_wave': elliott_daily
                            },
                            'weekly': {
                                'dow_theory': dow_weekly,
                                'elliott_wave': elliott_weekly
                            },
                            'monthly': {
                                'dow_theory': dow_monthly,
                                'elliott_wave': elliott_monthly
                            }
                        }
                    }
                except Exception as e:
                    logger.debug(f"处理指数 {name} 失败: {e}")
                    continue

            # 添加跨指数验证
            result['inter_index_validation'] = self.market_regime._validate_across_indices(result)

            return result
        except Exception as e:
            logger.warning(f"从本地数据计算指数表现失败: {e}")
            return {}

    def _calculate_index_performance_from_realtime(self, date: str = None) -> dict:
        """
        从本地存储的指数实时行情数据文件计算主要指数表现

        从分钟级实时 parquet 计算主要指数表现（每个 symbol 取最新快照）

        Args:
            date: 日期 YYYYMMDD，默认今天

        Returns:
            {指数名称: {'change': float, 'close': float, 'trend': str, 'dow_theory': {...}, 'elliott_wave': {...}}}
        """
        try:
            from DataHub.services.realtime_service import RealtimeDataService
            from DataHub.config import RAW_INDEX_PRICE_DIR
            from datetime import datetime

            today = date if date else datetime.now().strftime('%Y%m%d')
            service = RealtimeDataService()

            df = service.load_intraday_parquet(
                date_str=today, asset_type='index', latest_snapshot=True
            )
            if df is None or df.empty:
                print(f"    ⚠️ 未找到今日指数实时数据 parquet")
                return {}

            print(f"    从指数实时 parquet 计算: {len(df)} 个指数")

            result = {}

            # 指数代码映射（parquet 中的代码 -> 内部名称）
            for _, item in df.iterrows():
                try:
                    symbol = item.get('symbol', '')
                    idx_name = self.CORE_INDICES.get(symbol)
                    if not idx_name:
                        continue

                    change_pct = item.get('change_pct', 0)
                    close_price = item.get('close', 0)

                    if pd.isna(change_pct):
                        change_pct = 0
                    if pd.isna(close_price):
                        close_price = 0

                    # 判断趋势
                    if change_pct > 1:
                        trend = 'UP'
                    elif change_pct < -1:
                        trend = 'DOWN'
                    else:
                        trend = 'NEUTRAL'

                    # 从本地历史数据计算技术分析指标
                    dow_theory = {}
                    elliott_wave = {}
                    try:
                        file_path = RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
                        if file_path.exists():
                            df_hist = pd.read_parquet(file_path)
                            if not df_hist.empty and len(df_hist) >= 60:
                                tech_df = df_hist.rename(columns={
                                    'trade_date': 'date',
                                    'open': 'open',
                                    'high': 'high',
                                    'low': 'low',
                                    'close': 'close',
                                    'volume': 'volume'
                                }).copy()
                                tech_df['date'] = tech_df['date'].astype(str)
                                dow_theory = self.market_regime._dow_theory_analysis(tech_df)
                                elliott_wave = self.market_regime._elliott_wave_analysis(tech_df)
                    except Exception as tech_e:
                        logger.debug(f"计算 {idx_name} 技术分析失败: {tech_e}")

                    result[idx_name] = {
                        'change': change_pct,
                        'close': close_price,
                        'trend': trend,
                        'dow_theory': dow_theory,
                        'elliott_wave': elliott_wave
                    }
                except Exception:
                    continue

            # 补充港股指数（从本地历史数据计算，因为实时数据接口不支持港股）
            hk_indices = {k: v for k, v in self.CORE_INDICES.items() if k.endswith('.HK')}
            for symbol, idx_name in hk_indices.items():
                if idx_name in result:
                    continue
                try:
                    file_path = RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
                    if not file_path.exists():
                        continue

                    df_hist = pd.read_parquet(file_path)
                    if df_hist.empty or len(df_hist) < 2:
                        continue

                    df_hist = df_hist.sort_values('trade_date')
                    latest = df_hist.iloc[-1]
                    prev = df_hist.iloc[-2]

                    close_price = float(latest['close'])
                    change_pct = float((latest['close'] - prev['close']) / prev['close'] * 100)

                    if change_pct > 1:
                        trend = 'UP'
                    elif change_pct < -1:
                        trend = 'DOWN'
                    else:
                        trend = 'NEUTRAL'

                    dow_theory = {}
                    elliott_wave = {}
                    if len(df_hist) >= 60:
                        tech_df = df_hist.rename(columns={
                            'trade_date': 'date',
                            'open': 'open',
                            'high': 'high',
                            'low': 'low',
                            'close': 'close',
                            'volume': 'volume'
                        }).copy()
                        tech_df['date'] = tech_df['date'].astype(str)
                        dow_theory = self.market_regime._dow_theory_analysis(tech_df)
                        elliott_wave = self.market_regime._elliott_wave_analysis(tech_df)

                    result[idx_name] = {
                        'change': change_pct,
                        'close': close_price,
                        'trend': trend,
                        'dow_theory': dow_theory,
                        'elliott_wave': elliott_wave
                    }
                    print(f"    {idx_name}: 从本地历史数据计算 ({change_pct:+.2f}%)")
                except Exception as hk_e:
                    logger.debug(f"计算港股指数 {idx_name} 失败: {hk_e}")

            if result:
                result['inter_index_validation'] = self.market_regime._validate_across_indices(result)

            print(f"    成功计算 {len(result)} 个指数")
            return result
        except Exception as e:
            print(f"    ⚠️ 从实时 parquet 计算指数表现失败: {e}")
            logger.debug(f"从实时 parquet 计算指数表现失败: {e}")
            return {}

    def _get_index_intraday(self, code: str, name: str) -> list:
        """
        获取指数当日分时数据（仅从 akshare 接口实时获取）

        Args:
            code: 指数代码，如 '000001.SH'
            name: 指数名称

        Returns:
            分时数据列表，每个元素包含 time 和 price；失败返回空列表
        """
        from datetime import datetime

        try:
            import akshare as ak

            # 转换代码格式
            if '.SH' in code:
                ak_code = code.replace('.SH', '')
            elif '.SZ' in code:
                ak_code = code.replace('.SZ', '')
            else:
                return []

            # 获取当日分时数据
            df = ak.index_zh_a_hist_min_em(symbol=ak_code, period="1", start_date="", end_date="")

            if df is not None and not df.empty:
                # 重命名列
                df = df.rename(columns={
                    '时间': 'time',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume'
                })

                # 保存到 DataHub 供后续使用
                try:
                    from DataHub.core.data_reader import save_index_intraday
                    today_str = datetime.now().strftime('%Y%m%d')
                    save_index_intraday(df, code, today_str)
                except Exception as save_e:
                    logger.debug(f"保存 {name} 分时数据到 DataHub 失败: {save_e}")

                # 转换为列表格式
                records = []
                for _, row in df.iterrows():
                    records.append({
                        'time': str(row['time']),
                        'price': float(row['close']),
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'volume': int(row['volume']) if 'volume' in row else 0
                    })
                return records

        except Exception as e:
            logger.debug(f"从接口获取 {name} 分时数据失败: {e}")

        return []

        return None

    def _check_data_quality(self, date: str, asset_type: str = "stock") -> dict:
        """
        数据质量检查 - 扫描前验证关键数据完整性

        检查项：
        1. 复权因子更新状态（仅股票需要，指数/ETF 不需要）
        2. 价格数据最新日期

        Args:
            date: 扫描日期
            asset_type: 资产类型 - 'stock' 股票, 'index' 指数, 'etf' ETF

        Returns:
            检查结果字典
        """
        from DataHub.config import RAW_ADJUST_FACTOR_DIR, RAW_PRICE_DIR
        from datetime import datetime, timedelta

        print("\n🔍 数据质量检查...")
        issues = []

        # 1. 检查复权因子更新状态（仅股票需要）
        if asset_type == "stock":
            try:
                factor_files = list(RAW_ADJUST_FACTOR_DIR.glob("*.parquet"))
                if factor_files:
                    # 抽样10个文件检查
                    import random
                    sample_files = random.sample(factor_files, min(10, len(factor_files)))

                    outdated_count = 0
                    for f in sample_files:
                        try:
                            df = pd.read_parquet(f)
                            if not df.empty and 'trade_date' in df.columns:
                                latest_date = pd.to_datetime(df['trade_date']).max()
                                days_diff = (datetime.now() - latest_date).days
                                if days_diff > 7:
                                    outdated_count += 1
                        except:
                            pass

                    if outdated_count >= 3:  # 30%以上样本落后
                        msg = f"⚠️  复权因子可能未及时更新（{outdated_count}/10 样本落后超过7天）"
                        print(f"  {msg}")
                        issues.append(msg)
                    else:
                        print(f"  ✓ 复权因子更新正常（抽样检查通过）")
                else:
                    print(f"  ⚠️  未找到复权因子数据，可能影响前复权计算精度")
                    issues.append("缺少复权因子数据")
            except Exception as e:
                print(f"  ⚠️  复权因子检查失败: {e}")
        else:
            print(f"  ⏭️  跳过复权因子检查（{asset_type} 不需要）")

        # 2. 检查价格数据最新日期
        try:
            price_files = list(RAW_PRICE_DIR.glob("*.parquet"))
            if price_files:
                # 抽样检查
                import random
                sample_files = random.sample(price_files, min(5, len(price_files)))

                date_mismatches = []
                for f in sample_files:
                    try:
                        df = pd.read_parquet(f)
                        if not df.empty and 'trade_date' in df.columns:
                            latest_date = pd.to_datetime(df['trade_date']).max().strftime('%Y%m%d')
                            if latest_date != date:
                                date_mismatches.append(f.stem)
                    except:
                        pass

                # 盘中时段（9:30-15:00）：历史数据最新日期为昨天是正常的，只要有实时数据即可
                now = datetime.now()
                in_trading_hours = (9 <= now.hour < 15) or (now.hour == 15 and now.minute == 0)
                is_today = (date == get_trading_date())
                if is_today and in_trading_hours:
                    # 盘中不强制要求冷数据日期为当天，检查实时数据是否可用
                    has_rt = self._is_realtime_data_fresh(date)
                    if has_rt:
                        print(f"  ✓ 价格数据日期检查通过（盘中模式，实时数据可用）")
                    else:
                        msg = f"⚠️  价格数据可能未更新（盘中但无实时数据）"
                        print(f"  {msg}")
                        issues.append(msg)
                elif len(date_mismatches) == len(sample_files):
                    msg = f"⚠️  价格数据可能未更新（样本最新日期非当天）"
                    print(f"  {msg}")
                    issues.append(msg)
                else:
                    print(f"  ✓ 价格数据日期检查通过")
            else:
                print(f"  ❌ 未找到价格数据")
                issues.append("缺少价格数据")
        except Exception as e:
            print(f"  ⚠️  价格数据检查失败: {e}")

        if issues:
            print(f"\n  ⚠️  发现 {len(issues)} 个数据质量问题:")
            for issue in issues:
                print(f"    - {issue}")
            print(f"\n  💡 建议运行: python -m DataHub.services.sync --today")
        else:
            print(f"  ✓ 数据质量检查通过")

        return {
            'status': 'ok' if not issues else 'warning',
            'issues': issues
        }

    def generate_daily_signals(self, date: str = None) -> dict:
        """
        生成每日信号 - 宏观+技术面综合分析

        Args:
            date: 日期，格式 'YYYYMMDD'，默认当前交易日（考虑A股开盘时间）

        Returns:
            包含热点板块和推荐操作的字典
        """
        if date is None:
            date = get_trading_date()

        print(f"\n{'='*50}")
        print(f"扫描日期: {date}")
        print('='*50)

        # ========== 0. 数据质量检查 ==========
        # LimitUpScanner 主要扫描指数和涨停板，不需要个股复权因子
        self._check_data_quality(date, asset_type="index")

        # 判断是否为当日扫描（当日扫描不应回退到无当天数据的历史 parquet）
        is_today = (date == get_trading_date())

        # ========== 1. 技术面指标采集（优先使用实时数据）==========
        print("\n📊 从实时数据计算技术面指标...")

        # 判断实时数据是否真正新鲜（realtime 日期 > 冷数据日期）
        # 这是 intraday_mode 判断的核心：只有数据真正更新了才算盘中模式
        has_realtime_data = self._is_realtime_data_fresh(date)

        # 1.1 市场涨跌家数（广度）- 优先从实时数据计算
        breadth = self._calculate_market_breadth_from_realtime(date)
        if breadth['total'] == 0:
            if is_today:
                print("  ⚠️ 今日实时数据缺失，请运行: python DataHub/services/realtime_service.py")
            else:
                print("  ⚠️ 实时数据获取失败，回退到本地数据")
                breadth = self._calculate_market_breadth_from_local(date)
        print(f"  涨跌家数: 涨{breadth['up']}/跌{breadth['down']}/平{breadth['flat']}")
        print(f"  涨跌比: {breadth['up_ratio']:.1%}")

        # 1.2 主要指数表现 - 优先从实时数据计算
        indices = self._calculate_index_performance_from_realtime(date)
        if not indices:
            if is_today:
                print("  ⚠️ 今日实时指数数据缺失")
            else:
                print("  ⚠️ 实时指数数据获取失败，回退到本地数据")
                indices = self._calculate_index_performance_from_local()
        print(f"  指数表现:")
        for name, data in indices.items():
            if name == 'inter_index_validation':
                continue
            change_str = f"{data.get('change', 0):+.2f}%" if 'change' in data else "N/A"
            trend_str = data.get('trend', 'UNKNOWN') if 'trend' in data else "UNKNOWN"
            print(f"    {name}: {change_str} ({trend_str})")

        # 1.3 板块强度（进攻vs防守）- 暂时使用默认值
        sectors = {
            'offensive_avg': 0,
            'defensive_avg': 0,
            'bias': 0,
            'leader': '中性',
            'offensive_count': 0,
            'defensive_count': 0
        }
        print(f"  板块风格: 进攻板块{sectors['offensive_avg']:+.2f}% vs 防守板块{sectors['defensive_avg']:+.2f}%")
        print(f"  风格偏向: {sectors['leader']}")

        # ========== 2. 获取涨停数据（统一数据源）==========
        df_zt = self.get_today_zt_pool(date)

        # 1.4 涨停跌停统计 - 优先使用实时数据计算（与图表逻辑一致）
        print("\n  📈 涨跌停统计（基于实时数据）:")
        zt_count, dt_count, rt_source, rt_stats = self._calculate_zt_dt_from_realtime(date)
        print(f"    数据来源: {rt_source}")
        print(f"    涨停: {zt_count}家, 跌停: {dt_count}家")
        if rt_stats:
            print(f"    涨停分类: 主板{rt_stats['zt_breakdown']['主板']}/创业板{rt_stats['zt_breakdown']['创业板']}/科创板{rt_stats['zt_breakdown']['科创板']}/北交所{rt_stats['zt_breakdown']['北交所']}/ST{rt_stats['zt_breakdown']['ST']}")
            print(f"    跌停分类: 主板{rt_stats['dt_breakdown']['主板']}/创业板{rt_stats['dt_breakdown']['创业板']}/科创板{rt_stats['dt_breakdown']['科创板']}/北交所{rt_stats['dt_breakdown']['北交所']}/ST{rt_stats['dt_breakdown']['ST']}")

        # 如果实时数据获取失败，回退到本地数据（仅历史日期）
        if zt_count == 0 and dt_count == 0:
            if is_today:
                print("\n  ⚠️ 今日实时数据缺失，涨跌停统计不可用")
            else:
                print("\n  ⚠️ 实时数据获取失败，尝试从本地数据计算...")
                zt_count, dt_count = self._calculate_zt_dt_from_local(date)
                print(f"    本地数据: 涨停{zt_count}/跌停{dt_count}")

        # 热点板块统计仍基于股池数据
        if not df_zt.empty and '所属行业' in df_zt.columns:
            sector_counts = df_zt['所属行业'].value_counts()
            hot_sectors_count = len(sector_counts[sector_counts >= 3])
            max_sector_zt = int(sector_counts.iloc[0]) if len(sector_counts) > 0 else 0
        else:
            hot_sectors_count = 0
            max_sector_zt = 0

        # 评估市场情绪
        if zt_count >= 80:
            sentiment = '极热'
        elif zt_count >= 50:
            sentiment = '活跃'
        elif zt_count >= 30:
            sentiment = '正常'
        elif zt_count >= 15:
            sentiment = '低迷'
        else:
            sentiment = '冷清'

        zt_stats = {
            'zt_count': zt_count,
            'hot_sectors': hot_sectors_count,
            'max_sector_zt': max_sector_zt,
            'sentiment': sentiment,
            'assessment': f'{zt_count}家涨停/{hot_sectors_count}个热点板块' if hot_sectors_count > 0 else f'{zt_count}家涨停(分散)'
        }

        print(f"  涨停情绪: {zt_stats['zt_count']}家涨停，{zt_stats['hot_sectors']}个热点板块")
        print(f"  市场情绪: {zt_stats['sentiment']}")
        if df_zt.empty:
            print("\n未获取到涨停数据")
            return {
                'date': date,
                'hot_sectors': [],
                'technical_indicators': {
                    'breadth': breadth,
                    'indices': indices,
                    'sector_strength': sectors,
                    'zt_stats': zt_stats
                },
                'message': '无涨停数据'
            }

        print(f"\n今日涨停家数: {len(df_zt)}")

        # 2. 计算板块热度
        heat = self.calculate_sector_heat(df_zt)
        min_count = self.config['event_params']['min_zt_count']

        # 3. 筛选热点板块
        hot_sectors = heat[heat['limit_up_count'] >= min_count].sort_values(
            'limit_up_count', ascending=False
        )

        print(f"\n热点板块 (涨停家数 >= {min_count}):")
        if hot_sectors.empty:
            if len(df_zt) >= 15:
                print(f"  无集中热点（普涨行情，涨停分散在多个板块）")
            else:
                print("  无")
        else:
            for _, row in hot_sectors.iterrows():
                print(f"  {row['所属行业']}: {row['limit_up_count']} 家")

        # 4. 分析板块详情
        sector_details = []
        for _, row in hot_sectors.iterrows():
            sector_name = row['所属行业']
            perf = self.analyze_sector_performance(sector_name)

            zt_stocks = df_zt[df_zt['所属行业'] == sector_name]

            # 提取该板块下所有涨停个股代码（仅代码，不保存名称）
            stocks_list = []
            for _, stock_row in zt_stocks.iterrows():
                stock_code = stock_row.get('代码', '')
                if stock_code:
                    stocks_list.append(str(stock_code))

            detail = {
                'sector': sector_name,
                'zt_count': int(row['limit_up_count']),
                'lead_stock_code': str(zt_stocks.iloc[0]['代码']) if len(zt_stocks) > 0 else '',
                'lead_stock_pct': zt_stocks.iloc[0]['涨跌幅'] if len(zt_stocks) > 0 else 0,
                'performance_5d': perf.get('period_return', 0),
                'volatility': perf.get('volatility', 0),
                'stocks': stocks_list  # 仅保存股票代码列表
            }
            sector_details.append(detail)

        # 5. 生成交易信号
        signals = []
        for sector in sector_details:
            strength_score = (
                min(sector['zt_count'] / 10, 1.0) * 0.5 +
                (sector['lead_stock_pct'] > 9.5) * 0.3 +
                (sector['performance_5d'] > 0) * 0.2
            )

            action = '关注' if strength_score >= 0.5 else '观望'
            signals.append({
                'sector': sector['sector'],
                'action': action,
                'strength': round(strength_score, 2),
                'reason': f"涨停{sector['zt_count']}家，龙头{sector.get('lead_stock_code', 'N/A')}"
            })

        # 6. 保存结果
        # 判断市场类型
        if not sector_details and len(df_zt) >= 15:
            market_type = '普涨分散'
        elif not sector_details:
            market_type = '冷清'
        else:
            market_type = '热点集中'

        # 技术面综合评分
        tech_score = self._calculate_technical_score(breadth, indices, sectors, zt_stats)

        # 1.5 跌停统计 - 使用已计算的跌停数
        print("\n📉 跌停统计...")
        # 根据跌停数评估恐慌程度
        if dt_count >= 100:
            panic = '极度恐慌'
            risk_level = 5
        elif dt_count >= 50:
            panic = '高度恐慌'
            risk_level = 4
        elif dt_count >= 20:
            panic = '中度恐慌'
            risk_level = 3
        elif dt_count >= 10:
            panic = '轻度恐慌'
            risk_level = 2
        else:
            panic = '正常'
            risk_level = 1

        dt_stats = {
            'dt_count': dt_count,
            'panic': panic,
            'risk_level': risk_level,
            'assessment': f'{dt_count}家跌停' if dt_count > 0 else '无跌停'
        }
        print(f"  跌停家数: {dt_stats['dt_count']}家, 恐慌程度: {dt_stats['panic']}")

        # ========== 3. 宏观指标采集 ==========
        print("\n🌍 采集宏观指标...")

        # 3.1 汇率
        currency = self.market_regime.get_usd_cny_rate()
        print(f"  汇率 USD/CNY: {currency.get('current', 7.2)} (来源: {currency.get('source', '默认')})")

        # 3.2 北向资金
        north_money = self.market_regime.get_north_money_flow()
        print(f"  北向资金: 净买入{north_money.get('today', 0):.1f}亿")

        # 3.3 黄金价格
        gold = self.market_regime.get_gold_price()
        print(f"  黄金价格: {gold.get('current', 0):.2f} {gold.get('unit', '')} ({gold.get('change_pct', 0):+.2f}%) (来源: {gold.get('source', '默认')})")

        # 3.4 美元指数
        dxy = self.market_regime.get_dxy_index()
        print(f"  美元指数: {dxy.get('current', 103.5):.2f} ({dxy.get('change_pct', 0):+.2f}%) (来源: {dxy.get('source', '默认')})")

        # 3.5 原油价格
        oil = self.market_regime.get_oil_price()
        print(f"  原油价格: {oil.get('current', 0):.2f} {oil.get('unit', '')} ({oil.get('change_pct', 0):+.2f}%) (来源: {oil.get('source', '默认')})")

        # 4. 获取指数历史数据（用于图表展示）- 从本地数据文件读取，生成日线/周线/月线
        print("\n📈 从本地数据获取指数历史...")
        index_history_daily = {}
        index_history_weekly = {}
        index_history_monthly = {}
        from DataHub.config import RAW_INDEX_PRICE_DIR

        for code, name in self.CORE_INDICES.items():
            try:
                file_path = RAW_INDEX_PRICE_DIR / f"{code}.parquet"
                if not file_path.exists():
                    print(f"  ✗ {name}: 文件不存在")
                    continue

                hist_df = pd.read_parquet(file_path)
                if hist_df.empty or 'trade_date' not in hist_df.columns or 'close' not in hist_df.columns:
                    print(f"  ✗ {name}: 数据不完整")
                    continue

                # 按日期排序，取最近300天（足够生成周线/月线）
                hist_df['trade_date'] = pd.to_datetime(hist_df['trade_date'])
                hist_df = hist_df.sort_values('trade_date').tail(300)

                # 补充今天数据（如果本地数据未更新到当天）
                last_date = hist_df['trade_date'].iloc[-1].date()
                today_date = datetime.strptime(date, '%Y%m%d').date()
                if last_date != today_date:
                    try:
                        from DataHub.services.realtime_service import RealtimeDataService
                        rt_df = RealtimeDataService().load_intraday_parquet(
                            date_str=date, asset_type='index', latest_snapshot=True
                        )
                        if rt_df is not None and not rt_df.empty:
                            rt_row = rt_df[rt_df['symbol'] == code]
                            if not rt_row.empty:
                                r = rt_row.iloc[0]
                                today_row = pd.DataFrame([{
                                    'trade_date': pd.Timestamp(today_date),
                                    'open': r.get('open') if pd.notna(r.get('open')) else r.get('close'),
                                    'high': r.get('high') if pd.notna(r.get('high')) else r.get('close'),
                                    'low': r.get('low') if pd.notna(r.get('low')) else r.get('close'),
                                    'close': r.get('close'),
                                    'volume': int(r.get('volume', 0)) if pd.notna(r.get('volume')) else 0,
                                }])
                                hist_df = pd.concat([hist_df, today_row], ignore_index=True)
                                hist_df = hist_df.sort_values('trade_date').tail(300)
                                print(f"  ↻ {name}: 已从实时数据补充 {today_date}")
                    except Exception as e:
                        print(f"  ⚠ {name}: 补充实时数据失败 - {e}")

                # 标准化列名
                tech_df = hist_df.rename(columns={
                    'trade_date': 'date',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume'
                }).copy()
                tech_df['date'] = tech_df['date'].astype(str)

                # 日线
                index_history_daily[name] = tech_df[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')

                # 周线
                df_weekly = self.market_regime.resample_ohlcv(tech_df, 'weekly')
                if not df_weekly.empty:
                    index_history_weekly[name] = df_weekly[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')

                # 月线
                df_monthly = self.market_regime.resample_ohlcv(tech_df, 'monthly')
                if not df_monthly.empty:
                    index_history_monthly[name] = df_monthly[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')

                print(f"  ✓ {name}: {len(tech_df)} 日 / {len(df_weekly)} 周 / {len(df_monthly)} 月")
            except Exception as e:
                print(f"  ✗ {name}: 获取失败 - {e}")

        # 7. 指数分时图表功能已移除，仅保留日期+时分格式的时间戳
        index_intraday = {}
        # intraday_mode 基于是否成功获取实时数据判断
        intraday_mode = has_realtime_data
        price_fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M')

        # 转换 numpy 类型为 Python 原生类型（用于JSON序列化）
        def convert_to_native(obj):
            if hasattr(obj, 'item'):  # numpy types
                return obj.item()
            return obj

        result = {
            'date': date,
            'total_zt_count': int(zt_count),  # 使用同花顺统计的涨停数
            'market_type': market_type,
            'hot_sectors': sector_details,
            'signals': signals,
            # 新增：技术面指标
            'technical_indicators': {
                'market_breadth': {
                    'up_count': int(breadth['up']),
                    'down_count': int(breadth['down']),
                    'flat_count': int(breadth['flat']),
                    'total_count': int(breadth['total']),
                    'up_ratio': round(float(breadth['up_ratio']), 4),
                    'breadth_score': round(float(breadth['breadth_score']), 4),
                    'interpretation': self._interpret_breadth(breadth)
                },
                'index_performance': {
                    name: {
                        'change_pct': round(float(data['change']), 2),
                        'trend': str(data['trend']),
                        'close': round(float(data['close']), 2),
                        'analysis': data.get('analysis', {
                            'daily': {
                                'dow_theory': data.get('dow_theory', {}),
                                'elliott_wave': data.get('elliott_wave', {})
                            }
                        })
                    } for name, data in indices.items() if name != 'inter_index_validation'
                },
                'inter_index_validation': indices.get('inter_index_validation', {}),
                'sector_strength': {
                    'offensive_avg': round(float(sectors['offensive_avg']), 2),
                    'defensive_avg': round(float(sectors['defensive_avg']), 2),
                    'bias': round(float(sectors['bias']), 2),
                    'leader': str(sectors['leader']),
                    'offensive_count': int(sectors.get('offensive_count', 0)),
                    'defensive_count': int(sectors.get('defensive_count', 0))
                },
                'zt_sentiment': {
                    'zt_count': int(zt_stats['zt_count']),
                    'hot_sectors_count': int(zt_stats['hot_sectors']),
                    'max_sector_zt': int(convert_to_native(zt_stats['max_sector_zt'])),
                    'sentiment': str(zt_stats['sentiment']),
                    'assessment': str(zt_stats['assessment'])
                },
                'dt_sentiment': {
                    'dt_count': int(dt_stats.get('dt_count', 0)),
                    'panic': str(dt_stats.get('panic', '未知')),
                    'risk_level': int(dt_stats.get('risk_level', 0)),
                    'assessment': str(dt_stats.get('assessment', ''))
                },
                'composite_score': int(tech_score['score']),
                'technical_outlook': str(tech_score['outlook']),
                'technical_reasons': [str(r) for r in tech_score['reasons']]
            },
            # 新增：宏观指标
            'macro_indicators': {
                'currency': {
                    'current': float(currency.get('current', 7.2)),
                    'change_pct': float(currency.get('change_pct', 0)),
                    'change_5d': float(currency.get('change_5d', 0))
                },
                'north_money': {
                    'today': float(north_money.get('today', 0)),
                    'inflow': float(north_money.get('inflow', 0)),
                    'recent_3d_avg': float(north_money.get('recent_3d_avg', 0))
                },
                'gold': {
                    'current': float(gold.get('current', 550)),
                    'change_pct': float(gold.get('change_pct', 0)),
                    'change': float(gold.get('change', 0)),
                    'note': str(gold.get('note', ''))
                },
                'dxy': {
                    'current': float(dxy.get('current', 103.5)),
                    'change_pct': float(dxy.get('change_pct', 0)),
                    'note': str(dxy.get('note', ''))
                },
                'oil': {
                    'current': float(oil.get('current', 0)),
                    'change_pct': float(oil.get('change_pct', 0)),
                    'type': str(oil.get('type', '原油'))
                }
            },
            'index_history': index_history_daily,
            'index_history_weekly': index_history_weekly,
            'index_history_monthly': index_history_monthly,
            'index_intraday': index_intraday,
            'price_fetch_time': price_fetch_time,
            'intraday_mode': intraday_mode,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'market_close_time': (close_time := get_data_close_time())[0],
            'data_status': close_time[1]
        }

        # 确定输出目录
        from DataHub.config import get_storage_path
        base_output_dir = get_storage_path("outputs", "shortterm", "technical_overview")
        base_output_dir.mkdir(parents=True, exist_ok=True)

        # 清洗 NaN/Inf，确保输出标准 JSON
        import math
        def _sanitize(obj):
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_sanitize(item) for item in obj]
            elif isinstance(obj, float):
                return None if math.isnan(obj) or math.isinf(obj) else obj
            return obj
        result = _sanitize(result)

        # 保存两份文件（覆盖）：
        # 1. 最新文件（Dashboard读取）
        latest_file = base_output_dir / "latest.json"
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # 2. 按日期归档文件
        dated_file = base_output_dir / f"{date}.json"
        with open(dated_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"\n信号已保存至:")
        print(f"  最新: {latest_file}")
        print(f"  归档: {dated_file}")

        return result

    def _get_main_index_codes(self) -> dict:
        """
        从 official_indices.csv 读取主要指数代码

        Returns:
            dict: {名称: 代码}
        """
        import csv
        from pathlib import Path

        # 默认的主要指数
        default_indices = {
            '沪深300': '000300',
            '中证1000': '000852',
            '创业板': '399006',
            '上证指数': '000001'
        }

        # 使用环境变量配置的存储路径
        from DataHub.config import get_storage_path
        csv_path = get_storage_path('official_indices.csv')
        if not csv_path.exists():
            return default_indices

        # 目标指数代码集合
        target_codes = {'000300', '000852', '399006', '000001', '000016', '000905', '399001'}
        result = {}

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get('code', '').strip()
                    name = row.get('name', '').strip()
                    if code in target_codes and name:
                        result[name] = code
        except Exception:
            pass

        # 如果CSV读取失败或没有数据，使用默认值
        if not result:
            return default_indices

        return result

    def _calculate_technical_score(self, breadth: dict, indices: dict, sectors: dict, zt_stats: dict) -> dict:
        """
        计算技术面综合评分

        Returns:
            {'score': 0-100, 'outlook': '看多/中性/看空', 'reasons': []}
        """
        score = 50  # 基准分
        reasons = []

        # 1. 市场广度评分 (0-25分)
        if breadth['up_ratio'] > 0.6:
            score += 15
            reasons.append(f"涨多跌少({breadth['up_ratio']:.1%})")
        elif breadth['up_ratio'] > 0.5:
            score += 5
        elif breadth['up_ratio'] < 0.4:
            score -= 15
            reasons.append(f"跌多涨少({breadth['up_ratio']:.1%})")
        elif breadth['up_ratio'] < 0.5:
            score -= 5

        # 2. 指数趋势评分 (0-30分)
        # 过滤掉非指数数据（如 inter_index_validation）
        index_data = {k: v for k, v in indices.items() if k not in ['inter_index_validation'] and isinstance(v, dict)}
        up_indices = sum(1 for d in index_data.values() if d.get('trend') == 'UP')
        total_indices = len(index_data)

        if up_indices >= 3:
            score += 20
            reasons.append(f"多指数上行({up_indices}/{total_indices})")
        elif up_indices >= 2:
            score += 10
        elif up_indices == 0 and total_indices > 0:
            score -= 15
            reasons.append("指数全线走弱")

        # 3. 板块风格评分 (0-20分)
        if sectors['leader'] == '进攻':
            score += 15
            reasons.append("进攻板块领涨")
        elif sectors['leader'] == '防守':
            score -= 10
            reasons.append("防守板块领涨")

        # 4. 涨停情绪评分 (0-25分)
        if zt_stats['sentiment'] == '极热':
            score += 20
            reasons.append("涨停情绪极热")
        elif zt_stats['sentiment'] == '活跃':
            score += 15
            reasons.append("涨停情绪活跃")
        elif zt_stats['sentiment'] == '正常':
            score += 5
        elif zt_stats['sentiment'] == '低迷':
            score -= 10
            reasons.append("涨停情绪低迷")
        elif zt_stats['sentiment'] == '冷清':
            score -= 20
            reasons.append("涨停情绪冷清")

        # 确定 outlook
        if score >= 70:
            outlook = '看多'
        elif score >= 50:
            outlook = '中性偏多'
        elif score >= 40:
            outlook = '中性偏空'
        else:
            outlook = '看空'

        return {
            'score': max(0, min(100, score)),
            'outlook': outlook,
            'reasons': reasons if reasons else ['技术面无明显信号']
        }

    def _interpret_breadth(self, breadth: dict) -> str:
        """解读市场广度"""
        up_ratio = breadth['up_ratio']
        if up_ratio > 0.7:
            return "普涨格局，市场情绪高涨"
        elif up_ratio > 0.6:
            return "涨多跌少，市场情绪积极"
        elif up_ratio > 0.5:
            return "涨跌互现，市场情绪中性"
        elif up_ratio > 0.4:
            return "跌多涨少，市场情绪谨慎"
        else:
            return "普跌格局，市场情绪低迷"

    def save_to_history(self, heat: pd.DataFrame):
        """保存板块热度历史数据"""
        # 统一到 outputs/shortterm/services
        # 使用环境变量配置的存储路径
        from DataHub.config import get_storage_path
        output_dir = get_storage_path("outputs", "shortterm", "services")
        output_dir.mkdir(parents=True, exist_ok=True)

        # 最新历史文件
        history_file = output_dir / "sector_heat_history.csv"

        if os.path.exists(history_file):
            history = pd.read_csv(history_file)
        else:
            history = pd.DataFrame(columns=['date', 'industry', 'limit_up_count'])

        history = pd.concat([history, heat], ignore_index=True)
        history.to_csv(history_file, index=False, encoding='utf-8-sig')

        # 同时保存带日期的历史文件
        date_str = heat['date'].iloc[0] if not heat.empty else get_trading_date_str()
        dated_history_file = output_dir / f"sector_heat_history_{date_str}.csv"
        heat.to_csv(dated_history_file, index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    scanner = LimitUpScanner()
    result = scanner.generate_daily_signals()

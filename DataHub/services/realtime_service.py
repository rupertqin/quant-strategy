"""
实时数据服务 - 盘中行情获取与管理

负责获取、存储和提供实时行情数据
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

from DataHub.config import get_storage_path

# 实时数据默认保存目录 (原始数据放入 storage/raw)
REALTIME_OUTPUT_DIR = get_storage_path("raw", "realtime")
# 分钟级实时数据 parquet 目录（直接放在 realtime 下，按 asset_type 分子目录）
INTRADAY_DIR = get_storage_path("raw", "realtime")


class RealtimeDataService:
    """
    实时数据服务

    负责盘中行情的获取、存储和查询
    """

    def __init__(self, output_dir: Path = None):
        """
        初始化实时数据服务

        Args:
            output_dir: 实时数据输出目录
        """
        self.output_dir = output_dir or REALTIME_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_index_realtime_data(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取指数实时行情数据

        Args:
            symbols: 指数代码列表，如 ['000001.SH', '399001.SZ']，None表示主要指数

        Returns:
            DataFrame with real-time data
        """
        import akshare as ak

        self.logger.info("获取指数实时行情数据...")

        # 获取指数实时行情
        try:
            df = ak.stock_zh_index_spot_sina()
            self.logger.info("使用新浪接口获取指数实时数据")
        except Exception as e:
            self.logger.error(f"新浪接口失败: {e}")
            # 尝试东财接口
            try:
                df = ak.index_zh_a_spot_em()
                self.logger.info("使用东财备用接口获取指数实时数据")
            except Exception as e2:
                self.logger.error(f"备用接口也失败: {e2}")
                raise RuntimeError(f"无法获取指数实时数据")

        # 处理代码格式
        if '代码' in df.columns:
            sample_code = str(df['代码'].iloc[0])
            if sample_code.startswith(('sh', 'sz')):
                # 新浪接口格式: sh000001 -> 000001.SH
                df['symbol'] = df['代码'].apply(lambda x:
                    x[2:] + '.SH' if str(x).startswith('sh') else
                    x[2:] + '.SZ' if str(x).startswith('sz') else x
                )
            else:
                # 东财接口格式
                df['symbol'] = df['代码'].apply(lambda x:
                    x + '.SH' if str(x).startswith('0') else
                    x + '.SZ' if str(x).startswith('3') else x
                )

        # 筛选指定指数
        if symbols:
            df = df[df['symbol'].isin(symbols)]

        # 统一列名映射
        column_map = {
            '最新价': 'close',
            '最新': 'close',
            '开盘价': 'open',
            '开盘': 'open',
            '最高价': 'high',
            '最高': 'high',
            '最低价': 'low',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '名称': 'name',
        }

        available_cols = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=available_cols)
        df['trade_date'] = datetime.now().date()

        # 确保必要列存在
        required_cols = ['symbol', 'name', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        self.logger.info(f"获取到 {len(df)} 个指数实时数据")
        return df

    def fetch_etf_realtime_data(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取ETF实时行情数据

        Args:
            symbols: ETF代码列表，如 ['510300.SH', '159915.SZ']，None表示全市场

        Returns:
            DataFrame with ETF real-time data
        """
        import akshare as ak

        self.logger.info("获取ETF实时行情数据...")

        # 获取ETF实时行情（新浪优先）
        try:
            # 优先使用新浪接口
            df = ak.fund_etf_category_sina(symbol="ETF基金")
            self.logger.info("使用新浪接口获取ETF实时数据")
        except Exception as e:
            self.logger.error(f"新浪接口失败: {e}")
            # 尝试东财接口
            try:
                df = ak.fund_etf_spot_em()
                self.logger.info("使用东财备用接口获取ETF实时数据")
            except Exception as e2:
                self.logger.error(f"备用接口也失败: {e2}")
                raise RuntimeError(f"无法获取ETF实时数据")

        # 处理代码格式
        if '代码' in df.columns:
            sample_code = str(df['代码'].iloc[0])
            if sample_code.startswith(('sh', 'sz', 'bj')):
                # 新浪接口格式
                df['symbol'] = df['代码'].apply(lambda x:
                    x[2:] + '.SH' if str(x).startswith('sh') else
                    x[2:] + '.SZ' if str(x).startswith('sz') else
                    x[2:] + '.BJ' if str(x).startswith('bj') else x
                )
            else:
                # 东财接口格式
                df['symbol'] = df['代码'].apply(lambda x:
                    x + '.SH' if str(x).startswith('5') else
                    x + '.SZ' if str(x).startswith('1') else x
                )

        # 筛选指定ETF
        if symbols:
            df = df[df['symbol'].isin(symbols)]

        # 统一列名映射
        column_map = {
            '最新价': 'close',
            '最新': 'close',
            '开盘价': 'open',
            '开盘': 'open',
            '最高价': 'high',
            '最高': 'high',
            '最低价': 'low',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '名称': 'name',
        }

        available_cols = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=available_cols)
        df['trade_date'] = datetime.now().date()

        # 确保必要列存在
        required_cols = ['symbol', 'name', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        self.logger.info(f"获取到 {len(df)} 只ETF实时数据")
        return df

    def fetch_realtime_data(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取盘中实时行情数据（使用akshare sina源）

        Args:
            symbols: 股票代码列表，如 ['600519.SH', '300750.SZ']，None表示全市场

        Returns:
            DataFrame with real-time data
        """
        import akshare as ak

        self.logger.info("获取实时行情数据...")

        # 获取全市场实时行情
        try:
            df = ak.stock_zh_a_spot()
        except Exception as e:
            self.logger.error(f"新浪接口失败: {e}")
            # 尝试备用接口（仅一次）
            try:
                df = ak.stock_zh_a_spot_em()
                self.logger.info("使用东财备用接口成功")
            except Exception as e2:
                self.logger.error(f"备用接口也失败: {e2}")
                raise RuntimeError(f"无法获取实时数据，请稍后手动重试")

        # 判断接口类型并处理代码格式
        # 新浪接口: 代码列是 '代码'，格式如 'sh600000'
        # 东财接口: 代码列是 '代码'，格式如 '600000'
        if '代码' in df.columns:
            sample_code = str(df['代码'].iloc[0])
            if sample_code.startswith(('sh', 'sz', 'bj')):
                # 新浪接口格式
                df['symbol'] = df['代码'].apply(lambda x:
                    x[2:] + '.SH' if x.startswith('sh') else
                    x[2:] + '.SZ' if x.startswith('sz') else
                    x[2:] + '.BJ' if x.startswith('bj') else x
                )
            else:
                # 东财接口格式，需要判断交易所
                df['symbol'] = df['代码'].apply(lambda x:
                    x + '.SH' if x.startswith(('6', '68', '5')) else
                    x + '.SZ' if x.startswith(('0', '3', '1')) else
                    x + '.BJ' if x.startswith(('4', '8', '82', '83', '87', '88')) else x
                )
        elif '股票代码' in df.columns:
            # 东财接口另一种格式
            df['symbol'] = df['股票代码'].apply(lambda x:
                x + '.SH' if str(x).startswith(('6', '68', '5')) else
                x + '.SZ' if str(x).startswith(('0', '3', '1')) else x
            )

        # 筛选指定股票
        if symbols:
            df = df[df['symbol'].isin(symbols)]

        # 统一列名映射（兼容新浪和东财两种接口）
        column_map = {
            # 新浪列名
            '最新价': 'close',
            '今开': 'open',
            '最高价': 'high',
            '最低价': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '名称': 'name',
            # 东财列名
            '最新价': 'close',
            '开盘价': 'open',
            '最高价': 'high',
            '最低价': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '名称': 'name'
        }

        available_cols = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=available_cols)
        df['trade_date'] = datetime.now().date()

        # 确保必要列存在
        required_cols = ['symbol', 'name', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        self.logger.info(f"获取到 {len(df)} 只股票实时数据")
        return df

    def save_intraday_parquet(self, df: pd.DataFrame, asset_type: str = 'stock') -> str:
        """
        保存实时数据到 parquet（增量写入，自动去重）

        文件路径即含日期，列中只保留一个 timestamp 字段，不存 is_realtime。

        Args:
            df: 实时数据 DataFrame
            asset_type: 资产类型 stock/etf/index

        Returns:
            保存的文件路径
        """
        intraday_dir = INTRADAY_DIR / asset_type
        intraday_dir.mkdir(parents=True, exist_ok=True)

        today_str = datetime.now().strftime('%Y%m%d')
        filepath = intraday_dir / f"{today_str}.parquet"

        df_out = df.copy()

        # 统一用 timestamp 列（日期+时间），文件名已含日期，列里保留完整时间戳
        df_out['timestamp'] = pd.Timestamp.now()

        # 删除可能混入的旧列
        for col in ('is_realtime', 'trade_time', 'trade_date'):
            if col in df_out.columns:
                df_out.drop(columns=[col], inplace=True)

        # 确保必要列存在
        for col in ('open', 'high', 'low', 'volume', 'amount'):
            if col not in df_out.columns:
                df_out[col] = None

        # 选择标准列
        keep_cols = [
            'symbol', 'name', 'timestamp',
            'open', 'high', 'low', 'close', 'change_pct',
            'volume', 'amount',
        ]
        df_out = df_out[[c for c in keep_cols if c in df_out.columns]]

        # 按日期直接覆盖写入（不读取旧文件，避免旧格式列残留）
        df_out.to_parquet(filepath, index=False)
        self.logger.info(
            f"实时数据已保存: {filepath} ({len(df_out)} 条记录)"
        )
        return str(filepath)

    def load_intraday_parquet(
        self,
        date_str: str = None,
        asset_type: str = 'stock',
        latest_snapshot: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        加载实时数据 parquet

        Args:
            date_str: 日期 YYYYMMDD，默认今天
            asset_type: 资产类型 stock/etf/index
            latest_snapshot: 是否只返回每个 symbol 的最新快照

        Returns:
            DataFrame 或 None
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')

        filepath = INTRADAY_DIR / asset_type / f"{date_str}.parquet"
        if not filepath.exists():
            return None

        df = pd.read_parquet(filepath)
        if df.empty:
            return None

        if latest_snapshot and 'timestamp' in df.columns:
            df = df.sort_values('timestamp').groupby('symbol').tail(1)

        return df.reset_index(drop=True)

    def archive_realtime_data(self, date_str: str = None) -> None:
        """
        归档实时数据：日终日线同步完成后，删除当天 realtime 文件。

        因为 realtime 与日线分目录存储，且已不含 is_realtime 标记，
        归档动作直接删除文件即可。

        Args:
            date_str: 日期 YYYYMMDD，默认今天
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')

        for asset_type in ('stock', 'etf', 'index'):
            filepath = INTRADAY_DIR / asset_type / f"{date_str}.parquet"
            if filepath.exists():
                try:
                    filepath.unlink()
                    self.logger.info(f"已归档(删除)实时数据: {asset_type}/{date_str}.parquet")
                except Exception as e:
                    self.logger.warning(f"归档删除失败 {asset_type}/{date_str}: {e}")

    def merge_realtime_to_history(self, hist_df: pd.DataFrame, realtime: pd.Series) -> pd.DataFrame:
        """
        将实时数据合并到历史K线（内存中）

        冷热数据时间线规则：
        - 热数据日期 <= 冷数据最后日期：丢弃热数据
        - 热数据日期 > 冷数据最后日期：追加新行

        Args:
            hist_df: 历史日线数据
            realtime: 实时行情Series

        Returns:
            合并后的DataFrame
        """
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

        # 热数据不比冷数据新，不合并
        if rt_date <= last_date:
            return hist_df

        # 追加新行
        new_row = pd.DataFrame([{
            'trade_date': pd.Timestamp(rt_date),
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


# 便捷函数 - 供其他模块快速使用
def get_realtime_service() -> RealtimeDataService:
    """获取实时数据服务实例"""
    return RealtimeDataService()


if __name__ == "__main__":
    """直接运行此脚本时，获取并保存实时数据"""
    import logging
    import argparse

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 命令行参数
    parser = argparse.ArgumentParser(description='DataHub 实时数据获取服务')
    parser.add_argument('--type', type=str, choices=['stock', 'etf', 'index', 'all'], default='all',
                        help='获取类型: stock(股票) / etf(ETF) / index(指数) / all(全部)，默认 all')
    args = parser.parse_args()

    service = RealtimeDataService()

    # 股票实时数据
    if args.type in ['stock', 'all']:
        print("=" * 60)
        print("📡 DataHub 股票实时数据获取")
        print("=" * 60)

        try:
            # 获取并保存股票实时数据（分钟级 parquet）
            df = service.fetch_realtime_data()
            filepath = service.save_intraday_parquet(df, asset_type='stock')
            print(f"\n✅ 股票实时数据已保存: {filepath}")

            # 显示数据统计
            print(f"\n📊 数据统计:")
            print(f"   股票数量: {len(df)}")
            print(f"   数据列: {list(df.columns)}")

            # 显示前5条
            print(f"\n📈 前5只股票:")
            print(df[['symbol', 'name', 'close', 'change_pct']].head().to_string(index=False))

        except Exception as e:
            print(f"\n❌ 股票数据获取失败: {e}")
            import traceback
            traceback.print_exc()

    # ETF实时数据
    if args.type in ['etf', 'all']:
        if args.type == 'all':
            print("\n")

        print("=" * 60)
        print("📡 DataHub ETF实时数据获取")
        print("=" * 60)

        try:
            # 获取并保存ETF实时数据（分钟级 parquet）
            df = service.fetch_etf_realtime_data()
            filepath = service.save_intraday_parquet(df, asset_type='etf')
            print(f"\n✅ ETF实时数据已保存: {filepath}")

            # 显示数据统计
            print(f"\n📊 数据统计:")
            print(f"   ETF数量: {len(df)}")
            print(f"   数据列: {list(df.columns)}")

            # 显示前5条
            print(f"\n📈 前5只ETF:")
            print(df[['symbol', 'name', 'close', 'change_pct']].head().to_string(index=False))

        except Exception as e:
            print(f"\n❌ ETF数据获取失败: {e}")
            import traceback
            traceback.print_exc()

    # 指数实时数据
    if args.type in ['index', 'all']:
        if args.type == 'all':
            print("\n")

        print("=" * 60)
        print("📡 DataHub 指数实时数据获取")
        print("=" * 60)

        try:
            # 获取并保存指数实时数据（分钟级 parquet）
            df = service.fetch_index_realtime_data()
            filepath = service.save_intraday_parquet(df, asset_type='index')
            print(f"\n✅ 指数实时数据已保存: {filepath}")

            # 显示数据统计
            print(f"\n📊 数据统计:")
            print(f"   指数数量: {len(df)}")
            print(f"   数据列: {list(df.columns)}")

            # 显示主要指数
            main_indices = ['000001.SH', '399006.SZ', '000300.SH', '000688.SH']
            main_df = df[df['symbol'].isin(main_indices)]
            if not main_df.empty:
                print(f"\n📈 主要指数:")
                print(main_df[['symbol', 'name', 'close', 'change_pct']].to_string(index=False))

        except Exception as e:
            print(f"\n❌ 指数数据获取失败: {e}")
            import traceback
            traceback.print_exc()

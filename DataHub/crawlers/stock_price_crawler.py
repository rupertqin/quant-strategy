"""
股票价格爬虫 - 获取A股历史价格数据
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
import time
import akshare as ak

from .base_crawler import BaseCrawler
from DataHub.config import CRAWLER_REQUEST_DELAY


class StockPriceCrawler(BaseCrawler):
    """
    股票价格爬虫

    从akshare获取A股历史日线数据
    """

    def __init__(self, repository=None):
        super().__init__(repository)
        self.source = 'akshare'

    def fetch(
        self,
        symbol: str = None,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取股票价格数据

        Args:
            symbol: 单只股票代码，如 '600519.SH'
            symbols: 多只股票代码列表
            start_date: 开始日期 'YYYY-MM-DD'，默认一年前
            end_date: 结束日期 'YYYY-MM-DD'，默认今天
            adjust: 复权方式，'qfq'(前复权) | 'hfq'(后复权) | ''(不复权)

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        # 处理日期默认值
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        # 确保symbols是列表
        if symbols is None and symbol is not None:
            symbols = [symbol]
        elif symbols is None:
            raise ValueError("必须提供 symbol 或 symbols 参数")

        all_data = []

        for sym in symbols:
            try:
                df = self._fetch_single(sym, start_date, end_date, adjust)
                if df is not None and not df.empty:
                    all_data.append(df)
                    self.logger.info(f"成功获取 {sym}: {len(df)} 条记录")
                else:
                    self.logger.warning(f"获取 {sym} 数据为空")

                # 请求间隔，避免请求过快被封
                time.sleep(CRAWLER_REQUEST_DELAY)

            except Exception as e:
                self.logger.error(f"获取 {sym} 失败: {e}")
                continue

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    def _fetch_single(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str
    ) -> Optional[pd.DataFrame]:
        """
        获取单只股票数据
        """
        # 转换代码格式: 600519.SH -> sh600519
        code = self._format_code(symbol)

        # 转换日期格式: 2024-01-01 -> 20240101
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')

        # 调用akshare接口
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_str,
            end_date=end_str,
            adjust=adjust
        )

        if df.empty:
            return None

        # 添加symbol列
        df['symbol'] = symbol

        return df

    def _format_code(self, symbol: str) -> str:
        """
        转换代码格式

        600519.SH -> sh600519
        000001.SZ -> sz000001
        """
        if '.SH' in symbol:
            return 'sh' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return 'sz' + symbol.replace('.SZ', '')
        return symbol

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗数据
        """
        # 重命名列
        column_map = {
            '日期': 'trade_date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '换手率': 'turnover_ratio'
        }

        df = df.rename(columns=column_map)

        # 转换日期格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

        # 选择需要的列
        keep_cols = [
            'symbol', 'trade_date', 'open', 'high', 'low', 'close',
            'volume', 'amount', 'change_pct', 'turnover_ratio'
        ]

        # 只保留存在的列
        keep_cols = [c for c in keep_cols if c in df.columns]
        df = df[keep_cols]

        # 删除价格为0或负数的异常数据
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df = df[df[col] > 0]

        # 删除重复数据
        df = df.drop_duplicates(subset=['symbol', 'trade_date'], keep='first')

        return df

    def _save(self, df: pd.DataFrame) -> int:
        """
        保存到数据库
        """
        if self.repository is None:
            self.logger.warning("没有repository，数据未保存")
            return 0

        try:
            count = self.repository.save_daily_prices(df)
            return count
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            return 0

    def sync_latest(self, symbols: List[str] = None) -> dict:
        """
        同步最新数据（只获取最近几个交易日）

        Args:
            symbols: 股票代码列表，None表示全部股票

        Returns:
            同步结果
        """
        # 如果没有指定股票，从repository获取全部
        if symbols is None and self.repository:
            stocks = self.repository.get_all_stocks()
            symbols = stocks['symbol'].tolist() if not stocks.empty else []

        if not symbols:
            return {'status': 'failed', 'records': 0, 'message': 'No symbols to sync'}

        # 只获取最近5个交易日
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)  # 多取几天，确保包含交易日

        return self.sync(
            symbols=symbols,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

    def sync_single_stock(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None
    ) -> dict:
        """
        同步单只股票的历史数据

        适用于补数据或首次同步
        """
        return self.sync(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

"""Data Provider - Unified data fetching interface"""

import logging
from datetime import datetime, timedelta
from typing import List
import pandas as pd

from DataHub.core.data_client import UnifiedDataClient

logger = logging.getLogger(__name__)


class DataProvider:
    """Unified data provider supporting akshare and baostock"""

    def __init__(self):
        """Initialize data provider"""
        self.client = UnifiedDataClient(enable_baostock_fallback=True)
        logger.info("DataProvider initialized with UnifiedDataClient")

    def get_price_data(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        Get price data for given symbols

        Args:
            symbols: List of stock/ETF symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            adjust: Adjustment type - "qfq" (forward), "hfq" (backward), "" (no adjustment)

        Returns:
            DataFrame with Date as index and symbols as columns
        """
        all_data = []
        failed_symbols = []
        success_count = 0

        for i, symbol in enumerate(symbols):
            try:
                df = self.client.get_price_data(symbol, start_date, end_date, adjust)
                if df is not None and not df.empty:
                    # 获取收盘价列
                    close_col = None
                    if "收盘" in df.columns:
                        close_col = "收盘"
                    elif "close" in df.columns:
                        close_col = "close"

                    if close_col:
                        df = df[[close_col]].rename(columns={close_col: symbol})
                        all_data.append(df)
                        success_count += 1
                        logger.info(f"[{i+1}/{len(symbols)}] {symbol}: 获取成功 ({len(df)} 条)")
                    else:
                        failed_symbols.append(f"{symbol}(无收盘价列)")
                        logger.warning(f"[{i+1}/{len(symbols)}] {symbol}: 无收盘价列")
                else:
                    failed_symbols.append(f"{symbol}(无数据)")
                    logger.warning(f"[{i+1}/{len(symbols)}] {symbol}: 无数据")
            except Exception as e:
                failed_symbols.append(f"{symbol}({str(e)[:30]})")
                logger.error(f"[{i+1}/{len(symbols)}] {symbol}: 获取失败 - {e}")
                continue

        if not all_data:
            logger.warning(f"所有股票获取失败: {failed_symbols}")
            return pd.DataFrame()

        # Merge all data
        result = all_data[0]
        for df in all_data[1:]:
            result = result.join(df, how="outer")

        logger.info(f"数据获取完成: 成功 {success_count}/{len(symbols)}")
        if failed_symbols:
            logger.warning(f"失败列表: {failed_symbols[:5]}...")

        return result

    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        Get ZT (涨停) pool data for a given date

        Args:
            date: Date in YYYYMMDD format

        Returns:
            DataFrame with ZT pool data
        """
        try:
            return self.client.get_zt_pool(date)
        except Exception as e:
            logger.error(f"Error fetching ZT pool for {date}: {e}")
            return pd.DataFrame()

    def get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """Get trading calendar (list of trading dates)"""
        try:
            return self.client.get_trading_calendar(start_date, end_date)
        except Exception as e:
            logger.error(f"Error getting trading calendar: {e}")
            return []

    def get_latest_trading_date(self) -> str:
        """Get the latest trading date"""
        try:
            return self.client.get_latest_trading_date()
        except Exception as e:
            logger.error(f"Error getting latest trading date: {e}")
            # Fallback
            today = datetime.now()
            if today.weekday() < 5:
                return today.strftime("%Y%m%d")
            return (today - timedelta(days=(today.weekday() - 4))).strftime("%Y%m%d")

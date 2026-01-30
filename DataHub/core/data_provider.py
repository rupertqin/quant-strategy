"""Data Provider - Unified data fetching interface"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Union
import pandas as pd

logger = logging.getLogger(__name__)


class DataProvider:
    """Unified data provider supporting akshare and baostock"""

    def __init__(self):
        self._akshare_available = self._check_akshare()
        self._baostock_available = self._check_baostock()
        logger.info(f"DataProvider initialized: akshare={self._akshare_available}, baostock={self._baostock_available}")

    def _check_akshare(self) -> bool:
        """Check if akshare is available"""
        try:
            import akshare as ak
            return True
        except ImportError:
            logger.warning("akshare not available")
            return False

    def _check_baostock(self) -> bool:
        """Check if baostock is available"""
        try:
            import baostock as bs
            return True
        except ImportError:
            logger.warning("baostock not available")
            return False

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

        for symbol in symbols:
            try:
                df = self._fetch_single_price(symbol, start_date, end_date, adjust)
                if df is not None and not df.empty:
                    df = df.rename(columns={df.columns[0]: symbol})
                    all_data.append(df[[symbol]])
                else:
                    logger.warning(f"No data for {symbol}")
            except Exception as e:
                logger.error(f"Error fetching {symbol}: {e}")
                continue

        if not all_data:
            return pd.DataFrame()

        # Merge all data
        result = all_data[0]
        for df in all_data[1:]:
            result = result.join(df, how="outer")

        return result

    def _fetch_single_price(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str
    ) -> Optional[pd.DataFrame]:
        """Fetch price data for a single symbol"""
        # Try akshare first
        if self._akshare_available:
            try:
                return self._fetch_via_akshare(symbol, start_date, end_date, adjust)
            except Exception as e:
                logger.warning(f"akshare failed for {symbol}: {e}")

        # Try baostock
        if self._baostock_available:
            try:
                return self._fetch_via_baostock(symbol, start_date, end_date)
            except Exception as e:
                logger.warning(f"baostock failed for {symbol}: {e}")

        logger.error(f"All providers failed for {symbol}")
        return None

    def _fetch_via_akshare(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str
    ) -> pd.DataFrame:
        """Fetch data via akshare"""
        import akshare as ak

        # Convert to YYYYMMDD format
        start = start_date.replace("-", "")
        end = end_date.replace("-", "")

        if symbol.endswith(".SH") or symbol.endswith(".SZ"):
            # It's a stock
            if adjust == "qfq":
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start,
                    end_date=end,
                    adjust="qfq"
                )
            else:
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start,
                    end_date=end,
                    adjust=""
                )
        elif symbol.startswith("51") or symbol.startswith("15") or symbol.startswith("51"):
            # It's an ETF
            df = ak.fund_etf_hist_sina(
                symbol=symbol,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="" if adjust == "" else "qfq"
            )
        else:
            # Try as ETF
            df = ak.fund_etf_hist_sina(
                symbol=symbol,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="" if adjust == "" else "qfq"
            )

        if df is not None and not df.empty:
            df["日期"] = pd.to_datetime(df["日期"])
            df = df.set_index("日期")
            df = df.sort_index()
            # Select close price
            if "收盘" in df.columns:
                return df[["收盘"]]
            elif "close" in df.columns:
                return df[["close"]]

        return None

    def _fetch_via_baostock(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Fetch data via baostock"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            raise Exception(f"Baostock login failed: {lg.error_msg}")

        # Convert symbol format
        bs_symbol = symbol.replace(".", "-")

        rs = bs.query_history_k_data_plus(
            bs_symbol,
            "date,close",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"  # Forward adjusted
        )

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        bs.logout()

        if data_list:
            df = pd.DataFrame(data_list, columns=["日期", "收盘"])
            df["日期"] = pd.to_datetime(df["日期"])
            df = df.set_index("日期")
            df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
            return df

        return None

    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        Get ZT (涨停) pool data for a given date

        Args:
            date: Date in YYYYMMDD format

        Returns:
            DataFrame with ZT pool data
        """
        if not self._akshare_available:
            logger.error("akshare not available for ZT pool")
            return pd.DataFrame()

        try:
            import akshare as ak

            df = ak.stock_zt_pool_em(date=date)
            return df
        except Exception as e:
            logger.error(f"Error fetching ZT pool for {date}: {e}")
            return pd.DataFrame()

    def get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """Get trading calendar (list of trading dates)"""
        if not self._akshare_available:
            return []

        try:
            import akshare as ak

            df = ak.tool_trading_date()
            # Filter by date range
            mask = (df["calendarDate"] >= start_date) & (df["calendarDate"] <= end_date)
            return df[mask]["calendarDate"].tolist()
        except Exception as e:
            logger.error(f"Error getting trading calendar: {e}")
            return []

    def get_latest_trading_date(self) -> str:
        """Get the latest trading date"""
        today = datetime.now()
        # Try last 7 days
        for i in range(7):
            check_date = today - timedelta(days=i)
            date_str = check_date.strftime("%Y%m%d")

            calendar = self.get_trading_calendar(
                check_date.strftime("%Y-%m-%d"),
                check_date.strftime("%Y-%m-%d")
            )

            if calendar:
                return date_str

        # Fallback: return today's date if it's a weekday
        if today.weekday() < 5:
            return today.strftime("%Y%m%d")

        return (today - timedelta(days=(today.weekday() - 4))).strftime("%Y%m%d")

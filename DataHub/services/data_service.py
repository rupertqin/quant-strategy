"""Data Service - Unified data service interface"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from DataHub.core.data_provider import DataProvider
from DataHub.core.storage_engine import StorageEngine
from DataHub.config import STOCK_LIST

logger = logging.getLogger(__name__)


class DataService:
    """Unified data service for all strategies"""

    def __init__(self, storage_path: Optional[Path] = None):
        """
        Initialize data service

        Args:
            storage_path: Path to storage directory
        """
        self.provider = DataProvider()
        self.storage = StorageEngine(storage_path)
        self.stock_list = STOCK_LIST
        logger.info(f"DataService initialized with {len(self.stock_list)} stocks")

    # ========== Price Data Operations ==========

    def get_prices(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Get price data

        Args:
            symbols: List of symbols (uses stock_list if None)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            use_cache: Whether to use cached data

        Returns:
            DataFrame with Date as index and symbols as columns
        """
        if use_cache:
            cached = self.storage.load_prices()
            if not cached.empty:
                return self._filter_by_date(cached, start_date, end_date)

        # Fetch from provider
        symbols = symbols or self.stock_list
        start = start_date or self._get_default_start()
        end = end_date or datetime.now().strftime("%Y-%m-%d")

        df = self.provider.get_price_data(symbols, start, end)
        if not df.empty:
            self.storage.save_prices(df)

        return df

    def get_returns(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Get returns data (pct_change)

        Args:
            symbols: List of symbols
            start_date: Start date
            end_date: End date
            use_cache: Whether to use cached data

        Returns:
            DataFrame with returns
        """
        if use_cache:
            cached = self.storage.load_returns()
            if not cached.empty:
                return self._filter_by_date(cached, start_date, end_date)

        # Calculate from prices
        prices = self.get_prices(symbols, start_date, end_date)
        if prices.empty:
            return pd.DataFrame()

        returns = prices.pct_change()
        self.storage.save_returns(returns)

        return self._filter_by_date(returns, start_date, end_date)

    def refresh_prices(self) -> Dict[str, Any]:
        """
        Refresh price data from provider

        Returns:
            Status dict
        """
        logger.info("Refreshing price data...")
        try:
            prices = self.provider.get_price_data(
                self.stock_list,
                self._get_default_start(),
                datetime.now().strftime("%Y-%m-%d")
            )

            if prices.empty:
                return {"status": "error", "message": "No data received"}

            self.storage.save_prices(prices)

            # Also save returns
            returns = prices.pct_change()
            self.storage.save_returns(returns)

            return {
                "status": "success",
                "records": len(prices),
                "symbols": len(prices.columns)
            }
        except Exception as e:
            logger.error(f"Error refreshing prices: {e}")
            return {"status": "error", "message": str(e)}

    # ========== ZT Pool Operations ==========

    def get_zt_pool(
        self,
        date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Get ZT pool for a date

        Args:
            date: Date in YYYYMMDD format (uses latest trading day if None)
            use_cache: Whether to use cached data

        Returns:
            ZT pool DataFrame
        """
        date = date or self.provider.get_latest_trading_date()

        if use_cache:
            cached = self.storage.load_zt_pool(date)
            if not cached.empty:
                return cached

        # Fetch from provider
        df = self.provider.get_zt_pool(date)
        if not df.empty:
            self.storage.save_zt_pool(df, date)

        return df

    def refresh_zt_pool(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Refresh ZT pool for a date

        Args:
            date: Date in YYYYMMDD format

        Returns:
            Status dict
        """
        date = date or self.provider.get_latest_trading_date()
        logger.info(f"Refreshing ZT pool for {date}...")

        try:
            df = self.provider.get_zt_pool(date)
            if df.empty:
                return {"status": "error", "message": "No data received"}

            self.storage.save_zt_pool(df, date)

            return {
                "status": "success",
                "date": date,
                "records": len(df)
            }
        except Exception as e:
            logger.error(f"Error refreshing ZT pool: {e}")
            return {"status": "error", "message": str(e)}

    def list_zt_pool_dates(self) -> List[str]:
        """List available ZT pool dates"""
        return self.storage.list_zt_pool_dates()

    # ========== Status & Utility ==========

    def get_data_status(self) -> Dict[str, Any]:
        """Get overall data status"""
        status = self.storage.get_data_status()
        status["stock_list"] = self.stock_list
        status["stock_count"] = len(self.stock_list)
        return status

    def cleanup_old_data(self, zt_pool_days: int = 90) -> Dict[str, int]:
        """
        Clean up old data

        Args:
            zt_pool_days: Days to keep ZT pool data

        Returns:
            Cleanup summary
        """
        deleted = self.storage.delete_old_zt_pool(zt_pool_days)
        return {"zt_pool_deleted": deleted}

    # ========== Helper Methods ==========

    def _get_default_start(self) -> str:
        """Get default start date (5 years ago)"""
        return (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")

    def _filter_by_date(
        self,
        df: pd.DataFrame,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """Filter DataFrame by date range"""
        if df.empty:
            return df

        mask = pd.Series(True, index=df.index)

        if start_date:
            mask &= (df.index >= pd.to_datetime(start_date))

        if end_date:
            mask &= (df.index <= pd.to_datetime(end_date))

        return df[mask]


# Convenience function for quick access
def get_data_service(storage_path: Optional[Path] = None) -> DataService:
    """Get a DataService instance"""
    return DataService(storage_path)

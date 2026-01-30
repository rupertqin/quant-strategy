"""Price Data Model"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd


@dataclass
class PriceData:
    """Price data container"""
    data: pd.DataFrame
    symbol: str
    start_date: str
    end_date: str
    last_updated: datetime
    source: str = "akshare"

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        symbol: str,
        start_date: str,
        end_date: str,
        source: str = "akshare"
    ) -> "PriceData":
        """Create from DataFrame"""
        return cls(
            data=df,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            last_updated=datetime.now(),
            source=source
        )

    @property
    def is_empty(self) -> bool:
        """Check if data is empty"""
        return self.data.empty

    @property
    def row_count(self) -> int:
        """Get number of rows"""
        return len(self.data)

    @property
    def columns(self) -> list:
        """Get column names"""
        return self.data.columns.tolist()

    def get_column(self, col: str) -> pd.Series:
        """Get a specific column"""
        if col in self.data.columns:
            return self.data[col]
        raise KeyError(f"Column {col} not found")

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics"""
        if self.data.empty:
            return {}

        stats = {
            "symbol": self.symbol,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "columns": self.columns,
        }

        # Numeric stats for each column
        numeric_stats = {}
        for col in self.data.columns:
            if pd.api.types.is_numeric_dtype(self.data[col]):
                numeric_stats[col] = {
                    "mean": self.data[col].mean(),
                    "std": self.data[col].std(),
                    "min": self.data[col].min(),
                    "max": self.data[col].max(),
                    "latest": self.data[col].iloc[-1] if len(self.data) > 0 else None
                }

        stats["statistics"] = numeric_stats
        return stats

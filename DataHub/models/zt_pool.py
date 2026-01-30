"""ZT Pool Data Model"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd


@dataclass
class ZtPoolData:
    """ZT (涨停) pool data container"""
    data: pd.DataFrame
    date: str
    record_count: int
    last_updated: datetime
    source: str = "akshare"

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        date: str,
        source: str = "akshare"
    ) -> "ZtPoolData":
        """Create from DataFrame"""
        return cls(
            data=df,
            date=date,
            record_count=len(df),
            last_updated=datetime.now(),
            source=source
        )

    @property
    def is_empty(self) -> bool:
        """Check if data is empty"""
        return self.data.empty

    def get_industry_summary(self) -> Dict[str, int]:
        """Get summary by industry"""
        if self.data.empty:
            return {}

        # Try different column names that akshare might use
        industry_col = None
        for col in ["行业", "行业板块", "所属行业", "申万行业"]:
            if col in self.data.columns:
                industry_col = col
                break

        if industry_col is None:
            # Try to find any column containing "行业"
            for col in self.data.columns:
                if "行业" in col:
                    industry_col = col
                    break

        if industry_col:
            return self.data[industry_col].value_counts().to_dict()

        return {}

    def get_sector_counts(self) -> Dict[str, int]:
        """Alias for get_industry_summary"""
        return self.get_industry_summary()

    def get_top_industries(self, top_n: int = 10) -> List[tuple]:
        """Get top N industries by count"""
        summary = self.get_industry_summary()
        sorted_items = sorted(summary.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:top_n]

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics"""
        if self.data.empty:
            return {
                "date": self.date,
                "record_count": 0,
                "is_empty": True
            }

        stats = {
            "date": self.date,
            "record_count": self.record_count,
            "is_empty": False,
            "columns": self.data.columns.tolist(),
            "industry_summary": self.get_industry_summary()
        }

        return stats

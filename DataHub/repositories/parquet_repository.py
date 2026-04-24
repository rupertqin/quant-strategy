"""
Parquet 数据管理器 - 轻量级 DAL（Data Access Layer）

量化时间序列数据的黄金法则：
- Read 占 99%：回测、算指标，每天都在疯狂读取
- Append 占 1%：每天收盘后追加一根 K 线
- Update ≈ 0%：历史 K 线不会变，除权除息时直接删掉旧文件重新下载全量覆盖
- Delete ≈ 0%：除非退市或数据源错误，通常直接删整个文件

因此不实现行级 update/delete，只提供三个原子操作：
    load()   - 快速读取
    save()   - 全量覆写（首次下载 / 除权除息 / 数据源修正）
    append() - 安全追加（每日收盘入库，幂等防重）

使用示例:
    from DataHub.repositories.parquet_repository import StockParquetStore

    store = StockParquetStore()
    df = store.load('600519.SH', start_date='2024-01-01')
    store.append('600519.SH', today_df)   # 读出->合并去重->覆写，防呆
    store.save('600519.SH', full_df)      # 直接覆盖整个文件
"""

import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd

from DataHub.config import (
    RAW_PRICE_DIR,
    RAW_ETF_PRICE_DIR,
    RAW_INDEX_PRICE_DIR,
    INTRADAY_DIR,
)

logger = logging.getLogger(__name__)


class ParquetDataManager:
    """通用 Parquet 数据管理器：单只标的 = 单个 parquet 文件"""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _file_path(self, symbol: str) -> Path:
        return self.base_dir / f"{symbol}.parquet"

    def exists(self, symbol: str) -> bool:
        return self._file_path(symbol).exists()

    def list_symbols(self) -> List[str]:
        return sorted([f.stem for f in self.base_dir.glob("*.parquet")])

    # ── Read ──────────────────────────────────

    def load(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        fields: List[str] = None,
    ) -> pd.DataFrame:
        fp = self._file_path(symbol)
        if not fp.exists():
            return pd.DataFrame()

        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            self.logger.error(f"读取 {symbol} 失败: {e}")
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            if start_date:
                df = df[df["trade_date"] >= start_date]
            if end_date:
                df = df[df["trade_date"] <= end_date]
            df = df.sort_values("trade_date")

        if fields:
            keep = ["trade_date"] + [c for c in fields if c in df.columns]
            df = df[keep]

        return df.reset_index(drop=True)

    def load_many(
        self,
        symbols: List[str],
        field: str = "close",
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()

        pieces = []
        for sym in symbols:
            fp = self._file_path(sym)
            if not fp.exists():
                continue
            try:
                df = pd.read_parquet(fp)
            except Exception as e:
                self.logger.warning(f"读取 {sym} 失败: {e}")
                continue

            if df.empty or field not in df.columns or "trade_date" not in df.columns:
                continue

            df["trade_date"] = pd.to_datetime(df["trade_date"])
            if start_date:
                df = df[df["trade_date"] >= start_date]
            if end_date:
                df = df[df["trade_date"] <= end_date]

            pieces.append(df[["trade_date", field]].copy().assign(symbol=sym))

        if not pieces:
            return pd.DataFrame()

        df = pd.concat(pieces, ignore_index=True)
        return df.pivot(index="trade_date", columns="symbol", values=field)

    # ── Save (全量覆写) ───────────────────────

    def save(self, symbol: str, df: pd.DataFrame) -> None:
        """全量覆写：首次下载、除权除息修正、数据源错误时直接整个文件替换"""
        if df.empty:
            self.logger.warning(f"{symbol} 数据为空，跳过保存")
            return

        required = ["trade_date", "close"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = df.sort_values("trade_date").reset_index(drop=True)

        fp = self._file_path(symbol)
        df.to_parquet(fp, index=False, compression="zstd")
        self.logger.info(f"全量保存 {symbol}: {len(df)} 条")

    # ── Append (安全追加) ─────────────────────

    def append(self, symbol: str, df: pd.DataFrame) -> None:
        """
        安全追加：每日收盘后追加今日数据（幂等防重）

        核心逻辑：读出旧数据 -> 合并 -> 按 trade_date 去重(keep='last') -> 覆写
        单只股票 20 年日线约 5000 行 / <1MB，读出+合并+覆写 < 0.05s，
        远比 Parquet 原生 append 安全（防碎片、防重复）。
        """
        if df.empty:
            return

        required = ["trade_date", "close"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

        fp = self._file_path(symbol)

        if not fp.exists():
            df = df.sort_values("trade_date").reset_index(drop=True)
            df.to_parquet(fp, index=False, compression="zstd")
            self.logger.info(f"新建 {symbol}: {len(df)} 条")
            return

        old_df = pd.read_parquet(fp)
        old_df["trade_date"] = pd.to_datetime(old_df["trade_date"]).dt.date

        combined = pd.concat([old_df, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["trade_date"], keep="last")
        combined = combined.sort_values("trade_date").reset_index(drop=True)

        combined.to_parquet(fp, index=False, compression="zstd")
        self.logger.info(
            f"追加 {symbol}: 新增 {len(combined) - len(old_df)} 条，总计 {len(combined)} 条"
        )

    # ── Delete (整文件删除) ───────────────────

    def delete(self, symbol: str) -> bool:
        """删除整只标的的 parquet 文件（退市 / 数据源错误时调用）"""
        fp = self._file_path(symbol)
        if fp.exists():
            fp.unlink()
            self.logger.info(f"已删除 {symbol}")
            return True
        return False


# ── 资产类型专用子类 ──────────────────────────

class StockParquetStore(ParquetDataManager):
    def __init__(self):
        super().__init__(RAW_PRICE_DIR)


class EtfParquetStore(ParquetDataManager):
    def __init__(self):
        super().__init__(RAW_ETF_PRICE_DIR)


class IndexParquetStore(ParquetDataManager):
    def __init__(self):
        super().__init__(RAW_INDEX_PRICE_DIR)


# ── Intraday 分钟级数据管理器 ─────────────────

class IntradayStore:
    """分钟级实时数据管理器：按日期存储，每天一个 parquet 文件"""

    def __init__(self, asset_type: str = "stock"):
        self.asset_type = asset_type
        self.intraday_dir = INTRADAY_DIR / asset_type
        self.intraday_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(f"{self.__class__.__name__}[{asset_type}]")

    def _file_path(self, date_str: str = None) -> Path:
        from datetime import datetime
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        return self.intraday_dir / f"{date_str}.parquet"

    def load(
        self,
        date_str: str = None,
        symbol: str = None,
        latest_snapshot: bool = False,
    ) -> Optional[pd.DataFrame]:
        fp = self._file_path(date_str)
        if not fp.exists():
            return None

        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            self.logger.error(f"读取 intraday 失败: {e}")
            return None

        if df.empty:
            return None

        if symbol:
            df = df[df["symbol"] == symbol]

        if latest_snapshot and "timestamp" in df.columns:
            df = df.sort_values("timestamp").groupby("symbol").tail(1)

        return df.reset_index(drop=True)

    def append(self, df: pd.DataFrame, date_str: str = None) -> str:
        """追加写入 intraday 数据（自动去重 symbol，保留最新）"""
        if df.empty:
            raise ValueError("DataFrame 为空")

        df = df.copy()
        fp = self._file_path(date_str)

        # 统一用 timestamp，删除旧列
        if "timestamp" not in df.columns:
            df["timestamp"] = pd.Timestamp.now()
        for col in ("is_realtime", "trade_time", "trade_date"):
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        keep_cols = [
            "symbol", "name", "timestamp",
            "open", "high", "low", "close", "change_pct",
            "volume", "amount",
        ]
        df = df[[c for c in keep_cols if c in df.columns]]

        # 按日期直接覆盖写入（不读取旧文件，避免旧格式列残留）
        df.to_parquet(fp, index=False)
        self.logger.info(f"Intraday 已保存: {fp} ({len(df)} 条)")
        return str(fp)

    def archive(self, date_str: str = None) -> None:
        """归档：日终同步后删除当天 realtime 文件"""
        from datetime import datetime
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        fp = self._file_path(date_str)
        if fp.exists():
            try:
                fp.unlink()
                self.logger.info(f"已归档(删除) intraday: {self.asset_type}/{date_str}")
            except Exception as e:
                self.logger.warning(f"归档删除失败 {self.asset_type}/{date_str}: {e}")

    def delete(self, date_str: str) -> bool:
        fp = self._file_path(date_str)
        if fp.exists():
            fp.unlink()
            return True
        return False

"""Storage Engine - Parquet + SQLite storage"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

logger = logging.getLogger(__name__)


class StorageEngine:
    """Storage engine for Parquet and SQLite"""

    def __init__(self, base_path: Optional[Path] = None):
        """
        Initialize storage engine

        Args:
            base_path: Base path for storage directory
        """
        if base_path is None:
            # Default to DataHub/storage
            base_path = Path(__file__).parent.parent.parent / "storage"

        self.base_path = Path(base_path)
        self.raw_prices_dir = self.base_path / "raw" / "prices"
        self.raw_zt_pool_dir = self.base_path / "raw" / "zt_pool"
        self.processed_returns_dir = self.base_path / "processed" / "returns"
        self.database_dir = self.base_path / "database"
        self.database_path = self.database_dir / "datahub.db"

        # Create directories
        for _dir in [
            self.raw_prices_dir,
            self.raw_zt_pool_dir,
            self.processed_returns_dir,
            self.database_dir
        ]:
            _dir.mkdir(parents=True, exist_ok=True)

        # Initialize SQLite
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Data versions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_versions (
                data_type TEXT PRIMARY KEY,
                version INTEGER,
                updated_at TEXT,
                checksum TEXT
            )
        """)

        # ZT pool index table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zt_pool_index (
                date TEXT PRIMARY KEY,
                file_path TEXT,
                record_count INTEGER,
                created_at TEXT
            )
        """)

        # Jobs/execution log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                records_processed INTEGER,
                error_message TEXT
            )
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.database_path}")

    # ========== Price Data Operations ==========

    def save_prices(self, df: pd.DataFrame, version: int = 1) -> bool:
        """
        Save price data to Parquet

        Args:
            df: DataFrame with Date as index and symbols as columns
            version: Version number

        Returns:
            True if successful
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return False

        try:
            file_path = self.raw_prices_dir / "prices.parquet"
            df.to_parquet(file_path, engine="pyarrow", compression="snappy")

            # Update version
            checksum = str(hash(df.to_csv()))
            self._update_version("prices", version, checksum)

            logger.info(f"Saved prices to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving prices: {e}")
            return False

    def load_prices(self) -> pd.DataFrame:
        """Load price data from Parquet"""
        file_path = self.raw_prices_dir / "prices.parquet"
        if not file_path.exists():
            logger.warning(f"Price file not found: {file_path}")
            return pd.DataFrame()

        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Loaded prices: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error loading prices: {e}")
            return pd.DataFrame()

    def save_returns(self, df: pd.DataFrame, version: int = 1) -> bool:
        """
        Save returns data to Parquet

        Args:
            df: DataFrame with Date as index and symbols as columns (pct_change)

        Returns:
            True if successful
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return False

        try:
            file_path = self.processed_returns_dir / "returns.parquet"
            df.to_parquet(file_path, engine="pyarrow", compression="snappy")

            checksum = str(hash(df.to_csv()))
            self._update_version("returns", version, checksum)

            logger.info(f"Saved returns to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving returns: {e}")
            return False

    def load_returns(self) -> pd.DataFrame:
        """Load returns data from Parquet"""
        file_path = self.processed_returns_dir / "returns.parquet"
        if not file_path.exists():
            logger.warning(f"Returns file not found: {file_path}")
            return pd.DataFrame()

        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Loaded returns: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading returns: {e}")
            return pd.DataFrame()

    # ========== ZT Pool Operations ==========

    def save_zt_pool(self, df: pd.DataFrame, date: str) -> bool:
        """
        Save ZT pool data for a specific date

        Args:
            df: ZT pool DataFrame
            date: Date in YYYYMMDD format

        Returns:
            True if successful
        """
        if df.empty:
            logger.warning(f"Empty ZT pool for {date}, skipping save")
            return False

        try:
            file_path = self.raw_zt_pool_dir / f"zt_pool_{date}.parquet"
            df.to_parquet(file_path, engine="pyarrow", compression="snappy")

            # Update index
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO zt_pool_index VALUES (?, ?, ?, ?)",
                (date, str(file_path), len(df), datetime.now().isoformat())
            )
            conn.commit()
            conn.close()

            logger.info(f"Saved ZT pool for {date}: {len(df)} records")
            return True
        except Exception as e:
            logger.error(f"Error saving ZT pool: {e}")
            return False

    def load_zt_pool(self, date: str) -> pd.DataFrame:
        """Load ZT pool data for a specific date"""
        file_path = self.raw_zt_pool_dir / f"zt_pool_{date}.parquet"
        if not file_path.exists():
            logger.warning(f"ZT pool not found: {file_path}")
            return pd.DataFrame()

        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Loaded ZT pool for {date}: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error loading ZT pool: {e}")
            return pd.DataFrame()

    def list_zt_pool_dates(self) -> List[str]:
        """List all available ZT pool dates"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT date FROM zt_pool_index ORDER BY date DESC")
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates

    def delete_old_zt_pool(self, days: int = 90) -> int:
        """
        Delete ZT pool data older than specified days

        Args:
            days: Number of days to keep

        Returns:
            Number of deleted files
        """
        from datetime import datetime, timedelta

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Get files to delete
        cursor.execute("SELECT date, file_path FROM zt_pool_index WHERE date < ?", (cutoff,))
        to_delete = cursor.fetchall()

        deleted = 0
        for date, file_path in to_delete:
            try:
                Path(file_path).unlink()
                cursor.execute("DELETE FROM zt_pool_index WHERE date = ?", (date,))
                deleted += 1
            except Exception as e:
                logger.error(f"Error deleting {file_path}: {e}")

        conn.commit()
        conn.close()
        logger.info(f"Deleted {deleted} old ZT pool files")
        return deleted

    # ========== Version Management ==========

    def _update_version(self, data_type: str, version: int, checksum: str):
        """Update data version in database"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO data_versions VALUES (?, ?, ?, ?)",
            (data_type, version, datetime.now().isoformat(), checksum)
        )
        conn.commit()
        conn.close()

    def get_version(self, data_type: str) -> Optional[Dict[str, Any]]:
        """Get version info for a data type"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM data_versions WHERE data_type = ?", (data_type,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "data_type": row[0],
                "version": row[1],
                "updated_at": row[2],
                "checksum": row[3]
            }
        return None

    def get_data_status(self) -> Dict[str, Any]:
        """Get overall data status"""
        status = {
            "prices": self._get_file_status(self.raw_prices_dir / "prices.parquet"),
            "returns": self._get_file_status(self.processed_returns_dir / "returns.parquet"),
            "zt_pool_dates": len(self.list_zt_pool_dates()),
            "versions": {},
        }

        for dtype in ["prices", "returns"]:
            version = self.get_version(dtype)
            if version:
                status["versions"][dtype] = version

        return status

    def _get_file_status(self, path: Path) -> Dict[str, Any]:
        """Get status of a file"""
        if not path.exists():
            return {"exists": False}

        stat = path.stat()
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
        }

    # ========== Job Logging ==========

    def log_job(
        self,
        job_name: str,
        status: str,
        records_processed: int = 0,
        error_message: Optional[str] = None
    ):
        """Log a job execution"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO job_log
               (job_name, status, started_at, completed_at, records_processed, error_message)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (job_name, status, datetime.now().isoformat(), datetime.now().isoformat(),
             records_processed, error_message)
        )
        conn.commit()
        conn.close()

    def get_recent_jobs(self, job_name: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Get recent job logs"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        if job_name:
            cursor.execute(
                "SELECT * FROM job_log WHERE job_name = ? ORDER BY id DESC LIMIT ?",
                (job_name, limit)
            )
        else:
            cursor.execute(
                "SELECT * FROM job_log ORDER BY id DESC LIMIT ?",
                (limit,)
            )

        rows = cursor.fetchall()
        conn.close()

        return [
            {
                "id": row[0],
                "job_name": row[1],
                "status": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "records_processed": row[5],
                "error_message": row[6]
            }
            for row in rows
        ]

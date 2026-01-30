"""
统一数据管理层 - Parquet + SQLite 接口

提供高效的数据读写接口，支持:
- Parquet 格式存储行情数据 (prices, returns)
- SQLite 存储信号和元数据
- 向后兼容 CSV 读取
- DataHub 集成
"""

import os
import sys
import json
import logging
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
import pandas as pd

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

# 尝试导入 DataHub
try:
    from DataHub.services.data_service import DataService
    DATAHUB_AVAILABLE = True
except ImportError:
    DATAHUB_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class DataManager:
    """统一数据管理器"""

    def __init__(self, base_dir: Optional[str] = None, use_datahub: bool = True):
        """
        初始化 DataManager

        Args:
            base_dir: 基础目录，默认为当前文件所在目录
            use_datahub: 是否使用 DataHub (默认 True)
        """
        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        self.base_dir = base_dir
        self.data_dir = os.path.join(base_dir, "data")
        self.cache_dir = os.path.join(base_dir, "cache")
        self.metadata_dir = os.path.join(base_dir, "metadata")

        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

        # SQLite 数据库路径 (统一到 storage/outputs)
        self.db_path = os.path.join(
            os.path.dirname(os.path.dirname(base_dir)),
            "storage", "outputs", "shortterm", "database", "signals.db"
        )

        # 初始化 DataHub
        self.use_datahub = use_datahub and DATAHUB_AVAILABLE
        if self.use_datahub:
            self.datahub_service = DataService()
            logger.info("DataManager initialized with DataHub support")
        else:
            self.datahub_service = None
            logger.info("DataManager initialized without DataHub (local mode)")

        self._init_db()

    def _init_db(self):
        """初始化 SQLite 数据库"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建信号表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE NOT NULL,
                total_zt_count INTEGER,
                signals_json TEXT,
                hot_sectors_json TEXT,
                generated_at TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建权重历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weights_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                weight REAL,
                UNIQUE(date, symbol)
            )
        ''')

        # 创建数据版本表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_versions (
                name TEXT PRIMARY KEY,
                version INTEGER,
                last_updated TEXT,
                record_count INTEGER,
                checksum TEXT
            )
        ''')

        conn.commit()
        conn.close()

    # ============= Parquet 数据读写 =============

    def _get_parquet_path(self, name: str) -> str:
        """获取 Parquet 文件路径"""
        return os.path.join(self.data_dir, f"{name}.parquet")

    def save_prices(self, df: pd.DataFrame) -> bool:
        """
        保存价格数据到 Parquet

        Args:
            df: 宽格式 DataFrame，index 为日期，列为股票代码

        Returns:
            bool: 是否成功
        """
        if not PYARROW_AVAILABLE:
            # 回退到 CSV
            csv_path = os.path.join(self.data_dir, "prices.csv")
            df.to_csv(csv_path)
            return True

        try:
            path = self._get_parquet_path("prices")
            df.to_parquet(path, engine='pyarrow', index=True)
            self._update_version("prices", len(df))
            return True
        except Exception as e:
            print(f"保存 prices.parquet 失败: {e}")
            return False

    def get_prices(self, start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   use_datahub: bool = None) -> pd.DataFrame:
        """
        读取价格数据

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            use_datahub: 是否使用 DataHub (None=使用默认设置)

        Returns:
            pd.DataFrame: 价格数据
        """
        # 优先使用 DataHub
        if use_datahub is None:
            use_datahub = self.use_datahub

        if use_datahub and self.datahub_service:
            try:
                df = self.datahub_service.get_prices(
                    start_date=start_date,
                    end_date=end_date,
                    use_cache=True
                )
                if not df.empty:
                    logger.info(f"Loaded prices from DataHub: {len(df)} rows")
                    return df
            except Exception as e:
                logger.warning(f"DataHub unavailable: {e}")

        # 回退到本地文件
        parquet_path = self._get_parquet_path("prices")
        csv_path = os.path.join(self.data_dir, "prices.csv")

        # 优先读取 Parquet
        if PYARROW_AVAILABLE and os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                if start_date:
                    df = df[df.index >= start_date]
                if end_date:
                    df = df[df.index <= end_date]
                return df
            except Exception as e:
                print(f"读取 prices.parquet 失败: {e}")

        # 回退到 CSV
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            if start_date:
                df = df[df.index >= start_date]
            if end_date:
                df = df[df.index <= end_date]
            return df

        return pd.DataFrame()

    def save_returns(self, df: pd.DataFrame) -> bool:
        """
        保存收益率数据到 Parquet

        Args:
            df: 宽格式 DataFrame，index 为日期，列为股票代码

        Returns:
            bool: 是否成功
        """
        if not PYARROW_AVAILABLE:
            csv_path = os.path.join(self.data_dir, "returns.csv")
            df.to_csv(csv_path)
            return True

        try:
            path = self._get_parquet_path("returns")
            df.to_parquet(path, engine='pyarrow', index=True)
            self._update_version("returns", len(df))
            return True
        except Exception as e:
            print(f"保存 returns.parquet 失败: {e}")
            return False

    def get_returns(self, start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    use_datahub: bool = None) -> pd.DataFrame:
        """
        读取收益率数据

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            use_datahub: 是否使用 DataHub (None=使用默认设置)

        Returns:
            pd.DataFrame: 收益率数据
        """
        # 优先使用 DataHub
        if use_datahub is None:
            use_datahub = self.use_datahub

        if use_datahub and self.datahub_service:
            try:
                df = self.datahub_service.get_returns(
                    start_date=start_date,
                    end_date=end_date,
                    use_cache=True
                )
                if not df.empty:
                    logger.info(f"Loaded returns from DataHub: {len(df)} rows")
                    return df
            except Exception as e:
                logger.warning(f"DataHub unavailable: {e}")

        # 回退到本地文件
        parquet_path = self._get_parquet_path("returns")
        csv_path = os.path.join(self.data_dir, "returns.csv")

        # 优先读取 Parquet
        if PYARROW_AVAILABLE and os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                if start_date:
                    df = df[df.index >= start_date]
                if end_date:
                    df = df[df.index <= end_date]
                return df
            except Exception as e:
                print(f"读取 returns.parquet 失败: {e}")

        # 回退到 CSV
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            if start_date:
                df = df[df.index >= start_date]
            if end_date:
                df = df[df.index <= end_date]
            return df

        return pd.DataFrame()

    # ============= 涨停池缓存 =============

    def save_zt_pool(self, df: pd.DataFrame, date: str) -> bool:
        """
        保存涨停池数据 (按年/月组织)

        Args:
            df: 涨停池数据
            date: 日期 (YYYYMMDD 格式)

        Returns:
            bool: 是否成功
        """
        year = date[:4]
        month = date[4:6]

        cache_path = os.path.join(
            self.cache_dir, "zt_pool", year, month,
            f"{date}.parquet"
        )

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        if PYARROW_AVAILABLE:
            try:
                df.to_parquet(cache_path, engine='pyarrow', index=False)
                return True
            except Exception as e:
                print(f"保存涨停池缓存失败: {e}")

        # 回退到 CSV
        csv_path = cache_path.replace('.parquet', '.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        return True

    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        读取涨停池数据

        Args:
            date: 日期 (YYYYMMDD 格式)

        Returns:
            pd.DataFrame: 涨停池数据
        """
        year = date[:4]
        month = date[4:6]

        # 尝试 Parquet
        if PYARROW_AVAILABLE:
            parquet_path = os.path.join(
                self.cache_dir, "zt_pool", year, month,
                f"{date}.parquet"
            )
            if os.path.exists(parquet_path):
                return pd.read_parquet(parquet_path)

        # 回退到 CSV
        csv_path = os.path.join(
            self.cache_dir, "zt_pool", year, month,
            f"{date}.csv"
        )
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path, encoding='utf-8-sig')

        return pd.DataFrame()

    # ============= SQLite 信号存储 =============

    def save_daily_signals(self, date: str, signals: Dict[str, Any]) -> bool:
        """
        保存每日信号到 SQLite

        Args:
            date: 日期 (YYYYMMDD 格式)
            signals: 信号数据字典

        Returns:
            bool: 是否成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO daily_signals
                (date, total_zt_count, signals_json, hot_sectors_json, generated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                date,
                signals.get('total_zt_count'),
                json.dumps(signals.get('signals', []), ensure_ascii=False),
                json.dumps(signals.get('hot_sectors', []), ensure_ascii=False),
                signals.get('generated_at')
            ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"保存信号失败: {e}")
            return False

    def get_daily_signals(self, date: str) -> Dict[str, Any]:
        """
        读取每日信号

        Args:
            date: 日期 (YYYYMMDD 格式)

        Returns:
            Dict: 信号数据
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT total_zt_count, signals_json, hot_sectors_json, generated_at
            FROM daily_signals WHERE date = ?
        ''', (date,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'date': date,
                'total_zt_count': row[0],
                'signals': json.loads(row[1]) if row[1] else [],
                'hot_sectors': json.loads(row[2]) if row[2] else [],
                'generated_at': row[3]
            }

        return {}

    def save_weights_history(self, date: str, weights: pd.DataFrame) -> bool:
        """
        保存权重历史

        Args:
            date: 日期
            weights: 权重 DataFrame (包含 symbol, weight 列)

        Returns:
            bool: 是否成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            for _, row in weights.iterrows():
                cursor.execute('''
                    INSERT OR REPLACE INTO weights_history (date, symbol, weight)
                    VALUES (?, ?, ?)
                ''', (date, row['symbol'], row['weight']))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"保存权重历史失败: {e}")
            return False

    def get_weights_history(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        读取权重历史

        Args:
            date: 可选日期

        Returns:
            pd.DataFrame: 权重历史
        """
        conn = sqlite3.connect(self.db_path)

        if date:
            df = pd.read_sql_query(
                "SELECT * FROM weights_history WHERE date = ? ORDER BY weight DESC",
                conn, params=(date,)
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM weights_history ORDER BY date DESC, weight DESC",
                conn
            )

        conn.close()
        return df

    # ============= 版本管理 =============

    def _update_version(self, name: str, count: int):
        """更新数据版本信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO data_versions (name, version, last_updated, record_count)
            VALUES (?, COALESCE((SELECT version FROM data_versions WHERE name = ?), 0) + 1, ?, ?)
        ''', (name, name, datetime.now().isoformat(), count))

        conn.commit()
        conn.close()

    def get_version(self, name: str) -> Dict[str, Any]:
        """获取数据版本信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT version, last_updated, record_count FROM data_versions WHERE name = ?
        ''', (name,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'version': row[0],
                'last_updated': row[1],
                'record_count': row[2]
            }
        return {}

    def get_all_versions(self) -> Dict[str, Dict[str, Any]]:
        """获取所有数据版本"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM data_versions", conn)
        conn.close()

        return df.set_index('name').to_dict('index')

    # ============= 工具方法 =============

    def get_latest_date(self, data_type: str = 'returns') -> Optional[str]:
        """获取最新数据日期"""
        if data_type == 'returns':
            df = self.get_returns()
        else:
            df = self.get_prices()

        if not df.empty:
            return df.index.max().strftime('%Y-%m-%d')
        return None

    def check_data_freshness(self) -> Dict[str, Any]:
        """检查数据新鲜度"""
        result = {
            'prices': {'status': 'ok', 'latest_date': None},
            'returns': {'status': 'ok', 'latest_date': None},
            'signals': {'status': 'ok', 'latest_date': None}
        }

        # 检查 prices
        latest = self.get_latest_date('prices')
        if latest:
            result['prices']['latest_date'] = latest
        else:
            result['prices']['status'] = 'missing'

        # 检查 returns
        latest = self.get_latest_date('returns')
        if latest:
            result['returns']['latest_date'] = latest
        else:
            result['returns']['status'] = 'missing'

        # 检查 signals
        versions = self.get_all_versions()
        if 'signals' not in versions:
            result['signals']['status'] = 'missing'

        return result

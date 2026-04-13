import os
import sys
import json
import logging
import yaml
from pathlib import Path
from datetime import datetime
from typing import Optional

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

logger = logging.getLogger(__name__)

# 导入数据读取接口
from DataHub.core.data_reader import load_stock_prices


class ShortTermDataManager:
    """短线策略数据管理器 - 支持 DataHub"""

    def __init__(self, config_path: Optional[str] = None, use_datahub: bool = True):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
        self.config_path = config_path
        self.base_dir = os.path.dirname(config_path)
        self.config = self._load_config(config_path)

        self.cache_dir = self.config['cache']['dir']
        if not os.path.isabs(self.cache_dir):
            self.cache_dir = os.path.join(self.base_dir, self.cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

        # 输出路径
        output_config = self.config.get('output', {})
        self.signals_file = output_config.get('signals_file', '../storage/outputs/shortterm/signals/daily_signals.json')
        self.database_file = output_config.get('database_file', '../storage/outputs/shortterm/database/signals.db')
        if not os.path.isabs(self.signals_file):
            self.signals_file = os.path.join(self.base_dir, self.signals_file)
        if not os.path.isabs(self.database_file):
            self.database_file = os.path.join(self.base_dir, self.database_file)

        logger.info("ShortTermDataManager initialized")

    def _load_config(self, path: str) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def save_zt_pool(self, date: str, df: pd.DataFrame):
        """保存涨停池数据到本地缓存"""
        cache_file = os.path.join(self.cache_dir, f"zt_pool_{date}.csv")
        df.to_csv(cache_file, index=False, encoding='utf-8-sig')

    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """读取涨停池数据"""
        cache_file = os.path.join(self.cache_dir, f"zt_pool_{date}.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return pd.DataFrame()

    def save_daily_signals(self, date: str, signals: dict):
        """保存每日信号到 JSON"""
        os.makedirs(os.path.dirname(self.signals_file), exist_ok=True)
        signals['date'] = date
        signals['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.signals_file, 'w', encoding='utf-8') as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)

    def get_daily_signals(self, date: Optional[str] = None) -> dict:
        """读取每日信号"""
        if os.path.exists(self.signals_file):
            with open(self.signals_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if date is None or data.get('date') == date:
                    return data
        return {}


class ZTPoolManager:
    """涨停池管理器 (兼容旧接口)"""

    def __init__(self, use_datahub: bool = True):
        self.dm = ShortTermDataManager(use_datahub=use_datahub)

    def save(self, date: str, df: pd.DataFrame):
        self.dm.save_zt_pool(date, df)

    def load(self, date: str) -> pd.DataFrame:
        return self.dm.get_zt_pool(date)


class DailySignalManager:
    """每日信号管理器 (兼容旧接口)"""

    def __init__(self, use_datahub: bool = True):
        self.dm = ShortTermDataManager(use_datahub=use_datahub)

    def save(self, date: str, signals: dict):
        self.dm.save_daily_signals(date, signals)

    def load(self, date: Optional[str] = None) -> dict:
        return self.dm.get_daily_signals(date)

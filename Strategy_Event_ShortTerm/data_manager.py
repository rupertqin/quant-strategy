import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional


class ShortTermDataManager:
    """短线策略数据管理器 - 简化版"""

    def __init__(self, base_dir: Optional[str] = None):
        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_dir = base_dir
        self.cache_dir = os.path.join(self.base_dir, "cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    def save_zt_pool(self, date: str, df: pd.DataFrame):
        """保存涨停池数据"""
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
        output_file = os.path.join(self.base_dir, "daily_signals.json")
        signals['date'] = date
        signals['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)

    def get_daily_signals(self, date: Optional[str] = None) -> dict:
        """读取每日信号"""
        signals_file = os.path.join(self.base_dir, "daily_signals.json")
        if os.path.exists(signals_file):
            with open(signals_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if date is None or data.get('date') == date:
                    return data
        return {}


class ZTPoolManager:
    """涨停池管理器 (兼容旧接口)"""

    def __init__(self):
        self.dm = ShortTermDataManager()

    def save(self, date: str, df: pd.DataFrame):
        self.dm.save_zt_pool(date, df)

    def load(self, date: str) -> pd.DataFrame:
        return self.dm.get_zt_pool(date)


class DailySignalManager:
    """每日信号管理器 (兼容旧接口)"""

    def __init__(self):
        self.dm = ShortTermDataManager()

    def save(self, date: str, signals: dict):
        self.dm.save_daily_signals(date, signals)

    def load(self, date: Optional[str] = None) -> dict:
        return self.dm.get_daily_signals(date)

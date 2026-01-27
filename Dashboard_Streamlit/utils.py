"""
Dashboard 工具模块
使用 DataBridge 读取各策略数据，完全解耦
"""

import pandas as pd
import os
import sys
from datetime import datetime
from typing import Optional


class DashboardUtils:
    """看板工具类 - 使用 DataBridge"""

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._init_bridge()

    def _init_bridge(self):
        """初始化 DataBridge"""
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from data_bridge import DataBridge
            self.bridge = DataBridge()
        except ImportError:
            self.bridge = None

    def get_longterm_weights(self) -> pd.DataFrame:
        if self.bridge:
            return self.bridge.get_longterm_weights()
        return pd.DataFrame()

    def get_longterm_metrics(self) -> dict:
        if self.bridge:
            return self.bridge.get_longterm_metrics()
        return {'report_exists': False}

    def get_shortterm_signals(self) -> dict:
        if self.bridge:
            return self.bridge.get_shortterm_signals()
        return {}

    def get_shortterm_history(self) -> pd.DataFrame:
        if self.bridge:
            return self.bridge.get_sector_heat_history()
        return pd.DataFrame()

    def get_market_regime(self) -> dict:
        if self.bridge:
            return self.bridge.get_market_regime()
        return {
            'regime': 'UNKNOWN',
            'score': 0,
            'reasons': ['DataBridge not available'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def combine_signals(self) -> dict:
        if self.bridge:
            return self.bridge.get_all_data()
        return {
            'market_regime': self.get_market_regime(),
            'longterm': {'weights': [], 'metrics': self.get_longterm_metrics()},
            'shortterm': {}
        }

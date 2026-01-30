"""
统一数据访问层 - 解耦 Dashboard 与各策略项目
从 storage/outputs/ 读取统一输出
"""
import os
import json
import sys
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any


class DataBridge:
    """直接读取各项目数据，不依赖其他模块"""

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.longterm_dir = os.path.join(self.base_dir, "LongTerm")
        self.shortterm_dir = os.path.join(self.base_dir, "ShortTerm")
        # 新统一输出目录
        self.storage_outputs = os.path.join(self.base_dir, "storage", "outputs")

    def get_longterm_weights(self) -> pd.DataFrame:
        """读取长线权重配置 - 从 storage/outputs 读取"""
        weights_file = os.path.join(self.storage_outputs, "longterm", "weights", "output_weights.csv")
        if os.path.exists(weights_file):
            return pd.read_csv(weights_file)
        return pd.DataFrame(columns=['symbol', 'weight'])

    def get_longterm_metrics(self) -> dict:
        """获取长线绩效指标"""
        report_file = os.path.join(self.storage_outputs, "longterm", "reports", "portfolio_report.html")
        if os.path.exists(report_file):
            return {
                'report_exists': True,
                'last_update': datetime.fromtimestamp(
                    os.path.getmtime(report_file)
                ).strftime('%Y-%m-%d %H:%M')
            }
        return {'report_exists': False}

    def get_shortterm_signals(self) -> dict:
        """读取短线信号 - 从 storage/outputs 读取"""
        signals_file = os.path.join(self.storage_outputs, "shortterm", "signals", "daily_signals.json")
        if os.path.exists(signals_file):
            with open(signals_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """读取涨停池 - 优先从 DataHub storage"""
        from DataHub.config import RAW_ZT_POOL_DIR
        parquet_file = RAW_ZT_POOL_DIR / f"zt_pool_{date}.parquet"
        if parquet_file.exists():
            return pd.read_parquet(parquet_file)

        # 回退到 cache
        cache_file = os.path.join(self.shortterm_dir, "cache", f"zt_pool_{date}.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return pd.DataFrame()

    def get_sector_heat_history(self) -> pd.DataFrame:
        """读取板块热度历史 - 从 storage/outputs 读取"""
        history_file = os.path.join(self.storage_outputs, "shortterm", "history", "sector_heat_history.csv")
        if os.path.exists(history_file):
            return pd.read_csv(history_file)
        return pd.DataFrame()

    def get_market_regime(self) -> dict:
        """获取市场状态"""
        # 直接实例化 MarketRegime，避免复杂的路径问题
        try:
            sys.path.insert(0, self.shortterm_dir)
            from market_regime import MarketRegime
            regime = MarketRegime()
            return regime.get_market_status()
        except Exception as e:
            return {
                'regime': 'UNKNOWN',
                'score': 0,
                'reasons': [str(e)],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        finally:
            # 清理 path
            if self.shortterm_dir in sys.path:
                sys.path.remove(self.shortterm_dir)

    def get_all_data(self) -> dict:
        """获取所有数据"""
        return {
            'market_regime': self.get_market_regime(),
            'longterm': {
                'weights': self.get_longterm_weights().to_dict('records') if not self.get_longterm_weights().empty else [],
                'metrics': self.get_longterm_metrics()
            },
            'shortterm': self.get_shortterm_signals()
        }

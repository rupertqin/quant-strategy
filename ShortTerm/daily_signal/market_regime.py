"""
市场状态判断模块
宏观因子分析：汇率、黄金、北向资金
通过 DataHub 获取数据
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from DataHub.core.data_client import UnifiedDataClient

logger = logging.getLogger(__name__)


class MarketRegime:
    """市场状态判断"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            # 尝试多个路径找到 config.yaml
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            
            # 优先使用父目录(ShortTerm)的配置
            config_path = os.path.join(parent_dir, "config.yaml")
            if not os.path.exists(config_path):
                # 回退到当前目录
                config_path = os.path.join(current_dir, "config.yaml")
        
        self.config = self._load_config(config_path) if os.path.exists(config_path) else {}
        self.cache_dir = self.config.get('cache', {}).get('dir', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 初始化 DataHub 客户端
        self.data_client = UnifiedDataClient()

    def _load_config(self, path: str) -> dict:
        import yaml
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {path}, using default config")
            return {
                'cache': {'dir': 'cache'},
                'event_params': {'min_zt_count': 3},
                'output': {'signals_file': 'signals.json', 'history_file': 'history.csv'}
            }

    def get_usd_cny_rate(self) -> dict:
        """获取美元人民币汇率"""
        try:
            df = self.data_client.get_fx_rate("USD/CNY")
            if df.empty:
                return {'current': 7.2, 'change_5d': 0, 'date': None}

            latest = df.iloc[0]
            return {
                'current': float(latest['卖报价']),
                'change_5d': 0,
                'date': None
            }
        except Exception as e:
            logger.warning(f"获取汇率失败: {e}")
            return {'current': 7.2, 'change_5d': 0, 'date': None}

    def get_north_money_flow(self) -> dict:
        """获取北向资金流向"""
        try:
            df = self.data_client.get_north_money_flow()
            north_df = df[df['资金方向'] == '北向']
            if north_df.empty:
                return {'recent_3d_avg': 0, 'today': 0}

            today = north_df['成交净买额'].sum() if '成交净买额' in north_df.columns else 0

            return {
                'today': today,
                'recent_3d_avg': today,
                'recent_5d_avg': today
            }
        except Exception as e:
            logger.warning(f"获取北向资金失败: {e}")
            return {'recent_3d_avg': 0, 'today': 0}

    def get_gold_price(self) -> dict:
        """获取黄金价格"""
        try:
            df = self.data_client.get_gold_price()
            if df.empty:
                return {'current': 550, 'change_5d': 0}

            latest = df.iloc[0]
            # 适配不同的列名
            close_col = 'close' if 'close' in df.columns else '最新价' if '最新价' in df.columns else '收盘价'
            if close_col not in df.columns:
                return {'current': 550, 'change_5d': 0}
                
            current = latest[close_col]
            if len(df) >= 5 and close_col in df.columns:
                change_5d = (current - df.iloc[4][close_col]) / df.iloc[4][close_col]
            else:
                change_5d = 0

            return {
                'current': current,
                'change_5d': change_5d
            }
        except Exception as e:
            logger.warning(f"获取黄金价格失败: {e}")
            return {'current': 550, 'change_5d': 0}

    def get_market_status(self) -> dict:
        """
        综合判断市场状态

        Returns:
            {
                'regime': 'AGGRESSIVE' | 'DEFENSIVE' | 'NEUTRAL',
                'factors': {...},
                'score': 0-10
            }
        """
        currency = self.get_usd_cny_rate()
        north_money = self.get_north_money_flow()
        gold = self.get_gold_price()

        score = 0
        reasons = []

        if currency['change_5d'] > 0.02:
            score += 4
            reasons.append("汇率快速贬值")
        elif currency['change_5d'] > 0.01:
            score += 2

        if north_money['recent_3d_avg'] < -50:
            score += 4
            reasons.append("北向资金大幅流出")
        elif north_money['recent_3d_avg'] < -20:
            score += 2

        if gold['change_5d'] > 0.02:
            score += 2
            reasons.append("避险情绪升温")

        if score >= 6:
            regime = 'DEFENSIVE'
        elif score >= 3:
            regime = 'NEUTRAL'
        else:
            regime = 'AGGRESSIVE'

        return {
            'regime': regime,
            'score': score,
            'reasons': reasons,
            'factors': {
                'currency': currency,
                'north_money': north_money,
                'gold': gold
            },
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def get_position_multiplier(self) -> float:
        """根据市场状态返回仓位乘数"""
        status = self.get_market_status()

        if status['regime'] == 'AGGRESSIVE':
            return 1.0
        elif status['regime'] == 'NEUTRAL':
            return 0.7
        else:
            return 0.4

    def get_sector_preference(self) -> list:
        """根据市场状态返回推荐的板块方向"""
        status = self.get_market_status()

        if status['regime'] == 'DEFENSIVE':
            return ['黄金', '军工', '医药', '公用事业']
        elif status['regime'] == 'AGGRESSIVE':
            return ['科技', '新能源', '消费', '券商']
        else:
            return ['中特估', '高股息', '半导体']


if __name__ == "__main__":
    regime = MarketRegime()

    print("市场状态分析")
    print("=" * 50)

    status = regime.get_market_status()
    print(f"当前状态: {status['regime']}")
    print(f"风险评分: {status['score']}/10")
    print(f"风险因素: {', '.join(status['reasons']) if status['reasons'] else '无'}")
    print(f"\n仓位乘数: {regime.get_position_multiplier():.0%}")
    print(f"推荐板块: {regime.get_sector_preference()}")

"""
市场状态判断模块
宏观因子分析：汇率、黄金、北向资金
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


class MarketRegime:
    """市场状态判断"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            # 默认使用当前目录下的 config.yaml
            config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        self.config = self._load_config(config_path)
        self.cache_dir = self.config.get('cache', {}).get('dir', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

    def _load_config(self, path: str) -> dict:
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def get_usd_cny_rate(self) -> dict:
        """获取美元人民币汇率"""
        try:
            df = ak.fx_spot_quote()
            # 找美元人民币
            usd_row = df[df['货币对'] == 'USD/CNY']
            if usd_row.empty:
                return {'current': 7.2, 'change_5d': 0, 'date': None}

            latest = usd_row.iloc[0]
            return {
                'current': float(latest['卖报价']),
                'change_5d': 0,  # 简化为0
                'date': None
            }
        except Exception as e:
            print(f"获取汇率失败: {e}")
            return {'current': 7.2, 'change_5d': 0, 'date': None}

    def get_north_money_flow(self) -> dict:
        """获取北向资金流向"""
        try:
            df = ak.stock_hsgt_fund_flow_summary_em()
            # 筛选北向资金
            north_df = df[df['资金方向'] == '北向']
            if north_df.empty:
                return {'recent_3d_avg': 0, 'today': 0}

            # 今日净流入
            today = north_df['成交净买额'].sum() if '成交净买额' in north_df.columns else 0

            # 简化为返回今日数据
            return {
                'today': today,
                'recent_3d_avg': today,  # 简化
                'recent_5d_avg': today
            }
        except Exception as e:
            print(f"获取北向资金失败: {e}")
            return {'recent_3d_avg': 0, 'today': 0}

    def get_gold_price(self) -> dict:
        """获取黄金价格"""
        try:
            df = ak.spot_gold_sge()
            if df.empty:
                return {'change_5d': 0}

            latest = df.iloc[0]
            if len(df) >= 5:
                change_5d = (latest['收盘价'] - df.iloc[4]['收盘价']) / df.iloc[4]['收盘价']
            else:
                change_5d = 0

            return {
                'current': latest['收盘价'],
                'change_5d': change_5d
            }
        except:
            return {'current': 550, 'change_5d': 0}

    def get_market_status(self) -> dict:
        """
        综合判断市场状态

        Returns:
            {
                'regime': 'AGGRESSIVE' | 'DEFENSIVE' | 'NEUTRAL',
                'factors': {
                    'currency': {...},
                    'north_money': {...},
                    'gold': {...}
                },
                'score': 0-10  # 风险评分
            }
        """
        currency = self.get_usd_cny_rate()
        north_money = self.get_north_money_flow()
        gold = self.get_gold_price()

        score = 0
        reasons = []

        # 汇率风险 (权重 40%)
        if currency['change_5d'] > 0.02:  # 5天贬值2%
            score += 4
            reasons.append("汇率快速贬值")
        elif currency['change_5d'] > 0.01:
            score += 2

        # 北向资金 (权重 40%)
        if north_money['recent_3d_avg'] < -50:  # 连续流出50亿
            score += 4
            reasons.append("北向资金大幅流出")
        elif north_money['recent_3d_avg'] < -20:
            score += 2

        # 避险情绪 (权重 20%)
        if gold['change_5d'] > 0.02:  # 黄金大涨
            score += 2
            reasons.append("避险情绪升温")

        # 判断状态
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
        """
        根据市场状态返回仓位乘数

        Returns:
            0.0 ~ 1.0 之间的乘数
        """
        status = self.get_market_status()

        if status['regime'] == 'AGGRESSIVE':
            return 1.0
        elif status['regime'] == 'NEUTRAL':
            return 0.7
        else:
            return 0.4

    def get_sector_preference(self) -> list:
        """
        根据市场状态返回推荐的板块方向
        """
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

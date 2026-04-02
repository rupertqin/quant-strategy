"""
事件研究回测模块
验证板块热度与次日涨幅的关系
通过 DataHub 获取数据
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from scipy import stats
import logging

from DataHub.core.data_client import UnifiedDataClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventStudyBacktest:
    """事件研究回测器"""

    def __init__(self, config_path: str = None):
        # 自动查找 config.yaml
        if config_path is None:
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            config_path = os.path.join(parent_dir, "config.yaml")
        
        self.config = self._load_config(config_path)
        self.base_dir = os.path.dirname(config_path) if config_path else os.path.dirname(__file__)

        # 缓存目录
        cache_config = self.config.get('cache', {}).get('dir', 'cache')
        self.cache_dir = cache_config if os.path.isabs(cache_config) else os.path.join(self.base_dir, cache_config)
        os.makedirs(self.cache_dir, exist_ok=True)

        # 图表输出目录
        output_config = self.config.get('output', {})
        self.charts_dir = output_config.get('charts_dir', '../storage/outputs/shortterm/charts')
        if not os.path.isabs(self.charts_dir):
            self.charts_dir = os.path.join(self.base_dir, self.charts_dir)
        os.makedirs(self.charts_dir, exist_ok=True)
        
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

    def download_zt_history(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        下载历史涨停数据
        """
        all_zt = []

        current = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')

        while current <= end:
            date_str = current.strftime('%Y%m%d')

            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue

            try:
                df = self.data_client.get_zt_pool(date_str)
                if not df.empty:
                    df['date'] = date_str
                    all_zt.append(df)
                    print(f"已下载: {date_str}, 涨停 {len(df)} 家")
            except Exception as e:
                print(f"{date_str}: 无数据或错误")

            current += timedelta(days=1)

        if all_zt:
            result = pd.concat(all_zt, ignore_index=True)
            cache_file = os.path.join(self.cache_dir, "zt_history.parquet")
            result.to_parquet(cache_file)
            return result

        return pd.DataFrame()

    def load_zt_history(self) -> pd.DataFrame:
        """加载历史涨停数据"""
        cache_file = os.path.join(self.cache_dir, "zt_history.parquet")

        if os.path.exists(cache_file):
            return pd.read_parquet(cache_file)

        return pd.DataFrame()

    def calculate_sector_heat_history(self, df_zt: pd.DataFrame) -> pd.DataFrame:
        """计算每日每板块的涨停家数"""
        if df_zt.empty:
            return pd.DataFrame()

        heat = df_zt.groupby(['date', '行业']).size().reset_index(name='limit_up_count')
        heat.columns = ['date', 'industry', 'limit_up_count']

        return heat

    def get_industry_index_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取行业指数行情"""
        try:
            df = self.data_client.get_industry_hist(symbol, start_date, end_date)
            return df
        except Exception as e:
            logger.warning(f"获取行业指数失败: {e}")
            return pd.DataFrame()

    def run_event_study(self, df_heat: pd.DataFrame = None) -> dict:
        """
        运行事件研究

        核心假设: T日板块涨停家数多 -> T+1日板块有溢价
        """
        if df_heat is None:
            df_heat = self.load_zt_history()
            if df_heat.empty:
                print("请先下载历史数据: scanner.download_zt_history('20230101', '20231231')")
                return {}

        print("\n" + "=" * 60)
        print("事件研究: 板块热度与次日涨幅的关系")
        print("=" * 60)

        industries = df_heat['industry'].unique()
        print(f"共 {len(industries)} 个板块")

        df_heat = df_heat.sort_values(['industry', 'date'])
        df_heat['next_day_change'] = df_heat.groupby('industry')['limit_up_count'].shift(-1)

        print(f"\n数据点总数: {len(df_heat)}")

        thresholds = [3, 5, 8, 10]
        results = []

        for threshold in thresholds:
            subset = df_heat[df_heat['limit_up_count'] >= threshold]

            if len(subset) > 10:
                avg_next = subset['limit_up_count'].mean()
                win_rate = (subset['limit_up_count'] > threshold).mean()

                results.append({
                    'threshold': threshold,
                    'count': len(subset),
                    'avg_next_zt': avg_next,
                    'continuation_rate': win_rate
                })

                print(f"\n当涨停家数 >= {threshold}:")
                print(f"  样本数: {len(subset)}")
                print(f"  次日平均涨停: {avg_next:.1f}")
                print(f"  延续概率: {win_rate:.1%}")

        return df_heat, results

    def analyze_correlation(self, df_heat: pd.DataFrame) -> float:
        """计算热度与次日涨幅的相关性"""
        df = df_heat.dropna()

        if len(df) < 10:
            return 0

        corr = df['limit_up_count'].corr(df['next_day_change'])

        print(f"\n涨停家数与次日涨幅相关系数: {corr:.4f}")

        return corr

    def plot_analysis(self, df_heat: pd.DataFrame):
        """可视化分析"""
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        ax1 = axes[0, 0]
        df_heat['limit_up_count'].hist(bins=20, ax=ax1)
        ax1.set_xlabel('Limit Up Count')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Distribution of Daily Limit Up Count per Sector')

        ax2 = axes[0, 1]
        pivot = df_heat.pivot_table(
            values='next_day_change',
            index='limit_up_count',
            aggfunc='mean'
        )
        if not pivot.empty:
            sns.heatmap(pivot, ax=ax2, cmap='RdYlGn', center=0)
            ax2.set_title('Heatmap: ZT Count vs Next Day Performance')

        ax3 = axes[1, 0]
        sns.regplot(
            x='limit_up_count',
            y='next_day_change',
            data=df_heat,
            ax=ax3,
            scatter_kws={'alpha': 0.3},
            line_kws={'color': 'red'}
        )
        ax3.set_xlabel('Today ZT Count')
        ax3.set_ylabel('Next Day ZT Count')
        ax3.set_title('Correlation Analysis')

        ax4 = axes[1, 1]
        results = []
        ax4.bar([r['threshold'] for r in results], [r['continuation_rate'] for r in results])
        ax4.set_xlabel('ZT Threshold')
        ax4.set_ylabel('Continuation Rate')
        ax4.set_title('Continuation Rate by ZT Threshold')

        plt.tight_layout()
        chart_path = os.path.join(self.charts_dir, 'event_study_analysis.png')
        plt.savefig(chart_path, dpi=150)
        plt.show()
        print(f"图表已保存至: {chart_path}")

    def validate_strategy(self):
        """
        验证策略有效性
        """
        df_heat = self.load_zt_history()
        if df_heat.empty:
            print("请先下载历史数据")
            return

        df, results = self.run_event_study(df_heat)

        best = max(results, key=lambda x: x['continuation_rate'])

        print("\n" + "=" * 60)
        print("策略验证结论")
        print("=" * 60)
        print(f"最优触发阈值: 涨停 >= {best['threshold']} 家")
        print(f"预期延续概率: {best['continuation_rate']:.1%}")
        print(f"样本数: {best['count']}")

        if best['continuation_rate'] > 0.6:
            print("\n结论: 策略有效，建议采用")
        else:
            print("\n结论: 策略效果有限，需谨慎使用")

        return best


if __name__ == "__main__":
    backtest = EventStudyBacktest()

    df_heat = backtest.load_zt_history()
    if not df_heat.empty:
        df, results = backtest.run_event_study(df_heat)
        backtest.analyze_correlation(df)
    else:
        print("无历史数据，请先下载")

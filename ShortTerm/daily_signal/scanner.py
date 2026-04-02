"""
涨停板扫描器
每日收盘后扫描全市场涨停板，分析板块热度
使用 DataHub 统一数据管理
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
import logging

from DataHub.services.data_service import DataService
from DataHub.core.data_client import UnifiedDataClient
from .market_regime import MarketRegime

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class LimitUpScanner:
    """涨停板扫描器"""

    def __init__(self, config_path: str = None, use_datahub: bool = True):
        # 自动查找 config.yaml
        if config_path is None:
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            config_path = os.path.join(parent_dir, "config.yaml")
        
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.base_dir = os.path.dirname(config_path)
        self.cache_dir = self.config['cache']['dir']
        if not os.path.isabs(self.cache_dir):
            self.cache_dir = os.path.join(self.base_dir, self.cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

        # DataHub 集成
        self.datahub_service = DataService()
        self.data_client = UnifiedDataClient()
        
        # 市场状态判断（宏观+技术）
        self.market_regime = MarketRegime(config_path)
        
        logger.info("LimitUpScanner initialized with DataHub")

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

    def get_today_zt_pool(self, date: str = None) -> pd.DataFrame:
        """
        获取某日涨停板数据

        Args:
            date: 日期，格式 'YYYYMMDD'，默认今天

        Returns:
            涨停板 DataFrame
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        # 优先从 DataHub 获取
        try:
            df = self.datahub_service.get_zt_pool(date)
            if not df.empty:
                logger.info(f"Loaded ZT pool from DataHub for {date}")
                return df
        except Exception as e:
            logger.warning(f"DataHub unavailable: {e}")

        # 回退到本地缓存
        cache_file = os.path.join(self.cache_dir, f"zt_pool_{date}.csv")

        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            if not df.empty:
                return df

        # 从网络获取
        try:
            df = self.data_client.get_zt_pool(date)
            
            if not df.empty:
                df.to_csv(cache_file, index=False, encoding='utf-8-sig')

                # 同时保存到 DataHub
                try:
                    self.datahub_service.storage.save_zt_pool(df, date)
                except Exception as e:
                    logger.warning(f"Failed to save ZT pool to DataHub: {e}")

            return df
        except Exception as e:
            print(f"获取 {date} 涨停数据失败: {e}")
            return pd.DataFrame()

    def get_industry_index_change(self, date: str = None) -> pd.DataFrame:
        """获取行业指数涨跌幅"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        try:
            return self.data_client.get_industry_list()
        except Exception as e:
            logger.warning(f"获取行业指数失败: {e}")
            return pd.DataFrame()

    def calculate_sector_heat(self, df_zt: pd.DataFrame) -> pd.DataFrame:
        """
        计算板块热度（涨停家数）

        Args:
            df_zt: 涨停板数据，需包含 '所属行业' 列

        Returns:
            每个板块的涨停家数
        """
        if df_zt.empty or '所属行业' not in df_zt.columns:
            return pd.DataFrame()

        heat = df_zt.groupby('所属行业').size().reset_index(name='limit_up_count')
        heat['date'] = datetime.now().strftime('%Y%m%d')

        return heat

    def get_industry_list(self) -> pd.DataFrame:
        """获取同花顺行业分类列表"""
        try:
            return self.data_client.get_industry_list()
        except Exception as e:
            logger.warning(f"获取行业列表失败: {e}")
            return pd.DataFrame()

    def analyze_sector_performance(self, sector: str, days: int = 5) -> dict:
        """
        分析某板块近期表现

        Args:
            sector: 板块名称
            days: 回看天数

        Returns:
            绩效字典
        """
        try:
            df = self.data_client.get_industry_cons(sector)
            
            if df.empty:
                return {}

            df = df.tail(days + 1)
            if len(df) < 2:
                return {}

            change = (df['close'].iloc[-1] / df['close'].iloc[0] - 1)
            avg_change = df['pct_chg'].mean() if 'pct_chg' in df.columns else 0

            return {
                'sector': sector,
                'period_return': change,
                'avg_daily_change': avg_change,
                'volatility': df['pct_chg'].std() if 'pct_chg' in df.columns else 0
            }
        except Exception as e:
            logger.warning(f"分析板块 {sector} 表现失败: {e}")
            return {}

    def generate_daily_signals(self, date: str = None) -> dict:
        """
        生成每日信号 - 宏观+技术面综合分析

        Returns:
            包含热点板块和推荐操作的字典
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        print(f"\n{'='*50}")
        print(f"扫描日期: {date}")
        print('='*50)

        # ========== 1. 技术面指标采集 ==========
        print("\n📊 采集技术面指标...")
        
        # 1.1 市场涨跌家数（广度）
        breadth = self.market_regime.get_market_breadth()
        print(f"  涨跌家数: 涨{breadth['up']}/跌{breadth['down']}/平{breadth['flat']}")
        print(f"  涨跌比: {breadth['up_ratio']:.1%}")
        
        # 1.2 主要指数表现
        indices = self.market_regime.get_index_performance()
        print(f"  指数表现:")
        for name, data in indices.items():
            print(f"    {name}: {data['change']:+.2f}% ({data['trend']})")
        
        # 1.3 板块强度（进攻vs防守）
        sectors = self.market_regime.get_sector_strength()
        print(f"  板块风格: 进攻板块{sectors['offensive_avg']:+.2f}% vs 防守板块{sectors['defensive_avg']:+.2f}%")
        print(f"  风格偏向: {sectors['leader']}")
        
        # 1.4 涨停统计（情绪）
        zt_stats = self.market_regime.get_limit_up_stats()
        print(f"  涨停情绪: {zt_stats['zt_count']}家涨停，{zt_stats['hot_sectors']}个热点板块")
        print(f"  市场情绪: {zt_stats['sentiment']}")

        # ========== 2. 获取涨停数据 ==========
        df_zt = self.get_today_zt_pool(date)
        if df_zt.empty:
            print("\n未获取到涨停数据")
            return {
                'date': date, 
                'hot_sectors': [], 
                'technical_indicators': {
                    'breadth': breadth,
                    'indices': indices,
                    'sector_strength': sectors,
                    'zt_stats': zt_stats
                },
                'message': '无涨停数据'
            }

        print(f"\n今日涨停家数: {len(df_zt)}")

        # 2. 计算板块热度
        heat = self.calculate_sector_heat(df_zt)
        min_count = self.config['event_params']['min_zt_count']

        # 3. 筛选热点板块
        hot_sectors = heat[heat['limit_up_count'] >= min_count].sort_values(
            'limit_up_count', ascending=False
        )

        print(f"\n热点板块 (涨停家数 >= {min_count}):")
        if hot_sectors.empty:
            if len(df_zt) >= 15:
                print(f"  无集中热点（普涨行情，涨停分散在多个板块）")
            else:
                print("  无")
        else:
            for _, row in hot_sectors.iterrows():
                print(f"  {row['所属行业']}: {row['limit_up_count']} 家")

        # 4. 分析板块详情
        sector_details = []
        for _, row in hot_sectors.iterrows():
            sector_name = row['所属行业']
            perf = self.analyze_sector_performance(sector_name)

            zt_stocks = df_zt[df_zt['所属行业'] == sector_name]

            detail = {
                'sector': sector_name,
                'zt_count': int(row['limit_up_count']),
                'lead_stock': zt_stocks.iloc[0]['名称'] if len(zt_stocks) > 0 else '',
                'lead_stock_pct': zt_stocks.iloc[0]['涨跌幅'] if len(zt_stocks) > 0 else 0,
                'performance_5d': perf.get('period_return', 0),
                'volatility': perf.get('volatility', 0)
            }
            sector_details.append(detail)

        # 5. 生成交易信号
        signals = []
        for sector in sector_details:
            strength_score = (
                min(sector['zt_count'] / 10, 1.0) * 0.5 +
                (sector['lead_stock_pct'] > 9.5) * 0.3 +
                (sector['performance_5d'] > 0) * 0.2
            )

            action = '关注' if strength_score >= 0.5 else '观望'
            signals.append({
                'sector': sector['sector'],
                'action': action,
                'strength': round(strength_score, 2),
                'reason': f"涨停{sector['zt_count']}家，龙头{sector['lead_stock']}"
            })

        # 6. 保存结果
        # 判断市场类型
        if not sector_details and len(df_zt) >= 15:
            market_type = '普涨分散'
        elif not sector_details:
            market_type = '冷清'
        else:
            market_type = '热点集中'
        
        # 技术面综合评分
        tech_score = self._calculate_technical_score(breadth, indices, sectors, zt_stats)
        
        # 转换 numpy 类型为 Python 原生类型（用于JSON序列化）
        def convert_to_native(obj):
            if hasattr(obj, 'item'):  # numpy types
                return obj.item()
            return obj
        
        result = {
            'date': date,
            'total_zt_count': int(len(df_zt)),
            'market_type': market_type,
            'hot_sectors': sector_details,
            'signals': signals,
            # 新增：技术面指标
            'technical_indicators': {
                'market_breadth': {
                    'up_count': int(breadth['up']),
                    'down_count': int(breadth['down']),
                    'flat_count': int(breadth['flat']),
                    'total_count': int(breadth['total']),
                    'up_ratio': round(float(breadth['up_ratio']), 4),
                    'breadth_score': round(float(breadth['breadth_score']), 4),
                    'interpretation': self._interpret_breadth(breadth)
                },
                'index_performance': {
                    name: {
                        'change_pct': round(float(data['change']), 2),
                        'trend': str(data['trend']),
                        'close': round(float(data['close']), 2)
                    } for name, data in indices.items()
                },
                'sector_strength': {
                    'offensive_avg': round(float(sectors['offensive_avg']), 2),
                    'defensive_avg': round(float(sectors['defensive_avg']), 2),
                    'bias': round(float(sectors['bias']), 2),
                    'leader': str(sectors['leader']),
                    'offensive_count': int(sectors.get('offensive_count', 0)),
                    'defensive_count': int(sectors.get('defensive_count', 0))
                },
                'zt_sentiment': {
                    'zt_count': int(zt_stats['zt_count']),
                    'hot_sectors_count': int(zt_stats['hot_sectors']),
                    'max_sector_zt': int(zt_stats['max_sector_zt']),
                    'sentiment': str(zt_stats['sentiment']),
                    'assessment': str(zt_stats['assessment'])
                },
                'composite_score': int(tech_score['score']),
                'technical_outlook': str(tech_score['outlook']),
                'technical_reasons': [str(r) for r in tech_score['reasons']]
            },
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 确定输出目录 - 按日期分文件夹，支持分钟级报告
        # base_dir 是 ShortTerm/, 所以只需要 parent 到项目根目录
        base_output_dir = Path(self.base_dir).parent / "storage" / "outputs" / "shortterm" / "daily_signal"
        
        # 按日期创建子文件夹
        date_folder = base_output_dir / date
        date_folder.mkdir(parents=True, exist_ok=True)
        
        # 生成时间戳 (HHMMSS格式)
        timestamp = datetime.now().strftime('%H%M%S')
        
        # 保存三份文件：
        # 1. 最新文件（Dashboard读取）- 在根目录
        latest_file = base_output_dir / "daily_signals.json"
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 2. 日期文件夹内的分钟级文件
        timed_file = date_folder / f"daily_signals_{timestamp}.json"
        with open(timed_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 3. 日期文件夹内的最新文件（方便查看当天最新）
        daily_latest_file = date_folder / "daily_signals_latest.json"
        with open(daily_latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"\n信号已保存至:")
        print(f"  最新: {latest_file}")
        print(f"  当天: {daily_latest_file}")
        print(f"  历史: {timed_file}")

        return result

    def _calculate_technical_score(self, breadth: dict, indices: dict, sectors: dict, zt_stats: dict) -> dict:
        """
        计算技术面综合评分
        
        Returns:
            {'score': 0-100, 'outlook': '看多/中性/看空', 'reasons': []}
        """
        score = 50  # 基准分
        reasons = []
        
        # 1. 市场广度评分 (0-25分)
        if breadth['up_ratio'] > 0.6:
            score += 15
            reasons.append(f"涨多跌少({breadth['up_ratio']:.1%})")
        elif breadth['up_ratio'] > 0.5:
            score += 5
        elif breadth['up_ratio'] < 0.4:
            score -= 15
            reasons.append(f"跌多涨少({breadth['up_ratio']:.1%})")
        elif breadth['up_ratio'] < 0.5:
            score -= 5
        
        # 2. 指数趋势评分 (0-30分)
        up_indices = sum(1 for d in indices.values() if d['trend'] == 'UP')
        if up_indices >= 3:
            score += 20
            reasons.append(f"多指数上行({up_indices}/{len(indices)})")
        elif up_indices >= 2:
            score += 10
        elif up_indices == 0:
            score -= 15
            reasons.append("指数全线走弱")
        
        # 3. 板块风格评分 (0-20分)
        if sectors['leader'] == '进攻':
            score += 15
            reasons.append("进攻板块领涨")
        elif sectors['leader'] == '防守':
            score -= 10
            reasons.append("防守板块领涨")
        
        # 4. 涨停情绪评分 (0-25分)
        if zt_stats['sentiment'] == '极热':
            score += 20
            reasons.append("涨停情绪极热")
        elif zt_stats['sentiment'] == '活跃':
            score += 15
            reasons.append("涨停情绪活跃")
        elif zt_stats['sentiment'] == '正常':
            score += 5
        elif zt_stats['sentiment'] == '低迷':
            score -= 10
            reasons.append("涨停情绪低迷")
        elif zt_stats['sentiment'] == '冷清':
            score -= 20
            reasons.append("涨停情绪冷清")
        
        # 确定 outlook
        if score >= 70:
            outlook = '看多'
        elif score >= 50:
            outlook = '中性偏多'
        elif score >= 40:
            outlook = '中性偏空'
        else:
            outlook = '看空'
        
        return {
            'score': max(0, min(100, score)),
            'outlook': outlook,
            'reasons': reasons if reasons else ['技术面无明显信号']
        }
    
    def _interpret_breadth(self, breadth: dict) -> str:
        """解读市场广度"""
        up_ratio = breadth['up_ratio']
        if up_ratio > 0.7:
            return "普涨格局，市场情绪高涨"
        elif up_ratio > 0.6:
            return "涨多跌少，市场情绪积极"
        elif up_ratio > 0.5:
            return "涨跌互现，市场情绪中性"
        elif up_ratio > 0.4:
            return "跌多涨少，市场情绪谨慎"
        else:
            return "普跌格局，市场情绪低迷"
    
    def save_to_history(self, heat: pd.DataFrame):
        """保存板块热度历史数据"""
        # 统一到 storage/outputs/shortterm/daily_signal
        # base_dir 是 ShortTerm/, 所以只需要 parent 到项目根目录
        output_dir = Path(self.base_dir).parent / "storage" / "outputs" / "shortterm" / "daily_signal"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 最新历史文件
        history_file = output_dir / "sector_heat_history.csv"
        
        if os.path.exists(history_file):
            history = pd.read_csv(history_file)
        else:
            history = pd.DataFrame(columns=['date', 'industry', 'limit_up_count'])

        history = pd.concat([history, heat], ignore_index=True)
        history.to_csv(history_file, index=False, encoding='utf-8-sig')
        
        # 同时保存带日期的历史文件
        date_str = heat['date'].iloc[0] if not heat.empty else datetime.now().strftime('%Y-%m-%d')
        dated_history_file = output_dir / f"sector_heat_history_{date_str}.csv"
        heat.to_csv(dated_history_file, index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    scanner = LimitUpScanner()
    result = scanner.generate_daily_signals()

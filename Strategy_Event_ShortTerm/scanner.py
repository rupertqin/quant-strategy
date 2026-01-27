"""
涨停板扫描器
每日收盘后扫描全市场涨停板，分析板块热度
"""

import akshare as ak
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')


class LimitUpScanner:
    """涨停板扫描器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.cache_dir = self.config['cache']['dir']
        os.makedirs(self.cache_dir, exist_ok=True)

    def _load_config(self, path: str) -> dict:
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

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

        cache_file = os.path.join(self.cache_dir, f"zt_pool_{date}.csv")

        # 尝试从缓存读取
        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            return df

        try:
            # AkShare 接口获取涨停池
            df = ak.stock_zt_pool_em(date=date)
            df.to_csv(cache_file, index=False, encoding='utf-8-sig')
            return df
        except Exception as e:
            print(f"获取 {date} 涨停数据失败: {e}")
            return pd.DataFrame()

    def get_industry_index_change(self, date: str = None) -> pd.DataFrame:
        """获取行业指数涨跌幅"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        # 尝试获取同花顺行业指数
        try:
            df = ak.stock_board_industry_name_em()
            # 这个接口返回的是行业列表，需要另外获取行情
            return df
        except:
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

        # 统计每个行业的涨停家数
        heat = df_zt.groupby('所属行业').size().reset_index(name='limit_up_count')
        heat['date'] = datetime.now().strftime('%Y%m%d')

        return heat

    def get_industry_list(self) -> pd.DataFrame:
        """获取同花顺行业分类列表"""
        try:
            return ak.stock_board_industry_name_em()
        except:
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
            # 获取板块指数行情
            df = ak.stock_board_industry_cons_ths(symbol=sector)
            if df.empty:
                return {}

            # 计算近期涨幅
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
            return {}

    def generate_daily_signals(self, date: str = None) -> dict:
        """
        生成每日信号

        Returns:
            包含热点板块和推荐操作的字典
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        print(f"\n{'='*50}")
        print(f"扫描日期: {date}")
        print('='*50)

        # 1. 获取涨停数据
        df_zt = self.get_today_zt_pool(date)
        if df_zt.empty:
            print("未获取到涨停数据")
            return {'date': date, 'hot_sectors': [], 'message': '无涨停数据'}

        print(f"今日涨停家数: {len(df_zt)}")

        # 2. 计算板块热度
        heat = self.calculate_sector_heat(df_zt)
        min_count = self.config['event_params']['min_zt_count']

        # 3. 筛选热点板块
        hot_sectors = heat[heat['limit_up_count'] >= min_count].sort_values(
            'limit_up_count', ascending=False
        )

        print(f"\n热点板块 (涨停家数 >= {min_count}):")
        if hot_sectors.empty:
            print("  无")
        else:
            for _, row in hot_sectors.iterrows():
                print(f"  {row['所属行业']}: {row['limit_up_count']} 家")

        # 4. 分析板块详情
        sector_details = []
        for _, row in hot_sectors.iterrows():
            sector_name = row['所属行业']
            perf = self.analyze_sector_performance(sector_name)

            # 获取板块内涨停股
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
            # 简单信号逻辑：涨停越多、龙头封单越强，可能延续
            strength_score = (
                min(sector['zt_count'] / 10, 1.0) * 0.5 +  # 涨停数量 (权重50%)
                (sector['lead_stock_pct'] > 9.5) * 0.3 +   # 龙头是否硬板 (权重30%)
                (sector['performance_5d'] > 0) * 0.2       # 近期是否上涨 (权重20%)
            )

            action = '关注' if strength_score >= 0.5 else '观望'
            signals.append({
                'sector': sector['sector'],
                'action': action,
                'strength': round(strength_score, 2),
                'reason': f"涨停{sector['zt_count']}家，龙头{sector['lead_stock']}"
            })

        # 6. 保存结果
        result = {
            'date': date,
            'total_zt_count': len(df_zt),
            'hot_sectors': sector_details,
            'signals': signals,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        output_file = self.config['output']['signals_file']
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"\n信号已保存至: {output_file}")

        return result

    def save_to_history(self, heat: pd.DataFrame):
        """保存板块热度历史数据"""
        history_file = self.config['output']['history_file']

        if os.path.exists(history_file):
            history = pd.read_csv(history_file)
        else:
            history = pd.DataFrame(columns=['date', 'industry', 'limit_up_count'])

        history = pd.concat([history, heat], ignore_index=True)
        history.to_csv(history_file, index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    scanner = LimitUpScanner()
    result = scanner.generate_daily_signals()

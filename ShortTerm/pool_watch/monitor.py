"""
股票池监控模块 - 监控 LongTerm 股票池的短线指标
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import json

import pandas as pd
import numpy as np

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from DataHub.core.data_client import UnifiedDataClient
from DataHub.config import STOCK_LIST
from DataHub.stock_names import get_stock_name, get_all_names
from .analyzer import TechnicalAnalyzer, TechnicalIndicators, TrendState


@dataclass
class StockSignal:
    """股票信号"""
    symbol: str
    name: str
    action: str  # BUY, SELL, HOLD, WATCH
    score: float
    reasons: List[str]
    indicators: TechnicalIndicators


@dataclass
class PoolWatchReport:
    """股票池监控报告"""
    date: str
    total_stocks: int
    buy_signals: List[StockSignal]
    sell_signals: List[StockSignal]
    watch_list: List[StockSignal]
    rankings: List[TechnicalIndicators]
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'date': self.date,
            'total_stocks': self.total_stocks,
            'summary': {
                'buy_count': len(self.buy_signals),
                'sell_count': len(self.sell_signals),
                'watch_count': len(self.watch_list),
                'hold_count': self.total_stocks - len(self.buy_signals) - len(self.sell_signals) - len(self.watch_list)
            },
            'buy_signals': [
                {
                    'symbol': s.symbol,
                    'name': s.name,
                    'score': s.score,
                    'reasons': s.reasons,
                    'close': s.indicators.close,
                    'change_pct': round(s.indicators.change_pct, 2)
                } for s in self.buy_signals
            ],
            'sell_signals': [
                {
                    'symbol': s.symbol,
                    'name': s.name,
                    'score': s.score,
                    'reasons': s.reasons,
                    'close': s.indicators.close,
                    'change_pct': round(s.indicators.change_pct, 2)
                } for s in self.sell_signals
            ],
            'watch_list': [
                {
                    'symbol': s.symbol,
                    'name': s.name,
                    'score': s.score,
                    'reasons': s.reasons,
                    'close': s.indicators.close,
                    'change_pct': round(s.indicators.change_pct, 2)
                } for s in self.watch_list
            ],
            'top_rankings': [
                {
                    'symbol': r.symbol,
                    'name': r.name,
                    'score': round(r.composite_score, 1),
                    'close': r.close,
                    'change_pct': round(r.change_pct, 2),
                    'trend': r.trend.value,
                    'signals': r.signals[:3]  # 前3个信号
                } for r in self.rankings[:10]
            ],
            # 新增：所有股票的完整列表
            'all_stocks': [
                {
                    'symbol': r.symbol,
                    'name': r.name,
                    'score': round(r.composite_score, 1),
                    'close': r.close,
                    'change_pct': round(r.change_pct, 2),
                    'trend': r.trend.value,
                    'ma5': round(r.ma5, 2) if r.ma5 else None,
                    'ma10': round(r.ma10, 2) if r.ma10 else None,
                    'ma20': round(r.ma20, 2) if r.ma20 else None,
                    'ma60': round(r.ma60, 2) if r.ma60 else None,
                    'vol_ratio': round(r.vol_ratio, 2) if r.vol_ratio else None,
                    'volume_signal': r.volume_signal.value if r.volume_signal else None,
                    'signals': r.signals
                } for r in self.rankings
            ]
        }


class PoolMonitor:
    """股票池监控器"""
    
    def __init__(self, stock_list: List[str] = None, data_client: UnifiedDataClient = None):
        """
        初始化监控器
        
        Args:
            stock_list: 股票列表，默认使用 LongTerm 配置
            data_client: 数据客户端
        """
        self.stock_list = stock_list or STOCK_LIST
        self.data_client = data_client or UnifiedDataClient()
        self.analyzer = TechnicalAnalyzer()
        
        # 股票名称映射（简化处理）
        self.stock_names = self._load_stock_names()
        
    def _load_stock_names(self) -> Dict[str, str]:
        """加载股票名称映射 - 优先使用CSV数据库"""
        # 方式1: 使用CSV数据库 (推荐)
        try:
            name_map = get_all_names()
            if name_map:
                print(f"✓ 从CSV数据库加载了 {len(name_map)} 个股票名称")
                return name_map
        except Exception as e:
            print(f"CSV数据库加载失败: {e}")
        
        # 方式2: 使用 akshare (备用)
        try:
            import akshare as ak
            name_map = {}
            stock_df = ak.stock_zh_a_spot_em()
            for _, row in stock_df.iterrows():
                code = row['代码']
                name = row['名称']
                if code.startswith('6'):
                    symbol = f"{code}.SH"
                elif code.startswith('0') or code.startswith('3'):
                    symbol = f"{code}.SZ"
                else:
                    symbol = code
                name_map[symbol] = name
            print(f"✓ 从 akshare 加载了 {len(name_map)} 个股票名称")
            return name_map
        except Exception as e:
            print(f"akshare 获取名称失败: {e}")
        
        # 方式3: 空字典 (最后备用)
        print("⚠ 警告: 无法加载任何股票名称，返回空字典")
        return {}
    
    def fetch_stock_data(self, symbol: str, days: int = 70) -> pd.DataFrame:
        """
        获取股票历史数据
        
        Args:
            symbol: 股票代码
            days: 获取天数（至少60天用于计算MA60）
            
        Returns:
            DataFrame with OHLCV data
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = self.data_client.get_price_data(
                symbol=symbol,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                adjust='qfq'
            )
            
            # 标准化列名
            column_mapping = {
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume'
            }
            df = df.rename(columns=column_mapping)
            
            return df
        except Exception as e:
            print(f"获取 {symbol} 数据失败: {e}")
            return pd.DataFrame()
    
    def generate_signal(self, indicators: TechnicalIndicators) -> StockSignal:
        """
        根据技术指标生成交易信号
        
        Args:
            indicators: 技术指标
            
        Returns:
            StockSignal
        """
        score = indicators.composite_score
        reasons = []
        action = "HOLD"
        
        # 买入条件
        if score >= 80:
            action = "BUY"
            reasons.append(f"综合评分优秀 ({score:.0f})")
        elif score >= 65:
            action = "WATCH"
            reasons.append(f"综合评分良好 ({score:.0f})")
        
        # 趋势判断
        if indicators.trend == TrendState.STRONG_UP:
            if action == "HOLD":
                action = "WATCH"
            reasons.append("强势多头排列")
        elif indicators.trend == TrendState.STRONG_DOWN:
            action = "SELL"
            reasons.append("空头排列，趋势走弱")
        
        # 量能判断
        if indicators.vol_ratio > 2.0:
            reasons.append("巨量异动")
        elif indicators.vol_ratio > 1.5:
            reasons.append("明显放量")
        
        # 涨跌幅判断
        if indicators.change_pct > 5:
            reasons.append(f"大涨 {indicators.change_pct:.1f}%")
        elif indicators.change_pct < -5:
            if action not in ["SELL"]:
                action = "WATCH"
            reasons.append(f"大跌 {indicators.change_pct:.1f}%")
        
        # 均线位置
        if indicators.close > indicators.ma60:
            if indicators.close > indicators.ma5:
                reasons.append("站上60日线，短期强势")
        else:
            if action not in ["SELL"]:
                action = "WATCH"
            reasons.append("跌破60日线")
        
        return StockSignal(
            symbol=indicators.symbol,
            name=indicators.name,
            action=action,
            score=score,
            reasons=reasons,
            indicators=indicators
        )
    
    def scan_pool(self) -> PoolWatchReport:
        """
        扫描整个股票池
        
        Returns:
            PoolWatchReport
        """
        print(f"\n{'='*60}")
        print(f"股票池短线监控 - {datetime.now().strftime('%Y-%m-%d')}")
        print('='*60)
        print(f"监控股票数: {len(self.stock_list)}")
        print('-'*60)
        
        all_indicators = []
        buy_signals = []
        sell_signals = []
        watch_list = []
        
        for i, symbol in enumerate(self.stock_list, 1):
            print(f"[{i}/{len(self.stock_list)}] 分析 {symbol}...", end=' ')
            
            # 获取数据
            df = self.fetch_stock_data(symbol)
            if df.empty or len(df) < 20:
                print("数据不足")
                continue
            
            # 分析
            try:
                indicators = self.analyzer.analyze(
                    df, 
                    symbol, 
                    self.stock_names.get(symbol, "")
                )
                all_indicators.append(indicators)
                
                # 生成信号
                signal = self.generate_signal(indicators)
                
                if signal.action == "BUY":
                    buy_signals.append(signal)
                    print(f"[BUY] 评分{indicators.composite_score:.0f}")
                elif signal.action == "SELL":
                    sell_signals.append(signal)
                    print(f"[SELL]")
                elif signal.action == "WATCH":
                    watch_list.append(signal)
                    print(f"[WATCH]")
                else:
                    print(f"[HOLD] 评分{indicators.composite_score:.0f}")
                    
            except Exception as e:
                print(f"分析失败: {e}")
                continue
        
        # 排序
        all_indicators.sort(key=lambda x: x.composite_score, reverse=True)
        buy_signals.sort(key=lambda x: x.score, reverse=True)
        sell_signals.sort(key=lambda x: x.score)
        watch_list.sort(key=lambda x: x.score, reverse=True)
        
        # 生成报告
        report = PoolWatchReport(
            date=datetime.now().strftime('%Y-%m-%d'),
            total_stocks=len(self.stock_list),
            buy_signals=buy_signals,
            sell_signals=sell_signals,
            watch_list=watch_list,
            rankings=all_indicators
        )
        
        # 打印摘要
        self._print_summary(report)
        
        return report
    
    def _print_summary(self, report: PoolWatchReport):
        """打印报告摘要"""
        print('\n' + '='*60)
        print("监控结果摘要")
        print('='*60)
        
        print(f"\n买入信号 ({len(report.buy_signals)}):")
        for s in report.buy_signals[:5]:
            print(f"  {s.symbol} - 评分{s.score:.0f} - {', '.join(s.reasons[:2])}")
        
        print(f"\n卖出信号 ({len(report.sell_signals)}):")
        for s in report.sell_signals[:5]:
            print(f"  {s.symbol} - {', '.join(s.reasons[:2])}")
        
        print(f"\n观察列表 ({len(report.watch_list)}):")
        for s in report.watch_list[:5]:
            print(f"  {s.symbol} - 评分{s.score:.0f} - {', '.join(s.reasons[:2])}")
        
        print(f"\n评分排名 (Top 10):")
        for i, ind in enumerate(report.rankings[:10], 1):
            trend_icon = "📈" if ind.trend in [TrendState.UP, TrendState.STRONG_UP] else "📉" if ind.trend in [TrendState.DOWN, TrendState.STRONG_DOWN] else "➡️"
            print(f"  {i:2d}. {ind.symbol:<12} 评分{ind.composite_score:5.1f} {trend_icon} {ind.change_pct:+.2f}%")
        
        print('='*60)
    
    def save_report(self, report: PoolWatchReport, output_dir: str = None):
        """
        保存报告到文件
        支持分钟级：按日期分文件夹，文件名带时间戳
        
        Args:
            report: 监控报告
            output_dir: 输出目录
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent.parent / "storage" / "outputs" / "shortterm" / "pool_watch"
        
        base_output_dir = Path(output_dir)
        
        # 按日期创建子文件夹
        date_str = report.date  # YYYY-MM-DD 格式
        date_folder = base_output_dir / date_str
        date_folder.mkdir(parents=True, exist_ok=True)
        
        # 生成时间戳 (HHMMSS格式)
        from datetime import datetime
        timestamp = datetime.now().strftime('%H%M%S')
        
        report_dict = report.to_dict()
        
        # 1. 保存JSON报告 - 最新文件（在根目录，供Dashboard读取）
        json_file_latest = base_output_dir / "pool_watch_latest.json"
        with open(json_file_latest, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, ensure_ascii=False, indent=2)
        
        # 2. 保存JSON报告 - 分钟级历史文件（在日期文件夹）
        json_file_timed = date_folder / f"pool_watch_{timestamp}.json"
        with open(json_file_timed, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, ensure_ascii=False, indent=2)
        
        # 3. 保存JSON报告 - 当天最新文件（在日期文件夹）
        json_file_daily = date_folder / "pool_watch_latest.json"
        with open(json_file_daily, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, ensure_ascii=False, indent=2)
        
        # 准备CSV数据
        rankings_data = []
        for ind in report.rankings:
            rankings_data.append({
                'symbol': ind.symbol,
                'name': ind.name,
                'score': ind.composite_score,
                'close': ind.close,
                'change_pct': ind.change_pct,
                'ma5': ind.ma5,
                'ma10': ind.ma10,
                'ma20': ind.ma20,
                'ma60': ind.ma60,
                'vol_ratio': ind.vol_ratio,
                'trend': ind.trend.value,
                'volume_signal': ind.volume_signal.value,
                'signals': '|'.join(ind.signals)
            })
        
        df = pd.DataFrame(rankings_data)
        
        # 4. 保存CSV排名 - 最新文件（在根目录）
        csv_file_latest = base_output_dir / "pool_ranking_latest.csv"
        df.to_csv(csv_file_latest, index=False, encoding='utf-8-sig')
        
        # 5. 保存CSV排名 - 分钟级历史文件（在日期文件夹）
        csv_file_timed = date_folder / f"pool_ranking_{timestamp}.csv"
        df.to_csv(csv_file_timed, index=False, encoding='utf-8-sig')
        
        # 6. 保存CSV排名 - 当天最新文件（在日期文件夹）
        csv_file_daily = date_folder / "pool_ranking_latest.csv"
        df.to_csv(csv_file_daily, index=False, encoding='utf-8-sig')
        
        print(f"\n报告已保存:")
        print(f"  最新:     {json_file_latest}")
        print(f"  当天:     {json_file_daily}")
        print(f"  历史:     {json_file_timed}")


def main():
    """主函数 - 运行股票池监控"""
    monitor = PoolMonitor()
    report = monitor.scan_pool()
    monitor.save_report(report)


if __name__ == "__main__":
    main()

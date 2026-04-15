"""
多日回测聚合分析器

分析多天信号数据的综合表现，验证评分系统的稳定性

用法:
    python ShortTerm/backtest/multi_day_analyzer.py --start-date 20260401 --end-date 20260416
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict
import logging

from .scoring_validator import ScoringValidator, BacktestResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiDayAnalyzer:
    """多日回测分析器"""
    
    def __init__(self):
        self.daily_results: Dict[str, List[BacktestResult]] = {}
        
    def run_multi_day(self, start_date: str, end_date: str):
        """运行多日的回测"""
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        current = start_dt
        while current <= end_dt:
            date_str = current.strftime('%Y%m%d')
            
            validator = ScoringValidator()
            results = validator.run_backtest(date_str)
            
            if results:
                self.daily_results[date_str] = results
                logger.info(f"{date_str}: {len(results)} 只股票")
            else:
                logger.warning(f"{date_str}: 无数据")
                
            current += timedelta(days=1)
            
        logger.info(f"共分析 {len(self.daily_results)} 天的数据")
        
    def aggregate_analysis(self) -> pd.DataFrame:
        """聚合多日的分析结果"""
        all_results = []
        for date_str, results in self.daily_results.items():
            for r in results:
                all_results.append({
                    'date': date_str,
                    'symbol': r.symbol,
                    'score': r.score,
                    'score_bucket': r.score // 10 * 10,
                    'return_5d': r.return_5d,
                    'return_10d': r.return_10d,
                    'win_5d': r.win_5d,
                    'win_10d': r.win_10d,
                })
                
        if not all_results:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_results)
        
        # 按评分分桶聚合
        analysis = df.groupby('score_bucket').agg({
            'symbol': 'count',
            'return_5d': 'mean',
            'return_10d': 'mean',
            'win_5d': 'mean',
            'win_10d': 'mean',
        }).round(2)
        
        analysis.columns = ['样本数', '5日平均收益%', '10日平均收益%', '5日胜率', '10日胜率']
        
        return analysis
    
    def score_consistency_analysis(self) -> pd.DataFrame:
        """分析评分的一致性（同一股票多日的评分稳定性）"""
        symbol_scores = defaultdict(list)
        
        for date_str, results in self.daily_results.items():
            for r in results:
                symbol_scores[r.symbol].append({
                    'date': date_str,
                    'score': r.score,
                    'return_5d': r.return_5d,
                })
                
        # 统计重复出现的股票
        consistency_data = []
        for symbol, scores in symbol_scores.items():
            if len(scores) >= 2:  # 至少出现2天
                scores_only = [s['score'] for s in scores]
                returns = [s['return_5d'] for s in scores if s['return_5d'] is not None]
                
                consistency_data.append({
                    'symbol': symbol,
                    '出现次数': len(scores),
                    '平均评分': np.mean(scores_only),
                    '评分波动': np.std(scores_only),
                    '平均5日收益': np.mean(returns) if returns else None,
                })
                
        return pd.DataFrame(consistency_data)
    
    def generate_summary(self) -> str:
        """生成汇总报告"""
        lines = []
        lines.append("=" * 80)
        lines.append("多日回测汇总报告")
        lines.append("=" * 80)
        lines.append("")
        
        # 1. 基础统计
        total_stocks = sum(len(r) for r in self.daily_results.values())
        lines.append(f"分析天数: {len(self.daily_results)} 天")
        lines.append(f"总样本数: {total_stocks} 只股票-天")
        lines.append("")
        
        # 2. 评分分桶表现
        lines.append("-" * 80)
        lines.append("评分分桶综合表现")
        lines.append("-" * 80)
        agg = self.aggregate_analysis()
        if not agg.empty:
            lines.append(agg.to_string())
        lines.append("")
        
        # 3. 一致性分析
        lines.append("-" * 80)
        lines.append("评分稳定性分析（重复出现的股票）")
        lines.append("-" * 80)
        consistency = self.score_consistency_analysis()
        if not consistency.empty:
            lines.append(f"重复出现的股票: {len(consistency)} 只")
            lines.append(f"平均评分波动: {consistency['评分波动'].mean():.2f}")
        lines.append("")
        
        # 4. 关键指标
        all_results = []
        for results in self.daily_results.values():
            for r in results:
                all_results.append(r)
                
        high_score = [r for r in all_results if r.score >= 85]
        if high_score:
            win_rate_5d = sum(1 for r in high_score if r.win_5d) / len(high_score)
            win_rate_10d = sum(1 for r in high_score if r.win_10d) / len(high_score)
            avg_return_5d = np.mean([r.return_5d for r in high_score if r.return_5d is not None])
            
            lines.append("-" * 80)
            lines.append("高分股票(≥85)综合表现")
            lines.append("-" * 80)
            lines.append(f"样本数: {len(high_score)}")
            lines.append(f"5日胜率: {win_rate_5d:.1%}")
            lines.append(f"10日胜率: {win_rate_10d:.1%}")
            lines.append(f"5日平均收益: {avg_return_5d:.2f}%")
        
        lines.append("=" * 80)
        
        return '\n'.join(lines)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='多日回测分析')
    parser.add_argument('--start-date', type=str, required=True, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', type=str, required=True, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--export', type=str, help='导出CSV')
    
    args = parser.parse_args()
    
    analyzer = MultiDayAnalyzer()
    analyzer.run_multi_day(args.start_date, args.end_date)
    
    print(analyzer.generate_summary())
    
    if args.export:
        agg = analyzer.aggregate_analysis()
        agg.to_csv(args.export)
        logger.info(f"结果已导出: {args.export}")


if __name__ == "__main__":
    main()

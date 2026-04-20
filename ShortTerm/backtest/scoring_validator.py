"""
评分系统回测验证框架

验证评分系统的有效性：高分股票是否真的有更高收益？

用法:
    # 验证最新信号的历史表现
    python ShortTerm/backtest/scoring_validator.py
    
    # 验证指定日期的信号
    python ShortTerm/backtest/scoring_validator.py --date 20260415
    
    # 验证多日的综合表现
    python ShortTerm/backtest/scoring_validator.py --days 30
    
    # 导出详细报告
    python ShortTerm/backtest/scoring_validator.py --export report.html
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import logging

# 导入评分系统
sys.path.insert(0, str(project_root / 'Dashboard'))
from utils.scoring import calculate_stock_score, get_score_color, get_score_label

from DataHub.core.data_reader import load_stock_prices

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果"""
    symbol: str
    signal_date: str
    score: int
    signal_count: int
    periods: List[str]
    signal_types: List[str]

    # 未来收益（多周期）
    return_1d: Optional[float] = None
    return_3d: Optional[float] = None
    return_5d: Optional[float] = None
    return_10d: Optional[float] = None
    return_20d: Optional[float] = None

    # 风险指标
    max_drawdown_5d: Optional[float] = None
    max_drawdown_10d: Optional[float] = None

    # 胜率标签
    win_5d: bool = False  # 5日内是否上涨>3%
    win_10d: bool = False  # 10日内是否上涨>5%

    # 涨停质量（策略4）
    is_zt: bool = False  # 是否涨停
    zt_quality_score: Optional[int] = None  # 涨停质量分
    zt_quality_level: Optional[str] = None  # 涨停等级 A/B/C/D
    zt_risk_flags: List[str] = None  # 风险标记
    zt_valid: Optional[bool] = None  # 涨停是否有效（次日验证）


class ScoringValidator:
    """评分系统验证器"""
    
    def __init__(self, signals_dir: Path = None):
        self.signals_dir = signals_dir or project_root / "storage" / "outputs" / "signals"
        self.results: List[BacktestResult] = []
        
    def load_signals(self, date_str: str = None) -> List[Dict]:
        """加载指定日期的信号数据"""
        if date_str:
            filepath = self.signals_dir / f"stock_signals_{date_str}.json"
        else:
            filepath = self.signals_dir / "stock_signals_latest.json"
            
        if not filepath.exists():
            logger.error(f"信号文件不存在: {filepath}")
            return []
            
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return data.get('signals', [])
    
    def calculate_future_returns(self, symbol: str, signal_date: str) -> Dict[str, float]:
        """计算信号触发后的未来收益"""
        try:
            # 加载历史价格
            df = load_stock_prices(symbol)
            if df is None or df.empty:
                return {}
            
            # 检查必要列
            if 'trade_date' not in df.columns or 'close' not in df.columns:
                logger.warning(f"{symbol} 数据缺少必要列")
                return {}
            
            # 转换日期格式 - 处理各种可能的日期格式
            try:
                df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
                df = df.dropna(subset=['trade_date'])  # 删除无效日期
            except Exception as e:
                logger.warning(f"{symbol} 日期转换失败: {e}")
                return {}
            
            # 信号日期转换
            try:
                signal_dt = pd.to_datetime(signal_date)
            except Exception as e:
                logger.warning(f"{symbol} 信号日期解析失败: {signal_date}")
                return {}
            
            # 找到信号日期的位置（按日期查找）
            mask = df['trade_date'] >= signal_dt
            if not mask.any():
                return {}
            
            signal_row = df[mask].iloc[0]
            signal_price = signal_row['close']
            signal_pos = df[mask].index[0]  # 获取位置索引
            
            results = {}
            
            # 计算不同周期的收益（使用位置索引）
            periods = {
                '1d': 1,
                '3d': 3,
                '5d': 5,
                '10d': 10,
                '20d': 20,
            }
            
            for name, days in periods.items():
                future_pos = signal_pos + days
                if future_pos < len(df):
                    future_price = df.iloc[future_pos]['close']
                    ret = (future_price / signal_price - 1) * 100
                    results[f'return_{name}'] = round(ret, 2)
                    
            # 计算最大回撤（5日和10日）
            for name, days in [('5d', 5), ('10d', 10)]:
                future_pos = signal_pos + days
                if future_pos < len(df):
                    period_prices = df.iloc[signal_pos:future_pos+1]['close']
                    cummax = period_prices.cummax()
                    drawdown = (period_prices - cummax) / cummax * 100
                    results[f'max_drawdown_{name}'] = round(drawdown.min(), 2)
                    
            return results
            
        except Exception as e:
            logger.warning(f"计算 {symbol} 未来收益失败: {e}")
            return {}
    
    def run_backtest(self, date_str: str = None) -> List[BacktestResult]:
        """运行回测"""
        signals = self.load_signals(date_str)
        if not signals:
            logger.error("没有信号数据")
            return []
            
        logger.info(f"加载 {len(signals)} 个信号，开始回测...")
        
        # 按股票分组
        stock_groups = defaultdict(list)
        for sig in signals:
            stock_groups[sig['symbol']].append(sig)
        
        results = []
        
        for symbol, sigs in stock_groups.items():
            # 获取该股票的最新信号日期
            latest_sig = max(sigs, key=lambda x: x.get('trigger_date', ''))
            signal_date = latest_sig['trigger_date']
            
            # 获取涨跌幅信息（用于风险过滤）
            latest_signal = max(sigs, key=lambda x: x.get('trigger_date', ''))
            change_pct = latest_signal.get('change_pct', 0)
            
            # 计算综合评分，传入涨跌幅
            score = calculate_stock_score(sigs, change_pct=change_pct)
            
            # 计算未来收益
            returns = self.calculate_future_returns(symbol, signal_date)
            
            # 提取涨停质量信息（策略4）
            is_zt = change_pct >= 9.9
            zt_quality_score = None
            zt_quality_level = None
            zt_risk_flags = None
            zt_valid = None

            if is_zt:
                # 从信号的technicals中提取涨停质量
                for sig in sigs:
                    tech = sig.get('technicals', {})
                    if tech.get('zt_quality_score') is not None:
                        zt_quality_score = tech.get('zt_quality_score')
                        zt_quality_level = tech.get('zt_quality_level')
                        zt_risk_flags = tech.get('zt_risk_flags', [])
                        break

                # 验证涨停有效性：次日开盘是否高开
                if returns.get('return_1d') is not None:
                    # 如果次日收益>0，认为涨停有效
                    zt_valid = returns.get('return_1d') > 0

            # 创建结果
            result = BacktestResult(
                symbol=symbol,
                signal_date=signal_date,
                score=score,
                signal_count=len(sigs),
                periods=list(set(s.get('period', 'daily') for s in sigs)),
                signal_types=list(set(s.get('signal_type', 'left') for s in sigs)),
                return_1d=returns.get('return_1d'),
                return_3d=returns.get('return_3d'),
                return_5d=returns.get('return_5d'),
                return_10d=returns.get('return_10d'),
                return_20d=returns.get('return_20d'),
                max_drawdown_5d=returns.get('max_drawdown_5d'),
                max_drawdown_10d=returns.get('max_drawdown_10d'),
                win_5d=returns.get('return_5d', 0) > 3,
                win_10d=returns.get('return_10d', 0) > 5,
                is_zt=is_zt,
                zt_quality_score=zt_quality_score,
                zt_quality_level=zt_quality_level,
                zt_risk_flags=zt_risk_flags,
                zt_valid=zt_valid,
            )

            results.append(result)
            
        self.results = results
        logger.info(f"回测完成: {len(results)} 只股票")
        return results
    
    def analyze_by_score_bucket(self) -> pd.DataFrame:
        """按评分分桶分析收益"""
        if not self.results:
            return pd.DataFrame()
            
        # 转换为DataFrame
        df = pd.DataFrame([
            {
                'score': r.score,
                'score_bucket': r.score // 10 * 10,  # 0-9, 10-19, ...
                'return_1d': r.return_1d,
                'return_5d': r.return_5d,
                'return_10d': r.return_10d,
                'return_20d': r.return_20d,
                'win_5d': r.win_5d,
                'win_10d': r.win_10d,
                'max_dd_5d': r.max_drawdown_5d,
                'max_dd_10d': r.max_drawdown_10d,
            }
            for r in self.results
        ])
        
        # 按评分分桶统计
        analysis = df.groupby('score_bucket').agg({
            'score': 'count',  # 数量
            'return_1d': 'mean',
            'return_5d': 'mean',
            'return_10d': 'mean',
            'return_20d': 'mean',
            'win_5d': 'mean',  # 胜率
            'win_10d': 'mean',
            'max_dd_5d': 'mean',
            'max_dd_10d': 'mean',
        }).round(2)
        
        analysis.columns = [
            '数量', '1日收益%', '5日收益%', '10日收益%', '20日收益%',
            '5日胜率', '10日胜率', '5日最大回撤%', '10日最大回撤%'
        ]
        
        return analysis
    
    def analyze_high_score_stocks(self, threshold: int = 85) -> pd.DataFrame:
        """分析高分股票的详细表现"""
        high_score = [r for r in self.results if r.score >= threshold]
        
        if not high_score:
            return pd.DataFrame()
            
        df = pd.DataFrame([
            {
                '股票': r.symbol,
                '评分': r.score,
                '信号数': r.signal_count,
                '周期': ','.join(r.periods),
                '类型': ','.join(r.signal_types),
                '5日收益%': r.return_5d,
                '10日收益%': r.return_10d,
                '20日收益%': r.return_20d,
                '5日胜率': '✓' if r.win_5d else '',
                '10日胜率': '✓' if r.win_10d else '',
            }
            for r in sorted(high_score, key=lambda x: x.score, reverse=True)
        ])
        
        return df
    
    def generate_report(self) -> str:
        """生成回测报告"""
        if not self.results:
            return "无回测结果"
            
        lines = []
        lines.append("=" * 80)
        lines.append("评分系统回测验证报告")
        lines.append("=" * 80)
        lines.append("")
        
        # 1. 整体统计
        scores = [r.score for r in self.results]
        lines.append(f"样本数量: {len(self.results)} 只股票")
        lines.append(f"评分分布: 平均={np.mean(scores):.1f}, 中位数={np.median(scores):.1f}")
        lines.append(f"评分范围: {min(scores)} - {max(scores)}")
        lines.append("")
        
        # 2. 按评分分桶分析
        lines.append("-" * 80)
        lines.append("按评分分桶的收益分析")
        lines.append("-" * 80)
        bucket_analysis = self.analyze_by_score_bucket()
        lines.append(bucket_analysis.to_string())
        lines.append("")
        
        # 3. 高分股票表现
        lines.append("-" * 80)
        lines.append("高分股票表现 (评分≥85)")
        lines.append("-" * 80)
        high_score_df = self.analyze_high_score_stocks(85)
        if not high_score_df.empty:
            lines.append(high_score_df.to_string(index=False))
            
            # 统计
            win_5d_rate = high_score_df['5日胜率'].value_counts().get('✓', 0) / len(high_score_df)
            win_10d_rate = high_score_df['10日胜率'].value_counts().get('✓', 0) / len(high_score_df)
            lines.append("")
            lines.append(f"高分股票 5日胜率: {win_5d_rate:.1%}")
            lines.append(f"高分股票 10日胜率: {win_10d_rate:.1%}")
        else:
            lines.append("无高分股票")
        lines.append("")
        
        # 4. 关键结论
        lines.append("-" * 80)
        lines.append("关键结论")
        lines.append("-" * 80)
        
        # 计算相关性
        df = pd.DataFrame([
            {'score': r.score, 'return_5d': r.return_5d, 'return_10d': r.return_10d}
            for r in self.results
            if r.return_5d is not None and r.return_10d is not None
        ])
        
        if not df.empty:
            corr_5d = df['score'].corr(df['return_5d'])
            corr_10d = df['score'].corr(df['return_10d'])
            
            lines.append(f"评分 vs 5日收益 相关性: {corr_5d:.3f}")
            lines.append(f"评分 vs 10日收益 相关性: {corr_10d:.3f}")
            
            if corr_5d > 0.1:
                lines.append("✓ 评分与短期收益呈正相关，评分系统有效")
            elif corr_5d < -0.1:
                lines.append("✗ 评分与短期收益呈负相关，评分系统需要调整")
            else:
                lines.append("△ 评分与短期收益相关性较弱，建议积累更多数据")
        
        # 4. 涨停质量验证（策略4）
        lines.append("-" * 80)
        lines.append("涨停质量验证")
        lines.append("-" * 80)

        zt_results = [r for r in self.results if r.is_zt]
        if zt_results:
            lines.append(f"涨停股票数量: {len(zt_results)}")
            lines.append("")

            # 按质量等级分组
            for level in ['A', 'B', 'C', 'D']:
                level_results = [r for r in zt_results if r.zt_quality_level == level]
                if level_results:
                    valid_count = sum(1 for r in level_results if r.zt_valid)
                    total_count = len(level_results)
                    valid_rate = valid_count / total_count if total_count > 0 else 0

                    # 计算平均收益
                    avg_return_1d = np.mean([r.return_1d for r in level_results if r.return_1d is not None])
                    avg_return_5d = np.mean([r.return_5d for r in level_results if r.return_5d is not None])

                    lines.append(f"{level}级涨停 ({total_count}只):")
                    lines.append(f"  次日有效 rate: {valid_rate:.1%} ({valid_count}/{total_count})")
                    lines.append(f"  平均1日收益: {avg_return_1d:.2f}%")
                    lines.append(f"  平均5日收益: {avg_return_5d:.2f}%")
                    lines.append("")

            # 涨停陷阱识别
            trap_results = [r for r in zt_results if r.zt_quality_level in ['C', 'D']]
            if trap_results:
                trap_valid = sum(1 for r in trap_results if r.zt_valid)
                lines.append(f"⚠️  涨停陷阱 (C/D级): {len(trap_results)}只, 次日有效{trap_valid/len(trap_results):.1%}")

            # 优质涨停
            good_results = [r for r in zt_results if r.zt_quality_level in ['A', 'B']]
            if good_results:
                good_valid = sum(1 for r in good_results if r.zt_valid)
                lines.append(f"✅ 优质涨停 (A/B级): {len(good_results)}只, 次日有效{good_valid/len(good_results):.1%}")
        else:
            lines.append("无涨停信号")
        lines.append("")

        # 5. 关键结论
        lines.append("-" * 80)
        lines.append("关键结论")
        lines.append("-" * 80)

        # 计算相关性
        df = pd.DataFrame([
            {'score': r.score, 'return_5d': r.return_5d, 'return_10d': r.return_10d}
            for r in self.results
            if r.return_5d is not None and r.return_10d is not None
        ])

        if not df.empty:
            corr_5d = df['score'].corr(df['return_5d'])
            corr_10d = df['score'].corr(df['return_10d'])

            lines.append(f"评分 vs 5日收益 相关性: {corr_5d:.3f}")
            lines.append(f"评分 vs 10日收益 相关性: {corr_10d:.3f}")

            if corr_5d > 0.1:
                lines.append("✓ 评分与短期收益呈正相关，评分系统有效")
            elif corr_5d < -0.1:
                lines.append("✗ 评分与短期收益呈负相关，评分系统需要调整")
            else:
                lines.append("△ 评分与短期收益相关性较弱，建议积累更多数据")

        lines.append("=" * 80)

        return '\n'.join(lines)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='评分系统回测验证')
    parser.add_argument('--date', type=str, help='指定信号日期 (YYYYMMDD)')
    parser.add_argument('--days', type=int, default=1, help='回测多少天的信号')
    parser.add_argument('--export', type=str, help='导出报告到文件')
    parser.add_argument('--threshold', type=int, default=85, help='高分阈值')
    
    args = parser.parse_args()
    
    validator = ScoringValidator()
    
    # 运行回测
    if args.date:
        validator.run_backtest(args.date)
    else:
        # 自动查找最近有数据的日期
        validator.run_backtest()
    
    # 生成报告
    report = validator.generate_report()
    print(report)
    
    # 导出
    if args.export:
        with open(args.export, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"报告已导出: {args.export}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
评分系统回测验证 - 便捷入口

用法:
    # 验证最新信号
    python ShortTerm/run_backtest.py
    
    # 验证指定日期
    python ShortTerm/run_backtest.py --date 20260415
    
    # 多日回测
    python ShortTerm/run_backtest.py --start 20260401 --end 20260416
    
    # 查看高分股票
    python ShortTerm/run_backtest.py --top
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse


def main():
    parser = argparse.ArgumentParser(
        description='评分系统回测验证',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python ShortTerm/run_backtest.py                    # 验证最新信号
  python ShortTerm/run_backtest.py --date 20260415    # 验证指定日期
  python ShortTerm/run_backtest.py --start 20260401 --end 20260416  # 多日
  python ShortTerm/run_backtest.py --top              # 查看高分股票
        """
    )
    
    parser.add_argument('--date', type=str, help='指定信号日期 (YYYYMMDD)')
    parser.add_argument('--start', type=str, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end', type=str, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--top', action='store_true', help='只显示高分股票')
    parser.add_argument('--threshold', type=int, default=85, help='高分阈值')
    
    args = parser.parse_args()
    
    # 多日回测
    if args.start and args.end:
        from backtest.multi_day_analyzer import MultiDayAnalyzer
        
        print(f"运行多日回测: {args.start} 至 {args.end}")
        analyzer = MultiDayAnalyzer()
        analyzer.run_multi_day(args.start, args.end)
        print(analyzer.generate_summary())
        return
    
    # 单日回测
    from backtest.scoring_validator import ScoringValidator
    
    validator = ScoringValidator()
    validator.run_backtest(args.date)
    
    if args.top:
        # 只显示高分股票
        df = validator.analyze_high_score_stocks(args.threshold)
        if not df.empty:
            print(f"\n高分股票 (评分≥{args.threshold}):\n")
            print(df.to_string(index=False))
            print(f"\n共 {len(df)} 只，5日胜率: {df['5日胜率'].value_counts().get('✓', 0)/len(df):.1%}")
        else:
            print(f"无评分≥{args.threshold} 的股票")
    else:
        # 完整报告
        print(validator.generate_report())


if __name__ == "__main__":
    main()

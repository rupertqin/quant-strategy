#!/usr/bin/env python3
"""
ShortTerm 策略运行入口

支持两种模式:
1. daily_signal - 今日异动扫描 (涨停、板块热度)
2. pool_watch - 股票池监控 (LongTerm股票池的短线指标)
"""

import argparse
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_daily_signal():
    """运行今日异动扫描"""
    print("\n" + "="*60)
    print("今日异动扫描 - 涨停板与板块热度")
    print("="*60)
    
    from ShortTerm.daily_signal.scanner import LimitUpScanner
    
    scanner = LimitUpScanner()
    result = scanner.generate_daily_signals()
    
    return result


def run_pool_watch():
    """运行股票池监控"""
    print("\n" + "="*60)
    print("股票池短线监控 - 技术指标分析")
    print("="*60)
    
    from ShortTerm.pool_watch.monitor import PoolMonitor
    
    monitor = PoolMonitor()
    report = monitor.scan_pool()
    monitor.save_report(report)
    
    return report


def run_all():
    """运行所有短线策略"""
    # 1. 今日异动
    run_daily_signal()
    
    # 2. 股票池监控
    run_pool_watch()


def main():
    parser = argparse.ArgumentParser(
        description="ShortTerm 短线策略运行",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_scanner.py daily          # 运行今日异动扫描
  python run_scanner.py pool           # 运行股票池监控
  python run_scanner.py all            # 运行全部
        """
    )
    
    parser.add_argument(
        "mode",
        choices=["daily", "pool", "all"],
        help="运行模式: daily=今日异动, pool=股票池监控, all=全部"
    )
    
    args = parser.parse_args()
    
    if args.mode == "daily":
        run_daily_signal()
    elif args.mode == "pool":
        run_pool_watch()
    elif args.mode == "all":
        run_all()


if __name__ == "__main__":
    main()

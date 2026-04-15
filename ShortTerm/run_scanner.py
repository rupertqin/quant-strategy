#!/usr/bin/env python3
"""
ShortTerm 策略运行入口

支持模式:
1. daily_signal - 今日异动扫描 (涨停、板块热度)
2. pool_watch - 股票池监控 (LongTerm股票池的短线指标)
3. build-db - 构建股票基本信息数据库 (不定期执行)
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
    """运行股票池监控 - 使用信号扫描"""
    print("\n" + "="*60)
    print("股票池监控 - 信号扫描模式")
    print("="*60)
    
    # 直接调用信号扫描（股票池页面现在从信号数据读取）
    import subprocess
    result = subprocess.run(
        [sys.executable, "run_signal_scan.py"],
        cwd=Path(__file__).parent,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
    
    return result.returncode == 0


def run_all():
    """运行所有短线策略"""
    # 1. 今日异动
    run_daily_signal()
    
    # 2. 股票池监控
    run_pool_watch()


def run_build_database():
    """构建股票基本信息数据库（不定期执行）"""
    print("\n" + "="*60)
    print("构建股票基本信息数据库")
    print("="*60)
    print("注意: 此命令不定期执行（如季度/半年更新一次）")
    print("      日常任务不需要执行此命令")
    print("-"*60)
    
    from DataHub.build_stock_db import StockDatabaseBuilder
    
    builder = StockDatabaseBuilder()
    try:
        csv_path = builder.build_database()
        
        # 刷新缓存
        from DataHub import stock_names
        stock_names.refresh_cache()
        
        return csv_path
    except Exception as e:
        print(f"\n✗ 数据库构建失败: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="ShortTerm 短线策略运行",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_scanner.py daily          # 运行今日异动扫描
  python run_scanner.py pool           # 运行股票池监控
  python run_scanner.py all            # 运行全部日常任务
  python run_scanner.py build-db       # 构建股票数据库（不定期执行）
        """
    )
    
    parser.add_argument(
        "mode",
        choices=["daily", "pool", "all", "build-db"],
        help="运行模式: daily=今日异动, pool=股票池监控, all=全部日常任务, build-db=构建股票数据库"
    )
    
    args = parser.parse_args()
    
    if args.mode == "daily":
        run_daily_signal()
    elif args.mode == "pool":
        run_pool_watch()
    elif args.mode == "all":
        run_all()
    elif args.mode == "build-db":
        run_build_database()


if __name__ == "__main__":
    main()

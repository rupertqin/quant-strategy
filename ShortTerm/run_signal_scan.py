#!/usr/bin/env python3
"""
个股信号扫描启动脚本

用法:
    python ShortTerm/run_signal_scan.py                    # 扫描全部信号（左右侧）
    python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ShortTerm.daily_signal.stock_signal_scanner import StockSignalScanner
import argparse


def main():
    parser = argparse.ArgumentParser(description='个股信号扫描 - 支持多周期（日线/周线/月线）')
    parser.add_argument('--symbol', type=str, help='扫描指定股票')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')

    args = parser.parse_args()

    multi_period = not args.no_multi_period
    period_str = "多周期(日/周/月)" if multi_period else "仅日线"

    print("=" * 60)
    print(f"🔍 开始扫描个股信号 - 周期: {period_str}")
    print("=" * 60)

    scanner = StockSignalScanner()

    if args.symbol:
        # 扫描单只股票（同时扫描左右侧）
        from lib.utils import get_stock_name
        name = get_stock_name(args.symbol)
        signals = scanner.scan_stock(args.symbol, name, 'all', multi_period)

        print(f"\n📈 {args.symbol} {name} 扫描结果:")
        print("-" * 60)
        if signals:
            for sig in signals:
                period_tag = {"daily": "日", "weekly": "周", "monthly": "月"}.get(sig.period, "日")
                print(f"\n[{period_tag}线][{sig.signal_type.upper()}] {sig.signal_name}")
                print(f"  强度: {sig.strength} | 评分: {sig.score}")
                print(f"  日期: {sig.trigger_date} | 价格: {sig.close_price}")
                print(f"  {sig.description}")
        else:
            print("暂无信号")
    else:
        # 扫描所有股票（同时扫描左右侧）
        result = scanner.scan_all('all', args.limit, multi_period)

        if result.get('status') == 'success':
            print("\n" + "=" * 60)
            print("✅ 扫描完成!")
            print("=" * 60)
            print(f"\n📊 统计信息:")
            print(f"  扫描股票数: {result['total_stocks']}")
            print(f"  发现信号数: {result['total_signals']}")
            print(f"  左侧信号: {result['stats']['left']}")
            print(f"  右侧信号: {result['stats']['right']}")

            if result.get('multi_period'):
                by_period = result['stats'].get('by_period', {})
                print(f"\n📅 周期分布:")
                print(f"    日线: {by_period.get('daily', 0)}")
                print(f"    周线: {by_period.get('weekly', 0)}")
                print(f"    月线: {by_period.get('monthly', 0)}")

            if result['stats']['by_signal']:
                print(f"\n📈 按信号类型分布 (Top 15):")
                for sig_name, count in sorted(
                    result['stats']['by_signal'].items(),
                    key=lambda x: -x[1]
                )[:15]:
                    print(f"    {sig_name}: {count}")

            print(f"\n💾 数据已保存: stock_signals_latest.json")
            print(f"🌐 请在 Dashboard 中查看: streamlit run Dashboard/app.py")
            print(f"\n📋 提示: 所有信号保存在一个文件中，通过 signal_type 字段区分左侧/右侧")
        else:
            print(f"\n❌ 扫描失败: {result.get('message', '未知错误')}")


if __name__ == "__main__":
    main()

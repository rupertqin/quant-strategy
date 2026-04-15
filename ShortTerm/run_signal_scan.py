#!/usr/bin/env python3
"""
个股信号扫描启动脚本 - 专注信号加工

数据获取由 DataHub 负责，本脚本只处理信号扫描逻辑

数据逻辑：始终以最新价格数据为准
  - 有当天实时数据 → 合并到历史K线
  - 无当天数据 → 使用纯历史数据

用法:
    python ShortTerm/run_signal_scan.py                    # 扫描全部信号
    python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票
    python ShortTerm/run_signal_scan.py --watch 60         # 持续监控模式
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ShortTerm.daily_signal.stock_signal_scanner import StockSignalScanner, filter_excluded_symbols
from DataHub.services.realtime_service import RealtimeDataService, get_realtime_service
import argparse
import time
from datetime import datetime
import pandas as pd
import json


def scan_intraday_signals(scanner, symbol: str, realtime_df: pd.DataFrame, 
                          multi_period: bool = True, signal_type: str = "all") -> list:
    """
    扫描单只股票的盘中信号（完整信号检测）
    
    将实时数据合并到历史K线，然后运行完整的左侧+右侧信号检测
    
    Args:
        scanner: StockSignalScanner 实例
        symbol: 股票代码
        realtime_df: 实时行情DataFrame
        multi_period: 是否多周期分析
        signal_type: 信号类型 - all/left/right
        
    Returns:
        StockSignal 对象列表
    """
    from lib.utils import get_stock_name
    from ShortTerm.daily_signal.stock_signal_scanner import LeftSignalDetector, RightSignalDetector
    
    # 1. 获取该股票的实时数据
    row = realtime_df[realtime_df['symbol'] == symbol]
    if row.empty:
        return []
    
    realtime = row.iloc[0]
    name = get_stock_name(symbol) or realtime.get('name', '')
    
    # 2. 加载历史日线数据并合并实时数据
    hist_df = scanner.load_stock_data(symbol, period='daily')
    if hist_df is None or hist_df.empty:
        return []
    
    # 使用 RealtimeDataService 的合并方法
    rt_service = get_realtime_service()
    merged_df = rt_service.merge_realtime_to_history(hist_df, realtime)
    
    # 3. 使用完整的检测器进行信号检测（日线）
    signals = []
    left_detector = LeftSignalDetector()
    right_detector = RightSignalDetector()
    
    try:
        # 日线左侧信号
        if signal_type in ["all", "left"]:
            left_signals = left_detector.detect_all(merged_df, symbol, name, "daily")
            signals.extend(left_signals)
        
        # 日线右侧信号
        if signal_type in ["all", "right"]:
            right_signals = right_detector.detect_all(merged_df, symbol, name, "daily")
            signals.extend(right_signals)
            
    except Exception as e:
        print(f"  {symbol} 信号检测失败: {e}")
    
    return signals


def main():
    parser = argparse.ArgumentParser(
        description='个股信号扫描 - 支持多周期（日线/周线/月线），自动使用最新数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python ShortTerm/run_signal_scan.py                    # 扫描全部信号
  python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票
  python ShortTerm/run_signal_scan.py --watch 60         # 持续监控模式
        """
    )
    parser.add_argument('--symbol', type=str, help='扫描指定股票')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')
    parser.add_argument('--watch', type=int, default=0,
                        help='持续监控模式，每隔N秒刷新一次（如 --watch 60）')

    args = parser.parse_args()

    multi_period = not args.no_multi_period
    period_str = "多周期(日/周/月)" if multi_period else "仅日线"
    
    # 初始化实时数据服务（由 DataHub 提供）
    rt_service = get_realtime_service()
    
    # 简单逻辑：有当天实时数据就用，没有就用历史
    has_today_data = rt_service.find_todays_latest_file() is not None
    
    if has_today_data:
        mode_str = "当天数据模式（合并实时数据）"
    else:
        mode_str = "历史数据模式"

    # ========== 使用当天实时数据扫描 ==========
    if has_today_data:
        print("=" * 60)
        print(f"📊 盘中信号监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        print(f"\n💡 模式: {mode_str} - 实时行情由 DataHub 提供")

        scanner = StockSignalScanner()

        # 持续监控模式
        iteration = 0
        while True:
            iteration += 1

            # 非首次循环显示刷新信息
            if iteration > 1 or args.watch > 0:
                print(f"\n{'='*60}")
                print(f"🔄 刷新时间: {datetime.now().strftime('%H:%M:%S')}")
                print('='*60)

            try:
                # 从统一数据访问层获取最新实时数据
                print("\n📡 从 DataHub 获取实时数据...")
                
                from Dashboard.utils.data_access import get_latest_realtime_data
                realtime_df, price_time_str = get_latest_realtime_data(force_fetch=True, full_format=True)
                
                if realtime_df is None or realtime_df.empty:
                    raise FileNotFoundError("未能获取到实时数据")
                
                print(f"   ✓ 已获取最新实时数据 ({price_time_str}): {len(realtime_df)} 只股票")

                if args.symbol:
                    # 单只股票监控
                    symbol = args.symbol
                    if not symbol.endswith(('.SH', '.SZ', '.BJ')):
                        from lib.utils import StockCodeUtil
                        symbol = StockCodeUtil.with_suffix(symbol) or symbol
                    
                    # 扫描该股票的盘中信号
                    signals = scan_intraday_signals(scanner, symbol, realtime_df, multi_period)
                    
                    if signals:
                        print(f"\n🚨 {symbol} 盘中信号:")
                        for sig in signals:
                            period_tag = {"daily": "日", "weekly": "周", "monthly": "月"}.get(sig.period, "日")
                            print(f"\n   [{period_tag}线][{sig.signal_type.upper()}] {sig.signal_name}")
                            print(f"      强度: {sig.strength} | 评分: {sig.score}")
                            print(f"      价格: {sig.close_price} | {sig.description}")
                    else:
                        print(f"\n✓ {symbol} 暂无盘中信号")
                        
                else:
                    # 全市场扫描（默认全部，可通过 --limit 限制）
                    # 使用统一的过滤函数排除北交所等交易所
                    all_symbols = filter_excluded_symbols(realtime_df)
                    
                    if args.limit:
                        symbols = all_symbols[:args.limit]
                        print(f"   扫描前 {args.limit} 只股票...")
                    else:
                        symbols = all_symbols
                        print(f"   扫描全市场 {len(symbols)} 只股票（已排除北交所）...")
                    
                    all_signals = []
                    for symbol in symbols:
                        signals = scan_intraday_signals(scanner, symbol, realtime_df, multi_period)
                        if signals:
                            all_signals.extend(signals)
                    
                    # 构建结果并保存（接入主流程）
                    from collections import Counter
                    
                    by_signal = Counter([s.signal_name for s in all_signals])
                    left_count = sum(1 for s in all_signals if s.signal_type == 'left')
                    right_count = sum(1 for s in all_signals if s.signal_type == 'right')
                    by_period = Counter([s.period for s in all_signals])
                    
                    # price_fetch_time 已从 get_latest_realtime_data 获取
                    
                    result = {
                        'status': 'success',
                        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'price_fetch_time': price_time_str,  # 扫描时使用的实时数据时间
                        'total_stocks': len(symbols),
                        'total_signals': len(all_signals),
                        'multi_period': multi_period,
                        'intraday_mode': True,
                        'signals': [s.to_dict() for s in all_signals],
                        'stats': {
                            'left': left_count,
                            'right': right_count,
                            'by_signal': dict(by_signal),
                            'by_period': dict(by_period)
                        }
                    }
                    
                    # 保存结果到文件
                    output_dir = Path(project_root) / "storage" / "outputs" / "signals"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    date_str = datetime.now().strftime('%Y%m%d')
                    filename = f"stock_signals_intraday_{date_str}.json"
                    filepath = output_dir / filename
                    
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    
                    # 同时保存为 latest
                    latest_path = output_dir / "stock_signals_latest.json"
                    with open(latest_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    
                    # 按信号类型统计显示
                    if all_signals:
                        print(f"\n🚨 发现 {len(all_signals)} 个盘中信号:")
                        
                        # 按股票分组显示
                        by_symbol = {}
                        for sig in all_signals:
                            sym = sig.symbol
                            if sym not in by_symbol:
                                by_symbol[sym] = []
                            by_symbol[sym].append(sig)
                        
                        for symbol, sigs in list(by_symbol.items())[:10]:
                            name = sigs[0].name
                            print(f"\n   {symbol} {name}:")
                            for sig in sigs:
                                period_tag = {"daily": "日", "weekly": "周", "monthly": "月"}.get(sig.period, "日")
                                print(f"     [{period_tag}线][{sig.signal_type.upper()}] {sig.signal_name}")
                        
                        if len(by_symbol) > 10:
                            print(f"\n   ... 还有 {len(by_symbol)-10} 只股票有信号")
                        
                        print(f"\n📊 信号分布: 左侧 {left_count} 个, 右侧 {right_count} 个")
                        
                        if result['stats']['by_signal']:
                            print(f"\n📈 按信号类型分布 (Top 10):")
                            for sig_name, count in sorted(
                                result['stats']['by_signal'].items(),
                                key=lambda x: -x[1]
                            )[:10]:
                                print(f"    {sig_name}: {count}")
                        
                        print(f"\n💾 数据已保存: stock_signals_latest.json")
                        print(f"🌐 请在 Dashboard 中查看: streamlit run Dashboard/app.py")
                    else:
                        scanned_count = args.limit if args.limit else len(symbols)
                        print(f"\n✓ 扫描 {scanned_count} 只股票，暂无盘中信号")

            except FileNotFoundError as e:
                print(f"\n❌ {e}")
                print("\n📋 获取数据失败，请检查网络连接或 DataHub 配置")
            except Exception as e:
                print(f"\n❌ 扫描失败: {e}")
                import traceback
                traceback.print_exc()

            # 是否持续监控
            if args.watch > 0:
                print(f"\n  ⏱️  {args.watch}秒后刷新...")
                time.sleep(args.watch)
            else:
                break

        return

    print("=" * 60)
    print(f"🔍 开始扫描个股信号 - 周期: {period_str} - 模式: {mode_str}")
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

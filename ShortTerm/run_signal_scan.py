#!/usr/bin/env python3
"""
个股信号扫描启动脚本

用法:
    # 常规扫描模式
    python ShortTerm/run_signal_scan.py                    # 扫描全部信号（左右侧）
    python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票

    # 盘中监控模式（默认获取实时数据，合并历史K线分析信号）
    python ShortTerm/run_signal_scan.py --intraday         # 获取实时数据+分析信号（一步完成）
    python ShortTerm/run_signal_scan.py --intraday --skip-fetching  # 跳过获取，使用当天已存数据
    python ShortTerm/run_signal_scan.py --intraday --watch 60       # 持续监控模式
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ShortTerm.daily_signal.stock_signal_scanner import StockSignalScanner
import argparse
import time
from datetime import datetime
import pandas as pd
import json
import os

# 实时数据默认保存目录
REALTIME_OUTPUT_DIR = Path(project_root) / "storage" / "outputs" / "realtime"


def merge_realtime_to_history(hist_df: pd.DataFrame, realtime: pd.Series) -> pd.DataFrame:
    """
    将实时数据合并到历史K线（内存中）
    
    Args:
        hist_df: 历史日线数据
        realtime: 实时行情Series
        
    Returns:
        合并后的DataFrame
    """
    # 确保 trade_date 列是 datetime 类型
    hist_df['trade_date'] = pd.to_datetime(hist_df['trade_date'])
    
    # 检查是否已有今天数据
    today = datetime.now()
    today_date = today.date()
    
    # 获取最后一天日期
    last_date = hist_df['trade_date'].iloc[-1]
    if isinstance(last_date, pd.Timestamp):
        last_date = last_date.date()
    
    # 如果历史数据已有今天数据，更新它；否则追加新行
    if not hist_df.empty and last_date == today_date:
        # 更新今天的数据
        idx = hist_df.index[-1]
        hist_df.loc[idx, 'close'] = float(realtime['close'])
        hist_df.loc[idx, 'high'] = max(float(hist_df.loc[idx, 'high']), float(realtime.get('high', realtime['close'])))
        hist_df.loc[idx, 'low'] = min(float(hist_df.loc[idx, 'low']), float(realtime.get('low', realtime['close'])))
        hist_df.loc[idx, 'volume'] = float(realtime['volume'])
        hist_df.loc[idx, 'amount'] = float(realtime.get('amount', 0))
        hist_df.loc[idx, 'change_pct'] = float(realtime['change_pct'])
    else:
        # 添加新行（今天的盘中数据）
        new_row = pd.DataFrame([{
            'trade_date': today,
            'open': float(realtime.get('open', realtime['close'])),
            'high': float(realtime.get('high', realtime['close'])),
            'low': float(realtime.get('low', realtime['close'])),
            'close': float(realtime['close']),
            'volume': float(realtime['volume']),
            'amount': float(realtime.get('amount', 0)),
            'change_pct': float(realtime['change_pct']),
            'symbol': realtime.get('symbol', '')
        }])
        hist_df = pd.concat([hist_df, new_row], ignore_index=True)
    
    return hist_df


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
    
    merged_df = merge_realtime_to_history(hist_df, realtime)
    
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


def fetch_intraday_data(symbols: list) -> pd.DataFrame:
    """
    获取盘中实时行情数据（使用akshare sina源）

    Args:
        symbols: 股票代码列表，如 ['600519.SH', '300750.SZ']

    Returns:
        DataFrame with real-time data
    """
    import akshare as ak

    # 获取全市场实时行情
    df = ak.stock_zh_a_spot()

    # 转换代码格式: sh600000 -> 600000.SH
    df['symbol'] = df['代码'].apply(lambda x:
        x[2:] + '.SH' if x.startswith('sh') else
        x[2:] + '.SZ' if x.startswith('sz') else
        x[2:] + '.BJ' if x.startswith('bj') else x
    )

    # 筛选指定股票
    if symbols:
        df = df[df['symbol'].isin(symbols)]

    # 重命名列（使用实际的中文列名）
    column_map = {
        '最新价': 'close',
        '今开': 'open',
        '最高价': 'high',
        '最低价': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '涨跌幅': 'change_pct',
        '名称': 'name'
    }

    # 只保留存在的列
    available_cols = {k: v for k, v in column_map.items() if k in df.columns}
    df = df.rename(columns=available_cols)
    df['trade_date'] = datetime.now().date()

    # 确保必要列存在
    required_cols = ['symbol', 'name', 'trade_date', 'close']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    return df


def save_realtime_data(df: pd.DataFrame, output_dir: Path = None) -> str:
    """
    保存实时数据到JSON文件

    Args:
        df: 实时数据DataFrame
        output_dir: 输出目录，默认 storage/outputs

    Returns:
        保存的文件路径
    """
    if output_dir is None:
        output_dir = REALTIME_OUTPUT_DIR

    output_dir.mkdir(parents=True, exist_ok=True)

    # 生成文件名: realtime_YYYYMMDD_HHMMSS.json
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = output_dir / f"realtime_{timestamp}.json"

    # 转换为字典列表并保存
    records = df.to_dict('records')

    # 处理日期类型（JSON不可序列化）
    for record in records:
        if 'trade_date' in record and hasattr(record['trade_date'], 'isoformat'):
            record['trade_date'] = record['trade_date'].isoformat()

    data = {
        'fetch_time': timestamp,
        'record_count': len(records),
        'data': records
    }

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return str(filepath)


def load_realtime_data(filepath: str = None) -> pd.DataFrame:
    """
    从JSON文件加载实时数据

    Args:
        filepath: 文件路径，None则自动查找最新的实时数据文件

    Returns:
        DataFrame with real-time data
    """
    if filepath is None:
        # 自动查找最新的实时数据文件
        filepath = find_latest_realtime_file()
        if filepath is None:
            raise FileNotFoundError("未找到实时数据文件")

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data['data'])

    # 转换日期字符串回date对象
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

    return df


def find_latest_realtime_file(output_dir: Path = None) -> str:
    """
    查找最新的实时数据文件

    Returns:
        最新文件路径，如果没有则返回None
    """
    if output_dir is None:
        output_dir = REALTIME_OUTPUT_DIR

    if not output_dir.exists():
        return None

    files = list(output_dir.glob("realtime_*.json"))
    if not files:
        return None

    # 按修改时间排序，取最新
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return str(latest)


def fetch_and_save_realtime(symbols: list = None) -> str:
    """
    获取并保存实时数据（第一步）

    Args:
        symbols: 指定股票列表，None表示全市场

    Returns:
        保存的文件路径
    """
    print("📡 获取实时行情数据...")
    df = fetch_intraday_data(symbols or [])
    print(f"   获取到 {len(df)} 只股票数据")

    print("💾 保存实时数据...")
    filepath = save_realtime_data(df)
    print(f"   已保存: {filepath}")

    return filepath


def find_todays_realtime_file(output_dir: Path = None) -> str:
    """
    查找当天最新的实时数据文件

    Returns:
        当天最新文件路径，如果没有则返回None
    """
    if output_dir is None:
        output_dir = REALTIME_OUTPUT_DIR

    if not output_dir.exists():
        return None

    today_str = datetime.now().strftime('%Y%m%d')
    files = list(output_dir.glob(f"realtime_{today_str}_*.json"))

    if not files:
        return None

    # 按修改时间排序，取最新
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return str(latest)


def main():
    parser = argparse.ArgumentParser(
        description='个股信号扫描 - 支持多周期（日线/周线/月线）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 常规扫描
  python ShortTerm/run_signal_scan.py                    # 扫描全部信号
  python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票

  # 盘中监控（一步完成：获取实时数据+分析信号）
  python ShortTerm/run_signal_scan.py --intraday                    # 默认获取实时数据并分析
  python ShortTerm/run_signal_scan.py --intraday --skip-fetching    # 跳过获取，使用当天已存数据
  python ShortTerm/run_signal_scan.py --intraday --watch 60         # 持续监控模式（每分钟刷新）
        """
    )
    parser.add_argument('--symbol', type=str, help='扫描指定股票')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')
    parser.add_argument('--intraday', action='store_true',
                        help='盘中监控模式：获取实时数据，合并到历史K线，检测盘中信号')
    parser.add_argument('--skip-fetching', action='store_true',
                        help='跳过获取实时数据，直接使用当天已存储的数据（配合--intraday使用）')
    parser.add_argument('--watch', type=int, default=0,
                        help='持续监控模式，每隔N秒刷新一次（如 --watch 60）')

    args = parser.parse_args()

    multi_period = not args.no_multi_period
    period_str = "多周期(日/周/月)" if multi_period else "仅日线"

    # ========== 盘中信号分析 ==========
    if args.intraday:
        print("=" * 60)
        print(f"📊 盘中信号监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        print("\n💡 说明: 获取实时行情，合并到历史K线，检测盘中出现的信号")

        scanner = StockSignalScanner()

        # 首次获取实时数据（如果需要）
        todays_file = None
        if not args.skip_fetching:
            try:
                print("\n📡 获取实时行情数据...")
                todays_file = fetch_and_save_realtime()
            except Exception as e:
                print(f"\n⚠️ 获取实时数据失败: {e}")
                print("   尝试使用当天已存储的数据...")
        else:
            print("\n⏭️ 跳过获取数据，使用当天已存储的数据...")

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
                # 加载实时数据
                if todays_file and iteration == 1 and not args.skip_fetching:
                    # 首次且刚获取过数据，直接使用
                    print(f"  加载刚才获取的实时数据...")
                    realtime_df = load_realtime_data(todays_file)
                elif args.skip_fetching:
                    # 跳过获取模式：查找当天最新的数据文件
                    todays_file = find_todays_realtime_file()
                    if todays_file:
                        print(f"  加载当天数据: {Path(todays_file).name}")
                        realtime_df = load_realtime_data(todays_file)
                    else:
                        raise FileNotFoundError("未找到当天存储的实时数据文件")
                else:
                    # 重新获取实时数据（持续监控模式）
                    print("  重新获取实时数据...")
                    todays_file = fetch_and_save_realtime()
                    realtime_df = load_realtime_data(todays_file)

                print(f"   已加载 {len(realtime_df)} 只股票的实时数据")

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
                    if args.limit:
                        symbols = realtime_df['symbol'].head(args.limit).tolist()
                        print(f"   扫描前 {args.limit} 只股票...")
                    else:
                        symbols = realtime_df['symbol'].tolist()
                        print(f"   扫描全市场 {len(symbols)} 只股票...")
                    
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
                    
                    result = {
                        'status': 'success',
                        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                if args.skip_fetching:
                    print("\n📋 未找到当天数据，请去掉 --skip-fetching 参数重新运行")
                else:
                    print("\n📋 获取数据失败，请检查网络连接")
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

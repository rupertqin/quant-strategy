#!/usr/bin/env python3
"""
个股/ETF信号扫描启动脚本 - 专注信号加工

数据获取由 DataHub 负责，本脚本只处理信号扫描逻辑

数据逻辑：始终以最新价格数据为准
  - 有当天实时数据 → 合并到历史K线
  - 无当天数据 → 使用纯历史数据

用法:
    # 扫描全部（股票+ETF）
    python ShortTerm/run_signal_scan.py                    # 扫描全部信号
    python ShortTerm/run_signal_scan.py --watch 60         # 持续监控模式
    
    # 扫描指定标的（自动识别股票/ETF）
    python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票
    python ShortTerm/run_signal_scan.py --symbol 510300.SH # 扫描单只ETF
"""

import sys
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ShortTerm.services.stock_signal_scanner import StockSignalScanner, filter_excluded_symbols
from DataHub.core.data_reader import is_etf, load_stock_prices
from lib.utils import detect_asset_type
import argparse
import time
from datetime import datetime
import pandas as pd
import json
import numpy as np


def convert_to_serializable(obj):
    """将 numpy 类型转换为 JSON 可序列化的 Python 原生类型"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_to_serializable(item) for item in obj)
    return obj


def parse_scan_date(date_str: str) -> Optional[str]:
    """
    解析扫描日期，支持多种格式

    支持的格式:
    - '2026-04-25' (ISO)
    - '20260425' (纯数字)
    - '2026/04/25'
    - '2026.04.25'

    Returns:
        'YYYY-MM-DD' 格式字符串，或 None（解析失败时）
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # 已经是 ISO 格式
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str

    # 纯数字格式: 20260425
    if date_str.isdigit() and len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    # 尝试用 datetime 解析其他格式
    for fmt in ('%Y/%m/%d', '%Y.%m.%d', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def get_latest_market_date() -> Optional[str]:
    """获取本地价格数据的最新市场交易日（用于未指定--date时）"""
    for symbol in ['000001.SH', '000001.SZ', '600519.SH', '510300.SH']:
        try:
            df = load_stock_prices(symbol)
            if not df.empty and 'trade_date' in df.columns:
                latest = pd.to_datetime(df['trade_date']).max()
                return latest.strftime('%Y-%m-%d')
        except Exception:
            continue
    return None


@dataclass
class AssetConfig:
    """资产类型配置"""
    name: str                    # 显示名称（股票/ETF）
    name_en: str                 # 英文名（stock/etf）
    filename_prefix: str         # 文件名前缀
    has_realtime_data: bool = True  # 是否支持实时数据


# 资产类型配置映射
ASSET_CONFIGS = {
    'stock': AssetConfig(name='股票', name_en='stock', filename_prefix='stock'),
    'etf': AssetConfig(name='ETF', name_en='etf', filename_prefix='etf'),
    'index': AssetConfig(name='指数', name_en='index', filename_prefix='index'),
}


class RealtimeDataLoader(ABC):
    """实时数据加载器基类"""
    
    @abstractmethod
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """加载实时数据，返回 (DataFrame, 时间字符串)"""
        pass
    
    @abstractmethod
    def check_exists(self) -> bool:
        """检查当天实时数据是否存在"""
        pass


class StockRealtimeLoader(RealtimeDataLoader):
    """股票实时数据加载器"""
    
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        from Dashboard.utils.data_access import get_latest_realtime_data
        return get_latest_realtime_data(force_fetch=False, full_format=True, asset_type='stock')
    
    def check_exists(self) -> bool:
        from Dashboard.utils.data_access import get_todays_realtime_file
        return get_todays_realtime_file(asset_type='stock') is not None


class EtfRealtimeLoader(RealtimeDataLoader):
    """ETF实时数据加载器（从 intraday parquet 读取）"""

    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        from DataHub.services.realtime_service import RealtimeDataService
        rt_service = RealtimeDataService()
        df = rt_service.load_intraday_parquet(asset_type='etf', latest_snapshot=True)
        if df is None or df.empty:
            return None, ""

        time_str = ""
        if 'timestamp' in df.columns and not df.empty:
            time_str = df['timestamp'].max().strftime('%Y-%m-%d %H:%M')
        return df, time_str

    def check_exists(self) -> bool:
        from DataHub.services.realtime_service import RealtimeDataService
        df = RealtimeDataService().load_intraday_parquet(asset_type='etf', latest_snapshot=True)
        return df is not None and not df.empty


class IndexRealtimeLoader(RealtimeDataLoader):
    """指数实时数据加载器（从 intraday parquet 读取）"""

    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        from DataHub.services.realtime_service import RealtimeDataService
        rt_service = RealtimeDataService()
        df = rt_service.load_intraday_parquet(asset_type='index', latest_snapshot=True)
        if df is None or df.empty:
            return None, ""

        time_str = ""
        if 'timestamp' in df.columns and not df.empty:
            time_str = df['timestamp'].max().strftime('%Y-%m-%d %H:%M')
        return df, time_str

    def check_exists(self) -> bool:
        from DataHub.services.realtime_service import RealtimeDataService
        df = RealtimeDataService().load_intraday_parquet(asset_type='index', latest_snapshot=True)
        return df is not None and not df.empty


# 实时数据加载器工厂
REALTIME_LOADERS: Dict[str, Callable[[Path], RealtimeDataLoader]] = {
    'stock': lambda root: StockRealtimeLoader(),
    'etf': lambda root: EtfRealtimeLoader(),
    'index': lambda root: IndexRealtimeLoader(),
}


def get_asset_config(asset_type: str) -> AssetConfig:
    """获取资产类型配置"""
    return ASSET_CONFIGS.get(asset_type, ASSET_CONFIGS['stock'])


def get_realtime_loader(asset_type: str, project_root: Path) -> Optional[RealtimeDataLoader]:
    """获取实时数据加载器"""
    loader_factory = REALTIME_LOADERS.get(asset_type)
    return loader_factory(project_root) if loader_factory else None


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
    from ShortTerm.services.stock_signal_scanner import LeftSignalDetector, RightSignalDetector
    
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

    # 合并实时数据到历史K线（历史数据为前复权，需同步转换实时价格）
    from Dashboard.utils.data_access import merge_realtime_to_history
    merged_df = merge_realtime_to_history(hist_df, realtime, adjust="qfq")
    
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


def extract_symbols_from_realtime(realtime_df: pd.DataFrame, asset_type: str, 
                                   limit: Optional[int] = None) -> List[str]:
    """从实时数据中提取代码列表，根据资产类型过滤"""
    from lib.utils import detect_asset_type
    
    # 获取所有代码
    if isinstance(realtime_df, pd.DataFrame):
        all_symbols = realtime_df['symbol'].tolist()
    else:
        all_symbols = list(realtime_df)
    
    # 根据资产类型过滤
    if asset_type == 'stock':
        # 股票：过滤掉北交所、ETF和指数
        # 正常股票代码特征：不是ETF前缀，不是指数前缀，不是北交所
        filtered = []
        for s in all_symbols:
            # 排除北交所
            if s.endswith('.BJ'):
                continue
            # 检测资产类型，排除ETF和指数
            detected_type = detect_asset_type(s, default='stock')
            # default='stock' 表示如果识别不了，就认为是股票
            # 所以我们要排除明确是etf或index的情况
            if detected_type not in ['etf', 'index']:
                filtered.append(s)
        symbols = filtered
    elif asset_type == 'etf':
        # ETF：只保留ETF
        symbols = [s for s in all_symbols if detect_asset_type(s, default='stock') == 'etf']
    elif asset_type == 'index':
        # 指数：只保留指数
        symbols = [s for s in all_symbols if detect_asset_type(s, default='stock') == 'index']
    else:
        symbols = all_symbols
    
    if limit:
        symbols = symbols[:limit]
    
    return symbols


def build_scan_result(signals: list, symbols: List[str], price_time_str: str, 
                      multi_period: bool) -> Dict:
    """构建扫描结果字典"""
    from collections import Counter
    
    by_signal = Counter([s.signal_name for s in signals])
    left_count = sum(1 for s in signals if s.signal_type == 'left')
    right_count = sum(1 for s in signals if s.signal_type == 'right')
    by_period = Counter([s.period for s in signals])
    
    return {
        'status': 'success',
        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'price_fetch_time': price_time_str,
        'total_stocks': len(symbols),
        'total_signals': len(signals),
        'multi_period': multi_period,
        'intraday_mode': True,
        'signals': [s.to_dict() for s in signals],
        'stats': {
            'left': left_count,
            'right': right_count,
            'by_signal': dict(by_signal),
            'by_period': dict(by_period)
        }
    }


def save_scan_result(result: Dict, asset_type: str, project_root: Path) -> Path:
    """保存扫描结果到文件（统一合并文件）"""
    from DataHub.config import SHORTTERM_SIGNALS_DIR
    output_dir = SHORTTERM_SIGNALS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime('%Y%m%d')

    # 转换 numpy 类型为 Python 原生类型
    result = convert_to_serializable(result)

    # 统一带日期的文件名（覆盖）
    filepath = output_dir / f"signal_{date_str}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 统一 latest 文件名（覆盖）
    latest_path = output_dir / "signal_latest.json"
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return latest_path


def format_symbol(symbol: str) -> str:
    """格式化代码，确保有后缀"""
    if not symbol.endswith(('.SH', '.SZ', '.BJ')):
        from lib.utils import StockCodeUtil
        return StockCodeUtil.with_suffix(symbol) or symbol
    return symbol


def print_intraday_summary(signals: list, asset_config: AssetConfig, result: Dict):
    """打印盘中扫描结果摘要"""
    if not signals:
        print(f"\n✓ 暂无{asset_config.name}盘中信号")
        return
    
    print(f"\n🚨 发现 {len(signals)} 个盘中信号:")
    
    # 按标的分组显示
    by_symbol = {}
    for sig in signals:
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
        print(f"\n   ... 还有 {len(by_symbol)-10} 只{asset_config.name}有信号")
    
    left_count = result['stats']['left']
    right_count = result['stats']['right']
    print(f"\n📊 信号分布: 左侧 {left_count} 个, 右侧 {right_count} 个")
    
    if result['stats']['by_signal']:
        print(f"\n📈 按信号类型分布 (Top 10):")
        for sig_name, count in sorted(
            result['stats']['by_signal'].items(),
            key=lambda x: -x[1]
        )[:10]:
            print(f"    {sig_name}: {count}")
    
    print(f"\n💾 数据已保存: signal_latest.json")


def print_scan_summary(result: Dict, asset_config: AssetConfig):
    """打印扫描完成后的摘要"""
    print("\n" + "=" * 60)
    print("✅ 扫描完成!")
    print("=" * 60)
    print(f"\n📊 统计信息:")
    print(f"  扫描{asset_config.name}数: {result['total_stocks']}")
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

    print(f"\n💾 数据已保存: signal_latest.json")
    print(f"🌐 请在 Dashboard 中查看: streamlit run Dashboard/app.py")


def scan_single_symbol_intraday(scanner, symbol: str, realtime_df: pd.DataFrame, 
                                 multi_period: bool, asset_config: AssetConfig) -> list:
    """扫描单只标的的盘中信号"""
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
    
    return signals


def run_intraday_scan(args, asset_config: AssetConfig, single_mode: bool = True) -> int:
    """运行盘中扫描模式
    
    Args:
        args: 命令行参数
        asset_config: 资产类型配置
        single_mode: 是否为单只标的模式（影响循环逻辑）
    """
    if single_mode:
        scan_date = parse_scan_date(getattr(args, 'date', None))
        date_display = scan_date if scan_date else datetime.now().strftime('%Y-%m-%d')
        print("=" * 60)
        print(f"📊 盘中{asset_config.name}信号监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📅 数据日期: {date_display}")
        print("=" * 60)
        print(f"\n💡 模式: 当天数据模式（合并实时数据）- 实时行情由 DataHub 提供")

    scanner = StockSignalScanner(asset_type=asset_config.name_en, scan_date=parse_scan_date(getattr(args, 'date', None)))
    loader = get_realtime_loader(asset_config.name_en, project_root)
    
    if not loader:
        print(f"❌ 不支持的资产类型: {asset_config.name_en}")
        return 1

    iteration = 0
    while True:
        iteration += 1

        if single_mode and (iteration > 1 or args.watch > 0):
            print(f"\n{'='*60}")
            print(f"🔄 刷新时间: {datetime.now().strftime('%H:%M:%S')}")
            print('='*60)

        try:
            if single_mode:
                print("\n📡 读取 DataHub 实时数据...")
            
            realtime_df, price_time_str = loader.load()
            
            if realtime_df is None or realtime_df.empty:
                print(f"\n❌ 未找到当天{asset_config.name}实时数据文件")
                print(f"   请先运行: python DataHub/services/realtime_service.py --type {asset_config.name_en}")
                print("   或设置定时任务自动获取")
                return 1

            if single_mode:
                print(f"   ✓ 已读取实时数据 ({price_time_str}): {len(realtime_df)} 只{asset_config.name}")

            multi_period = not args.no_multi_period
            
            if args.symbol:
                # 单只标的
                symbol = format_symbol(args.symbol)
                scan_single_symbol_intraday(scanner, symbol, realtime_df, multi_period, asset_config)
            else:
                # 全市场扫描
                symbols = extract_symbols_from_realtime(realtime_df, asset_config.name_en, args.limit)
                
                if args.limit:
                    print(f"   扫描前 {args.limit} 只{asset_config.name}...")
                else:
                    suffix = "（已排除北交所）" if asset_config.name_en == 'stock' else ""
                    print(f"   扫描全市场 {len(symbols)} 只{asset_config.name}{suffix}...")
                
                all_signals = []
                for symbol in symbols:
                    signals = scan_intraday_signals(scanner, symbol, realtime_df, multi_period)
                    if signals:
                        all_signals.extend(signals)
                
                # 构建并保存结果
                result = build_scan_result(all_signals, symbols, price_time_str, multi_period)
                save_scan_result(result, asset_config.name_en, project_root)
                
                # 打印摘要
                print_intraday_summary(all_signals, asset_config, result)

        except FileNotFoundError as e:
            print(f"\n❌ {e}")
            print("\n📋 获取数据失败，请检查网络连接或 DataHub 配置")
        except Exception as e:
            print(f"\n❌ 扫描失败: {e}")
            import traceback
            traceback.print_exc()

        if args.watch > 0:
            print(f"\n  ⏱️  {args.watch}秒后刷新...")
            time.sleep(args.watch)
        else:
            break

    return 0


def run_historical_scan(args, asset_config: AssetConfig):
    """运行历史数据扫描模式（非实时）"""
    multi_period = not args.no_multi_period
    period_str = "多周期(日/周/月)" if multi_period else "仅日线"
    
    scan_date = parse_scan_date(getattr(args, 'date', None))
    if scan_date:
        date_display = scan_date
    else:
        latest_date = get_latest_market_date()
        date_display = latest_date if latest_date else "最新可用数据"
    print("=" * 60)
    print(f"🔍 开始扫描{asset_config.name}信号 - 周期: {period_str} - 模式: 历史数据模式")
    print(f"📅 扫描基准日期: {date_display}")
    print("=" * 60)

    scanner = StockSignalScanner(asset_type=asset_config.name_en, scan_date=parse_scan_date(getattr(args, 'date', None)))

    if args.symbol:
        # 扫描单只标的
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
        # 扫描所有标的
        result = scanner.scan_all('all', args.limit, multi_period)
        
        if result.get('status') == 'success':
            print_scan_summary(result, asset_config)
        else:
            print(f"\n❌ 扫描失败: {result.get('message', '未知错误')}")


def scan_asset_type_historical(asset_type: str, symbol: Optional[str] = None,
                                limit: Optional[int] = None, multi_period: bool = True,
                                save_result: bool = True, scan_date: Optional[str] = None) -> Dict:
    """
    扫描单一资产类型的历史信号

    Args:
        asset_type: 资产类型 'stock' / 'etf' / 'index'
        symbol: 指定代码（单只模式），None为全市场
        limit: 扫描数量限制
        multi_period: 是否多周期分析
        save_result: 是否保存结果到文件（组合扫描时由外部统一保存）
        scan_date: 扫描基准日期 'YYYY-MM-DD'，None 表示使用最新数据

    Returns:
        扫描结果字典
    """
    config = get_asset_config(asset_type)
    scanner = StockSignalScanner(asset_type=asset_type, scan_date=scan_date)

    if symbol:
        # 单只模式
        from lib.utils import get_stock_name
        name = get_stock_name(symbol)
        signals = scanner.scan_stock(symbol, name, 'all', multi_period)
        return {
            'status': 'success',
            'total_stocks': 1,
            'total_signals': len(signals),
            'signals': [s.to_dict() for s in signals],
            'stats': {
                'left': sum(1 for s in signals if s.signal_type == 'left'),
                'right': sum(1 for s in signals if s.signal_type == 'right'),
                'by_signal': {},
                'by_period': {}
            }
        }
    else:
        # 全市场模式（scan_all 内部已使用 etf_basic_info.csv 获取ETF列表）
        return scanner.scan_all('all', limit, multi_period, save_result=save_result)


def _merge_and_save_combined_results(results: Dict[str, Dict]) -> Path:
    """合并股票+ETF+指数的扫描结果，统一保存到 signal_latest.json"""
    from DataHub.config import SHORTTERM_SIGNALS_DIR
    output_dir = SHORTTERM_SIGNALS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 收集所有信号和健康度
    all_signals = []
    all_health_scores = []
    total_stocks = 0

    for result in results.values():
        if result.get('status') != 'success':
            continue
        all_signals.extend(result.get('signals', []))
        health = result.get('health_scores', {})
        all_health_scores.extend(health.get('all_scores', []))
        total_stocks += result.get('total_stocks', 0)

    # 按 symbol 组织信号
    signals_by_stock = {}
    for sig in all_signals:
        symbol = sig.get('symbol')
        if symbol not in signals_by_stock:
            signals_by_stock[symbol] = []
        signals_by_stock[symbol].append(sig)

    # 构建 stocks 数组
    stocks = []
    for health in all_health_scores:
        symbol = health['symbol']
        stock_signals = signals_by_stock.get(symbol, [])
        scores = [s.get('score', 0) for s in stock_signals]

        # 直接使用预计算的风险雷达和维度分
        risk_score = health.get('risk_score', 0)
        risk_explanations = health.get('risk_explanations', [])
        signal_score = health.get('dimension_score', 0)

        # 价格：优先从健康度记录取（代表最新交易日），没有则从最新信号补
        close_price = health.get('close_price', 0)
        change_pct = health.get('change_pct', 0)
        if not close_price and stock_signals:
            latest_sig = max(stock_signals, key=lambda s: s.get('trigger_date', ''))
            close_price = latest_sig.get('close_price', 0)
            change_pct = latest_sig.get('change_pct', 0)

        stocks.append({
            'symbol': symbol,
            'name': health.get('name', ''),
            'risk_score': risk_score,
            'risk_explanations': risk_explanations or ['暂无风险详情'],
            'signal_score': signal_score,
            'stage': health.get('stage', 'unknown'),
            'dimension_breakdown': health.get('dimension_breakdown', {}),
            'has_buy_signal': len(stock_signals) > 0,
            'signal_count': len(stock_signals),
            'signals': stock_signals,
            'close_price': close_price,
            'change_pct': change_pct,
            'risk_level': health.get('risk_level', 'unknown'),
            'risk_warnings': health.get('warnings', []),
            'risk_details': {},
            'risk_recommendation': health.get('recommendation', ''),
            'technicals': stock_signals[0].get('technicals', {}) if stock_signals else {},
        })

    # 按信号分数排序
    stocks.sort(key=lambda x: x['signal_score'], reverse=True)

    unified = {
        'status': 'success',
        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_stocks': total_stocks,
        'total_signals': len(all_signals),
        'stocks': stocks,
    }

    unified = convert_to_serializable(unified)

    latest_path = output_dir / "signal_latest.json"
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(unified, f, ensure_ascii=False, indent=2)

    print(f"\n💾 合并结果已保存: {latest_path} ({len(all_signals)} 个信号, {len(stocks)} 只标的)")
    return latest_path


def run_combined_scan(args, include_index: bool = True):
    """运行组合扫描（股票+ETF+指数）"""
    multi_period = not args.no_multi_period
    period_str = "多周期(日/周/月)" if multi_period else "仅日线"
    
    scan_targets = "股票+ETF+指数" if include_index else "股票+ETF"
    scan_date = parse_scan_date(getattr(args, 'date', None))
    if scan_date:
        date_display = scan_date
    else:
        latest_date = get_latest_market_date()
        date_display = latest_date if latest_date else "最新可用数据"
    print("=" * 60)
    print(f"🔍 开始扫描全部信号（{scan_targets}）- 周期: {period_str}")
    print(f"📅 扫描基准日期: {date_display}")
    print("=" * 60)
    
    results = {}
    asset_types = ['stock', 'etf', 'index'] if include_index else ['stock', 'etf']
    for asset_type in asset_types:
        config = get_asset_config(asset_type)
        print(f"\n📊 扫描{config.name}...")
        results[asset_type] = scan_asset_type_historical(
            asset_type, args.symbol, args.limit, multi_period,
            save_result=False, scan_date=parse_scan_date(getattr(args, 'date', None))
        )
    
    # 合并三份结果，统一保存到 signal_latest.json（避免覆盖）
    _merge_and_save_combined_results(results)
    
    # 打印结果
    print("\n" + "=" * 60)
    print("✅ 全部扫描完成!")
    print("=" * 60)
    
    total_signals = 0
    for asset_type, result in results.items():
        config = get_asset_config(asset_type)
        if result.get('status') == 'success':
            print(f"\n📈 {config.name}信号:")
            print(f"  扫描数量: {result['total_stocks']}")
            print(f"  发现信号: {result['total_signals']}")
            print(f"  左侧: {result['stats']['left']} | 右侧: {result['stats']['right']}")
            total_signals += result['total_signals']
    
    print(f"\n📊 总计: {total_signals} 个信号")
    print(f"💾 数据已保存: signal_latest.json")


def scan_asset_type_intraday(realtime_df: pd.DataFrame, asset_type: str,
                              multi_period: bool, limit: Optional[int] = None) -> Tuple[List, List[str]]:
    """
    扫描单一资产类型的盘中信号

    Args:
        realtime_df: 实时数据DataFrame
        asset_type: 资产类型 'stock' / 'etf' / 'index'
        multi_period: 是否多周期分析
        limit: 扫描数量限制

    Returns:
        (signals, symbols): (信号列表, 扫描的代码列表)
    """
    config = get_asset_config(asset_type)
    scanner = StockSignalScanner(asset_type=asset_type)
    symbols = extract_symbols_from_realtime(realtime_df, asset_type, limit)

    # ETF只扫描 etf_basic_info.csv 中列出的（避免同步全市场ETF历史数据）
    if asset_type == 'etf':
        from DataHub.config import get_storage_path
        etf_csv = get_storage_path("etf_basic_info.csv")
        if etf_csv.exists():
            import pandas as pd
            etf_list = pd.read_csv(etf_csv)['symbol'].tolist()
            symbols = [s for s in symbols if s in etf_list]

    # 指数只扫描 official_indices.csv 中列出的（避免扫描全市场指数）
    if asset_type == 'index':
        from DataHub.config import get_storage_path
        index_csv = get_storage_path("official_indices.csv")
        if index_csv.exists():
            import pandas as pd
            index_list = pd.read_csv(index_csv)['symbol'].tolist()
            symbols = [s for s in symbols if s in index_list]
        else:
            print(f"   ⚠️ 未找到指数列表文件: {index_csv}，将扫描全部 {len(symbols)} 只指数")

    if limit:
        print(f"   扫描前 {limit} 只{config.name}...")
    else:
        suffix = "（已排除北交所）" if asset_type == 'stock' else ""
        print(f"   扫描全市场 {len(symbols)} 只{config.name}{suffix}...")

    signals = []
    for symbol in symbols:
        sigs = scan_intraday_signals(scanner, symbol, realtime_df, multi_period)
        if sigs:
            signals.extend(sigs)

    return signals, symbols


def run_all_intraday_scan(args, include_index: bool = True):
    """运行全市场盘中扫描（股票+ETF+指数合并）"""
    scan_date = parse_scan_date(getattr(args, 'date', None))
    date_display = scan_date if scan_date else datetime.now().strftime('%Y-%m-%d')
    print("=" * 60)
    print(f"📊 全市场信号监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 数据日期: {date_display}")
    print("=" * 60)
    print(f"\n💡 模式: 当天数据模式（合并实时数据）- 实时行情由 DataHub 提供")
    
    multi_period = not args.no_multi_period
    
    # 分别加载股票、ETF和指数实时数据
    stock_loader = get_realtime_loader('stock', project_root)
    etf_loader = get_realtime_loader('etf', project_root)
    index_loader = get_realtime_loader('index', project_root)
    
    stock_df, stock_time = stock_loader.load() if stock_loader else (None, "")
    etf_df, etf_time = etf_loader.load() if etf_loader else (None, "")
    index_df, index_time = index_loader.load() if index_loader else (None, "")
    
    has_any_data = (stock_df is not None and not stock_df.empty) or \
                   (etf_df is not None and not etf_df.empty) or \
                   (index_df is not None and not index_df.empty)
    
    if not has_any_data:
        print("\n❌ 未找到当天实时数据文件")
        print("   请先运行: python DataHub/services/realtime_service.py")
        return 1
    
    if stock_df is not None and not stock_df.empty:
        print(f"   ✓ 股票实时数据 ({stock_time}): {len(stock_df)} 只")
    if etf_df is not None and not etf_df.empty:
        print(f"   ✓ ETF实时数据 ({etf_time}): {len(etf_df)} 只")
    if index_df is not None and not index_df.empty:
        print(f"   ✓ 指数实时数据 ({index_time}): {len(index_df)} 个")
    
    # 扫描各资产类型
    all_signals = []
    all_symbols = []
    
    asset_types = [('stock', stock_df), ('etf', etf_df)]
    if include_index:
        asset_types.append(('index', index_df))
    
    for asset_type, df in asset_types:
        if df is not None and not df.empty:
            print(f"\n📊 扫描{get_asset_config(asset_type).name}...")
            signals, symbols = scan_asset_type_intraday(df, asset_type, multi_period, args.limit)
            all_signals.extend(signals)
            all_symbols.extend(symbols)
    
    # 统一价格时间（取最新的一个）
    price_time_str = stock_time or etf_time or index_time or ""

    # 保存结果
    result = build_scan_result(all_signals, all_symbols, price_time_str, multi_period)
    save_all_intraday_results(result, all_signals, stock_df, etf_df, stock_time, etf_time, multi_period, args.limit, index_df, index_time)
    
    # 打印摘要
    print_all_intraday_summary(all_signals, stock_time, etf_time, index_time)
    
    return 0


def save_all_intraday_results(result: Dict, all_signals: List,
                               stock_df: Optional[pd.DataFrame], etf_df: Optional[pd.DataFrame],
                               stock_time: str, etf_time: str, multi_period: bool, limit: Optional[int],
                               index_df: Optional[pd.DataFrame] = None, index_time: str = ""):
    """保存全市场盘中扫描结果（统一合并文件，覆盖保存）"""
    from DataHub.config import SHORTTERM_SIGNALS_DIR
    output_dir = SHORTTERM_SIGNALS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime('%Y%m%d')

    # 转换 numpy 类型为 Python 原生类型
    result = convert_to_serializable(result)

    # 统一保存合并文件（覆盖）
    filepath = output_dir / f"signal_{date_str}.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    latest_path = output_dir / "signal_latest.json"
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def print_all_intraday_summary(all_signals: List, stock_time: str, etf_time: str, index_time: str = ""):
    """打印全市场盘中扫描摘要"""
    print("\n" + "=" * 60)
    print("✅ 扫描完成!")
    print("=" * 60)
    
    if not all_signals:
        print("\n✓ 暂无盘中信号")
        return
    
    stock_signals = [s for s in all_signals if detect_asset_type(s.symbol) == 'stock']
    etf_signals = [s for s in all_signals if detect_asset_type(s.symbol) == 'etf']
    index_signals = [s for s in all_signals if detect_asset_type(s.symbol) == 'index']
    
    print(f"\n🚨 发现 {len(all_signals)} 个盘中信号:")
    print(f"   股票: {len(stock_signals)} 个")
    print(f"   ETF: {len(etf_signals)} 个")
    if index_signals:
        print(f"   指数: {len(index_signals)} 个")
    
    by_symbol = {}
    for sig in all_signals:
        sym = sig.symbol
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(sig)
    
    for symbol, sigs in list(by_symbol.items())[:10]:
        name = sigs[0].name
        asset_type_map = {'etf': 'ETF', 'index': '指数'}
        asset_type = asset_type_map.get(detect_asset_type(symbol), '股票')
        print(f"\n   [{asset_type}] {symbol} {name}:")
        for sig in sigs[:3]:
            period_tag = {"daily": "日", "weekly": "周", "monthly": "月"}.get(sig.period, "日")
            print(f"     [{period_tag}线][{sig.signal_type.upper()}] {sig.signal_name}")
        if len(sigs) > 3:
            print(f"     ... 还有 {len(sigs)-3} 个信号")
    
    if len(by_symbol) > 10:
        print(f"\n   ... 还有 {len(by_symbol)-10} 只标的有信号")
    
    print(f"\n💾 数据已保存: signal_latest.json")


def main():
    parser = argparse.ArgumentParser(
        description='个股/ETF/指数信号扫描 - 支持多周期（日线/周线/月线），自动使用最新数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 扫描全部（股票+ETF+指数）
  python ShortTerm/run_signal_scan.py                    # 扫描全部信号
  python ShortTerm/run_signal_scan.py --watch 60         # 持续监控模式
  
  # 扫描指定标的（自动识别股票/ETF/指数）
  python ShortTerm/run_signal_scan.py --symbol 600519.SH # 扫描单只股票
  python ShortTerm/run_signal_scan.py --symbol 510300.SH # 扫描单只ETF
  python ShortTerm/run_signal_scan.py --symbol 000001.SH # 扫描单只指数
        """
    )
    parser.add_argument('--symbol', type=str, help='扫描指定股票/ETF（自动识别类型）')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--date', type=str, help='扫描基准日期，支持 2026-04-25 / 20260425 / 2026/04/25 等格式（默认使用最新数据）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')
    parser.add_argument('--watch', type=int, default=0,
                        help='持续监控模式，每隔N秒刷新一次（如 --watch 60）')

    args = parser.parse_args()

    scan_date = parse_scan_date(args.date)
    if scan_date:
        date_display = scan_date
    else:
        latest_date = get_latest_market_date()
        date_display = latest_date if latest_date else "最新可用数据"

    print("=" * 60)
    print(f"📊 信号扫描 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 扫描基准日期: {date_display}")
    print("=" * 60)

    # 单只标扫描模式
    if args.symbol:
        # 自动检测资产类型
        asset_type = detect_asset_type(args.symbol)
        asset_config = get_asset_config(asset_type)
        
        print(f"\n📌 检测到: {asset_config.name} ({args.symbol})")
        
        # 检查是否有实时数据
        loader = get_realtime_loader(asset_type, project_root)
        has_today_data = loader.check_exists() if loader else False
        
        if has_today_data:
            return run_intraday_scan(args, asset_config)
        else:
            run_historical_scan(args, asset_config)
            return

    # 全市场扫描模式（默认）
    # 检查是否有实时数据
    stock_loader = get_realtime_loader('stock', project_root)
    etf_loader = get_realtime_loader('etf', project_root)
    index_loader = get_realtime_loader('index', project_root)
    
    has_stock_data = stock_loader.check_exists() if stock_loader else False
    has_etf_data = etf_loader.check_exists() if etf_loader else False
    has_index_data = index_loader.check_exists() if index_loader else False
    
    if has_stock_data or has_etf_data or has_index_data:
        # 有实时数据：盘中扫描模式
        iteration = 0
        while True:
            iteration += 1
            if iteration > 1:
                print(f"\n{'='*60}")
                print(f"🔄 刷新时间: {datetime.now().strftime('%H:%M:%S')}")
                print('='*60)
            
            run_all_intraday_scan(args, include_index=True)
            
            if args.watch > 0:
                print(f"\n  ⏱️  {args.watch}秒后刷新...")
                time.sleep(args.watch)
            else:
                break
    else:
        # 无实时数据：历史扫描模式
        print("\n💡 模式: 历史数据模式（无实时数据）")
        run_combined_scan(args, include_index=True)


if __name__ == "__main__":
    main()

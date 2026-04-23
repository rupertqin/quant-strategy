"""
同步服务命令行接口

重构后与原有命令完全兼容：
- python -m DataHub.services.sync --today                    # 同步当天数据
- python -m DataHub.services.sync --sync-factors             # 只同步复权因子
- python -m DataHub.services.sync --daily                    # 每日增量更新（全部股票）
- python -m DataHub.services.sync --daily --symbol etf       # 同步全部ETF
- python -m DataHub.services.sync --daily --symbol index     # 同步全部指数
- python -m DataHub.services.sync --daily --symbol 600519    # 同步指定股票
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from .sync_manager import SyncManager


def parse_symbol_arg(symbol_str: str) -> Tuple[List[str], str]:
    """
    解析 --symbol 参数
    
    支持:
    - 类型简写: "etf" -> ([], 'etf'), "index" -> ([], 'index'), "stock" -> ([], 'stock')
    - 具体代码: "600519.SH,300750.SZ" -> (['600519.SH', '300750.SZ'], None)
    
    Returns:
        (symbol_list, asset_type) - symbol_list 为空列表表示同步全部该类型
    """
    if not symbol_str:
        return [], 'stock'  # 默认全部股票
    
    # 类型简写映射
    type_aliases = {'stock', 'etf', 'index'}
    
    if symbol_str.lower() in type_aliases:
        return [], symbol_str.lower()
    
    # 具体代码列表
    symbols = [s.strip() for s in symbol_str.split(',')]
    return symbols, None


def parse_date_arg(date_str: str) -> Tuple[str, str]:
    """
    解析日期参数
    
    支持:
    - 单日: 20260413, 2026-04-13
    - 范围: 20260413~20260414, 2026-04-13~2026-04-14
    """
    import re
    
    # 统一格式：去掉分隔符
    normalized = date_str.replace('-', '').replace('/', '')
    
    # 检查是否有范围分隔符
    if '~' in normalized or '—' in normalized or '－' in normalized:
        # 范围格式
        parts = re.split(r'[~—－]', normalized)
        if len(parts) != 2:
            raise ValueError(f"日期范围格式错误: {date_str}")
        start = parts[0].strip()
        end = parts[1].strip()
    else:
        # 单日格式
        start = normalized
        end = normalized
    
    # 验证日期格式
    for d in [start, end]:
        if len(d) != 8 or not d.isdigit():
            raise ValueError(f"日期格式错误: {d}, 应为 YYYYMMDD")
    
    return start, end


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='数据同步服务 - 与原有命令完全兼容',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 同步当天数据（极速模式）
  python -m DataHub.services.sync --today
  python -m DataHub.services.sync --today --sync-factors
  
  # 每日增量更新
  python -m DataHub.services.sync --daily
  python -m DataHub.services.sync --daily 20260413
  python -m DataHub.services.sync --daily 20260413~20260414
  
  # 同步全部ETF
  python -m DataHub.services.sync --daily --symbol etf
  
  # 同步全部指数
  python -m DataHub.services.sync --daily --symbol index
  
  # 同步指定股票
  python -m DataHub.services.sync --daily --symbol 600519.SH
  
  # 只同步复权因子
  python -m DataHub.services.sync --sync-factors
  
  # 并发设置
  python -m DataHub.services.sync --daily --symbol etf --workers 3
        """
    )
    
    parser.add_argument('--today', action='store_true',
                       help='同步当天数据（极速模式，默认不同步复权因子）')
    parser.add_argument('--daily', nargs='?', const=True, default=False,
                       help='每日增量更新。可指定日期: 20260413, 2026-04-13, 20260413~20260414')
    parser.add_argument('--sync-factors', action='store_true',
                       help='同步复权因子（在 --today 时生效）')
    parser.add_argument('--symbol', type=str,
                       help='指定代码或类型: 1) etf/index/stock 2) 具体代码如600519.SH')
    parser.add_argument('--override', action='store_true',
                       help='覆盖已有数据（默认增量）')
    parser.add_argument('--skip-existing', action='store_true',
                       help='首次同步时跳过已有文件的股票（大幅提速）')
    parser.add_argument('--summary', action='store_true',
                       help='显示同步摘要')
    parser.add_argument('--sync-index-intraday', action='store_true',
                       help='同步指数分时数据（1分钟线）')
    parser.add_argument('--workers', type=int, default=1,
                       help='并发线程数（默认1）')
    parser.add_argument('--limit', type=int,
                       help='限制处理数量（仅用于测试）')
    parser.add_argument('--include-bj', action='store_true',
                       help='包含北交所股票（仅对股票类型有效）')
    parser.add_argument('--no-logout', action='store_true',
                       help='不执行 baostock 登出（用于并行执行时不影响其他进程）')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    manager = SyncManager(max_workers=args.workers)
    
    try:
        if args.sync_factors and not args.today and not args.daily:
            # 只同步复权因子（单独使用 --sync-factors）
            print("\n" + "="*60)
            print("只同步复权因子（不下载价格数据）")
            print("="*60)
            result = manager.sync_factors_only()
            print("\n结果:")
            print(f"  成功: {result['success']}")
            print(f"  失败: {result['failed']}")
            print("="*60)

        elif args.summary:
            # 显示同步摘要
            summary = manager.get_sync_summary()
            print("\n" + "="*60)
            print("同步摘要")
            print("="*60)
            print(f"  总文件数: {summary['total_files']}")
            print(f"  总记录数: {summary['total_records']:,}")

            print("\n" + "-"*60)
            print("最新日期分布")
            print("-"*60)
            dist = manager.get_latest_date_distribution()
            if dist['total_stocks'] == 0:
                print("  没有找到任何股票数据文件")
            else:
                print(f"  全局最新日期: {dist['latest_overall']}")
                print(f"  {'日期':<15} {'股票数':>10} {'占比':>10}")
                print("  " + "-"*40)
                for date, count in dist['distribution'].items():
                    pct = count / dist['total_stocks'] * 100
                    marker = " <-- 最新" if date == dist['latest_overall'] else ""
                    print(f"  {str(date):<15} {count:>10,} {pct:>9.1f}%{marker}")

            print("\n" + "-"*60)
            print(f"文件列表 (前20个):")
            print("-"*60)
            for f in summary['files'][:20]:
                print(f"  {f['symbol']}: {f['records']:,} 条 ({f['date_range']})")
            if len(summary['files']) > 20:
                print(f"  ... 还有 {len(summary['files']) - 20} 个文件")
            print("="*60)

        elif args.sync_index_intraday:
            # 同步指数分时数据
            print("\n" + "="*60)
            print("同步指数分时数据（1分钟线）")
            print("="*60)
            result = manager.sync_index_intraday()
            print(f"\n指数分时数据同步完成: 成功 {result['success']}, 失败 {result['failed']}")

        elif args.today:
            # 同步当天数据（极速模式）
            today_str = datetime.now().strftime('%Y%m%d')
            print("\n" + "="*60)
            print(f"执行当天数据同步: {today_str}")
            print("="*60)
            print("⚡ 极速模式：使用 stock_zh_a_spot 获取全市场当日数据")
            sync_factor = args.sync_factors
            if sync_factor:
                print("📊 同时更新复权因子")
            else:
                print("📊 复权因子: 跳过（加 --sync-factors 可同步）")
            print()

            result = manager.sync_today_data(sync_factors=sync_factor)

            print("\n" + "="*60)
            print("当天数据同步结果")
            print("="*60)
            print(f"  交易日期: {result.get('trade_date', today_str)}")
            print(f"  价格数据:")
            print(f"    - 更新: {result.get('updated', 0)} 只")
            print(f"    - 跳过(无数据): {result.get('skipped', 0)} 只")
            print(f"    - 失败: {result.get('failed', 0)} 只")
            if sync_factor:
                print(f"  复权因子:")
                print(f"    - 更新: {result.get('factor_updated', 0)} 只")
                print(f"    - 跳过(无需更新): {result.get('factor_skipped', 0)} 只")
            print("="*60)

        elif args.daily:
            # 每日增量更新
            print("\n" + "="*60)
            print("执行每日增量更新")
            print("="*60)

            # 解析日期参数
            start_date = None
            end_date = None
            if isinstance(args.daily, str):
                try:
                    start_date, end_date = parse_date_arg(args.daily)
                except ValueError as e:
                    print(f"错误: {e}")
                    return

            # 解析 --symbol 参数
            symbol_list, asset_type = parse_symbol_arg(args.symbol)

            if asset_type == "etf":
                asset_type_str = "ETF"
            elif asset_type == "index":
                asset_type_str = "指数"
            else:
                asset_type_str = "股票"

            if symbol_list:
                symbols = symbol_list
                print(f"指定{asset_type_str}: {len(symbols)} 只")
                print(f"  {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
            elif args.limit:
                if asset_type == "etf":
                    symbols = manager._get_etf_list()[:args.limit]
                elif asset_type == "index":
                    symbols = manager._get_index_list()[:args.limit]
                else:
                    symbols = manager._get_stock_list()[:args.limit]
                print(f"测试模式: 只同步前 {args.limit} 只{asset_type_str}")
            else:
                if asset_type == "etf":
                    symbols = manager._get_etf_list()
                elif asset_type == "index":
                    symbols = manager._get_index_list()
                else:
                    symbols = manager._get_stock_list()
                print(f"将同步全部 {len(symbols)} 只{asset_type_str}")

            # 默认跳过北交所股票
            if asset_type != "index" and not args.include_bj:
                bj_count = sum(1 for s in symbols if '.BJ' in s)
                symbols = [s for s in symbols if '.BJ' not in s]
                if bj_count > 0:
                    print(f"提示: 已跳过 {bj_count} 只北交所股票（使用 --include-bj 可包含）")

            if start_date or end_date:
                print(f"指定日期范围: {start_date or 'auto'} ~ {end_date or 'today'}")
            else:
                print("自动同步到最新日期")

            print(f"并发数: {args.workers}，每只请求间隔: 0.5-2秒")
            print("\n" + "-"*60)
            print("开始同步")
            print("-"*60)

            # 根据类型调用对应方法（保持与测试兼容）
            if asset_type == 'etf':
                result = manager.sync_etf_daily(
                    start_date=start_date,
                    end_date=end_date,
                    incremental=True,
                    limit=args.limit,
                    skip_existing=args.skip_existing,
                    override=args.override
                )
            elif asset_type == 'index':
                result = manager.sync_index_daily(
                    start_date=start_date,
                    end_date=end_date,
                    incremental=True,
                    limit=args.limit,
                    skip_existing=args.skip_existing,
                    override=args.override
                )
            else:
                if symbol_list:
                    result = manager.sync_stock_list(
                        symbols=symbol_list,
                        incremental=True
                    )
                else:
                    result = manager.sync_stock_daily(
                        start_date=start_date,
                        end_date=end_date,
                        incremental=True,
                        limit=args.limit,
                        include_bj=args.include_bj,
                        skip_existing=args.skip_existing,
                        override=args.override
                    )
            print("\n同步结果:")
            print(f"  状态: {result['status']}")
            print(f"  总{asset_type_str}: {result['total']}")
            print(f"  成功: {result['success']}")
            print(f"  失败: {result['failed']}")
            if result.get('failed_symbols'):
                print(f"\n  失败代码:")
                for symbol in result['failed_symbols']:
                    print(f"    - {symbol}")
            print("\n" + "="*60)
            print(f"每日增量更新完成（{asset_type_str}数据已同步）")
            print("="*60)

        elif args.symbol:
            # 解析 --symbol 参数（无 --daily 时）
            symbol_list, asset_type = parse_symbol_arg(args.symbol)

            if not symbol_list:
                # 类型简写: 同步全部该类型
                if asset_type == "etf":
                    symbols = manager._get_etf_list()
                    asset_type_str = "ETF"
                elif asset_type == "index":
                    symbols = manager._get_index_list()
                    asset_type_str = "指数"
                else:
                    symbols = manager._get_stock_list()
                    asset_type_str = "股票"

                print(f"\n将同步全部 {len(symbols)} 只{asset_type_str}")
                if asset_type != "index" and not args.include_bj:
                    bj_count = sum(1 for s in symbols if '.BJ' in s)
                    symbols = [s for s in symbols if '.BJ' not in s]
                    if bj_count > 0:
                        print(f"提示: 已跳过 {bj_count} 只北交所股票")

                result = manager.sync_daily(
                    asset_type=asset_type,
                    incremental=not args.override,
                    max_workers=args.workers,
                    limit=args.limit,
                    skip_existing=args.skip_existing,
                    override=args.override
                )
                print("\n同步结果:")
                print(f"  成功: {result['success']}/{result['total']}")
                print(f"  失败: {result['failed']}")
            else:
                # 具体代码列表
                from lib.utils import StockCodeUtil
                from lib.utils.stock_code import detect_asset_type
                symbol_list = [StockCodeUtil.with_suffix(s) or s for s in symbol_list]
                detected_type = detect_asset_type(symbol_list[0], 'stock')

                # 选择对应同步服务
                if detected_type == 'etf':
                    svc = manager.etf_sync
                elif detected_type == 'index':
                    svc = manager.index_sync
                else:
                    svc = manager.stock_sync

                if len(symbol_list) == 1:
                    # 单只
                    if args.override:
                        from DataHub.config import RAW_PRICE_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR
                        if detected_type == "etf":
                            fp = RAW_ETF_PRICE_DIR / f"{symbol_list[0]}.parquet"
                        elif detected_type == "index":
                            fp = RAW_INDEX_PRICE_DIR / f"{symbol_list[0]}.parquet"
                        else:
                            fp = RAW_PRICE_DIR / f"{symbol_list[0]}.parquet"
                        if fp.exists():
                            fp.unlink()
                            print(f"已删除旧文件: {fp}")
                    r = svc.sync(symbol_list, incremental=not args.override)
                    print("\n同步结果:")
                    print(f"  状态: {r['status']}")
                    print(f"  代码: {symbol_list[0]}")
                    print(f"  成功: {r['success']}")
                else:
                    # 多只
                    print(f"\n同步 {len(symbol_list)} 只: {', '.join(symbol_list[:5])}{'...' if len(symbol_list) > 5 else ''}")
                    r = svc.sync(symbol_list, incremental=not args.override)
                    print("\n同步结果:")
                    print(f"  状态: {r['status']}")
                    print(f"  成功: {r['success']}/{r['total']}")
                    print(f"  失败: {r['failed']}")

        elif args.override:
            # 纯 --override（不带 --symbol）：覆盖同步全部股票
            symbols = manager._get_stock_list()
            print(f"\n将同步全部 {len(symbols)} 只股票（覆盖模式）")

            if not args.include_bj:
                bj_count = sum(1 for s in symbols if '.BJ' in s)
                symbols = [s for s in symbols if '.BJ' not in s]
                if bj_count > 0:
                    print(f"提示: 已跳过 {bj_count} 只北交所股票")

            print("\n" + "="*60)
            print("执行股票覆盖同步")
            print("="*60)

            result = manager.sync_daily(
                asset_type='stock',
                incremental=False,
                skip_existing=args.skip_existing,
                max_workers=args.workers,
                override=True,
                limit=args.limit
            )
            print("\n同步结果:")
            print(f"  状态: {result['status']}")
            print(f"  总股票: {result['total']}")
            print(f"  成功: {result['success']}")
            print(f"  失败: {result['failed']}")
            print("="*60)

        else:
            # 默认行为：同步全部股票（增量）
            symbols = manager._get_stock_list()
            print(f"\n将同步全部 {len(symbols)} 只股票")

            if not args.include_bj:
                bj_count = sum(1 for s in symbols if '.BJ' in s)
                symbols = [s for s in symbols if '.BJ' not in s]
                if bj_count > 0:
                    print(f"提示: 已跳过 {bj_count} 只北交所股票")

            print("\n" + "="*60)
            print("执行股票增量同步")
            print("="*60)

            result = manager.sync_daily(
                asset_type='stock',
                incremental=True,
                skip_existing=args.skip_existing,
                max_workers=args.workers,
                limit=args.limit
            )
            print("\n同步结果:")
            print(f"  状态: {result['status']}")
            print(f"  总股票: {result['total']}")
            print(f"  成功: {result['success']}")
            print(f"  失败: {result['failed']}")
            print("="*60)

    finally:
        if not args.no_logout:
            manager.close()


if __name__ == '__main__':
    main()

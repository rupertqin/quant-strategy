#!/usr/bin/env python3
"""
全量重跑 vs 旧数据对比分析脚本

对比维度：
1. 价格数据（change_pct, close, open, high, low, volume）
2. 复权因子（adjust_factor）

用法:
    python DataHub/scripts/compare_stock_data.py
    python DataHub/scripts/compare_stock_data.py --recent-days 30
    python DataHub/scripts/compare_stock_data.py --symbols 600519.SH,300750.SZ
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
import numpy as np
from collections import defaultdict

from DataHub.config import STORAGE_DIR

OLD_PRICE_DIR = STORAGE_DIR / "raw" / "stocks" / "price_old"
NEW_PRICE_DIR = STORAGE_DIR / "raw" / "stocks" / "price"
OLD_FACTOR_DIR = STORAGE_DIR / "raw" / "stocks" / "adjust_factor_old"
NEW_FACTOR_DIR = STORAGE_DIR / "raw" / "stocks" / "adjust_factor"


def load_df(path: Path, cols=None):
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if cols:
            missing = [c for c in cols if c not in df.columns]
            if missing:
                return None
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        return df.sort_values('trade_date').reset_index(drop=True)
    except Exception:
        return None


def compare_price(old_path: Path, new_path: Path, recent_days: int = None):
    """对比单只股票的价格数据"""
    old_df = load_df(old_path)
    new_df = load_df(new_path)

    if old_df is None or new_df is None:
        return None

    # 按日期合并
    merged = pd.merge(
        old_df, new_df,
        on='trade_date', suffixes=('_old', '_new'),
        how='outer',
        indicator=True
    )

    if recent_days:
        cutoff = pd.Timestamp.now().date() - pd.Timedelta(days=recent_days)
        merged = merged[merged['trade_date'] >= cutoff]

    if merged.empty:
        return None

    diffs = []

    # 0. 数据来源差异
    old_source = old_df['data_source'].iloc[0] if 'data_source' in old_df.columns else 'baostock'
    new_source = new_df['data_source'].iloc[0] if 'data_source' in new_df.columns else 'baostock'
    if old_source != new_source:
        diffs.append(f"数据来源差异: 旧={old_source}, 新={new_source}")

    # 1. 日期范围差异
    only_old = merged[merged['_merge'] == 'left_only']
    only_new = merged[merged['_merge'] == 'right_only']
    if len(only_old) > 0 or len(only_new) > 0:
        diffs.append(f"日期差异: 旧-only={len(only_old)} 天, 新-only={len(only_new)} 天")

    # 2. change_pct 差异（核心关注点）
    if 'change_pct_old' in merged.columns and 'change_pct_new' in merged.columns:
        both = merged[merged['_merge'] == 'both'].copy()
        if not both.empty:
            both['change_pct_diff'] = (both['change_pct_new'] - both['change_pct_old']).abs()
            # 忽略两边都是 NaN 的
            mask = both['change_pct_old'].notna() | both['change_pct_new'].notna()
            both = both[mask]
            if not both.empty:
                max_diff_row = both.loc[both['change_pct_diff'].idxmax()]
                max_diff = max_diff_row['change_pct_diff']
                if max_diff > 0.01:  # > 0.01% 算差异
                    diffs.append(
                        f"change_pct 最大差异: {max_diff:.4f}% @ {max_diff_row['trade_date']} "
                        f"(旧={max_diff_row['change_pct_old']:.4f}, 新={max_diff_row['change_pct_new']:.4f})"
                    )

    # 3. close 价格差异（判断数据源是否一致）
    if 'close_old' in merged.columns and 'close_new' in merged.columns:
        both = merged[merged['_merge'] == 'both'].copy()
        if not both.empty:
            both['close_diff_pct'] = ((both['close_new'] - both['close_old']) / both['close_old']).abs() * 100
            max_diff_row = both.loc[both['close_diff_pct'].idxmax()]
            max_diff = max_diff_row['close_diff_pct']
            if max_diff > 0.1:  # close 差 > 0.1% 算差异（复权数据源不同可能导致）
                diffs.append(
                    f"close 最大差异: {max_diff:.4f}% @ {max_diff_row['trade_date']} "
                    f"(旧={max_diff_row['close_old']:.4f}, 新={max_diff_row['close_new']:.4f})"
                )

    return diffs if diffs else None


def compare_factor(old_path: Path, new_path: Path, recent_days: int = None):
    """对比复权因子"""
    old_df = load_df(old_path)
    new_df = load_df(new_path)

    if old_df is None and new_df is not None:
        return ["旧数据无复权因子"]
    if old_df is not None and new_df is None:
        return ["新数据无复权因子"]
    if old_df is None or new_df is None:
        return None

    merged = pd.merge(
        old_df, new_df,
        on='trade_date', suffixes=('_old', '_new'),
        how='outer',
        indicator=True
    )

    if recent_days:
        cutoff = pd.Timestamp.now().date() - pd.Timedelta(days=recent_days)
        merged = merged[merged['trade_date'] >= cutoff]

    if merged.empty:
        return None

    diffs = []

    only_old = merged[merged['_merge'] == 'left_only']
    only_new = merged[merged['_merge'] == 'right_only']
    if len(only_old) > 0 or len(only_new) > 0:
        diffs.append(f"日期差异: 旧-only={len(only_old)} 天, 新-only={len(only_new)} 天")

    if 'adjust_factor_old' in merged.columns and 'adjust_factor_new' in merged.columns:
        both = merged[merged['_merge'] == 'both'].copy()
        if not both.empty:
            both['factor_diff_pct'] = ((both['adjust_factor_new'] - both['adjust_factor_old']) / both['adjust_factor_old']).abs() * 100
            mask = both['factor_diff_pct'].notna()
            both = both[mask]
            if not both.empty:
                max_diff_row = both.loc[both['factor_diff_pct'].idxmax()]
                max_diff = max_diff_row['factor_diff_pct']
                if max_diff > 0.01:  # 复权因子差 > 0.01%
                    diffs.append(
                        f"复权因子最大差异: {max_diff:.4f}% @ {max_diff_row['trade_date']} "
                        f"(旧={max_diff_row['adjust_factor_old']:.6f}, 新={max_diff_row['adjust_factor_new']:.6f})"
                    )

    return diffs if diffs else None


def main():
    parser = argparse.ArgumentParser(description='对比全量重跑前后的股票数据差异')
    parser.add_argument('--recent-days', type=int, default=30,
                        help='只对比最近 N 天的数据（默认30天，0表示全部）')
    parser.add_argument('--symbols', type=str, help='指定对比的代码，逗号分隔')
    parser.add_argument('--output', type=str, default='data_comparison_report.txt',
                        help='输出报告路径')
    args = parser.parse_args()

    recent_days = args.recent_days if args.recent_days > 0 else None

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    else:
        # 取新旧都有的股票
        old_files = set(p.stem for p in OLD_PRICE_DIR.glob('*.parquet'))
        new_files = set(p.stem for p in NEW_PRICE_DIR.glob('*.parquet'))
        symbols = sorted(old_files & new_files)

    print(f"对比 {len(symbols)} 只股票, 关注最近 {args.recent_days} 天...")

    price_issues = defaultdict(list)
    factor_issues = defaultdict(list)
    price_diff_count = 0
    factor_diff_count = 0

    for i, symbol in enumerate(symbols):
        if (i + 1) % 500 == 0:
            print(f"  进度: {i + 1}/{len(symbols)}")

        old_price = OLD_PRICE_DIR / f"{symbol}.parquet"
        new_price = NEW_PRICE_DIR / f"{symbol}.parquet"

        diffs = compare_price(old_price, new_price, recent_days)
        if diffs:
            price_diff_count += 1
            for d in diffs:
                price_issues[d[:50]].append(symbol)

        old_factor = OLD_FACTOR_DIR / f"{symbol}.parquet"
        new_factor = NEW_FACTOR_DIR / f"{symbol}.parquet"

        diffs = compare_factor(old_factor, new_factor, recent_days)
        if diffs:
            factor_diff_count += 1
            for d in diffs:
                factor_issues[d[:50]].append(symbol)

    # 生成报告
    lines = []
    lines.append("=" * 60)
    lines.append("股票数据对比报告")
    lines.append(f"对比股票数: {len(symbols)}")
    lines.append(f"关注时间窗口: 最近 {args.recent_days} 天" if recent_days else "关注时间窗口: 全部历史")
    lines.append("=" * 60)

    lines.append("")
    lines.append(f"【价格数据异常】涉及 {price_diff_count} 只股票")
    if price_issues:
        for desc, syms in sorted(price_issues.items(), key=lambda x: -len(x[1])):
            lines.append(f"  - {desc}... (共 {len(syms)} 只, 如: {','.join(syms[:3])})")
    else:
        lines.append("  价格数据完全一致，无异常")

    lines.append("")
    lines.append(f"【复权因子异常】涉及 {factor_diff_count} 只股票")
    if factor_issues:
        for desc, syms in sorted(factor_issues.items(), key=lambda x: -len(x[1])):
            lines.append(f"  - {desc}... (共 {len(syms)} 只, 如: {','.join(syms[:3])})")
    else:
        lines.append("  复权因子完全一致，无异常")

    report = "\n".join(lines)
    print("\n" + report)

    output_path = project_root / args.output
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\n报告已保存: {output_path}")


if __name__ == "__main__":
    main()

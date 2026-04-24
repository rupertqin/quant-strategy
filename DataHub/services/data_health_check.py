"""
数据健康度检查模块

检查项：
1. Parquet 污染检测 - 日线 parquet 是否混入了实时数据列（timestamp）
2. 实时数据归档状态 - 历史日期的 realtime parquet 是否已删除（日终同步后应清理）
3. 交易日缺失检测 - 各 symbol 的日线数据是否存在交易日断档
4. 复权因子完整性 - 价格数据转前复权后是否因缺少复权因子产生异常

用法：
    python -m DataHub.services.data_health_check
    python -m DataHub.services.data_health_check --check contamination
    python -m DataHub.services.data_health_check --check missing_dates --limit 100
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

from DataHub.config import (
    RAW_PRICE_DIR,
    RAW_ETF_PRICE_DIR,
    RAW_INDEX_PRICE_DIR,
    RAW_ADJUST_FACTOR_DIR,
    RAW_ETF_ADJUST_FACTOR_DIR,
    INTRADAY_DIR,
)
from Dashboard.utils.adjustment import convert_to_qfq, load_adjust_factor

logger = logging.getLogger(__name__)


class DataHealthChecker:
    """数据健康度检查器"""

    def __init__(self):
        self.issues = []
        self.warnings = []
        self.infos = []

    def _add_issue(self, category: str, message: str, detail: dict = None):
        self.issues.append({"category": category, "message": message, "detail": detail or {}})
        logger.error(f"[{category}] {message}")

    def _add_warning(self, category: str, message: str, detail: dict = None):
        self.warnings.append({"category": category, "message": message, "detail": detail or {}})
        logger.warning(f"[{category}] {message}")

    def _add_info(self, category: str, message: str):
        self.infos.append({"category": category, "message": message})
        logger.info(f"[{category}] {message}")

    def check_daily_price_contamination(self, sample_limit: Optional[int] = None) -> Dict:
        """检查日线 parquet 是否被实时数据列污染"""
        self._add_info("contamination", "开始检查日线 parquet 污染...")
        contaminated = []
        checked = 0
        forbidden_cols = {"timestamp"}

        dirs = [
            ("stock", RAW_PRICE_DIR),
            ("etf", RAW_ETF_PRICE_DIR),
            ("index", RAW_INDEX_PRICE_DIR),
        ]

        for asset_type, price_dir in dirs:
            if not price_dir.exists():
                continue
            files = sorted(price_dir.glob("*.parquet"))
            if sample_limit:
                files = files[:sample_limit]
            for f in files:
                checked += 1
                try:
                    df = pd.read_parquet(f)
                    bad = list(forbidden_cols & set(df.columns))
                    if bad:
                        contaminated.append({
                            "file": str(f),
                            "symbol": f.stem,
                            "asset_type": asset_type,
                            "forbidden_columns": bad,
                            "rows": len(df),
                        })
                        self._add_issue(
                            "contamination",
                            f"{asset_type} {f.stem} 日线 parquet 包含实时数据列: {bad}",
                            {"file": str(f), "columns": bad},
                        )
                except Exception as e:
                    self._add_warning("contamination", f"读取 {f} 失败: {e}")

        self._add_info("contamination", f"日线 parquet 污染检查完成: 检查 {checked} 个文件, 发现 {len(contaminated)} 个污染")
        return {"checked": checked, "contaminated": contaminated}

    def check_intraday_archive_status(self, lookback_days: int = 7) -> Dict:
        """检查 realtime parquet 的归档状态（历史日期文件应已被删除）"""
        self._add_info("archive", "开始检查实时数据归档状态...")
        unarchived = []
        checked = 0
        today_str = datetime.now().strftime("%Y%m%d")

        for asset_type in ["stock", "etf", "index"]:
            intraday_dir = INTRADAY_DIR / asset_type
            if not intraday_dir.exists():
                continue

            files = sorted(intraday_dir.glob("*.parquet"))
            for f in files:
                checked += 1
                date_str = f.stem
                if date_str == today_str:
                    continue

                # 历史日期的 realtime 文件应当已被删除
                unarchived.append({
                    "file": str(f),
                    "asset_type": asset_type,
                    "date": date_str,
                })
                self._add_issue(
                    "archive",
                    f"{asset_type} {date_str} 实时数据未归档(删除): {f.name}",
                    {"file": str(f)},
                )

        self._add_info("archive", f"归档检查完成: 检查 {checked} 个文件, 发现 {len(unarchived)} 个未归档")
        return {"checked": checked, "unarchived": unarchived}

    def _get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日历, 失败时回退到工作日"""
        try:
            import akshare as ak
            df = ak.tool_trading_date()
            df["calendarDate"] = pd.to_datetime(df["calendarDate"])
            mask = (df["calendarDate"] >= start_date) & (df["calendarDate"] <= end_date)
            return df.loc[mask, "calendarDate"].dt.strftime("%Y-%m-%d").tolist()
        except Exception:
            dates = []
            cur = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            while cur <= end:
                if cur.weekday() < 5:
                    dates.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
            return dates

    def check_missing_dates(self, sample_limit: Optional[int] = None, max_gap_days: int = 5) -> Dict:
        """检查各 symbol 是否存在交易日缺失"""
        self._add_info("missing_dates", "开始检查交易日缺失...")
        missing_report = []
        checked = 0

        dirs = [
            ("stock", RAW_PRICE_DIR),
            ("etf", RAW_ETF_PRICE_DIR),
            ("index", RAW_INDEX_PRICE_DIR),
        ]

        for asset_type, price_dir in dirs:
            if not price_dir.exists():
                continue
            files = sorted(price_dir.glob("*.parquet"))
            if sample_limit:
                files = files[:sample_limit]

            for f in files:
                checked += 1
                try:
                    df = pd.read_parquet(f)
                    if df.empty or "trade_date" not in df.columns:
                        continue

                    df["trade_date"] = pd.to_datetime(df["trade_date"])
                    df = df.sort_values("trade_date").drop_duplicates("trade_date")

                    start = df["trade_date"].min().strftime("%Y-%m-%d")
                    end = df["trade_date"].max().strftime("%Y-%m-%d")
                    trading_dates = set(self._get_trading_calendar(start, end))
                    actual_dates = set(df["trade_date"].dt.strftime("%Y-%m-%d").tolist())
                    missing = sorted(trading_dates - actual_dates)

                    if len(missing) > max_gap_days:
                        symbol = f.stem
                        missing_report.append({
                            "symbol": symbol,
                            "asset_type": asset_type,
                            "missing_count": len(missing),
                            "missing_dates": missing[:10],
                            "range": f"{start} ~ {end}",
                        })
                        self._add_issue(
                            "missing_dates",
                            f"{asset_type} {symbol} 缺失 {len(missing)} 个交易日 (>{max_gap_days})",
                            {"symbol": symbol, "missing": missing[:10], "total_missing": len(missing)},
                        )
                except Exception as e:
                    self._add_warning("missing_dates", f"检查 {f} 失败: {e}")

        self._add_info("missing_dates", f"缺失检查完成: 检查 {checked} 个文件, 发现 {len(missing_report)} 个异常")
        return {"checked": checked, "missing": missing_report}

    def check_adjust_factor_integrity(self, sample_limit: Optional[int] = None) -> Dict:
        """检查复权因子完整性"""
        self._add_info("adjust_factor", "开始检查复权因子完整性...")
        bad_factors = []
        checked = 0

        dirs = [
            ("stock", RAW_PRICE_DIR, RAW_ADJUST_FACTOR_DIR),
            ("etf", RAW_ETF_PRICE_DIR, RAW_ETF_ADJUST_FACTOR_DIR),
        ]

        for asset_type, price_dir, factor_dir in dirs:
            if not price_dir.exists():
                continue
            files = sorted(price_dir.glob("*.parquet"))
            if sample_limit:
                files = files[:sample_limit]

            for f in files:
                checked += 1
                symbol = f.stem
                try:
                    df = pd.read_parquet(f)
                    if df.empty or "trade_date" not in df.columns:
                        continue

                    df["trade_date"] = pd.to_datetime(df["trade_date"])
                    factor_df = load_adjust_factor(symbol)

                    if factor_df is None or factor_df.empty:
                        continue

                    merged = pd.merge(df, factor_df, on="trade_date", how="left")
                    missing_factor = merged["adjust_factor"].isna().sum()

                    if missing_factor > 0:
                        bad_factors.append({
                            "symbol": symbol,
                            "asset_type": asset_type,
                            "missing_factor_rows": int(missing_factor),
                            "total_rows": len(df),
                        })
                        self._add_issue(
                            "adjust_factor",
                            f"{asset_type} {symbol} 有 {int(missing_factor)} 行缺少复权因子",
                            {"symbol": symbol, "missing_rows": int(missing_factor)},
                        )
                        continue

                    qfq = convert_to_qfq(df, factor_df=factor_df, symbol=symbol)
                    price_cols = ["open", "high", "low", "close"]
                    nan_after_qfq = qfq[price_cols].isna().sum().sum()
                    if nan_after_qfq > 0:
                        bad_factors.append({
                            "symbol": symbol,
                            "asset_type": asset_type,
                            "nan_after_qfq": int(nan_after_qfq),
                            "total_rows": len(df),
                        })
                        self._add_issue(
                            "adjust_factor",
                            f"{asset_type} {symbol} 前复权后出现 {int(nan_after_qfq)} 个 NaN",
                            {"symbol": symbol, "nan_count": int(nan_after_qfq)},
                        )

                except Exception as e:
                    self._add_warning("adjust_factor", f"检查 {symbol} 复权因子失败: {e}")

        self._add_info("adjust_factor", f"复权因子检查完成: 检查 {checked} 个文件, 发现 {len(bad_factors)} 个异常")
        return {"checked": checked, "bad_factors": bad_factors}

    def run_all_checks(self, sample_limit: Optional[int] = None) -> Dict:
        """执行全部检查"""
        self.issues = []
        self.warnings = []
        self.infos = []

        results = {
            "contamination": self.check_daily_price_contamination(sample_limit),
            "archive": self.check_intraday_archive_status(),
            "missing_dates": self.check_missing_dates(sample_limit),
            "adjust_factor": self.check_adjust_factor_integrity(sample_limit),
        }

        return {
            "summary": {
                "issues": len(self.issues),
                "warnings": len(self.warnings),
                "infos": len(self.infos),
            },
            "issues": self.issues,
            "warnings": self.warnings,
            "infos": self.infos,
            "details": results,
        }

    def print_report(self, result: Dict):
        """打印检查报告"""
        print("\n" + "=" * 70)
        print("数据健康度检查报告")
        print("=" * 70)
        print(f"问题: {result['summary']['issues']} | 警告: {result['summary']['warnings']} | 信息: {result['summary']['infos']}")
        print("-" * 70)

        if result["issues"]:
            print("\n❌ 问题列表:")
            for i, issue in enumerate(result["issues"][:20], 1):
                print(f"  {i}. [{issue['category']}] {issue['message']}")
            if len(result["issues"]) > 20:
                print(f"  ... 还有 {len(result['issues']) - 20} 个问题")
        else:
            print("\n✅ 未发现严重问题")

        if result["warnings"]:
            print(f"\n⚠️  警告列表 (前10条):")
            for i, w in enumerate(result["warnings"][:10], 1):
                print(f"  {i}. [{w['category']}] {w['message']}")
            if len(result["warnings"]) > 10:
                print(f"  ... 还有 {len(result['warnings']) - 10} 个警告")

        print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="数据健康度检查")
    parser.add_argument("--check", type=str, choices=["all", "contamination", "archive", "missing_dates", "adjust_factor"], default="all", help="检查类型")
    parser.add_argument("--limit", type=int, help="抽样检查数量（仅用于测试）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    checker = DataHealthChecker()

    if args.check == "all":
        result = checker.run_all_checks(sample_limit=args.limit)
    elif args.check == "contamination":
        checker.check_daily_price_contamination(sample_limit=args.limit)
        result = {"summary": {"issues": len(checker.issues), "warnings": len(checker.warnings), "infos": len(checker.infos)}, "issues": checker.issues, "warnings": checker.warnings, "infos": checker.infos, "details": {}}
    elif args.check == "archive":
        checker.check_intraday_archive_status()
        result = {"summary": {"issues": len(checker.issues), "warnings": len(checker.warnings), "infos": len(checker.infos)}, "issues": checker.issues, "warnings": checker.warnings, "infos": checker.infos, "details": {}}
    elif args.check == "missing_dates":
        checker.check_missing_dates(sample_limit=args.limit)
        result = {"summary": {"issues": len(checker.issues), "warnings": len(checker.warnings), "infos": len(checker.infos)}, "issues": checker.issues, "warnings": checker.warnings, "infos": checker.infos, "details": {}}
    elif args.check == "adjust_factor":
        checker.check_adjust_factor_integrity(sample_limit=args.limit)
        result = {"summary": {"issues": len(checker.issues), "warnings": len(checker.warnings), "infos": len(checker.infos)}, "issues": checker.issues, "warnings": checker.warnings, "infos": checker.infos, "details": {}}

    checker.print_report(result)

    if result["summary"]["issues"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

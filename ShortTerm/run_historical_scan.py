#!/usr/bin/env python3
"""
批量历史信号扫描
生成指定日期范围的历史交易信号

用法:
    python ShortTerm/run_historical_scan.py --start 20250101 --end 20260422
    python ShortTerm/run_historical_scan.py --start 20250601 --end 20250630 --workers 4
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import json

from ShortTerm.services.stock_signal_scanner import MultiPeriodScanner
from DataHub.config import SHORTTERM_SIGNALS_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scan_single_date(date_str: str) -> dict:
    """扫描单日的信号"""
    try:
        scanner = MultiPeriodScanner()
        
        # 解析日期
        date = datetime.strptime(date_str, '%Y%m%d')
        
        # 扫描信号
        signals = scanner.scan_all(date_str=date_str)
        
        # 保存结果
        output_dir = SHORTTERM_SIGNALS_DIR
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"signal_{date_str}.json"
        
        result = {
            "status": "success",
            "scan_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "date": date_str,
            "total_signals": len(signals),
            "signals": signals
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"[{date_str}] 扫描完成: {len(signals)} 个信号")
        return {"date": date_str, "status": "success", "count": len(signals)}
        
    except Exception as e:
        logger.error(f"[{date_str}] 扫描失败: {e}")
        return {"date": date_str, "status": "failed", "error": str(e)}


def get_trading_dates(start_date: str, end_date: str) -> list:
    """获取交易日列表（简化版，假设每天都是交易日）"""
    dates = []
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    current = start
    while current <= end:
        # 跳过周末
        if current.weekday() < 5:  # 周一到周五
            dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    return dates


def main():
    parser = argparse.ArgumentParser(
        description='批量历史信号扫描',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python ShortTerm/run_historical_scan.py --start 20250101 --end 20250422
  python ShortTerm/run_historical_scan.py --start 20250601 --end 20250630 --workers 4
        """
    )
    
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end', type=str, required=True, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--workers', type=int, default=1, help='并行进程数 (默认1)')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已存在的文件')
    
    args = parser.parse_args()
    
    # 获取交易日列表
    dates = get_trading_dates(args.start, args.end)
    logger.info(f"准备扫描 {len(dates)} 个交易日: {dates[0]} 至 {dates[-1]}")
    
    # 检查已存在的文件
    if args.skip_existing:
        output_dir = SHORTTERM_SIGNALS_DIR
        existing_files = set(f.stem.replace("signal_", "")
                            for f in output_dir.glob("signal_*.json")
                            if "latest" not in f.name)
        dates = [d for d in dates if d not in existing_files]
        logger.info(f"跳过已存在文件，剩余 {len(dates)} 个交易日需扫描")
    
    if not dates:
        logger.info("没有需要扫描的日期")
        return
    
    # 执行扫描
    results = []
    
    if args.workers > 1:
        # 并行扫描
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(scan_single_date, date): date for date in dates}
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
    else:
        # 串行扫描
        for date in dates:
            result = scan_single_date(date)
            results.append(result)
    
    # 统计结果
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count
    total_signals = sum(r.get("count", 0) for r in results if r["status"] == "success")
    
    logger.info("=" * 60)
    logger.info("扫描完成统计")
    logger.info("=" * 60)
    logger.info(f"成功: {success_count} 天")
    logger.info(f"失败: {failed_count} 天")
    logger.info(f"总信号数: {total_signals}")
    
    if failed_count > 0:
        logger.info("\n失败日期:")
        for r in results:
            if r["status"] == "failed":
                logger.info(f"  {r['date']}: {r.get('error', 'Unknown')}")


if __name__ == "__main__":
    main()

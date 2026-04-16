#!/usr/bin/env python3
"""
ShortTerm 策略运行入口 - 今日技术面扫描

涨停板与板块热度分析

直接运行:
  python ShortTerm/run_today_technical.py

其他命令:
  python DataHub/run_build_stock_db.py     # 构建股票数据库
  python ShortTerm/run_signal_scan.py  # 个股信号扫描
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    """运行今日技术面扫描"""
    print("\n" + "="*60)
    print("今日技术面扫描 - 涨停板与板块热度")
    print("="*60)
    
    from ShortTerm.services.scanner import LimitUpScanner
    
    scanner = LimitUpScanner()
    result = scanner.generate_daily_signals()
    
    return result


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ShortTerm 策略运行入口 - 今日技术面扫描
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

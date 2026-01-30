#!/usr/bin/env python3
"""
主入口: 短线事件驱动扫描
运行方式: python run_scanner.py
"""

import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from scanner import LimitUpScanner
from market_regime import MarketRegime


def main():
    # 使用当前目录下的 config.yaml
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

    print("\n" + "=" * 60)
    print("短线雷达 - 每日扫描")
    print("=" * 60)

    # 1. 市场状态检查
    print("\n[1/2] 分析市场状态...")
    regime = MarketRegime()
    status = regime.get_market_status()

    print(f"    市场状态: {status['regime']}")
    print(f"    风险评分: {status['score']}/10")
    if status['reasons']:
        print(f"    风险因素: {', '.join(status['reasons'])}")
    print(f"    建议仓位: {regime.get_position_multiplier():.0%}")
    print(f"    推荐板块: {regime.get_sector_preference()}")

    # 2. 板块热度扫描
    print("\n[2/2] 扫描板块热度...")
    scanner = LimitUpScanner(config_path=config_path)
    signals = scanner.generate_daily_signals()

    print("\n" + "=" * 60)
    print("操作建议")
    print("=" * 60)

    if signals.get('signals'):
        for sig in signals['signals'][:5]:  # 只显示前5个
            emoji = "🔥" if sig['action'] == '关注' else "👀"
            print(f"  {emoji} {sig['sector']}: {sig['action']} (强度:{sig['strength']})")
            print(f"      {sig['reason']}")
    else:
        print("  今日无明确信号")

    print("\n说明:")
    print("  - '关注': 可考虑买入板块内强势股")
    print("  - '观望': 等待更好时机")
    print("  - 注意控制仓位，遵守交易纪律")


if __name__ == "__main__":
    main()

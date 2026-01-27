#!/usr/bin/env python3
"""
主入口: 长线组合优化
运行方式: python run_optimization.py
"""

import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from optimizer import PortfolioOptimizer
from report import PortfolioReport


def main():
    optimizer = PortfolioOptimizer()
    weights, metrics = optimizer.run()

    # 生成报告
    report = PortfolioReport()
    report.run(metrics)

    print("\n下一步操作:")
    print("1. 手动检查 output_weights.csv")
    print("2. 根据推荐调整持仓")
    print("3. 运行 Dashboard 看板查看结果")


if __name__ == "__main__":
    main()

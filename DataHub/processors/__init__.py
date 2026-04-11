"""
DataHub Processors - 数据处理层

提供数据计算和衍生功能：
- 指数计算
- 技术指标
- 基本面分析
"""

# IndexCalculator 在 core 模块
from DataHub.core.index_calculator import IndexCalculator, get_index_summary

__all__ = [
    'IndexCalculator',
    'get_index_summary',
]

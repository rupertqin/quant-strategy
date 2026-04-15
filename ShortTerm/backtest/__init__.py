"""
回测验证框架

验证评分系统有效性的工具集
"""

from .scoring_validator import ScoringValidator, BacktestResult
from .multi_day_analyzer import MultiDayAnalyzer

__all__ = ['ScoringValidator', 'BacktestResult', 'MultiDayAnalyzer']

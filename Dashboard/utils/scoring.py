"""
评分计算工具 - 统一信号评分计算逻辑
"""

from typing import List, Dict, Any


def calculate_stock_score(signals: List[Dict[str, Any]]) -> int:
    """
    计算股票的综合评分

    评分规则：
    1. 优先使用后端计算的 portfolio_score（如果存在）
    2. 否则使用标准计算方式：
       - 基础分 = 单个信号最高分
       - 多信号加成 = (信号数 - 1) * 10
       - 多周期共振 = 不同周期额外 +15
       - 上限 100 分

    Args:
        signals: 信号列表

    Returns:
        综合评分 (0-100)
    """
    if not signals:
        return 0

    # 优先使用后端计算的组合评分 portfolio_score
    portfolio_scores = []
    for s in signals:
        # 检查多种可能的 portfolio_score 位置
        score = (
            s.get('portfolio_score') or
            s.get('technicals', {}).get('portfolio_score')
        )
        if score:
            portfolio_scores.append(score)

    if portfolio_scores:
        return round(max(portfolio_scores))

    # 标准计算方式
    periods = set(s.get('period', 'daily') for s in signals)
    base_score = max([s.get('score', 0) for s in signals])
    extra_score = (len(signals) - 1) * 10
    if len(periods) > 1:
        extra_score += 15  # 多周期共振加分

    return min(base_score + extra_score, 100)


def get_score_class(score: int) -> str:
    """根据评分获取样式类名"""
    if score >= 80:
        return "score-high"
    elif score >= 60:
        return "score-medium"
    else:
        return "score-low"


def get_score_color(score: int) -> str:
    """根据评分获取颜色"""
    if score >= 80:
        return "#ff6b6b"  # 红色
    elif score >= 60:
        return "#feca57"  # 黄色
    else:
        return "#dfe6e9"  # 灰色

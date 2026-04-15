"""
评分计算工具 - 统一信号评分计算逻辑

改进版评分系统：增强分离度，突出高确定性机会
"""

from typing import List, Dict, Any, Set


def calculate_stock_score(signals: List[Dict[str, Any]], 
                          change_pct: float = None,
                          recent_changes: List[float] = None) -> int:
    """
    计算股票的综合评分（改进版，带风险过滤）

    评分维度：
    1. 基础质量分（0-60）- 最强信号的质量
    2. 信号协同分（0-20）- 多信号协同效应
    3. 周期共振分（0-10）- 多周期确认
    4. 一致性奖励（0-5）- 方向一致+指标多样

    关键规则：
    - 必须有≥1个 strong 信号才能突破80分
    - 左右侧信号混合会扣减协同分
    - 同类型指标重复会降权
    - 95+分极其稀缺（需完美组合）
    
    风险过滤（新增）：
    - 当日跌停 (-10%)：最高50分
    - 连续2日大跌 (>7%)：最高60分
    - 信号日期异常：扣20分

    Args:
        signals: 信号列表
        change_pct: 当日涨跌幅
        recent_changes: 近期涨跌幅列表（如最近5日）

    Returns:
        综合评分 (0-100)
    """
    if not signals:
        return 0

    # 优先使用后端计算的组合评分 portfolio_score
    portfolio_scores = []
    for s in signals:
        score = (
            s.get('portfolio_score') or
            s.get('technicals', {}).get('portfolio_score')
        )
        if score:
            portfolio_scores.append(score)

    if portfolio_scores:
        return round(max(portfolio_scores))

    # === 风险过滤检查 ===
    risk_penalty = 0
    
    # 1. 当日大跌检查
    if change_pct is not None:
        if change_pct <= -9.9:  # 跌停
            risk_penalty += 50  # 严重惩罚
        elif change_pct <= -7:
            risk_penalty += 30
        elif change_pct <= -5:
            risk_penalty += 15
    
    # 2. 连续大跌检查
    if recent_changes:
        # 检查最近2日是否都大跌
        if len(recent_changes) >= 2:
            if recent_changes[-1] < -5 and recent_changes[-2] < -5:
                risk_penalty += 20
        # 检查最近5日累计跌幅
        if sum(recent_changes[-5:]) < -15:
            risk_penalty += 15
    
    # 3. 信号日期异常检查
    from datetime import datetime
    today = datetime.now().date()
    for s in signals:
        date_str = s.get('trigger_date', '')
        if date_str:
            try:
                signal_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                # 未来日期异常
                if signal_date > today:
                    risk_penalty += 30
                    break
            except:
                # 日期格式异常
                risk_penalty += 20
                break
    
    # === 新评分系统 ===
    
    # 1. 信号分类统计
    strong_signals = [s for s in signals if s.get('strength') == 'strong']
    medium_signals = [s for s in signals if s.get('strength') == 'medium']
    
    # 获取各类属性
    periods: Set[str] = set(s.get('period', 'daily') for s in signals)
    types: Set[str] = set(s.get('signal_type', 'left') for s in signals)
    indicators: Set[str] = set(s.get('signal_name', '') for s in signals)
    
    # 2. 基础质量分 (0-60)
    if strong_signals:
        best_strong = max(strong_signals, key=lambda x: x.get('score', 0))
        base_score = min(best_strong.get('score', 0) * 0.6, 60)
    elif medium_signals:
        best_medium = max(medium_signals, key=lambda x: x.get('score', 0))
        base_score = min(best_medium.get('score', 0) * 0.5, 45)
    else:
        base_score = max([s.get('score', 0) for s in signals]) * 0.4
    
    # 3. 信号协同分 (0-20)
    synergy_score = 0
    effective_count = (
        len(strong_signals) * 1.0 + 
        len(medium_signals) * 0.6 +
        (len(signals) - len(strong_signals) - len(medium_signals)) * 0.3
    )
    
    if effective_count >= 1:
        synergy_score = min((effective_count - 1) * 5, 15)
    
    if len(types) > 1:
        synergy_score *= 0.6
    
    # 4. 周期共振分 (0-10)
    period_score = 0
    if len(periods) >= 3:
        period_score = 10
    elif len(periods) == 2:
        period_score = 6
    elif len(periods) == 1 and 'daily' in periods:
        period_score = 2
    
    # 5. 一致性奖励 (0-5)
    consistency_bonus = 0
    if len(types) == 1 and len(indicators) >= 2 and len(strong_signals) >= 1:
        consistency_bonus = 3
        if len(periods) >= 2:
            consistency_bonus = 5
    
    # 6. 计算总分
    total = base_score + synergy_score + period_score + consistency_bonus
    
    # 7. 质量门槛检查
    if not strong_signals and total > 80:
        total = 80
    if effective_count < 2 and total > 85:
        total = 85
    if len(types) > 1 and total > 75:
        total = 75
    if len(periods) < 2 and total > 90:
        total = 90
    
    # 8. 应用风险惩罚
    total -= risk_penalty
    
    # 硬性天花板
    if change_pct is not None and change_pct <= -9.9:
        total = min(total, 50)  # 跌停最高50分
    if recent_changes and len(recent_changes) >= 2:
        if recent_changes[-1] < -5 and recent_changes[-2] < -5:
            total = min(total, 60)  # 连续大跌最高60分
    
    return max(round(min(total, 100)), 0)


def get_score_class(score: int) -> str:
    """根据评分获取样式类名（5档分离）"""
    if score >= 95:
        return "score-exceptional"  # 极品（95-100）
    elif score >= 85:
        return "score-high"          # 优秀（85-94）
    elif score >= 75:
        return "score-good"          # 良好（75-84）
    elif score >= 60:
        return "score-medium"        # 一般（60-74）
    else:
        return "score-low"           # 弱（<60）


def get_score_color(score: int) -> str:
    """根据评分获取颜色（5档分离）"""
    if score >= 95:
        return "#9b59b6"   # 紫色 - 极品
    elif score >= 85:
        return "#ff6b6b"   # 红色 - 优秀
    elif score >= 75:
        return "#f39c12"   # 橙色 - 良好
    elif score >= 60:
        return "#feca57"   # 黄色 - 一般
    else:
        return "#95a5a6"   # 灰色 - 弱


def get_score_label(score: int) -> str:
    """获取评分标签文字"""
    if score >= 95:
        return "极品"
    elif score >= 85:
        return "优秀"
    elif score >= 75:
        return "良好"
    elif score >= 60:
        return "一般"
    else:
        return "观察"

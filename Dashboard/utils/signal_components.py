"""
信号展示组件模块 - 统一信号和风险展示UI

供以下页面复用:
- 4_signal_watch.py (信号监控页面)
- stock_chart.py (个股详情页面)
- 2_pool_watch.py (股票池页面)
"""

import sys
from pathlib import Path
import streamlit as st
from typing import List, Dict, Tuple, Optional

# 统一风险评分核心模块（供 Scanner 与 Dashboard 共用）
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
from DataHub.core.risk_scorer import calculate_risk_score


def get_risk_color_emoji(risk_score: int) -> str:
    """根据风险分返回颜色表情"""
    if risk_score < 40:
        return "🟢"
    elif risk_score < 70:
        return "🟡"
    else:
        return "🔴"


def get_risk_color_css(risk_score: int) -> str:
    """根据风险分返回CSS颜色值"""
    if risk_score < 40:
        return "#27ae60"
    elif risk_score < 70:
        return "#f39c12"
    else:
        return "#e74c3c"


def render_signal_list(signals: List[dict], show_header: bool = True) -> None:
    """
    渲染信号列表（信号详情）
    
    Args:
        signals: 信号列表
        show_header: 是否显示标题
    """
    if show_header:
        st.markdown("<div style='font-size: 13px; font-weight: 600; color: #333; margin-bottom: 8px;'>📈 信号详情</div>", unsafe_allow_html=True)

    if not signals:
        st.markdown("<div style='font-size: 12px; color: #888;'>无买入信号</div>", unsafe_allow_html=True)
        return

    for sig in signals:
        sig_type = sig.get('signal_type', 'left')
        period = sig.get('period', 'daily')
        sig_name = sig.get('signal_name', '')
        sig_score = sig.get('score', 0)
        type_emoji = "📈" if sig_type == 'right' else "📉"
        type_text = "右侧" if sig_type == 'right' else "左侧"
        period_name = {'daily': '日线', 'weekly': '周线', 'monthly': '月线'}.get(period, period)

        # 分数颜色
        if sig_score >= 70:
            score_color = "#27ae60"
        elif sig_score >= 50:
            score_color = "#f39c12"
        else:
            score_color = "#e74c3c"

        st.markdown(f"""
        <div style="font-size: 12px; padding: 3px 0;">
            {type_emoji} [{type_text}] {sig_name} ({period_name})
            <span style="color: {score_color}; font-weight: 600;">{sig_score}分</span>
        </div>
        """, unsafe_allow_html=True)


def render_risk_assessment(risk_score: int, risk_explanations: List[str], show_header: bool = True) -> None:
    """
    渲染风险评估区域
    
    Args:
        risk_score: 风险分数
        risk_explanations: 风险解释列表
        show_header: 是否显示标题
    """
    if show_header:
        st.markdown("<div style='font-size: 13px; font-weight: 600; color: #333; margin-bottom: 8px;'>⚠️ 风险评估</div>", unsafe_allow_html=True)

    if risk_explanations:
        for exp in risk_explanations[:5]:  # 最多显示5条
            st.markdown(f"<div style='font-size: 11px; color: #666; padding: 2px 0;'>{exp}</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='font-size: 12px; color: #888;'>暂无风险评估</div>", unsafe_allow_html=True)


def render_signal_and_risk(
    signals: List[dict],
    signal_score: int,
    risk_score: int,
    risk_explanations: List[str],
    layout: str = "vertical"
) -> None:
    """
    统一渲染信号详情和风险评估（两列布局）
    
    Args:
        signals: 信号列表
        signal_score: 信号分数
        risk_score: 风险分数
        risk_explanations: 风险解释列表
        layout: 布局方式，"vertical"(垂直) 或 "horizontal"(水平两列)
    """
    if layout == "horizontal":
        cols = st.columns(2)
        with cols[0]:
            render_signal_list(signals)
        with cols[1]:
            render_risk_assessment(risk_score, risk_explanations)
    else:
        # 垂直布局 - 信号在上，风险在下
        render_signal_list(signals)
        st.markdown("---")
        render_risk_assessment(risk_score, risk_explanations)


def render_expander_header(
    signal_count: int,
    signal_score: int,
    signal_label: str,
    risk_score: int
) -> str:
    """
    生成折叠区域标题（包含信号分和风险分）
    
    Args:
        signal_count: 信号数量
        signal_score: 信号分数
        signal_label: 信号评级标签
        risk_score: 风险分数
        
    Returns:
        折叠区域标题字符串
    """
    risk_emoji = get_risk_color_emoji(risk_score)
    return f"📡 当前信号 ({signal_count}个) 　　信号分:{signal_score}·{signal_label} 　　风险分:{risk_score}{risk_emoji}"


def calculate_stock_metrics(signals: List[dict], change_pct: float = None) -> Tuple[int, int, List[str]]:
    """
    计算股票的完整指标（信号分、风险分、风险解释）

    Args:
        signals: 该股票的所有信号
        change_pct: 当日涨跌幅，None 则从信号中获取

    Returns:
        (信号分数, 风险分数, 风险解释列表)
    """
    from utils.scoring import calculate_stock_score

    # 计算信号分
    if change_pct is None:
        change_pct = signals[0].get("change_pct", 0) if signals else 0
    # 兼容旧数据：异常大的值自动修正
    if abs(change_pct) > 100:
        change_pct = change_pct / 100
    signal_score = calculate_stock_score(signals, change_pct)

    # 计算风险分
    best_signal = max(signals, key=lambda x: x.get("score", 0)) if signals else {}
    tech = best_signal.get("technicals", {})
    risk_score, risk_explanations = calculate_risk_score(tech, signals)

    return signal_score, risk_score, risk_explanations

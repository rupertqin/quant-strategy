"""
信号展示组件模块 - 统一信号和风险展示UI

供以下页面复用:
- 4_signal_watch.py (信号监控页面)
- stock_chart.py (个股详情页面)
- 2_pool_watch.py (股票池页面)
"""

import streamlit as st
from typing import List, Dict, Tuple, Optional


def detect_risk_signals(signals: List[dict]) -> List[str]:
    """
    从信号列表中检测风险信号（顶背离、趋势走坏等）
    
    Args:
        signals: 信号列表
        
    Returns:
        风险描述列表
    """
    risks = []

    for sig in signals:
        sig_name = sig.get("signal_name", "")
        sig_type = sig.get("signal_type", "left")

        # 顶背离风险
        if "顶背离" in sig_name or ("背离" in sig_name and sig_type == "right"):
            risks.append(f"🔴 {sig_name} - 价格新高但指标未新高，见顶信号")

        # 量价背离（高位缩量上涨）
        if sig_name == "量价背离":
            desc = sig.get("description", "")
            if "高位" in desc or "顶背离" in desc:
                risks.append(f"🔴 量价顶背离 - 价格高位+量能萎缩，上涨乏力")

        # 趋势走坏确认
        if sig_type == "right" and sig_name in ["MA5死叉MA10", "MA5死叉MA20", "MACD死叉", "KDJ死叉"]:
            risks.append(f"⚠️ {sig_name} - 趋势转弱信号")

    return risks


def calculate_risk_score(technicals: dict, signals: List[dict]) -> Tuple[int, List[str]]:
    """
    基于均线数据和信号计算风险分 (0-100，越高越危险) 和详细解释
    
    Args:
        technicals: 技术指标数据
        signals: 信号列表
        
    Returns:
        (风险分数, 风险解释列表)
    """
    explanations = []
    risk = 50  # 基准

    # 1. 从信号中检测风险信号
    signal_risks = detect_risk_signals(signals)
    for sr in signal_risks:
        risk += 15
        explanations.append(sr)

    # 2. 技术面分析
    if not technicals:
        return min(100, risk + 10), explanations + ["⚠️ 无技术指标数据"]

    ma5 = technicals.get("ma5", 0)
    ma10 = technicals.get("ma10", 0)
    ma20 = technicals.get("ma20", 0)
    ma60 = technicals.get("ma60", 0)
    close = technicals.get("close", 0) or ma5

    # 均线排列
    if ma5 and ma10 and ma20:
        if ma5 > ma10 > ma20:  # 多头排列
            risk -= 10
            explanations.append("✅ 多头排列 - 趋势健康")
        elif ma5 < ma10 < ma20:  # 空头排列
            risk += 15
            explanations.append("🔴 空头排列 - 趋势走坏")
        elif ma5 < ma10:  # 短期走弱
            risk += 8
            explanations.append("⚠️ 短期均线下穿 - 短期偏弱")

    # MA60位置
    if ma60 and close:
        pct_from_ma60 = (close - ma60) / ma60 * 100
        if close > ma60:
            risk -= 8
            explanations.append(f"✅ 站上MA60 (+{pct_from_ma60:.1f}%)")
        else:
            risk += 12
            explanations.append(f"🔴 跌破MA60 ({pct_from_ma60:.1f}%)")

    # MACD状态
    macd_dif = technicals.get("macd_dif", 0)
    macd_dea = technicals.get("macd_dea", 0)
    if macd_dif and macd_dea:
        if macd_dif > macd_dea:
            risk -= 8
            explanations.append("✅ MACD金叉")
        else:
            risk += 10
            explanations.append("⚠️ MACD死叉/空头")

    # KDJ超买/超卖
    kdj_j = technicals.get("kdj_j", 0)
    if kdj_j:
        if kdj_j > 90:
            risk += 12
            explanations.append(f"🔴 KDJ超买 (J={kdj_j:.1f})")
        elif kdj_j < 10:
            risk -= 8
            explanations.append(f"✅ KDJ超卖 (J={kdj_j:.1f})")

    # 限制在 0-100
    final_risk = max(0, min(100, int(risk)))

    # 如果没有具体风险项，添加总体评价
    if not explanations:
        if final_risk <= 30:
            explanations.append("✅ 趋势健康")
        elif final_risk <= 60:
            explanations.append("⚠️ 趋势中性，注意风险")
        else:
            explanations.append("🔴 趋势走坏，建议回避")

    return final_risk, explanations


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


def calculate_stock_metrics(signals: List[dict]) -> Tuple[int, int, List[str]]:
    """
    计算股票的完整指标（信号分、风险分、风险解释）
    
    Args:
        signals: 该股票的所有信号
        
    Returns:
        (信号分数, 风险分数, 风险解释列表)
    """
    from utils.scoring import calculate_stock_score

    # 计算信号分
    change_pct = signals[0].get("change_pct", 0) if signals else 0
    signal_score = calculate_stock_score(signals, change_pct)

    # 计算风险分
    best_signal = max(signals, key=lambda x: x.get("score", 0)) if signals else {}
    tech = best_signal.get("technicals", {})
    risk_score, risk_explanations = calculate_risk_score(tech, signals)

    return signal_score, risk_score, risk_explanations

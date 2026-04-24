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


def get_change_pct_display(change_pct: float):
    """统一返回涨跌幅的颜色和格式化字符串（兼容旧数据大值）"""
    if abs(change_pct) > 100:
        change_pct = change_pct / 100
    if change_pct > 0:
        return "#ff4757", f"+{change_pct:.2f}%"
    elif change_pct < 0:
        return "#2ed573", f"{change_pct:.2f}%"
    else:
        return "#888", "0.00%"


def get_score_style(score: int):
    """统一返回信号分的背景色和文字色"""
    if score >= 70:
        return "#27ae60", "white"
    elif score >= 50:
        return "#f39c12", "white"
    else:
        return "#e74c3c", "white"


def get_risk_style(risk_score: int):
    """统一返回风险分的背景色、文字色和标签"""
    if risk_score < 40:
        return "#27ae60", "white", "低风险"
    elif risk_score < 70:
        return "#f39c12", "white", "中风险"
    else:
        return "#e74c3c", "white", "高风险"


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

    # 提取公共信息（同一股票的多条信号通常日期/价格相同）
    common_date = signals[0].get('trigger_date', '')
    common_price = signals[0].get('close_price', 0)
    common_pct = signals[0].get('change_pct', 0)
    # 兼容旧数据：异常大的值自动修正
    if abs(common_pct) > 100:
        common_pct = common_pct / 100

    all_same = all(
        s.get('trigger_date') == common_date and
        abs(s.get('close_price', 0) - common_price) < 0.01 and
        abs(s.get('change_pct', 0) - common_pct) < 0.01
        for s in signals
    )

    # 公共的日期/价格/涨跌幅只在顶部显示一次
    if all_same and signals:
        if common_pct > 0:
            pct_color = "#ff4757"
            pct_str = f"+{common_pct:.2f}%"
        elif common_pct < 0:
            pct_color = "#2ed573"
            pct_str = f"{common_pct:.2f}%"
        else:
            pct_color = "#888"
            pct_str = "0.00%"
        st.markdown(f"""
        <div style="font-size: 11px; color: #666; margin-bottom: 6px; padding-bottom: 4px; border-bottom: 1px solid #eee;">
            📅 {common_date}　💰 ¥{common_price:.2f}　<span style="color: {pct_color};">{pct_str}</span>
        </div>
        """, unsafe_allow_html=True)

    for sig in signals:
        sig_type = sig.get('signal_type', 'left')
        period = sig.get('period', 'daily')
        sig_name = sig.get('signal_name', '')
        sig_score = sig.get('score', 0)
        description = sig.get('description', '')
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
        <div style="font-size: 12px; padding: 4px 0; border-bottom: 1px solid #f0f0f0;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>{type_emoji} [{type_text}] {sig_name} ({period_name})</span>
                <span style="color: {score_color}; font-weight: 600;">{sig_score}分</span>
            </div>
            <div style="font-size: 11px; color: #555; margin-top: 2px; line-height: 1.4;">
                {description}
            </div>
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


def render_stock_signal_expander(
    signals: List[dict],
    signal_score: int,
    risk_score: int,
    risk_explanations: List[str],
    score_label: str = "",
    expanded: bool = False
) -> None:
    """
    渲染股票的信号+风险折叠区域（通用组件）

    供 signal_watch、pool_watch、stock_chart 共用，避免三处重复内联代码。

    Args:
        signals: 信号列表
        signal_score: 信号分数
        risk_score: 风险分数
        risk_explanations: 风险解释列表
        score_label: 信号评级标签，为空时自动计算
        expanded: 是否默认展开
    """
    from Dashboard.utils.scoring import get_score_label

    if not score_label:
        score_label = get_score_label(signal_score)

    expander_title = render_expander_header(
        len(signals), signal_score, score_label, risk_score
    )

    with st.expander(expander_title, expanded=expanded):
        sig_col, risk_col = st.columns(2)
        with sig_col:
            if signals:
                render_signal_list(signals, show_header=False)
            else:
                st.markdown("""
                <div style="padding: 20px; background: #fdf2f2; border-radius: 8px; border-left: 4px solid #e74c3c;">
                    <div style="font-size: 16px; font-weight: 600; color: #e74c3c; margin-bottom: 8px;">⚠️ 风险预警</div>
                    <div style="font-size: 13px; color: #666;">该股票当前无买入信号，但检测到风险信号，建议关注。</div>
                </div>
                """, unsafe_allow_html=True)
        with risk_col:
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

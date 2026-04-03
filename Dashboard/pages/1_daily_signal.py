"""
今日异动页面 - 读取 Scanner 生成的 JSON 数据
对应 ShortTerm/daily_signal 模块
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime

# 添加项目路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

# JSON 文件路径
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal")
JSON_FILE = os.path.join(OUTPUT_DIR, "daily_signals.json")


def load_signals_data() -> dict:
    """加载 Scanner 生成的 JSON 数据"""
    try:
        if os.path.exists(JSON_FILE):
            with open(JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 添加文件修改时间
                mtime = os.path.getmtime(JSON_FILE)
                data['_generated_at'] = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                return data
    except Exception as e:
        st.error(f"加载数据文件失败: {e}")
    return {}


st.set_page_config(
    page_title="今日异动 - Quant Dashboard",
    page_icon="🔥",
    layout="wide"
)

# ============= 样式 =============
st.markdown("""
<style>
    .hot-sector-card {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        color: white;
        padding: 20px;
        border-radius: 12px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .signal-card {
        background-color: #f8f9fa;
        border-left: 4px solid #28a745;
        padding: 15px;
        border-radius: 8px;
        margin: 8px 0;
    }
    .metric-box {
        background-color: #e3f2fd;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# ============= 加载数据 =============
data = load_signals_data()

if not data:
    st.error("❌ 无法加载数据文件，请确保已运行 `python ShortTerm/run_scanner.py daily`")
    st.stop()

# 提取数据
date = data.get('date', '未知')
generated_at = data.get('_generated_at', '未知')
total_zt = data.get('total_zt_count', 0)
market_type = data.get('market_type', '未知')
signals = data.get('signals', [])
hot_sectors = data.get('hot_sectors', [])
tech_indicators = data.get('technical_indicators', {})
market_breadth = tech_indicators.get('market_breadth', {})
index_performance = tech_indicators.get('index_performance', {})

# ============= 页面标题 =============
st.title("🔥 今日异动")
st.caption(f"涨停板扫描 | 板块热度分析 | 市场状态监控 | 数据生成时间: {generated_at}")

# ============= 市场状态卡片 =============
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    regime_color = {'AGGRESSIVE': '🟢', 'DEFENSIVE': '🔴', 'NEUTRAL': '🟡', 'UNKNOWN': '⚪'}
    regime_name = {'AGGRESSIVE': '积极进攻', 'DEFENSIVE': '防御避险', 'NEUTRAL': '震荡中性', 'UNKNOWN': '未知'}
    regime_type = data.get('regime', 'NEUTRAL')
    st.metric(
        label="市场状态",
        value=f"{regime_color.get(regime_type, '⚪')} {regime_name.get(regime_type, '未知')}"
    )

with col2:
    composite_score = data.get('composite_score', 50)
    total_score = int(composite_score / 10)
    st.metric(
        label="综合评分",
        value=f"{total_score}/10",
        delta_color="inverse" if total_score < 5 else "normal"
    )

with col3:
    zt_count = data.get('zt_count', 0)
    dt_count = data.get('dt_count', 0)
    if zt_count > dt_count * 5:
        sentiment = "🔥极热"
    elif zt_count > dt_count * 2:
        sentiment = "🟢活跃"
    elif zt_count > dt_count:
        sentiment = "🟡正常"
    else:
        sentiment = "🔴谨慎"
    st.metric("涨停:跌停", f"{zt_count}:{dt_count}", sentiment)

with col4:
    hot_sector_count = len(hot_sectors) if hot_sectors else 0
    st.metric("热点板块", hot_sector_count)

with col5:
    st.metric("涨停总数", total_zt, market_type)

# ============= 技术面指标展示 =============
st.markdown("### 📊 技术面分析")

# 第一行：综合评分和主要指标
tcol1, tcol2, tcol3, tcol4 = st.columns(4)

with tcol1:
    score = data.get('composite_score', 50)
    outlook = '积极' if score >= 70 else '中性' if score >= 50 else '谨慎'
    color = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
    st.metric("技术面评分", f"{score}/100", f"{color} {outlook}")

with tcol2:
    up = market_breadth.get('up_count', 0)
    down = market_breadth.get('down_count', 0)
    ratio = market_breadth.get('up_ratio', 0.5)
    if up > down:
        delta_color = "normal"
        arrow = "▲"
    elif up < down:
        delta_color = "inverse"
        arrow = "▼"
    else:
        delta_color = "off"
        arrow = "▶"
    st.metric("涨跌家数", f"{up}:{down}", f"{arrow} 上涨{ratio:.1%}", delta_color=delta_color)

with tcol3:
    interpretation = market_breadth.get('interpretation', '未知')
    st.metric("市场情绪", interpretation)

with tcol4:
    if zt_count > 0 and dt_count > 0:
        zt_dt_ratio = zt_count / dt_count
        if zt_dt_ratio >= 5:
            delta_color = "normal"
            dt_delta = "🔥 极热"
        elif zt_dt_ratio >= 2:
            delta_color = "normal"
            dt_delta = "🟢 活跃"
        elif zt_dt_ratio >= 1:
            delta_color = "off"
            dt_delta = "🟡 平衡"
        else:
            delta_color = "inverse"
            dt_delta = "🔴 恐慌"
        st.metric("涨跌停比", f"{zt_count}:{dt_count}", dt_delta, delta_color=delta_color)
    else:
        st.metric("涨跌停比", f"{zt_count}:{dt_count}", "🟡 平衡")

# 第二行：指数表现
st.markdown("**主要指数表现**")
if index_performance:
    idx_cols = st.columns(len(index_performance))
    for idx_col, (name, idx_data) in zip(idx_cols, index_performance.items()):
        with idx_col:
            change = idx_data.get('change_pct', 0)
            trend = idx_data.get('trend', 'NEUTRAL')
            if change > 0:
                color_html = '<span style="color:#00C853;font-weight:bold">▲ 上涨</span>'
                delta_color = "normal"
            elif change < 0:
                color_html = '<span style="color:#FF1744;font-weight:bold">▼ 下跌</span>'
                delta_color = "inverse"
            else:
                color_html = '<span style="color:#9E9E9E;font-weight:bold">▶ 平盘</span>'
                delta_color = "off"

            trend_icon = "📈" if trend == "UP" else "📉" if trend == "DOWN" else "➡️"
            st.markdown(f"**{name}** {color_html}", unsafe_allow_html=True)
            st.metric(label=f"趋势: {trend_icon}", value=f"{change:+.2f}%", delta_color=delta_color)
else:
    st.info("指数数据暂无")

st.divider()

# ============= 主要内容 =============
st.subheader("📅 扫描结果")

col_left, col_right = st.columns([1, 1])

# ========== 左侧: 热点板块 ==========
with col_left:
    st.markdown("### 🔥 热点板块")

    if hot_sectors:
        for sector in hot_sectors[:10]:
            with st.container():
                st.markdown(f"""
                <div class="hot-sector-card">
                    <h4>{sector.get('sector', '未知')}</h4>
                    <p>涨停 {sector.get('zt_count', 0)} 家 | 龙头: {sector.get('lead_stock', '-')}</p>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无热点板块数据")

# ========== 右侧: 操作建议 & 技术分析 ==========
with col_right:
    st.markdown("### 📊 操作建议")

    regime_type = data.get('regime', 'NEUTRAL')
    if regime_type == 'AGGRESSIVE':
        action = "积极进攻"
        emoji = "🔥"
        color = "#ff6b6b"
        desc = "市场强势，可积极参与"
    elif regime_type == 'DEFENSIVE':
        action = "防御避险"
        emoji = "🛡️"
        color = "#48dbfb"
        desc = "市场弱势，注意风险"
    else:
        action = "观望等待"
        emoji = "⏳"
        color = "#feca57"
        desc = "市场震荡，谨慎操作"

    st.markdown(f"""
    <div class="signal-card">
        {emoji} <strong>当前策略</strong>
        <span style="color: {color}; font-weight: bold;">{action}</span>
        <br>
        <small>{desc}</small>
    </div>
    """, unsafe_allow_html=True)

    # ============= 道氏理论与波浪理论分析 =============
    st.markdown("### 📈 技术分析")

    # 获取主要指数的技术分析数据
    main_index = None
    main_index_name = None
    for name in ['沪深300', '上证指数']:
        if name in index_performance:
            idx_data = index_performance[name]
            if 'dow_theory' in idx_data:
                main_index = idx_data
                main_index_name = name
                break

    if main_index and 'dow_theory' in main_index:
        dow = main_index['dow_theory']
        elliott = main_index.get('elliott_wave', {})

        # 道氏理论展示
        with st.expander("📊 道氏理论分析", expanded=True):
            primary = dow.get('primary_desc', '未知')
            secondary = dow.get('secondary_desc', '未知')
            volume = dow.get('volume_signal', 'neutral')

            trend_color = {'BULL': '🟢', 'BEAR': '🔴', 'SIDEWAYS': '🟡'}
            primary_trend = dow.get('primary_trend', 'UNKNOWN')
            st.markdown(f"**主要趋势**: {trend_color.get(primary_trend, '⚪')} {primary}")
            st.markdown(f"**次要趋势**: {secondary}")

            strength = dow.get('trend_strength', {})
            adx = strength.get('adx', 0)
            strength_text = strength.get('strength', 'weak')
            st.progress(min(adx/100, 1.0), text=f"趋势强度 ADX: {adx} ({strength_text})")

            vol_emoji = {'confirming': '✅', 'warning': '⚠️', 'neutral': '➖'}
            vol_text = {'confirming': '确认趋势', 'warning': '背离警示', 'neutral': '中性'}
            st.caption(f"成交量信号: {vol_emoji.get(volume, '➖')} {vol_text.get(volume, '中性')}")

        # 波浪理论展示
        with st.expander("🌊 波浪理论分析"):
            if elliott and 'current_phase' in elliott:
                phase = elliott.get('current_phase', '未知')
                st.markdown(f"**当前阶段**: {phase}")

                structure = elliott.get('structure', {})
                if structure:
                    volatility = structure.get('volatility_pct', 0)
                    st.caption(f"近期波动率: {volatility:.2f}%")

                    fib_382 = structure.get('fib_382')
                    fib_500 = structure.get('fib_500')
                    fib_618 = structure.get('fib_618')

                    if fib_382 and fib_500 and fib_618:
                        st.markdown("**斐波那契回调位**:")
                        fib_col1, fib_col2, fib_col3 = st.columns(3)
                        with fib_col1:
                            st.metric("38.2%", f"{fib_382:.0f}")
                        with fib_col2:
                            st.metric("50.0%", f"{fib_500:.0f}")
                        with fib_col3:
                            st.metric("61.8%", f"{fib_618:.0f}")
            else:
                st.caption("波浪分析数据暂不可用")

        # 跨指数验证
        inter_validation = tech_indicators.get('inter_index_validation', {})
        if inter_validation:
            val_status = inter_validation.get('validation', '')
            consistency = inter_validation.get('consistency', 0)
            note = inter_validation.get('note', '')

            val_emoji = {'CONFIRMED': '✅', 'PARTIAL': '⚠️', 'DIVERGENCE': '❌'}
            st.markdown(f"**指数验证**: {val_emoji.get(val_status, '➖')} {note}")
            st.progress(consistency, text=f"一致性: {consistency*100:.0f}%")
    else:
        st.caption("技术分析数据加载中...")

    # 生成时间
    st.caption(f"数据日期: {date}")

st.divider()

# ============= 信号列表 =============
st.markdown("### 📋 涨停信号列表")

if signals:
    df_signals = pd.DataFrame(signals)
    st.dataframe(df_signals, use_container_width=True)
else:
    st.info("暂无涨停信号")

# ============= 快捷操作 =============
with st.sidebar:
    st.header("🔥 今日异动操作")

    if st.button("🔄 刷新数据", type="primary"):
        st.rerun()


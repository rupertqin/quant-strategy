"""
今日异动页面 - 涨停板扫描、板块热度
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


def load_daily_signals():
    """加载今日异动信号 - 读取最新的信号文件"""
    # 首先尝试读取不带日期的最新文件
    signals_file = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal", "daily_signals.json")
    
    if os.path.exists(signals_file):
        with open(signals_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # 如果没有最新文件，尝试找到最新的带日期文件
    report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal")
    if os.path.exists(report_dir):
        files = [f for f in os.listdir(report_dir) if f.startswith("daily_signals_") and f.endswith(".json")]
        if files:
            files.sort(reverse=True)
            latest_file = os.path.join(report_dir, files[0])
            with open(latest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
    return {}


def load_sector_history():
    """加载板块热度历史"""
    # 首先尝试读取不带日期的最新文件
    history_file = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal", "sector_heat_history.csv")
    
    if os.path.exists(history_file):
        return pd.read_csv(history_file)
    
    # 回退到旧路径
    old_history_file = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal", "history", "sector_heat_history.csv")
    if os.path.exists(old_history_file):
        return pd.read_csv(old_history_file)
    return pd.DataFrame()


def get_market_regime():
    """获取市场状态"""
    try:
        sys.path.insert(0, os.path.join(BASE_DIR, "ShortTerm"))
        from daily_signal.market_regime import MarketRegime
        regime = MarketRegime()
        return regime.get_market_status()
    except Exception as e:
        return {
            'regime': 'UNKNOWN',
            'score': 0,
            'reasons': [],
            'factors': {}
        }


# ============= 页面标题 =============
st.title("🔥 今日异动")
st.caption("涨停板扫描 | 板块热度分析 | 市场状态监控")

# ============= 市场状态卡片 =============
regime = get_market_regime()
signals = load_daily_signals()

# 第一行：主要指标
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    regime_color = {'AGGRESSIVE': '🟢', 'DEFENSIVE': '🔴', 'NEUTRAL': '🟡', 'UNKNOWN': '⚪'}
    regime_name = {'AGGRESSIVE': '积极进攻', 'DEFENSIVE': '防御避险', 'NEUTRAL': '震荡中性', 'UNKNOWN': '未知'}
    st.metric(
        label="市场状态",
        value=f"{regime_color.get(regime.get('regime'), '⚪')} {regime_name.get(regime.get('regime'), '未知')}"
    )

with col2:
    macro_score = regime.get('macro_score', 0)
    tech_score = regime.get('tech_score', 0)
    total_score = regime.get('score', 0)
    st.metric(
        label="综合评分",
        value=f"{total_score}/10",
        delta=f"宏观{macro_score} 技术{tech_score}",
        delta_color="inverse"
    )

with col3:
    zt_count = signals.get('total_zt_count', 0)
    zt_assessment = "活跃" if zt_count >= 50 else "正常" if zt_count >= 30 else "低迷"
    st.metric("涨停家数", f"{zt_count}", zt_assessment)

with col4:
    hot_sectors = signals.get('hot_sectors', [])
    st.metric("热点板块", len(hot_sectors))

with col5:
    # 技术面展望
    tech_indicators = signals.get('technical_indicators', {})
    outlook = tech_indicators.get('technical_outlook', '未知')
    st.metric("技术面", outlook)

# 第二行：详细信息
tech_indicators = signals.get('technical_indicators', {})
# ============= 技术面指标展示（从scanner输出读取）=============
if tech_indicators:
    st.markdown("### 📊 技术面分析")
    
    # 第一行：综合评分和主要指标
    tcol1, tcol2, tcol3, tcol4 = st.columns(4)
    
    with tcol1:
        score = tech_indicators.get('composite_score', 50)
        outlook = tech_indicators.get('technical_outlook', '中性')
        color = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
        st.metric("技术面评分", f"{score}/100", f"{color} {outlook}")
    
    with tcol2:
        breadth = tech_indicators.get('market_breadth', {})
        up = breadth.get('up_count', 0)
        down = breadth.get('down_count', 0)
        ratio = breadth.get('up_ratio', 0.5)
        st.metric("涨跌家数", f"↑{up} ↓{down}", f"上涨比例 {ratio:.1%}")
    
    with tcol3:
        sectors = tech_indicators.get('sector_strength', {})
        leader = sectors.get('leader', '中性')
        off_avg = sectors.get('offensive_avg', 0)
        def_avg = sectors.get('defensive_avg', 0)
        st.metric("板块风格", leader, f"进攻{off_avg:+.1f}% vs 防守{def_avg:+.1f}%")
    
    with tcol4:
        zt = tech_indicators.get('zt_sentiment', {})
        zt_count = zt.get('zt_count', 0)
        sentiment = zt.get('sentiment', '未知')
        st.metric("涨停情绪", f"{zt_count}家", sentiment)
    
    # 第二行：指数表现
    st.markdown("**主要指数表现**")
    indices = tech_indicators.get('index_performance', {})
    idx_cols = st.columns(len(indices))
    for idx_col, (name, data) in zip(idx_cols, indices.items()):
        with idx_col:
            change = data.get('change_pct', 0)
            trend = data.get('trend', 'NEUTRAL')
            emoji = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
            trend_emoji = "📈" if trend == "UP" else "📉" if trend == "DOWN" else "➡️"
            st.caption(f"{emoji} {name}")
            st.metric(label=f"{trend_emoji}", value=f"{change:+.2f}%")
    
    # 第三行：道氏理论分析
    st.markdown("**📊 道氏理论分析**")
    dow_cols = st.columns(4)
    for i, (name, data) in enumerate(indices.items()):
        if i < 4:
            with dow_cols[i]:
                dow = data.get('dow_theory', {})
                if dow and 'primary_trend' in dow:
                    primary = dow.get('primary_desc', '未知')
                    secondary = dow.get('secondary_desc', '未知')
                    strength = dow.get('trend_strength', {})
                    adx = strength.get('adx', 0)
                    st.caption(f"**{name}**")
                    st.write(f"{primary}")
                    st.write(f"ADX: {adx}")
                else:
                    st.caption(f"**{name}**")
                    st.write("数据不足")
    
    # 第四行：波浪理论分析
    st.markdown("**🌊 波浪理论分析**")
    wave_data = []
    for name, data in indices.items():
        wave = data.get('elliott_wave', {})
        if wave and 'current_phase' in wave:
            wave_data.append({
                '指数': name,
                '当前阶段': wave.get('current_phase', '未知'),
                '最近峰值': wave.get('last_peak', '-'),
                '最近谷值': wave.get('last_trough', '-'),
                '距峰值': f"{wave.get('current_vs_peak', 0):+.1f}%"
            })
    
    if wave_data:
        st.dataframe(wave_data, hide_index=True, use_container_width=True)
        
        # 显示斐波那契位
        st.markdown("**斐波那契回调位参考**")
        fib_cols = st.columns(len(indices))
        for idx_col, (name, data) in zip(fib_cols, indices.items()):
            with idx_col:
                wave = data.get('elliott_wave', {})
                struct = wave.get('structure', {})
                if struct:
                    st.caption(f"**{name}**")
                    st.write(f"38.2%: {struct.get('fib_382', '-')}")
                    st.write(f"50%: {struct.get('fib_500', '-')}")
                    st.write(f"61.8%: {struct.get('fib_618', '-')}")
    
    # 跨指数验证
    validation = tech_indicators.get('inter_index_validation', {})
    if validation:
        val_status = validation.get('validation', 'UNKNOWN')
        val_note = validation.get('note', '')
        if val_status == 'CONFIRMED':
            st.success(f"📈 **跨指数验证**: {val_note}")
        elif val_status == 'DIVERGENCE':
            st.error(f"📉 **跨指数验证**: {val_note}")
        else:
            st.info(f"📊 **跨指数验证**: {val_note}")
    
    # 技术面理由
    reasons = tech_indicators.get('technical_reasons', [])
    if reasons:
        st.info(f"📋 **技术面看点**: {', '.join(reasons)}")

# 宏观指标（从regime读取）
st.markdown("### 🌍 宏观环境")
mcol1, mcol2, mcol3 = st.columns(3)
with mcol1:
    macro = regime.get('macro', {})
    currency = macro.get('currency', {})
    st.metric("汇率 USD/CNY", f"{currency.get('current', 7.2):.3f}")
with mcol2:
    north = macro.get('north_money', {})
    st.metric("北向资金", f"{north.get('today', 0):+.0f}亿")
with mcol3:
    gold = macro.get('gold', {})
    st.metric("黄金价格", f"{gold.get('current', 550):.0f}")

# 风险/机会因素
all_reasons = regime.get('reasons', [])
if all_reasons:
    st.warning(f"⚠️ **宏观风险**: {', '.join(all_reasons)}")

st.divider()

# ============= 主要内容 =============
signals = load_daily_signals()

# 调试信息
if st.checkbox("显示调试信息", value=False):
    st.write(f"BASE_DIR: {BASE_DIR}")
    st.write(f"Signals file: {os.path.join(BASE_DIR, 'storage', 'outputs', 'shortterm', 'daily_signal', 'daily_signals.json')}")
    st.write(f"Signals loaded: {bool(signals)}")
    if not signals:
        report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "daily_signal")
        st.write(f"Report dir exists: {os.path.exists(report_dir)}")
        if os.path.exists(report_dir):
            st.write(f"Files: {os.listdir(report_dir)}")

if not signals:
    st.info("暂无数据，请运行: `python ShortTerm/run_scanner.py daily`")
else:
    st.subheader(f"📅 {signals.get('date', '今日')} 扫描结果")
    
    col_left, col_right = st.columns([1, 1])
    
    # ========== 左侧: 热点板块 ==========
    with col_left:
        st.markdown("### 🔥 热点板块")
        
        hot_sectors = signals.get('hot_sectors', [])
        market_type = signals.get('market_type', '')
        
        if not hot_sectors and market_type == '普涨分散':
            st.info(f"📊 **{market_type}**\n\n"
                   f"今日涨停 {signals.get('total_zt_count', 0)} 家，" 
                   f"但分布较分散，没有单一板块集中爆发。")
        elif hot_sectors:
            for sector in hot_sectors[:10]:
                with st.container():
                    st.markdown(f"""
                    <div class="hot-sector-card">
                        <h4>{sector['sector']}</h4>
                        <p>涨停 {sector['zt_count']} 家 | 龙头: {sector['lead_stock']}</p>
                        <small>5日涨幅: {sector.get('performance_5d', 0):.1%} | 波动率: {sector.get('volatility', 0):.2f}</small>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("暂无热点板块数据")
    
    # ========== 右侧: 操作信号 ==========
    with col_right:
        st.markdown("### 📊 操作信号")
        
        signal_list = signals.get('signals', [])
        if signal_list:
            for sig in signal_list:
                action_emoji = "🔥" if sig['action'] == '关注' else "👀"
                strength_color = "#ff6b6b" if sig['strength'] >= 0.7 else "#feca57" if sig['strength'] >= 0.5 else "#48dbfb"
                
                st.markdown(f"""
                <div class="signal-card">
                    {action_emoji} <strong>{sig['sector']}</strong> 
                    <span style="color: {strength_color}; font-weight: bold;">{sig['action']}</span>
                    <br>
                    <small>强度: {sig['strength']} | {sig['reason']}</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("暂无操作信号")
        
        # 生成时间
        st.caption(f"生成时间: {signals.get('generated_at', 'N/A')}")

st.divider()

# ============= 板块热度历史 =============
st.subheader("📈 板块热度历史")

history_df = load_sector_history()
if not history_df.empty:
    # 显示最近的热门板块趋势
    recent_dates = history_df['date'].unique()[-10:] if 'date' in history_df.columns else []
    
    if len(recent_dates) > 0:
        recent_data = history_df[history_df['date'].isin(recent_dates)]
        
        # 透视表：日期 x 板块
        if 'industry' in recent_data.columns and 'limit_up_count' in recent_data.columns:
            pivot_df = recent_data.pivot_table(
                index='date',
                columns='industry',
                values='limit_up_count',
                aggfunc='sum',
                fill_value=0
            )
            
            # 只显示有数据的板块
            active_sectors = pivot_df.sum().sort_values(ascending=False).head(10).index
            pivot_df = pivot_df[active_sectors]
            
            st.line_chart(pivot_df)
        else:
            st.dataframe(recent_data.head(20))
    else:
        st.dataframe(history_df.head(20))
else:
    st.info("暂无历史数据")

# ============= 快捷操作 =============
with st.sidebar:
    st.header("🔥 今日异动操作")
    
    if st.button("运行涨停扫描", type="primary"):
        with st.spinner("正在扫描涨停板..."):
            import subprocess
            try:
                result = subprocess.run(
                    [sys.executable, "run_scanner.py", "daily"],
                    cwd=os.path.join(BASE_DIR, "ShortTerm"),
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if result.returncode == 0:
                    st.success("扫描完成!")
                    st.rerun()
                else:
                    st.error(f"运行失败: {result.stderr}")
            except Exception as e:
                st.error(f"错误: {e}")
    
    st.divider()
    st.caption("""
    数据来源: ShortTerm/daily_signal
    - 每日收盘后自动更新
    - 监控全市场涨停板
    - 分析板块热度
    """)

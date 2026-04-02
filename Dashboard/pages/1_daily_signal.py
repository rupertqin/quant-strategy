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

# ============= 顶部状态栏 =============
col1, col2, col3, col4 = st.columns(4)

regime = get_market_regime()

with col1:
    regime_color = {'AGGRESSIVE': '🟢', 'DEFENSIVE': '🔴', 'NEUTRAL': '🟡', 'UNKNOWN': '⚪'}
    regime_name = {'AGGRESSIVE': '积极', 'DEFENSIVE': '防御', 'NEUTRAL': '中性', 'UNKNOWN': '未知'}
    st.metric(
        label="市场状态",
        value=f"{regime_color.get(regime.get('regime'), '⚪')} {regime_name.get(regime.get('regime'), '未知')}"
    )

with col2:
    st.metric("风险评分", f"{regime.get('score', 0)}/10")

with col3:
    signals = load_daily_signals()
    st.metric("涨停家数", signals.get('total_zt_count', 0))

with col4:
    hot_sectors = signals.get('hot_sectors', [])
    st.metric("热点板块", len(hot_sectors))

# 风险因素
if regime.get('reasons'):
    st.warning(f"⚠️ 风险因素: {', '.join(regime['reasons'])}")

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
        if hot_sectors:
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

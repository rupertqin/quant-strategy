"""
股票池监控页面 - LongTerm股票池的短线技术指标
对应 ShortTerm/pool_watch 模块
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
    page_title="股票池监控 - Quant Dashboard",
    page_icon="📊",
    layout="wide"
)

# ============= 样式 =============
st.markdown("""
<style>
    .buy-card {
        background: linear-gradient(135deg, #00b894 0%, #00a085 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 8px 0;
    }
    .sell-card {
        background: linear-gradient(135deg, #ff7675 0%, #d63031 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 8px 0;
    }
    .watch-card {
        background: linear-gradient(135deg, #fdcb6e 0%, #f39c12 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 8px 0;
    }
    .score-high { color: #00b894; font-weight: bold; }
    .score-medium { color: #fdcb6e; font-weight: bold; }
    .score-low { color: #ff7675; font-weight: bold; }
    .trend-up { color: #00b894; }
    .trend-down { color: #ff7675; }
    .trend-neutral { color: #636e72; }
</style>
""", unsafe_allow_html=True)


def load_pool_watch_report():
    """加载股票池监控报告 - 读取最新的报告文件"""
    report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "pool_watch")
    
    # 首先尝试读取不带日期的最新文件
    latest_file = os.path.join(report_dir, "pool_watch_latest.json")
    if os.path.exists(latest_file):
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # 如果没有最新文件，尝试找到最新的带日期文件
    if os.path.exists(report_dir):
        files = [f for f in os.listdir(report_dir) if f.startswith("pool_watch_") and f.endswith(".json") and f != "pool_watch_latest.json"]
        if files:
            files.sort(reverse=True)
            latest_file = os.path.join(report_dir, files[0])
            with open(latest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
    return {}


def load_pool_ranking():
    """加载股票池排名CSV - 读取最新的排名文件"""
    report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "pool_watch")
    
    # 首先尝试读取不带日期的最新文件
    latest_file = os.path.join(report_dir, "pool_ranking_latest.csv")
    if os.path.exists(latest_file):
        return pd.read_csv(latest_file)
    
    # 如果没有最新文件，尝试找到最新的带日期文件
    if os.path.exists(report_dir):
        files = [f for f in os.listdir(report_dir) if f.startswith("pool_ranking_") and f.endswith(".csv") and f != "pool_ranking_latest.csv"]
        if files:
            files.sort(reverse=True)
            latest_file = os.path.join(report_dir, files[0])
            return pd.read_csv(latest_file)
    return pd.DataFrame()


def get_score_class(score):
    """获取评分样式类"""
    if score >= 80:
        return "score-high"
    elif score >= 60:
        return "score-medium"
    else:
        return "score-low"


def get_trend_class(trend):
    """获取趋势样式类"""
    if trend in ['STRONG_UP', 'UP']:
        return "trend-up"
    elif trend in ['STRONG_DOWN', 'DOWN']:
        return "trend-down"
    else:
        return "trend-neutral"


# ============= 页面标题 =============
st.title("📊 股票池监控")
st.caption("LongTerm股票池短线技术指标 | 均线系统 | 量价分析")

# ============= 加载数据 =============
report = load_pool_watch_report()
ranking_df = load_pool_ranking()

# 调试信息
if st.checkbox("显示调试信息", value=False):
    st.write(f"BASE_DIR: {BASE_DIR}")
    st.write(f"Report dir: {os.path.join(BASE_DIR, 'storage', 'outputs', 'shortterm', 'pool_watch')}")
    st.write(f"Exists: {os.path.exists(os.path.join(BASE_DIR, 'storage', 'outputs', 'shortterm', 'pool_watch'))}")
    if os.path.exists(os.path.join(BASE_DIR, 'storage', 'outputs', 'shortterm', 'pool_watch')):
        files = os.listdir(os.path.join(BASE_DIR, 'storage', 'outputs', 'shortterm', 'pool_watch'))
        st.write(f"Files: {files}")
    st.write(f"Report loaded: {bool(report)}")

if not report:
    st.info("暂无数据，请运行: `python ShortTerm/run_scanner.py pool`")
    st.stop()

# ============= 顶部摘要 =============
st.subheader(f"📅 {report.get('date', '今日')} 监控报告")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("监控股票数", report.get('total_stocks', 0))

with col2:
    summary = report.get('summary', {})
    st.metric("买入信号", summary.get('buy_count', 0), delta=None)

with col3:
    st.metric("卖出信号", summary.get('sell_count', 0), delta=None)

with col4:
    st.metric("观察列表", summary.get('watch_count', 0), delta=None)

st.divider()

# ============= 信号区域 =============
col_left, col_right = st.columns([1, 1])

# ========== 左侧: 买入信号 ==========
with col_left:
    st.markdown("### 🟢 买入信号")
    
    buy_signals = report.get('buy_signals', [])
    if buy_signals:
        for sig in buy_signals[:10]:
            with st.container():
                st.markdown(f"""
                <div class="buy-card">
                    <h4>{sig['symbol']} {sig.get('name', '')}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:3])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无买入信号")
    
    st.markdown("### 🔴 卖出信号")
    
    sell_signals = report.get('sell_signals', [])
    if sell_signals:
        for sig in sell_signals[:5]:
            with st.container():
                st.markdown(f"""
                <div class="sell-card">
                    <h4>{sig['symbol']} {sig.get('name', '')}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:3])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无卖出信号")

# ========== 右侧: 观察列表 ==========
with col_right:
    st.markdown("### 🟡 观察列表")
    
    watch_list = report.get('watch_list', [])
    if watch_list:
        for sig in watch_list[:10]:
            with st.container():
                st.markdown(f"""
                <div class="watch-card">
                    <h4>{sig['symbol']} {sig.get('name', '')}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:2])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无观察股票")

st.divider()

# ============= 排名表格 =============
st.subheader("📈 综合评分排名 (Top 20)")

top_rankings = report.get('top_rankings', [])
if top_rankings:
    # 转换为DataFrame显示
    df_data = []
    for i, item in enumerate(top_rankings[:20], 1):
        score_class = get_score_class(item['score'])
        trend_class = get_trend_class(item.get('trend', ''))
        
        df_data.append({
            '排名': i,
            '代码': item['symbol'],
            '名称': item.get('name', ''),
            '评分': item['score'],
            '最新价': f"¥{item['close']:.2f}",
            '涨跌幅': f"{item['change_pct']:+.2f}%",
            '趋势': item.get('trend', ''),
            '主要信号': ' | '.join(item.get('signals', [])[:2])
        })
    
    df = pd.DataFrame(df_data)
    
    # 使用自定义样式
    st.dataframe(
        df,
        column_config={
            "评分": st.column_config.NumberColumn(
                "评分",
                help="综合评分 0-100",
                format="%.1f"
            ),
            "涨跌幅": st.column_config.TextColumn(
                "涨跌幅",
                help="当日涨跌幅"
            )
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("暂无排名数据")

# ============= 详细数据表格 =============
if not ranking_df.empty:
    st.divider()
    st.subheader("📋 详细技术指标")
    
    # 选择显示的列
    display_cols = ['symbol', 'name', 'score', 'close', 'change_pct', 
                   'ma5', 'ma10', 'ma20', 'ma60', 'vol_ratio', 'trend', 'signals']
    
    available_cols = [c for c in display_cols if c in ranking_df.columns]
    df_display = ranking_df[available_cols].head(30)
    
    # 重命名列
    col_names = {
        'symbol': '代码',
        'name': '名称',
        'score': '评分',
        'close': '最新价',
        'change_pct': '涨跌幅%',
        'ma5': 'MA5',
        'ma10': 'MA10',
        'ma20': 'MA20',
        'ma60': 'MA60',
        'vol_ratio': '量比',
        'trend': '趋势',
        'signals': '信号'
    }
    df_display = df_display.rename(columns={k: v for k, v in col_names.items() if k in df_display.columns})
    
    st.dataframe(df_display, hide_index=True, use_container_width=True)

# ============= 快捷操作 =============
with st.sidebar:
    st.header("📊 股票池操作")
    
    if st.button("运行股票池监控", type="primary"):
        with st.spinner("正在分析股票池..."):
            import subprocess
            try:
                result = subprocess.run(
                    [sys.executable, "run_scanner.py", "pool"],
                    cwd=os.path.join(BASE_DIR, "ShortTerm"),
                    capture_output=True,
                    text=True,
                    timeout=180
                )
                if result.returncode == 0:
                    st.success("分析完成!")
                    st.rerun()
                else:
                    st.error(f"运行失败: {result.stderr}")
            except Exception as e:
                st.error(f"错误: {e}")
    
    st.divider()
    
    # 筛选器
    st.subheader("筛选")
    
    if not ranking_df.empty and 'trend' in ranking_df.columns:
        trends = ranking_df['trend'].unique().tolist()
        selected_trend = st.selectbox("趋势", ["全部"] + trends)
        
        if selected_trend != "全部":
            filtered = ranking_df[ranking_df['trend'] == selected_trend]
            st.caption(f"筛选结果: {len(filtered)} 只股票")
    
    if not ranking_df.empty and 'score' in ranking_df.columns:
        min_score = st.slider("最低评分", 0, 100, 50)
        filtered = ranking_df[ranking_df['score'] >= min_score]
        st.caption(f"评分≥{min_score}: {len(filtered)} 只股票")
    
    st.divider()
    st.caption("""
    数据来源: ShortTerm/pool_watch
    - 监控LongTerm股票池
    - 技术指标: MA5/10/20/60
    - 量价关系分析
    - 综合评分系统
    """)

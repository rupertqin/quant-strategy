"""
量化交易看板 - 主页面
整合长线和短线策略结果

启动方式: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
import sys
import subprocess

# 添加项目路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# ============= 配置 =============
st.set_page_config(
    page_title="Quant Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============= 样式 =============
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .hot-sector {
        background-color: #ff4b4b;
        color: white;
        padding: 5px 10px;
        border-radius: 5px;
        margin: 2px;
        display: inline-block;
    }
    .cold-sector {
        background-color: #4CAF50;
        color: white;
        padding: 5px 10px;
        border-radius: 5px;
        margin: 2px;
        display: inline-block;
    }
    .signal-box {
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .signal-attention {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
    }
    .signal-watch {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
    }
    .nav-button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 20px;
        border-radius: 5px;
        text-decoration: none;
        display: inline-block;
        margin: 5px;
    }
</style>
""", unsafe_allow_html=True)


# ============= 工具函数 =============
def get_base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_longterm_data():
    """加载长线数据 - 从 storage/outputs 读取"""
    base = get_base_dir()
    weights_file = os.path.join(base, "storage", "outputs", "longterm", "weights", "output_weights.csv")

    if os.path.exists(weights_file):
        return pd.read_csv(weights_file)
    return pd.DataFrame()


def load_daily_signals():
    """加载今日异动信号"""
    base = get_base_dir()
    signals_file = os.path.join(base, "storage", "outputs", "shortterm", "daily_signal", "signals", "daily_signals.json")

    if os.path.exists(signals_file):
        with open(signals_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def load_pool_watch_summary():
    """加载股票池监控摘要"""
    base = get_base_dir()
    report_dir = os.path.join(base, "storage", "outputs", "shortterm", "pool_watch")
    
    if os.path.exists(report_dir):
        files = [f for f in os.listdir(report_dir) if f.startswith("pool_watch_") and f.endswith(".json")]
        if files:
            files.sort(reverse=True)
            latest_file = os.path.join(report_dir, files[0])
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('summary', {}), data.get('date', '')
    return {}, ''


def get_market_regime():
    """获取市场状态"""
    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from daily_signal.market_regime import MarketRegime
        regime = MarketRegime()
        return regime.get_market_status()
    except Exception as e:
        return {
            'regime': 'UNKNOWN',
            'score': 0,
            'reasons': [str(e)],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def run_longterm_optimization():
    """运行长线优化"""
    base = get_base_dir()
    longterm_dir = os.path.join(base, "LongTerm")
    try:
        result = subprocess.run(
            [sys.executable, "run_optimization.py"],
            cwd=longterm_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except subprocess.TimeoutExpired:
        return {'success': False, 'stderr': '执行超时'}
    except Exception as e:
        return {'success': False, 'stderr': str(e)}


def run_daily_scanner():
    """运行今日异动扫描"""
    base = get_base_dir()
    shortterm_dir = os.path.join(base, "ShortTerm")
    try:
        result = subprocess.run(
            [sys.executable, "run_scanner.py", "daily"],
            cwd=shortterm_dir,
            capture_output=True,
            text=True,
            timeout=120
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except subprocess.TimeoutExpired:
        return {'success': False, 'stderr': '执行超时'}
    except Exception as e:
        return {'success': False, 'stderr': str(e)}


def run_pool_watch():
    """运行股票池监控"""
    base = get_base_dir()
    shortterm_dir = os.path.join(base, "ShortTerm")
    try:
        result = subprocess.run(
            [sys.executable, "run_scanner.py", "pool"],
            cwd=shortterm_dir,
            capture_output=True,
            text=True,
            timeout=180
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except subprocess.TimeoutExpired:
        return {'success': False, 'stderr': '执行超时'}
    except Exception as e:
        return {'success': False, 'stderr': str(e)}


def refresh_data():
    """刷新数据"""
    base = get_base_dir()
    datahub_dir = os.path.join(base, "DataHub")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/refresh_data.py", "prices"],
            cwd=datahub_dir,
            capture_output=True,
            text=True,
            timeout=180
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except subprocess.TimeoutExpired:
        return {'success': False, 'stderr': '执行超时'}
    except Exception as e:
        return {'success': False, 'stderr': str(e)}


# ============= 主界面 =============
st.title("📈 Quant Dashboard - 量化交易看板")
st.caption("长线战略配置 + 短线战术扫描")

# 顶部状态栏
col1, col2, col3, col4 = st.columns(4)

with col1:
    regime = get_market_regime()
    regime_color = {'AGGRESSIVE': 'green', 'DEFENSIVE': 'red', 'NEUTRAL': 'orange'}
    regime_name = {'AGGRESSIVE': '积极', 'DEFENSIVE': '防御', 'NEUTRAL': '中性'}
    st.markdown(f"""
    <div class="metric-card" style="text-align: center;">
        <h3>市场状态</h3>
        <h2 style="color: {regime_color.get(regime.get('regime', 'UNKNOWN'), 'gray')};">
            {regime_name.get(regime.get('regime', 'UNKNOWN'), '未知')}
        </h2>
    </div>
    """, unsafe_allow_html=True)

with col2:
    risk_score = regime.get('score', 0)
    st.metric("风险评分", f"{risk_score}/10")

with col3:
    longterm_weights = load_longterm_data()
    if not longterm_weights.empty:
        top_weight = longterm_weights.iloc[0]
        st.metric("长线首选", top_weight['symbol'], f"{top_weight['weight']:.1%}")
    else:
        st.metric("长线首选", "N/A", "-")

with col4:
    pool_summary, pool_date = load_pool_watch_summary()
    st.metric("股票池买入信号", pool_summary.get('buy_count', 0), delta=None)

# 市场状态详情
if regime.get('reasons'):
    st.info(f"风险因素: {', '.join(regime['reasons'])}")

st.divider()

# ============= 导航到子页面 =============
st.subheader("📑 功能模块")

col_nav1, col_nav2, col_nav3 = st.columns(3)

with col_nav1:
    st.markdown("""
    ### 🔥 今日异动
    全市场涨停板扫描、板块热度分析
    
    - 涨停家数统计
    - 热点板块排名
    - 操作信号生成
    """)
    if st.button("进入今日异动 ➡️", key="nav_daily"):
        st.switch_page("pages/1_daily_signal.py")

with col_nav2:
    st.markdown("""
    ### 📊 股票池监控
    LongTerm股票池短线技术指标
    
    - MA5/10/20/60均线系统
    - 量价关系分析
    - 综合评分排名
    """)
    if st.button("进入股票池监控 ➡️", key="nav_pool"):
        st.switch_page("pages/2_pool_watch.py")

with col_nav3:
    st.markdown("""
    ### 📈 长线配置
    均值-方差优化、资产配置
    
    - 最优权重计算
    - 风险收益分析
    - 月度调仓建议
    """)
    st.info("当前页面下方查看")

st.divider()

# ============= 两栏布局: 长线 + 短线摘要 =============
col_left, col_right = st.columns([1, 1])

# ========== 左侧: 长线配置 ==========
with col_left:
    st.header("📈 长线配置 (战略)")

    if not longterm_weights.empty:
        # 饼图
        import plotly.express as px
        fig_pie = px.pie(
            longterm_weights,
            values='weight',
            names='symbol',
            title="资产配置",
            hole=0.4
        )
        st.plotly_chart(fig_pie, use_container_width=True)

        # 权重表格
        st.subheader("目标权重")
        st.dataframe(
            longterm_weights.style.format({'weight': '{:.2%}'}),
            use_container_width=True
        )
    else:
        st.info("长线策略未运行，请先运行 LongTerm/run_optimization.py")

# ========== 右侧: 短线摘要 ==========
with col_right:
    st.header("⚡ 短线摘要 (战术)")
    
    # 今日异动摘要
    signals = load_daily_signals()
    if signals:
        st.subheader("🔥 今日涨停")
        st.metric("涨停家数", signals.get('total_zt_count', 0))
        
        hot_sectors = signals.get('hot_sectors', [])
        if hot_sectors:
            st.write("热点板块:")
            for sector in hot_sectors[:3]:
                st.markdown(f"<span class='hot-sector'>{sector['sector']} ({sector['zt_count']})</span>", 
                          unsafe_allow_html=True)
    else:
        st.info("今日异动未运行")
    
    st.divider()
    
    # 股票池监控摘要
    if pool_summary:
        st.subheader("📊 股票池信号")
        
        col_sig1, col_sig2, col_sig3 = st.columns(3)
        with col_sig1:
            st.metric("🟢 买入", pool_summary.get('buy_count', 0))
        with col_sig2:
            st.metric("🔴 卖出", pool_summary.get('sell_count', 0))
        with col_sig3:
            st.metric("🟡 观察", pool_summary.get('watch_count', 0))
        
        if st.button("查看详情 ➡️", key="goto_pool"):
            st.switch_page("pages/2_pool_watch.py")
    else:
        st.info("股票池监控未运行")

st.divider()

# ============= 底部: 综合建议 =============
st.header("💡 综合交易建议")

col1, col2 = st.columns(2)

with col1:
    st.subheader("仓位建议")
    multiplier = 1.0
    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from daily_signal.market_regime import MarketRegime
        multiplier = MarketRegime().get_position_multiplier()
    except:
        pass

    st.progress(multiplier)
    st.write(f"建议仓位: {multiplier:.0%}")

    if regime.get('regime') == 'DEFENSIVE':
        st.warning("⚠️ 市场风险较高，建议降低仓位，减少操作")
    elif regime.get('regime') == 'AGGRESSIVE':
        st.success("✅ 市场积极，可适当加大仓位")
    else:
        st.info("ℹ️ 市场中性，保持现有仓位")

with col2:
    st.subheader("板块偏好")

    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from daily_signal.market_regime import MarketRegime
        preferred = MarketRegime().get_sector_preference()
        st.write("推荐关注板块:")
        for sector in preferred:
            st.markdown(f"<span class='hot-sector'>{sector}</span>", unsafe_allow_html=True)
    except:
        st.write("请运行短线策略获取板块推荐")

# ============= 侧边栏 =============
with st.sidebar:
    st.header("⚡ 快捷操作")
    
    st.subheader("数据管理")
    if st.button("🔄 刷新价格数据"):
        with st.spinner("正在刷新价格数据..."):
            result = refresh_data()
            if result['success']:
                st.success("数据刷新完成!")
            else:
                st.warning(f"刷新失败: {result.get('stderr', '未知错误')}")
    
    st.divider()
    
    st.subheader("策略运行")
    
    if st.button("📈 运行长线优化", type="primary"):
        with st.spinner("正在运行长线优化..."):
            result = run_longterm_optimization()
            if result['success']:
                st.success("长线优化完成!")
                st.rerun()
            else:
                st.error(f"运行失败: {result.get('stderr', '未知错误')}")
                with st.expander("查看输出"):
                    st.text(result.get('stdout', '') or result.get('stderr', ''))

    if st.button("🔥 运行今日异动"):
        with st.spinner("正在扫描涨停板..."):
            result = run_daily_scanner()
            if result['success']:
                st.success("今日异动扫描完成!")
                st.rerun()
            else:
                st.error(f"运行失败: {result.get('stderr', '未知错误')}")
    
    if st.button("📊 运行股票池监控"):
        with st.spinner("正在分析股票池..."):
            result = run_pool_watch()
            if result['success']:
                st.success("股票池监控完成!")
                st.rerun()
            else:
                st.error(f"运行失败: {result.get('stderr', '未知错误')}")
    
    st.divider()
    
    if st.button("🔄 刷新看板"):
        st.rerun()
    
    st.divider()
    
    st.write("📚 使用说明")
    st.caption("""
    **导航页面:**
    - 🔥 今日异动: 涨停板扫描
    - 📊 股票池监控: 技术指标分析
    
    **策略说明:**
    - 长线: 均值-方差优化
    - 短线: 事件驱动 + 技术分析
    
    ⚠️ 仅供参考，不构成投资建议
    """)

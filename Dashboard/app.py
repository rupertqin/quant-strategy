"""
量化交易看板
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
    layout="wide"
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


def load_shortterm_signals():
    """加载短线信号 - 从 storage/outputs 读取"""
    base = get_base_dir()
    signals_file = os.path.join(base, "storage", "outputs", "shortterm", "signals", "daily_signals.json")

    if os.path.exists(signals_file):
        with open(signals_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def get_market_regime():
    """获取市场状态"""
    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from market_regime import MarketRegime
        regime = MarketRegime()
        return regime.get_market_status()
    except Exception as e:
        return {
            'regime': 'UNKNOWN',
            'score': 0,
            'reasons': [str(e)],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    finally:
        shortterm_dir = os.path.join(base, "ShortTerm")
        if shortterm_dir in sys.path:
            sys.path.remove(shortterm_dir)


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


def run_shortterm_scanner():
    """运行短线扫描"""
    base = get_base_dir()
    shortterm_dir = os.path.join(base, "ShortTerm")
    try:
        result = subprocess.run(
            [sys.executable, "run_scanner.py"],
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
st.title("Quant Dashboard - 量化交易看板")

# 顶部状态栏
col1, col2, col3, col4 = st.columns(4)

with col1:
    regime = get_market_regime()
    regime_color = {'AGGRESSIVE': 'green', 'DEFENSIVE': 'red', 'NEUTRAL': 'orange'}
    st.markdown(f"""
    <div class="metric-card" style="text-align: center;">
        <h3>市场状态</h3>
        <h2 style="color: {regime_color.get(regime.get('regime', 'UNKNOWN'), 'gray')};">
            {regime.get('regime', 'N/A')}
        </h2>
    </div>
    """, unsafe_allow_html=True)

with col2:
    risk_score = regime.get('score', 0)
    st.metric("风险评分", f"{risk_score}/10", delta=None)

with col3:
    longterm_weights = load_longterm_data()
    if not longterm_weights.empty:
        top_weight = longterm_weights.iloc[0]
        st.metric("长线首选", top_weight['symbol'], f"{top_weight['weight']:.1%}")
    else:
        st.metric("长线首选", "N/A", "-")

with col4:
    signals = load_shortterm_signals()
    signal_count = len(signals.get('signals', []))
    st.metric("短线信号", f"{signal_count}个", delta=None)

# 市场状态详情
if regime.get('reasons'):
    st.info(f"风险因素: {', '.join(regime['reasons'])}")

st.divider()

# ============= 两栏布局 =============
col_left, col_right = st.columns([1, 1])

# ========== 左侧: 长线配置 ==========
with col_left:
    st.header("长线配置 (战略)")

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
        st.plotly_chart(fig_pie, width='stretch')

        # 权重表格
        st.subheader("目标权重")
        st.dataframe(
            longterm_weights.style.format({'weight': '{:.2%}'}),
            width='stretch'
        )
    else:
        st.info("长线策略未运行，请先运行 LongTerm/run_optimization.py")

# ========== 右侧: 短线雷达 ==========
with col_right:
    st.header("短线雷达 (战术)")

    if signals:
        st.subheader(f"📅 {signals.get('date', '今日')}")

        # 热点板块
        hot_sectors = signals.get('hot_sectors', [])
        if hot_sectors:
            st.write("🔥 热点板块:")
            for sector in hot_sectors[:5]:
                st.markdown(f"""
                <div class="signal-box signal-attention">
                    <strong>{sector['sector']}</strong> - {sector['zt_count']}家涨停
                    <br><small>龙头: {sector['lead_stock']}</small>
                </div>
                """, unsafe_allow_html=True)

        # 操作信号
        st.write("📊 操作信号:")
        for sig in signals.get('signals', []):
            emoji = "🔥" if sig['action'] == '关注' else "👀"
            st.markdown(f"""
            <div class="signal-box signal-watch">
                {emoji} <strong>{sig['sector']}</strong> - {sig['action']}
                <br><small>强度: {sig['strength']} | {sig['reason']}</small>
            </div>
            """, unsafe_allow_html=True)

        st.caption(f"生成时间: {signals.get('generated_at', 'N/A')}")
    else:
        st.info("短线策略未运行，请先运行 ShortTerm/run_scanner.py")

st.divider()

# ============= 底部: 综合建议 =============
st.header("综合交易建议")

col1, col2 = st.columns(2)

with col1:
    st.subheader("仓位建议")
    multiplier = 1.0
    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from market_regime import MarketRegime
        multiplier = MarketRegime().get_position_multiplier()
    except:
        pass

    st.progress(multiplier)
    st.write(f"建议仓位: {multiplier:.0%}")

    if regime.get('regime') == 'DEFENSIVE':
        st.warning("市场风险较高，建议降低仓位，减少操作")
    elif regime.get('regime') == 'AGGRESSIVE':
        st.success("市场积极，可适当加大仓位")
    else:
        st.info("市场中性，保持现有仓位")

with col2:
    st.subheader("板块偏好")

    try:
        base = get_base_dir()
        sys.path.insert(0, os.path.join(base, "ShortTerm"))
        from market_regime import MarketRegime
        preferred = MarketRegime().get_sector_preference()
        st.write("推荐关注板块:")
        for sector in preferred:
            st.markdown(f"<span class='hot-sector'>{sector}</span>", unsafe_allow_html=True)
    except:
        st.write("请运行短线策略获取板块推荐")
    finally:
        shortterm_dir = os.path.join(base, "ShortTerm")
        if shortterm_dir in sys.path:
            sys.path.remove(shortterm_dir)

# ============= 侧边栏 =============
with st.sidebar:
    st.header("快捷操作")

    # 长线策略
    st.write("长线策略")
    if st.button("运行长线优化", type="primary"):
        with st.spinner("正在运行长线优化..."):
            result = run_longterm_optimization()
            if result['success']:
                st.success("长线优化完成!")
                st.rerun()
            else:
                st.error(f"运行失败: {result.get('stderr', '未知错误')}")
                with st.expander("查看输出"):
                    st.text(result.get('stdout', '') or result.get('stderr', ''))

    # 短线策略
    st.write("短线策略")
    if st.button("运行短线扫描", type="primary"):
        with st.spinner("正在运行短线扫描..."):
            result = run_shortterm_scanner()
            if result['success']:
                st.success("短线扫描完成!")
                st.rerun()
            else:
                st.error(f"运行失败: {result.get('stderr', '未知错误')}")
                with st.expander("查看输出"):
                    st.text(result.get('stdout', '') or result.get('stderr', ''))

    # 数据刷新
    st.write("数据管理")
    if st.button("刷新价格数据"):
        with st.spinner("正在刷新价格数据..."):
            result = refresh_data()
            if result['success']:
                st.success("数据刷新完成!")
            else:
                st.warning(f"刷新失败: {result.get('stderr', '未知错误')}")

    st.divider()

    # 刷新看板
    if st.button("刷新看板"):
        st.rerun()

    st.divider()

    st.write("说明")
    st.caption("""
    - 长线: 均值-方差优化
    - 短线: 事件驱动分析
    - 数据: storage/outputs/
    - 建议: 仅供参考，不构成投资建议
    """)

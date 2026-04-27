"""
股票池监控页面 - 展示 LongTerm 股票池的信号

展示逻辑复用 signal_watch，只展示股票池内的信号
"""

import streamlit as st
import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime
import math

# 添加项目路径
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "Dashboard"))

from lib.utils import StockCodeUtil, get_stock_name
from Dashboard.utils.formatters import render_signal_card
from Dashboard.utils.scoring import calculate_stock_score
from Dashboard.utils.signal_components import (
    calculate_stock_metrics, render_signal_list, render_risk_assessment,
    render_expander_header, render_stock_signal_expander,
    get_change_pct_display, get_score_style, get_risk_style, get_risk_color_css
)
from DataHub.config import get_storage_path

st.set_page_config(
    page_title="股票池监控 - 秦项投资量化",
    page_icon="📊",
    layout="wide"
)

# ============= 样式 =============
st.markdown("""
<style>
    .stats-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 12px;
        text-align: center;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stats-number { font-size: 32px; font-weight: bold; }
    .stats-label { font-size: 14px; opacity: 0.9; margin-top: 5px; }
</style>
""", unsafe_allow_html=True)


def load_stock_pool() -> list:
    """加载 LongTerm 股票池列表"""
    # 尝试从 LongTerm/config.yaml 加载
    try:
        import yaml
        yaml_file = BASE_DIR / "LongTerm" / "config.yaml"
        if yaml_file.exists():
            with open(yaml_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            stock_list = config.get('data_source', {}).get('stock_list', [])
            if stock_list:
                return stock_list
    except Exception as e:
        st.warning(f"读取 config.yaml 失败: {e}")

    # 回退：尝试 stock_pool.json
    pool_file = get_storage_path("stock_pool.json")
    if pool_file.exists():
        with open(pool_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    # 回退：尝试旧路径
    try:
        import yaml
        yaml_file = BASE_DIR / "LongTerm" / "config" / "stock_pool.yaml"
        if yaml_file.exists():
            with open(yaml_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config.get('stocks', [])
    except:
        pass

    return []


def load_signals() -> dict:
    """加载信号数据"""
    from DataHub.config import SHORTTERM_SIGNALS_DIR
    signals_file = SHORTTERM_SIGNALS_DIR / "signal_latest.json"
    if not signals_file.exists():
        return {}

    with open(signals_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def filter_pool_signals(signals: list, pool_symbols: list) -> list:
    """过滤出股票池内的信号"""
    pool_set = set(pool_symbols)
    return [s for s in signals if s.get('symbol') in pool_set]


# ============= 加载数据 =============
pool_stocks = load_stock_pool()
signals_data = load_signals()

# 提取元数据
scan_time = signals_data.get('scan_time', '未知')
price_fetch_time = signals_data.get('price_fetch_time', '')

# 兼容旧格式：多资产类型时间拼接时只取第一个时间
if price_fetch_time and ('股票' in price_fetch_time or 'ETF' in price_fetch_time or '指数' in price_fetch_time):
    import re
    match = re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}', price_fetch_time)
    if match:
        price_fetch_time = match.group(0)

# 兼容格式A（stocks数组）和格式B（顶层signals数组）
all_signals = signals_data.get('signals', [])
if not all_signals and 'stocks' in signals_data:
    for stock in signals_data.get('stocks', []):
        all_signals.extend(stock.get('signals', []))

# 过滤股票池信号
pool_signals = filter_pool_signals(all_signals, pool_stocks)

# 按股票分组（包括有买入信号的和只有风险信号的股票）
signals_by_stock = {}

# 先从信号数据中获取所有股票池相关的股票
pool_set = set(pool_stocks)

# 建立股票数据快速查找表（优先使用 JSON 预存分数）
all_stocks_data = signals_data.get('stocks', [])
stock_map = {s['symbol']: s for s in all_stocks_data if s.get('symbol')}

# 1. 处理有买入信号的股票
for sig in pool_signals:
    symbol = sig['symbol']
    if symbol not in signals_by_stock:
        signals_by_stock[symbol] = {
            'name': sig.get('name', get_stock_name(symbol)),
            'signals': [],
            'periods': set(),
            'total_score': 0,
            'risk_score': 0,
            'risk_explanations': [],
            'has_buy_signal': True
        }
    signals_by_stock[symbol]['signals'].append(sig)
    signals_by_stock[symbol]['periods'].add(sig.get('period', 'daily'))

# 2. 从 signals_data 中获取股票池内只有风险信号的股票（无买入信号）
for symbol in pool_set:
    if symbol not in signals_by_stock:
        stock = stock_map.get(symbol, {})
        signals_by_stock[symbol] = {
            'name': stock.get('name', get_stock_name(symbol)),
            'signals': [],
            'periods': set(),
            'total_score': 0,
            'risk_score': stock.get('risk_score', 50),
            'risk_explanations': stock.get('risk_explanations', stock.get('risk_warnings', [])),
            'has_buy_signal': False
        }

# 3. 计算/填充分数（优先使用 JSON 预存值，与 signal_watch / stock_chart 完全一致）
for symbol, data in signals_by_stock.items():
    stock = stock_map.get(symbol, {})
    signals = data['signals']

    if data['has_buy_signal']:
        # 信号分：优先用 JSON 预存，否则实时计算
        if stock.get('signal_score') is not None:
            data['total_score'] = stock['signal_score']
        else:
            change_pct = signals[0].get('change_pct', 0) if signals else 0
            data['total_score'] = calculate_stock_score(signals, change_pct)

        # 风险分：优先用 JSON 预存，否则实时计算
        if stock.get('risk_score') is not None:
            data['risk_score'] = stock['risk_score']
            data['risk_explanations'] = stock.get('risk_explanations', stock.get('risk_warnings', []))
        else:
            _, data['risk_score'], data['risk_explanations'] = calculate_stock_metrics(signals)

# 侧边栏
with st.sidebar:
    st.header("📊 股票池")

    # 筛选
    st.subheader("筛选")
    filter_signal_type = st.selectbox("信号类型", ["全部", "左侧信号", "右侧信号"])
    filter_strength = st.selectbox("信号强度", ["全部", "强", "中", "弱"])

    # 新增：显示范围筛选
    st.subheader("显示范围")
    filter_display = st.radio(
        "显示范围",
        ["全部（买入信号+风险信号）", "仅买入信号", "仅风险信号"],
        index=0,
        label_visibility="collapsed"
    )

# ============= 页面标题 =============
st.title("📊 股票池监控")
st.caption(f"LongTerm 股票池信号监控 | 扫描: {scan_time} | 价格: {price_fetch_time or '历史数据'}")

# 检查数据
if not pool_stocks:
    st.error("❌ 未找到股票池数据")
    st.stop()

if not signals_data:
    st.info("暂无信号数据，请先运行扫描")
    st.stop()

# ============= 统计卡片 =============
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="stats-card">
        <div class="stats-number">{len(pool_stocks)}</div>
        <div class="stats-label">股票池数量</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="stats-card">
        <div class="stats-number">{len(signals_by_stock)}</div>
        <div class="stats-label">有信号股票</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    left_count = sum(1 for s in pool_signals if s.get('signal_type') == 'left')
    st.markdown(f"""
    <div class="stats-card">
        <div class="stats-number">{left_count}</div>
        <div class="stats-label">左侧信号</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    # 统计只有风险信号的股票（无买入信号但风险分>=60）
    risk_only_count = sum(1 for s in signals_by_stock.values() if not s.get('has_buy_signal', True))
    st.markdown(f"""
    <div class="stats-card" style="background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);">
        <div class="stats-number">{risk_only_count}</div>
        <div class="stats-label">风险预警</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ============= 股票列表 =============
st.subheader(f"📈 股票池信号 ({len(signals_by_stock)} / {len(pool_stocks)})")

# 应用筛选
filtered_stocks = []
for symbol, data in signals_by_stock.items():
    signals = data['signals']
    has_buy_signal = data.get('has_buy_signal', len(signals) > 0)

    # 根据显示范围筛选
    if filter_display == "仅买入信号" and not has_buy_signal:
        continue
    if filter_display == "仅风险信号" and has_buy_signal:
        continue

    # 信号类型筛选（仅对有买入信号的股票有效）
    if filter_signal_type != "全部" and has_buy_signal:
        target_type = 'left' if filter_signal_type == "左侧信号" else 'right'
        signals = [s for s in signals if s.get('signal_type') == target_type]

    # 强度筛选（仅对有买入信号的股票有效）
    if filter_strength != "全部" and has_buy_signal:
        strength_map = {"强": "strong", "中": "medium", "弱": "weak"}
        target_strength = strength_map.get(filter_strength)
        signals = [s for s in signals if s.get('strength') == target_strength]

    # 显示条件：有买入信号 或 有高风险（风险分>=60）
    risk_score = data.get('risk_score', 0)
    if has_buy_signal or risk_score >= 60:
        # 直接使用已计算的分数，不再重复计算
        signal_score = data['total_score']
        risk_score_calc = data['risk_score']
        risk_explanations = data.get('risk_explanations', [])

        filtered_stocks.append({
            'symbol': symbol,
            'name': data['name'],
            'signals': signals,
            'periods': set(s.get('period', 'daily') for s in signals),
            'total_score': signal_score,
            'risk_score': risk_score_calc,
            'risk_explanations': risk_explanations,
            'has_buy_signal': has_buy_signal
        })

# 按总评分排序
filtered_stocks.sort(key=lambda x: -x['total_score'])

if not filtered_stocks:
    st.info("没有符合条件的信号")
    st.stop()

# 分页
page_size = 10
total_pages = (len(filtered_stocks) + page_size - 1) // page_size
page = st.number_input("页码", min_value=1, max_value=max(1, total_pages), value=1) - 1

start_idx = page * page_size
end_idx = min(start_idx + page_size, len(filtered_stocks))

st.caption(f"显示第 {start_idx + 1} - {end_idx} 条，共 {len(filtered_stocks)} 条")

# 显示股票卡片
for idx, stock_data in enumerate(filtered_stocks[start_idx:end_idx], start_idx):
    symbol = stock_data['symbol']
    stock_name = stock_data['name']
    signals = stock_data['signals']
    periods = stock_data['periods']
    total_score = stock_data['total_score']
    risk_score = stock_data['risk_score']
    risk_explanations = stock_data['risk_explanations']
    has_buy_signal = stock_data.get('has_buy_signal', len(signals) > 0)

    # 获取最新价格和涨跌幅：优先从股票级别取，次优先从最新信号取
    close_price = stock_data.get('close_price', 0)
    change_pct = stock_data.get('change_pct', 0)

    # 处理 NaN
    if isinstance(close_price, float) and math.isnan(close_price):
        close_price = 0
    if isinstance(change_pct, float) and math.isnan(change_pct):
        change_pct = 0

    if not close_price and signals:
        # 取最新日期的信号（而非第一个）
        latest_sig = max(signals, key=lambda s: s.get('trigger_date', ''))
        close_price = latest_sig.get('close_price', 0)
        change_pct = latest_sig.get('change_pct', 0)
        if isinstance(close_price, float) and math.isnan(close_price):
            close_price = 0
        if isinstance(change_pct, float) and math.isnan(change_pct):
            change_pct = 0
    elif not close_price:
        # 只有风险信号的股票，尝试从 signals_data 获取价格
        for s in signals_data.get('stocks', []):
            if s.get('symbol') == symbol:
                tech = s.get('technicals', {})
                close_price = tech.get('close', 0)
                change_pct = tech.get('change_pct', 0)
                break

    # 构建显示
    col1, col2 = st.columns([6, 1])

    with col1:
        # 股票名称和价格
        name_col, price_col = st.columns([3, 1])
        with name_col:
            # 根据信号类型选择图标和按钮样式
            icon = "📈" if has_buy_signal else "⚠️"
            btn_type = "primary" if has_buy_signal else "secondary"
            btn_help = f"点击跳转到 {symbol} K线图" + (" (有买入信号)" if has_buy_signal else " (风险预警)")

            if st.button(
                f"{icon} {stock_name} ({symbol})",
                key=f"pool_btn_{symbol}_{idx}",
                help=btn_help,
                type=btn_type
            ):
                st.session_state['selected_stock'] = symbol
                st.session_state['selected_name'] = stock_name
                st.switch_page("pages/stock_chart.py")

        with price_col:
            # 价格和时间（使用通用格式化函数）
            price_color, change_display = get_change_pct_display(change_pct)
            price_display = f"¥{close_price:.2f}" if close_price else "-"

            st.markdown(f'''
            <div style="text-align: right; font-size: 11px; margin-top: 8px;">
                <span style="font-size: 14px; font-weight: bold; color: {price_color};">{price_display} {change_display}</span>
            </div>
            ''', unsafe_allow_html=True)

        # 周期标签或风险预警标签
        if has_buy_signal:
            period_colors = {'daily': '#3498db', 'weekly': '#9b59b6', 'monthly': '#e74c3c'}
            period_names = {'daily': '日', 'weekly': '周', 'monthly': '月'}
            period_tags = []
            for p in sorted(periods):
                color = period_colors.get(p, '#666')
                name = period_names.get(p, p)
                period_tags.append(f'<span style="background: {color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; margin-right: 5px;">{name}</span>')

            # 多周期共振标识
            if len(periods) >= 2:
                period_tags.append('<span style="background: linear-gradient(135deg, #ff6b6b, #feca57); color: white; padding: 2px 8px; border-radius: 12px; font-size: 11px;">🔥 多周期共振</span>')

            st.markdown(f'<div style="margin: 5px 0;">{"".join(period_tags)}</div>', unsafe_allow_html=True)
        else:
            # 只有风险信号的股票显示风险预警标签
            st.markdown(f'<div style="margin: 5px 0;"><span style="background: #e74c3c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;">⚠️ 风险预警</span></div>', unsafe_allow_html=True)

        # 使用通用组件渲染信号和风险折叠区域
        from Dashboard.utils.scoring import get_score_label
        score_label = get_score_label(total_score)
        render_stock_signal_expander(
            signals, total_score, risk_score, risk_explanations,
            score_label=score_label, expanded=False,
            fetch_time=price_fetch_time
        )

    with col2:
        # 信号评分（只有风险信号的股票显示为"-")
        if has_buy_signal:
            score_bg, score_color = get_score_style(total_score)
            score_display = str(total_score)
        else:
            score_bg, score_color = "#95a5a6", "white"
            score_display = "-"

        # 风险分和标签（使用通用格式化函数）
        risk_bg, risk_color, risk_text = get_risk_style(risk_score)

        st.markdown(f'''
        <div style="background: {score_bg}; color: {score_color}; padding: 10px; border-radius: 10px; text-align: center; margin-bottom: 8px;">
            <div style="font-size: 20px; font-weight: bold;">{score_display}</div>
            <div style="font-size: 11px;">信号分</div>
        </div>
        <div style="background: {risk_bg}; color: {risk_color}; padding: 10px; border-radius: 10px; text-align: center;">
            <div style="font-size: 20px; font-weight: bold;">{risk_score}</div>
            <div style="font-size: 11px;">{risk_text}</div>
        </div>
        ''', unsafe_allow_html=True)

    st.divider()

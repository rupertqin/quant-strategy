"""
个股信号监控页面 - 展示左侧/右侧交易信号

读取 ShortTerm/daily_signal/stock_signal_scanner.py 生成的信号数据
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# 导入共享格式化工具
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.formatters import format_technicals, format_flat_mas, render_flat_ma_badge, format_ma_bonding, render_signal_card

# 导入底层数据接口（默认前复权）
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from DataHub.core.data_reader import load_stock_latest_price, load_stock_latest_date

import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置页面
st.set_page_config(
    page_title="个股信号监控",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 添加项目路径
BASE_DIR = Path(__file__).parent.parent.parent

# ============= 样式 =============
st.markdown("""
<style>
    .signal-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
    }
    .signal-card {
        background: white;
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border-left: 4px solid #ddd;
    }
    .signal-left {
        border-left-color: #2ed573;
    }
    .signal-right {
        border-left-color: #ff4757;
    }
    .signal-title {
        font-size: 16px;
        font-weight: 600;
        margin-bottom: 8px;
    }
    .signal-left .signal-title {
        color: #2ed573;
    }
    .signal-right .signal-title {
        color: #ff4757;
    }
    .signal-meta {
        font-size: 13px;
        color: #666;
        margin-bottom: 8px;
    }
    .signal-desc {
        font-size: 13px;
        color: #333;
        margin-bottom: 10px;
    }
    .signal-tech {
        font-size: 12px;
        color: #888;
        background: #f8f9fa;
        padding: 8px 12px;
        border-radius: 6px;
        font-family: monospace;
    }
    .strength-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: 600;
    }
    .strength-strong {
        background: #ff6b6b;
        color: white;
    }
    .strength-medium {
        background: #feca57;
        color: #333;
    }
    .strength-weak {
        background: #dfe6e9;
        color: #333;
    }
    .score-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        border-radius: 50%;
        font-weight: bold;
        font-size: 14px;
    }
    .score-high {
        background: #ff6b6b;
        color: white;
    }
    .score-medium {
        background: #feca57;
        color: #333;
    }
    .score-low {
        background: #dfe6e9;
        color: #333;
    }
    .stats-card {
        background: white;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    .stats-number {
        font-size: 32px;
        font-weight: bold;
        color: #667eea;
    }
    .stats-label {
        font-size: 14px;
        color: #666;
        margin-top: 5px;
    }
</style>
""", unsafe_allow_html=True)


# ============= 数据加载 =============
def _load_signals_impl() -> dict:
    """加载信号数据实现 - 从 stock_signals_latest.json 读取"""
    filepath = BASE_DIR / "storage" / "outputs" / "signals" / "stock_signals_latest.json"

    if not filepath.exists():
        # 尝试查找日期版本
        signals_dir = BASE_DIR / "storage" / "outputs" / "signals"
        if signals_dir.exists():
            files = sorted(signals_dir.glob("stock_signals_*.json"))
            # 排除 *_latest.json，找日期版本
            date_files = [f for f in files if not f.name.endswith("_latest.json")]
            if date_files:
                filepath = date_files[-1]

    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    return {"status": "error", "message": "暂无信号数据，请先运行扫描器"}


@st.cache_data(ttl=60)
def _load_signals_cached() -> dict:
    """带缓存的信号加载（生产环境使用）"""
    return _load_signals_impl()


def load_signals() -> dict:
    """加载信号数据（开发环境无缓存，生产环境有缓存）"""
    try:
        mode = st.secrets.get("environment", {}).get("mode", "prod")
        is_dev = mode == "dev"
    except Exception:
        is_dev = False

    if is_dev:
        return _load_signals_impl()
    else:
        return _load_signals_cached()


def format_technicals(tech: dict) -> str:
    """格式化技术指标显示"""
    parts = []

    # 均线
    ma_parts = []
    for ma in ['ma5', 'ma10', 'ma20', 'ma60']:
        if tech.get(ma) is not None:
            ma_parts.append(f"{ma.upper()[-2:]}:{tech[ma]}")
    if ma_parts:
        parts.append(" | ".join(ma_parts))

    # MACD
    if tech.get('macd_dif') is not None and tech.get('macd_dea') is not None:
        parts.append(f"MACD:{tech['macd_dif']}/{tech['macd_dea']}")

    # KDJ
    if tech.get('kdj_k') is not None and tech.get('kdj_d') is not None:
        kdj_str = f"KDJ:{tech['kdj_k']}/{tech['kdj_d']}"
        if tech.get('kdj_j') is not None:
            kdj_str += f"/J:{tech['kdj_j']}"
        parts.append(kdj_str)

    return " | ".join(parts) if parts else "暂无数据"


def get_score_class(score: int) -> str:
    """根据评分获取样式类"""
    if score >= 80:
        return "score-high"
    elif score >= 60:
        return "score-medium"
    return "score-low"


def get_strength_class(strength: str) -> str:
    """根据强度获取样式类"""
    return f"strength-{strength}"


# ============= 页面主函数 =============
def main():
    # 头部
    st.markdown("""
    <div class="signal-header">
        <h1>📡 个股信号监控</h1>
        <p>基于技术面分析生成左侧（抄底）和右侧（追涨）交易信号</p>
        <div style="margin-top: 10px; font-size: 12px; opacity: 0.9;">
            <span style="background: rgba(255,255,255,0.2); padding: 3px 10px; border-radius: 12px;">
                📊 价格数据已前复权处理
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 侧边栏筛选
    with st.sidebar:
        st.header("🔍 筛选条件")

        # 数据说明
        st.info("📊 当前显示的价格均为**前复权**数据", icon="📈")

        signal_type = st.radio(
            "信号类型",
            ["全部", "左侧信号", "右侧信号"],
            index=0
        )

        signal_type_map = {"全部": "all", "左侧信号": "left", "右侧信号": "right"}
        selected_type = signal_type_map[signal_type]

        # 加载数据（统一文件，通过 signal_type 筛选）
        data = load_signals()

        if data.get("status") == "success":
            all_signals = data.get("signals", [])

            # 根据左侧/右侧/全部进行筛选
            if selected_type != "all":
                signals = [s for s in all_signals if s.get("signal_type") == selected_type]
            else:
                signals = all_signals

            # 强度筛选
            strengths = list(set([s.get("strength", "medium") for s in signals]))
            if strengths:
                selected_strengths = st.multiselect(
                    "信号强度",
                    options=strengths,
                    default=strengths
                )
            else:
                selected_strengths = []

            # 评分筛选
            min_score = st.slider("最低评分", 0, 100, 50)

            # 信号名称筛选
            signal_names = list(set([s.get("signal_name", "") for s in signals]))
            if signal_names:
                selected_names = st.multiselect(
                    "信号类型",
                    options=signal_names,
                    default=[]
                )
        else:
            signals = []
            selected_strengths = []
            min_score = 0
            selected_names = []

        st.divider()
        st.info("""
        **使用说明**

        **左侧信号**（抄底/反转）
        - MACD/KDJ底背离
        - 超跌反弹
        - 缩量十字星
        - 长下影线

        **右侧信号**（追涨/确认）
        - 均线金叉
        - MACD/KDJ金叉
        - 量价突破
        - 均线多头排列

        **📏 均线走平指标**
        当单根均线（MA10/MA20/MA60）长期趋于水平直线时，代表价格围绕某个中枢长期稳定波动。这个"走平"的价格位置就是强支撑/阻力位。

        显示格式：`MAxx@价格`
        - 🔴红色：强烈走平（分数≥0.90）- 长期稳定中枢
        - 🟡黄色：较走平（分数≥0.80）- 中期稳定区间
        - 🔵蓝色：走平（分数≥0.75）- 短期参考位

        **📊 均线粘合指标**
        当多根均线纠缠在一起时，意味着市场在选择方向，一旦突破往往有大行情。
        """)

    # 主内容区
    if data.get("status") != "success":
        st.warning(data.get("message", "暂无数据"))
        st.info("请运行扫描器生成数据:\n```\npython ShortTerm/daily_signal/stock_signal_scanner.py\n```")
        return

    # 统计数据
    stats = data.get("stats", {})
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="stats-card">
            <div class="stats-number">{data.get('total_signals', 0)}</div>
            <div class="stats-label">总信号数</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="stats-card">
            <div class="stats-number">{stats.get('left', 0)}</div>
            <div class="stats-label">左侧信号</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="stats-card">
            <div class="stats-number">{stats.get('right', 0)}</div>
            <div class="stats-label">右侧信号</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        scan_time = data.get('scan_time', '未知')
        intraday_mode = data.get('intraday_mode', False)

        # 获取扫描时使用的实时数据时间（信号数据里的价格时间）
        price_fetch_time = data.get('price_fetch_time')

        if price_fetch_time:
            time_display = f"扫描: {scan_time}<br><span style='color:#ff6b6b'>● 价格: {price_fetch_time}</span>"
            label_text = "盘中监控" if intraday_mode else "扫描时间"
        else:
            # 没有实时数据，只显示扫描时间
            time_display = scan_time
            label_text = "扫描时间"

        st.markdown(f"""
        <div class="stats-card">
            <div class="stats-number" style="font-size: 14px;">{time_display}</div>
            <div class="stats-label">{label_text}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # 筛选信号
    filtered_signals = signals.copy()

    if selected_strengths:
        filtered_signals = [s for s in filtered_signals if s.get("strength") in selected_strengths]

    filtered_signals = [s for s in filtered_signals if s.get("score", 0) >= min_score]

    if selected_names:
        filtered_signals = [s for s in filtered_signals if s.get("signal_name") in selected_names]

    # 按股票分组聚合信号
    def group_signals_by_stock(signals_list):
        """将同一股票的多个信号聚合在一起"""
        stock_groups = {}
        for sig in signals_list:
            symbol = sig.get('symbol', '')
            if symbol not in stock_groups:
                stock_groups[symbol] = {
                    'symbol': symbol,
                    'name': sig.get('name', ''),
                    'signals': [],
                    'total_score': 0,
                    'periods': set(),
                    'types': set()
                }
            stock_groups[symbol]['signals'].append(sig)
            stock_groups[symbol]['periods'].add(sig.get('period', 'daily'))
            stock_groups[symbol]['types'].add(sig.get('signal_type', 'left'))

        # 计算每个股票的总分（使用后端计算的组合评分）
        for symbol, group in stock_groups.items():
            signals = group['signals']
            # 优先使用后端计算的组合评分 portfolio_score
            portfolio_scores = [
                s.get('technicals', {}).get('portfolio_score', 0)
                for s in signals
                if s.get('technicals', {}).get('portfolio_score')
            ]
            if portfolio_scores:
                # 使用组合评分（所有信号共享同一个 portfolio_score）
                group['total_score'] = round(max(portfolio_scores))
            else:
                # 回退到旧计算方式
                base_score = max([s.get('score', 0) for s in signals])
                extra_score = (len(signals) - 1) * 10
                if len(group['periods']) > 1:
                    extra_score += 15
                group['total_score'] = min(base_score + extra_score, 100)

            # 添加信号数量、质量集中度和维度覆盖率信息用于展示
            signal_count = len(signals)
            quality_conc = signals[0].get('technicals', {}).get('quality_concentration', 0)
            dim_coverage = signals[0].get('technicals', {}).get('dimension_coverage', 0)
            dim_details = signals[0].get('technicals', {}).get('dimension_details', {})
            group['signal_count'] = signal_count
            group['quality_concentration'] = quality_conc
            group['dimension_coverage'] = dim_coverage
            group['dimension_details'] = dim_details

        return stock_groups

    stock_groups = group_signals_by_stock(filtered_signals)
    grouped_stocks = list(stock_groups.values())

    # 显示信号列表
    st.subheader(f"信号列表 (共 {len(grouped_stocks)} 只股票, {len(filtered_signals)} 个信号)")

    if not filtered_signals:
        st.info("没有符合条件的信号")
        return

    # 排序选项
    sort_col1, sort_col2 = st.columns([1, 4])
    with sort_col1:
        sort_by = st.selectbox("排序方式", ["评分", "维度覆盖率", "质量集中度", "信号数量", "日期", "涨跌幅"], index=0)

    # 排序
    if sort_by == "评分":
        grouped_stocks.sort(key=lambda x: x['total_score'], reverse=True)
    elif sort_by == "维度覆盖率":
        grouped_stocks.sort(key=lambda x: x.get('dimension_coverage', 0), reverse=True)
    elif sort_by == "质量集中度":
        grouped_stocks.sort(key=lambda x: x.get('quality_concentration', 0), reverse=True)
    elif sort_by == "信号数量":
        # 适中数量(2-5个)排在前面
        grouped_stocks.sort(key=lambda x: (abs(len(x['signals']) - 3.5), len(x['signals'])))
    elif sort_by == "日期":
        grouped_stocks.sort(key=lambda x: max([s.get('trigger_date', '') for s in x['signals']]), reverse=True)
    elif sort_by == "涨跌幅":
        grouped_stocks.sort(key=lambda x: max([s.get('change_pct', 0) for s in x['signals']]), reverse=True)

    # 分页显示
    page_size = 20
    total_pages = (len(grouped_stocks) + page_size - 1) // page_size

    if total_pages > 1:
        page = st.number_input("页码", 1, total_pages, 1) - 1
    else:
        page = 0

    start_idx = page * page_size
    end_idx = min(start_idx + page_size, len(grouped_stocks))
    page_stocks = grouped_stocks[start_idx:end_idx]

    # 渲染股票卡片（包含多个信号）
    for idx, stock_group in enumerate(page_stocks):
        symbol = stock_group['symbol']
        stock_name = stock_group['name']
        signals = stock_group['signals']
        total_score = stock_group['total_score']
        periods = stock_group['periods']
        signal_types = stock_group['types']

        # 确定主导信号类型（左侧/右侧）
        dominant_type = 'right' if 'right' in signal_types else 'left'
        signal_type_class = f"signal-{dominant_type}"

        # 获取最新价格和涨跌幅
        latest_signal = max(signals, key=lambda x: x.get('trigger_date', ''))
        change_pct = latest_signal.get('change_pct', 0)
        change_color = "#ff4757" if change_pct > 0 else "#2ed573" if change_pct < 0 else "#333"
        change_symbol = "+" if change_pct > 0 else ""

        # 获取最新价格和数据日期（底层接口默认前复权）
        latest_price = load_stock_latest_price(symbol)
        price_display = f"¥{latest_price:.2f}" if latest_price else "-"
        latest_data_date = load_stock_latest_date(symbol) or '未知'

        # 获取扫描时使用的实时数据时间（信号数据里的价格时间）
        price_fetch_time = data.get('price_fetch_time')

        # 显示价格时间（优先使用信号扫描时的价格时间）
        if price_fetch_time:
            time_display = f"⏱️ {price_fetch_time}"
            time_color = "#ff6b6b"  # 红色表示盘中
            time_tooltip = "价格最新时间"
        else:
            time_display = f"📅 {latest_data_date}"
            time_color = "#888"  # 灰色表示历史
            time_tooltip = "历史数据日期"

        # 周期标签
        period_tags = []
        period_colors = {'daily': '#3498db', 'weekly': '#9b59b6', 'monthly': '#e74c3c'}
        for p in sorted(periods):
            p_name = {'daily': '日', 'weekly': '周', 'monthly': '月'}.get(p, p)
            p_color = period_colors.get(p, '#666')
            period_tags.append(f'<span style="background: {p_color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; margin-right: 5px;">{p_name}</span>')
        period_html = ''.join(period_tags)

        # 多周期共振标识
        resonance_badge = ""
        if len(periods) >= 2:
            resonance_badge = '<span style="background: linear-gradient(135deg, #ff6b6b, #feca57); color: white; padding: 2px 8px; border-radius: 12px; font-size: 11px; margin-left: 8px;">🔥 多周期共振</span>'

        # 使用容器卡片
        with st.container():
            col1, col2 = st.columns([6, 1])

            with col1:
                # 股票名称和数据日期同一行左右分布
                name_col, date_col = st.columns([3, 1])
                with name_col:
                    # 点击股票名称跳转图表页面
                    if st.button(
                        f"📈 {stock_name} ({symbol})",
                        key=f"btn_stock_{symbol}_{idx}_{page}",
                        help=f"点击跳转到 {symbol} K线图",
                        type="primary"
                    ):
                        st.session_state['selected_stock'] = symbol
                        st.session_state['selected_name'] = stock_name
                        st.switch_page("pages/stock_chart.py")
                with date_col:
                    # 价格时间右对齐（优先显示盘中时间）
                    st.markdown(f'<div style="text-align: right; font-size: 11px; color: {time_color}; margin-top: 8px;"><span style="font-size: 10px; opacity: 0.7;">{time_tooltip}</span>: {time_display}</div>', unsafe_allow_html=True)

                # 周期标签和共振标识
                st.markdown(f'<div style="margin: 5px 0;">{period_html}{resonance_badge}</div>', unsafe_allow_html=True)

                # 使用模块化信号卡片显示所有信号
                for sig in sorted(signals, key=lambda x: ({'daily': 0, 'weekly': 1, 'monthly': 2}.get(x.get('period', 'daily'), 0), -x.get('score', 0))):
                    st.markdown(render_signal_card(sig, idx), unsafe_allow_html=True)

            with col2:
                # 总评分徽章
                score_bg = "#ff6b6b" if total_score >= 80 else "#feca57" if total_score >= 60 else "#dfe6e9"
                score_color = "white" if total_score >= 80 else "#333"

                # 信号数量标签颜色
                signal_count = stock_group.get('signal_count', len(signals))
                if signal_count <= 4:
                    count_color = "#27ae60"  # 绿色 - 适中
                    count_label = "适中"
                elif signal_count <= 6:
                    count_color = "#f39c12"  # 橙色 - 偏多
                    count_label = "偏多"
                else:
                    count_color = "#e74c3c"  # 红色 - 过多
                    count_label = "过多"

                # 质量集中度
                quality_conc = stock_group.get('quality_concentration', 0)
                quality_pct = int(quality_conc * 100)

                # 维度覆盖率（核心指标）
                dim_coverage = stock_group.get('dimension_coverage', 0)
                dim_pct = int(dim_coverage * 100)
                dim_details = stock_group.get('dimension_details', {})

                # 维度详情文本
                dim_text = []
                if dim_details.get('directions'):
                    dirs = dim_details['directions']
                    if len(dirs) >= 2:
                        dim_text.append("双侧")
                    else:
                        dim_text.append(dirs[0][:1].upper())
                if dim_details.get('periods'):
                    periods = dim_details['periods']
                    if len(periods) >= 2:
                        dim_text.append(f"{len(periods)}周期")
                if dim_details.get('indicators'):
                    n_indicators = len(dim_details['indicators'])
                    if n_indicators >= 2:
                        dim_text.append(f"{n_indicators}指标")

                dim_badge = " | ".join(dim_text) if dim_text else "单维度"

                # 维度覆盖率颜色
                if dim_pct >= 75:
                    dim_color = "#27ae60"  # 绿色 - 高覆盖
                elif dim_pct >= 50:
                    dim_color = "#f39c12"  # 橙色 - 中等
                else:
                    dim_color = "#e74c3c"  # 红色 - 低覆盖

                st.markdown(f"""
                <div style="
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                ">
                    <div style="
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        width: 50px;
                        height: 50px;
                        border-radius: 50%;
                        background: {score_bg};
                        color: {score_color};
                        font-weight: bold;
                        font-size: 16px;
                    ">{total_score}</div>
                    <div style="font-size: 11px; color: #888; margin-top: 5px;">组合评分</div>
                    <div style="font-size: 10px; color: {dim_color}; margin-top: 4px; font-weight: 600;">
                        维度覆盖: {dim_pct}% [{dim_badge}]
                    </div>
                    <div style="font-size: 10px; color: {count_color}; margin-top: 2px; font-weight: 500;">
                        {signal_count}个信号({count_label})
                    </div>
                    <div style="font-size: 10px; color: #666; margin-top: 2px;">
                        质量集中度: {quality_pct}%
                    </div>
                    <div style="font-size: 12px; color: {change_color}; margin-top: 6px;">{change_symbol}{change_pct}%</div>
                    <div style="font-size: 14px; color: #333; margin-top: 6px; font-weight: 600;">{price_display}</div>
                    <div style="font-size: 10px; color: #999; margin-top: 2px;">前复权</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("---")

    # 分页信息
    if total_pages > 1:
        st.caption(f"显示第 {start_idx+1}-{end_idx} 只股票，共 {len(grouped_stocks)} 只，总信号数 {len(filtered_signals)} 个")

    # 底部说明
    st.markdown("---")
    with st.expander("📊 信号类型说明"):
        st.markdown("""
        ### 左侧信号（抄底/反转）
        | 信号名称 | 说明 | 适用场景 |
        |---------|------|---------|
        | MACD底背离 | 价格创新低但MACD未创新低 | 中长期底部 |
        | KDJ底背离 | 价格创新低但KDJ未创新低 | 短期反弹 |
        | 超跌反弹 | 股价大幅偏离MA60，出现企稳K线 | 超跌修复 |
        | 缩量十字星 | 下跌后出现缩量十字星 | 企稳信号 |
        | 长下影线 | 出现长下影线，下方有支撑 | 支撑确认 |

        ### 右侧信号（追涨/确认）
        | 信号名称 | 说明 | 适用场景 |
        |---------|------|---------|
        | MA5金叉MA10 | 短期均线上穿 | 短线转强 |
        | MA5金叉MA20 | 短期上穿中期均线 | 趋势转强 |
        | MACD金叉 | DIF上穿DEA | 动量增强 |
        | KDJ金叉 | K线上穿D线 | 短期动能 |
        | 量价突破 | 放量上涨突破 | 资金入场 |
        | 均线多头排列 | MA5>MA10>MA20 | 趋势良好 |
        | 突破平台 | 放量突破整理平台 | 主升浪 |
        """)


if __name__ == "__main__":
    main()

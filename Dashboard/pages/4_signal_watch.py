"""
个股信号监控页面 - 展示左侧/右侧交易信号和风险分

读取 ShortTerm/services/stock_signal_scanner.py 生成的信号数据
"""

import streamlit as st
import json
from pathlib import Path
from datetime import datetime

# 导入共享格式化工具
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.scoring import calculate_stock_score
from utils.signal_components import (
    calculate_risk_score, render_signal_list, render_risk_assessment
)

# 导入底层数据接口
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from DataHub.core.data_reader import load_stock_latest_date
from DataHub.config import get_storage_path

# 设置页面
st.set_page_config(
    page_title="个股信号监控",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

BASE_DIR = Path(__file__).parent.parent.parent

# ============= 样式 =============
st.markdown("""
<style>
    .page-header {
        padding: 15px 0;
        margin-bottom: 15px;
        border-bottom: 1px solid #eee;
    }
    .page-title {
        font-size: 22px;
        font-weight: 600;
        color: #333;
        margin-bottom: 5px;
    }
    .page-meta {
        font-size: 12px;
        color: #888;
    }
    .toolbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 10px 0;
        margin-bottom: 10px;
    }
    .filter-group {
        display: flex;
        gap: 15px;
        align-items: center;
    }
    .list-container {
        background: white;
        border-radius: 8px;
        border: 1px solid #e9ecef;
        overflow: hidden;
    }
    .list-header {
        display: grid;
        grid-template-columns: 2fr 1fr 1fr 1.5fr 0.8fr;
        padding: 12px 20px;
        background: #f8f9fa;
        font-size: 12px;
        font-weight: 600;
        color: #666;
        border-bottom: 1px solid #e9ecef;
    }
    .stock-row {
        display: grid;
        grid-template-columns: 2fr 1fr 1fr 1.5fr 0.8fr;
        padding: 14px 20px;
        border-bottom: 1px solid #f0f0f0;
        align-items: center;
        transition: background 0.15s;
        text-decoration: none;
        color: inherit;
    }
    .stock-row:hover {
        background: #f8f9fa;
    }
    .stock-row:last-child {
        border-bottom: none;
    }
    .stock-info {
        display: flex;
        flex-direction: column;
        gap: 3px;
    }
    .stock-name {
        font-size: 14px;
        font-weight: 600;
        color: #333;
    }
    .stock-code {
        font-size: 11px;
        color: #888;
    }
    .price-info {
        display: flex;
        flex-direction: column;
        gap: 2px;
    }
    .stock-price {
        font-size: 14px;
        font-weight: 500;
        color: #333;
    }
    .stock-change {
        font-size: 12px;
        font-weight: 500;
    }
    .score-cell {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .score-value {
        font-size: 15px;
        font-weight: 700;
        width: 32px;
        height: 32px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 6px;
        color: white;
    }
    .score-high { background: #27ae60; }
    .score-medium { background: #f39c12; }
    .score-low { background: #e74c3c; }
    .score-detail {
        font-size: 11px;
        color: #888;
    }
    .signals-preview {
        font-size: 12px;
        color: #666;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .signals-count {
        font-size: 11px;
        color: #888;
        background: #f0f0f0;
        padding: 2px 8px;
        border-radius: 10px;
        display: inline-block;
    }
    .pagination {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 20px;
        padding: 20px 0;
    }
    .empty-state {
        text-align: center;
        padding: 60px 20px;
        color: #888;
    }
    /* 覆盖 Streamlit 默认的 p 标签 margin */
    div[data-testid="stVerticalBlock"] p {
        margin-bottom: 0 !important;
    }
</style>
""", unsafe_allow_html=True)


# ============= 数据加载 =============



def _transform_signals_to_stocks(data: dict) -> list:
    """将信号列表转换为股票列表"""
    # 如果数据已经是股票格式，直接返回
    if "stocks" in data and data.get("stocks"):
        stocks = data["stocks"]
        # 确保每个股票都有风险解释
        for stock in stocks:
            if "risk_explanations" not in stock:
                warnings = stock.get("risk_warnings", [])
                stock["risk_explanations"] = warnings if warnings else ["暂无风险详情"]
        return stocks

    # 从信号列表转换
    signals = data.get("signals", [])
    if not signals:
        return []

    # 按股票分组
    stock_map = {}
    for sig in signals:
        symbol = sig.get("symbol")
        if not symbol:
            continue

        if symbol not in stock_map:
            stock_map[symbol] = {
                "symbol": symbol,
                "name": sig.get("name", ""),
                "signals": [],
                "signal_score": 0,
                "risk_score": 50,
                "risk_explanations": [],
                "has_buy_signal": False,
                "signal_count": 0
            }

        stock_map[symbol]["signals"].append(sig)
        stock_map[symbol]["has_buy_signal"] = True
        stock_map[symbol]["signal_count"] += 1

    # 计算每个股票的信号分和风险分
    for symbol, stock in stock_map.items():
        all_signals = stock["signals"]

        # 使用统一的综合评分算法计算信号分
        change_pct = all_signals[0].get("change_pct", 0) if all_signals else 0
        stock["signal_score"] = calculate_stock_score(all_signals, change_pct)

        # 取最高分信号的技术指标用于风险计算
        best_signal = max(all_signals, key=lambda x: x.get("score", 0)) if all_signals else {}
        tech = best_signal.get("technicals", {})

        risk_score, risk_explanations = calculate_risk_score(tech, all_signals)
        stock["risk_score"] = risk_score
        stock["risk_explanations"] = risk_explanations

    return list(stock_map.values())


def _load_data_impl(asset_type: str = "stock") -> dict:
    """加载信号数据"""
    prefix = "etf_signals" if asset_type == "etf" else "index_signals" if asset_type == "index" else "stock_signals"
    filepath = get_storage_path("outputs", "signals", f"{prefix}_latest.json")

    if not filepath.exists():
        signals_dir = get_storage_path("outputs", "signals")
        if signals_dir.exists():
            files = sorted(signals_dir.glob(f"{prefix}_*.json"))
            date_files = [f for f in files if not f.name.endswith("_latest.json")]
            if date_files:
                filepath = date_files[-1]

    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 转换信号格式为股票格式
            if "signals" in data and "stocks" not in data:
                data["stocks"] = _transform_signals_to_stocks(data)
            return data

    asset_name = "ETF" if asset_type == "etf" else "指数" if asset_type == "index" else "股票"
    return {"status": "error", "message": f"暂无{asset_name}数据"}


@st.cache_data(ttl=60)
def _load_data_cached(asset_type: str = "stock") -> dict:
    return _load_data_impl(asset_type)


def load_data(asset_type: str = "stock") -> dict:
    """加载数据"""
    try:
        mode = st.secrets.get("environment", {}).get("mode", "prod")
        is_dev = mode == "dev"
    except Exception:
        is_dev = False

    return _load_data_impl(asset_type) if is_dev else _load_data_cached(asset_type)


# ============= 页面主函数 =============
def main():
    # URL参数
    query_params = st.query_params
    url_type = query_params.get("type", "stock").lower()
    if url_type not in ["stock", "etf", "index"]:
        url_type = "stock"

    # Session state
    if "asset_type" not in st.session_state:
        st.session_state.asset_type = {"stock": "股票", "etf": "ETF", "index": "指数"}.get(url_type, "股票")

    # 侧边栏
    with st.sidebar:
        st.header("📊 资产类型")

        def on_asset_change():
            new_type = st.session_state.asset_type
            selected = {"股票": "stock", "ETF": "etf", "指数": "index"}[new_type]
            st.query_params["type"] = selected

        asset_options = ["股票", "ETF", "指数"]
        current_index = asset_options.index(st.session_state.asset_type) if st.session_state.asset_type in asset_options else 0

        asset_type = st.radio(
            "选择监控对象",
            asset_options,
            index=current_index,
            key="asset_type",
            on_change=on_asset_change
        )
        selected_asset = {"股票": "stock", "ETF": "etf", "指数": "index"}[asset_type]
        st.divider()

        # 数据加载
        data = load_data(selected_asset)

        if data.get("status") != "error":
            stocks = data.get("stocks", [])
        else:
            stocks = []

        # 🔴 风险控制（一票否决）
        st.header("🔴 风险控制")
        st.caption("先排除高危股票，保命第一")

        risk_filter = st.selectbox(
            "风险筛选",
            ["显示全部", "仅低风险（风险分<40）", "仅中低风险（风险分<60）", "仅高风险（风险分≥60）"],
            index=1,  # 默认低风险
            key="risk_filter"
        )

        st.divider()

        # 📈 快速筛选
        st.header("📈 快速筛选")
        quick_filter = st.radio(
            "显示范围",
            ["全部", "仅信号股（有买入信号）"],
            index=0,
            key="quick_filter"
        )

    # 主内容区
    if data.get("status") == "error":
        st.warning(data.get("message", "暂无数据"))
        return

    # 筛选 - 🔴 风险控制（一票否决）
    filtered_stocks = stocks.copy()

    # 风险筛选
    if risk_filter == "仅低风险（风险分<40）":
        filtered_stocks = [s for s in filtered_stocks if s['risk_score'] < 40]
    elif risk_filter == "仅中低风险（风险分<60）":
        filtered_stocks = [s for s in filtered_stocks if s['risk_score'] < 60]
    elif risk_filter == "仅高风险（风险分≥60）":
        filtered_stocks = [s for s in filtered_stocks if s['risk_score'] >= 60]

    # 快速筛选 - 仅显示有信号的股票
    if quick_filter == "仅信号股（有买入信号）":
        filtered_stocks = [s for s in filtered_stocks if s['has_buy_signal']]

    # 页面标题
    title_map = {"stock": "个股", "etf": "ETF", "index": "指数"}
    scan_time = data.get("scan_time", "")

    st.markdown(f"""
    <div class="page-header">
        <div>
            <div class="page-title">📡 {title_map[selected_asset]}信号监控</div>
            <div class="page-meta">共 {len(filtered_stocks)} 只股票 · {sum(s['signal_count'] for s in filtered_stocks)} 个信号 · 更新于 {scan_time}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 分页设置
    page_size = 50
    total_pages = (len(filtered_stocks) + page_size - 1) // page_size
    
    # 初始化页码到 session_state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 0
    
    # 📈 信号排序（择优录取）- 在通过风险筛选的股票中排序
    st.markdown("<div style='font-size: 12px; color: #888; margin-bottom: 4px;'>📈 在通过风险筛选的股票中，按以下方式排序：</div>", unsafe_allow_html=True)
    
    # 排序和分页放在同一行
    toolbar_cols = st.columns([1, 2])
    
    with toolbar_cols[0]:
        sort_by = st.selectbox(
            "排序方式",
            ["风险分(低到高)", "风险分(高到低)", "信号分(高到低)", "信号分(低到高)"],
            index=2,  # 默认信号分高到低
            key="sort_by",
            label_visibility="collapsed"
        )
    
    with toolbar_cols[1]:
        if total_pages > 1:
            # 页码输入和总页数显示放在一行
            page_cols = st.columns([1, 3])
            with page_cols[0]:
                page_input = st.number_input(
                    "页码",
                    min_value=1,
                    max_value=total_pages,
                    value=st.session_state.current_page + 1,
                    key="page_input",
                    label_visibility="collapsed"
                )
                page = page_input - 1
                st.session_state.current_page = page
            with page_cols[1]:
                st.markdown(f"<div style='padding-top: 8px; font-size: 14px; color: #666;'>/ {total_pages}</div>", unsafe_allow_html=True)
        else:
            page = 0
    
    # 排序
    if sort_by == "风险分(低到高)":
        filtered_stocks.sort(key=lambda x: x['risk_score'])
    elif sort_by == "风险分(高到低)":
        filtered_stocks.sort(key=lambda x: x['risk_score'], reverse=True)
    elif sort_by == "信号分(高到低)":
        filtered_stocks.sort(key=lambda x: x['signal_score'], reverse=True)
    elif sort_by == "信号分(低到高)":
        filtered_stocks.sort(key=lambda x: x['signal_score'])

    # 显示股票列表
    if not filtered_stocks:
        st.markdown('<div class="empty-state">没有符合条件的股票</div>', unsafe_allow_html=True)
        return

    start_idx = page * page_size
    end_idx = min(start_idx + page_size, len(filtered_stocks))
    page_stocks = filtered_stocks[start_idx:end_idx]

    # 列表头部
    header_cols = st.columns([2, 1, 1, 1, 0.5])
    with header_cols[0]:
        st.markdown("**股票**")
    with header_cols[1]:
        st.markdown("**价格/涨跌**")
    with header_cols[2]:
        st.markdown("**信号分**")
    with header_cols[3]:
        st.markdown("**风险分**")
    with header_cols[4]:
        st.markdown("**详情**")

    st.markdown("<hr style='margin: 0; border-color: #e9ecef;'>", unsafe_allow_html=True)

    # 使用 fragment 优化展开/折叠性能 - 将当前页数据存入 session_state
    st.session_state['_current_page_stocks'] = page_stocks
    st.session_state['_current_page_info'] = {'page': page, 'total_pages': total_pages, 'total': len(filtered_stocks)}
    _render_stock_rows()


@st.fragment
def _render_stock_rows():
    """渲染股票行（使用 fragment 优化展开/折叠性能，点击按钮只刷新此部分）"""
    page_stocks = st.session_state.get('_current_page_stocks', [])
    page_info = st.session_state.get('_current_page_info', {'page': 0, 'total_pages': 1, 'total': 0})

    # 股票行（带折叠详情）
    for stock in page_stocks:
        symbol = stock['symbol']
        name = stock['name']
        signal_score = stock['signal_score']
        risk_score = stock['risk_score']
        signals = stock.get('signals', [])
        risk_explanations = stock.get('risk_explanations', [])

        latest_signal = signals[0] if signals else {}
        close_price = latest_signal.get('close_price', 0)
        change_pct = latest_signal.get('change_pct', 0) if latest_signal else 0

        # 涨跌幅颜色
        if change_pct > 0:
            change_color = "#ff4757"
            change_str = f"+{change_pct:.2f}%"
        elif change_pct < 0:
            change_color = "#2ed573"
            change_str = f"{change_pct:.2f}%"
        else:
            change_color = "#888"
            change_str = "0.00%"

        # 分数样式
        if signal_score >= 60:
            sig_color = "#27ae60"
        elif signal_score >= 40:
            sig_color = "#f39c12"
        else:
            sig_color = "#e74c3c"

        if risk_score < 40:
            risk_color = "#27ae60"
        elif risk_score < 70:
            risk_color = "#f39c12"
        else:
            risk_color = "#e74c3c"

        # 生成唯一 key
        stock_key = symbol.replace(".", "_")
        expand_key = f"expand_{stock_key}"

        # 初始化展开状态
        if expand_key not in st.session_state:
            st.session_state[expand_key] = False

        # 显示股票行
        cols = st.columns([2, 1, 1, 1, 0.5])

        with cols[0]:
            # 股票名称点击跳转
            st.markdown(f"""
            <div style="line-height: 1.2;">
                <a href="/stock_chart?symbol={symbol}" target="_self"
                   style="text-decoration: none; color: #333; font-weight: 600;">
                    {name}
                </a>
                <div style="font-size: 11px; color: #888; line-height: 1.2;">{symbol}</div>
            </div>
            """, unsafe_allow_html=True)

        with cols[1]:
            st.markdown(f"""
            <div style="font-size: 14px; color: #333;">¥{close_price:.2f}</div>
            <div style="font-size: 12px; color: {change_color};">{change_str}</div>
            """, unsafe_allow_html=True)

        with cols[2]:
            st.markdown(f"""
            <div style="display: inline-block; background: {sig_color}; color: white;
                        padding: 3px 10px; border-radius: 4px; font-weight: 600; font-size: 13px;">
                {signal_score}
            </div>
            """, unsafe_allow_html=True)

        with cols[3]:
            st.markdown(f"""
            <div style="display: inline-block; background: {risk_color}; color: white;
                        padding: 3px 10px; border-radius: 4px; font-weight: 600; font-size: 13px;">
                {risk_score}
            </div>
            """, unsafe_allow_html=True)

        with cols[4]:
            # 展开/折叠按钮
            btn_label = "▼" if st.session_state[expand_key] else "▶"
            if st.button(btn_label, key=f"btn_{stock_key}", help="展开/折叠详情"):
                st.session_state[expand_key] = not st.session_state[expand_key]
                st.rerun()

        # 展开时显示详情 - 使用与列表行相同的列宽比例对齐
        if st.session_state[expand_key]:
            # 使用 [2, 1, 1, 0.5] 比例，信号详情对齐信号分列，风险评估对齐风险分列
            expand_cols = st.columns([2, 1, 1, 0.5])

            # 添加展开区域的内边距容器
            st.markdown("<div style='padding: 10px 0 15px 0;'>", unsafe_allow_html=True)

            with expand_cols[0]:
                pass  # 股票列下方留空，保持对齐

            with expand_cols[1]:
                render_signal_list(signals)

            with expand_cols[2]:
                render_risk_assessment(risk_score, risk_explanations)

            # 关闭内边距容器
            st.markdown("</div>", unsafe_allow_html=True)

    # 分页控件
    page_info = st.session_state.get('_current_page_info', {'page': 0, 'total_pages': 1, 'total': 0})
    if page_info['total_pages'] > 1:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px 0; color: #666; font-size: 13px;">
            第 {page_info['page'] + 1} / {page_info['total_pages']} 页 · 共 {page_info['total']} 条
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

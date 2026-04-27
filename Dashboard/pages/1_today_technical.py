"""
今日技术面页面 - 读取 Scanner 生成的 JSON 数据
对应 ShortTerm/services 模块
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
sys.path.insert(0, os.path.join(BASE_DIR, "Dashboard"))

# 导入股票代码工具和配置
from lib.utils import StockCodeUtil, get_stock_name
from DataHub.config import get_storage_path

# JSON 文件路径（使用环境变量配置的 storage 路径）
OUTPUT_DIR = get_storage_path("outputs", "shortterm", "technical_overview")
JSON_FILE = OUTPUT_DIR / "latest.json"


def load_signals_data() -> dict:
    """加载 Scanner 生成的 JSON 数据"""
    try:
        if JSON_FILE.exists():
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
    page_title="今日技术面 - 秦项投资量化",
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
    st.error("❌ 无法加载数据文件，请确保已运行 `python ShortTerm/run_today_technical.py`")
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

# 提取宏观指标
macro_indicators = data.get('macro_indicators', {})
currency = macro_indicators.get('currency', {})
dxy = macro_indicators.get('dxy', {})
oil = macro_indicators.get('oil', {})
gold = macro_indicators.get('gold', {})

# 提取涨跌停数据（优先从 technical_indicators 获取）
zt_sentiment = tech_indicators.get('zt_sentiment', {})
dt_sentiment = tech_indicators.get('dt_sentiment', {})
zt_count = zt_sentiment.get('zt_count', data.get('zt_count', 0))
dt_count = dt_sentiment.get('dt_count', data.get('dt_count', 0))

# 从JSON获取指数历史数据（由后台脚本生成）
index_history_daily = data.get('index_history', {})
index_history_weekly = data.get('index_history_weekly', {})
index_history_monthly = data.get('index_history_monthly', {})

# ============= 页面标题 =============
market_close_time = data.get('market_close_time', date)
data_status = data.get('data_status', '实时计算')
st.title("🔥 今日技术面")
st.caption(f"涨停板扫描 | 板块热度分析 | 市场状态监控 | 数据时间: {market_close_time} [{data_status}] | 生成时间: {generated_at}")

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

# ============= 宏观指标卡片 =============
st.markdown("### 🌍 宏观指标")
macro_col1, macro_col2, macro_col3, macro_col4 = st.columns(4)

with macro_col1:
    # 人民币兑美元
    cny_current = currency.get('current', 0)
    cny_change = currency.get('change_pct', 0)
    if cny_current > 0:
        cny_delta = f"{cny_change:+.2f}%"
        cny_color = "inverse" if cny_change > 0 else "normal"  # 人民币升值(数值下降)是利好
    else:
        cny_delta = "暂无数据"
        cny_color = "off"
    st.metric("💱 离岸人民币", f"{cny_current:.4f}" if cny_current > 0 else "--", cny_delta, delta_color=cny_color)

with macro_col2:
    # 美元指数
    dxy_current = dxy.get('current', 0)
    dxy_change = dxy.get('change_pct', 0)
    if dxy_current > 0:
        dxy_delta = f"{dxy_change:+.2f}%"
        dxy_color = "normal" if dxy_change < 0 else "inverse"  # 美元指数下跌是利好A股
    else:
        dxy_delta = "暂无数据"
        dxy_color = "off"
    st.metric("📊 美元指数", f"{dxy_current:.2f}" if dxy_current > 0 else "--", dxy_delta, delta_color=dxy_color)

with macro_col3:
    # 原油价格
    oil_current = oil.get('current', 0)
    oil_change = oil.get('change_pct', 0)
    oil_type = oil.get('type', 'WTI原油')
    if oil_current > 0:
        oil_delta = f"{oil_change:+.2f}%"
        oil_color = "inverse" if oil_change > 3 else "normal" if oil_change < -3 else "off"
    else:
        oil_delta = "暂无数据"
        oil_color = "off"
    st.metric(f"🛢️ {oil_type}", f"${oil_current:.2f}" if oil_current > 0 else "--", oil_delta, delta_color=oil_color)

with macro_col4:
    # 黄金价格
    gold_current = gold.get('current', 0)
    gold_change = gold.get('change_pct', 0)
    if gold_current > 0:
        gold_delta = f"{gold_change:+.2f}%"
        gold_color = "normal" if gold_change > 0 else "inverse"
    else:
        gold_delta = "暂无数据"
        gold_color = "off"
    st.metric("🥇 黄金价格", f"${gold_current:.2f}" if gold_current > 0 else "--", gold_delta, delta_color=gold_color)

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

# 第二行：指数表现与图表
st.markdown("**主要指数表现**")
# 过滤掉深证成指
filtered_index_performance = {k: v for k, v in index_performance.items() if k != '深证成指'}
if filtered_index_performance:
    # 每行显示4个指数，避免拥挤
    items = list(filtered_index_performance.items())
    cols_per_row = 4

    for i in range(0, len(items), cols_per_row):
        row_items = items[i:i + cols_per_row]
        idx_cols = st.columns(cols_per_row)

        for idx_col, (name, idx_data) in zip(idx_cols, row_items):
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

# ============= 指数图表函数 =============
def create_index_chart_html(hist_data: list, name: str, is_intraday: bool = False) -> str:
    """生成指数 Lightweight Charts HTML"""
    import json

    df = pd.DataFrame(hist_data)
    if df.empty:
        return "<div>数据为空</div>"

    if is_intraday:
        # 分时数据
        if 'time' not in df.columns or 'price' not in df.columns:
            return "<div>分时数据不完整</div>"

        df['time'] = pd.to_datetime(df['time'])

        # 准备数据
        line_data = []
        for _, row in df.iterrows():
            timestamp = int(row['time'].timestamp())
            line_data.append({
                'time': timestamp,
                'value': round(float(row['price']), 2)
            })

        line_json = json.dumps(line_data)

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
            <style>
                body {{ margin: 0; padding: 0; background: #ffffff; }}
                #chart-container {{ width: 100%; height: 280px; }}
            </style>
        </head>
        <body>
            <div id="chart-container"></div>
            <script>
                const chart = LightweightCharts.createChart(document.getElementById('chart-container'), {{
                    layout: {{ background: {{ type: 'solid', color: '#ffffff' }}, textColor: '#333' }},
                    grid: {{ vertLines: {{ color: '#f0f0f0' }}, horzLines: {{ color: '#f0f0f0' }} }},
                    crosshair: {{ mode: LightweightCharts.CrosshairMode.Magnet }},
                    rightPriceScale: {{ borderColor: '#e0e0e0' }},
                    timeScale: {{ borderColor: '#e0e0e0', timeVisible: true }},
                }});

                const lineSeries = chart.addLineSeries({{
                    color: '#2196F3',
                    lineWidth: 2,
                }});
                lineSeries.setData({line_json});

                chart.timeScale().fitContent();
            </script>
        </body>
        </html>
        """
    else:
        # 日线数据 - 使用K线图
        if 'date' not in df.columns or 'close' not in df.columns:
            return "<div>日线数据不完整</div>"

        df['date'] = pd.to_datetime(df['date'])

        # 计算均线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()

        # 准备K线数据
        candles = []
        for _, row in df.iterrows():
            timestamp = int(row['date'].timestamp())
            candles.append({
                'time': timestamp,
                'open': round(float(row['open']), 2) if 'open' in row and pd.notna(row['open']) else round(float(row['close']), 2),
                'high': round(float(row['high']), 2) if 'high' in row and pd.notna(row['high']) else round(float(row['close']), 2),
                'low': round(float(row['low']), 2) if 'low' in row and pd.notna(row['low']) else round(float(row['close']), 2),
                'close': round(float(row['close']), 2)
            })

        # 准备均线数据
        ma5_data = []
        ma10_data = []
        ma20_data = []

        for _, row in df.iterrows():
            timestamp = int(row['date'].timestamp())
            if pd.notna(row['ma5']):
                ma5_data.append({'time': timestamp, 'value': round(row['ma5'], 2)})
            if pd.notna(row['ma10']):
                ma10_data.append({'time': timestamp, 'value': round(row['ma10'], 2)})
            if pd.notna(row['ma20']):
                ma20_data.append({'time': timestamp, 'value': round(row['ma20'], 2)})

        candles_json = json.dumps(candles)
        ma5_json = json.dumps(ma5_data)
        ma10_json = json.dumps(ma10_data)
        ma20_json = json.dumps(ma20_data)

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
            <style>
                body {{ margin: 0; padding: 0; background: #ffffff; }}
                #chart-container {{ width: 100%; height: 280px; }}
            </style>
        </head>
        <body>
            <div id="chart-container"></div>
            <script>
                const chart = LightweightCharts.createChart(document.getElementById('chart-container'), {{
                    layout: {{ background: {{ type: 'solid', color: '#ffffff' }}, textColor: '#333' }},
                    grid: {{ vertLines: {{ color: '#f0f0f0' }}, horzLines: {{ color: '#f0f0f0' }} }},
                    crosshair: {{ mode: LightweightCharts.CrosshairMode.Magnet }},
                    rightPriceScale: {{ borderColor: '#e0e0e0' }},
                    timeScale: {{ borderColor: '#e0e0e0', timeVisible: false }},
                }});

                // K线系列
                const candleSeries = chart.addCandlestickSeries({{
                    upColor: '#ff4757', downColor: '#2ed573',
                    borderUpColor: '#ff4757', borderDownColor: '#2ed573',
                    wickUpColor: '#ff4757', wickDownColor: '#2ed573',
                }});
                candleSeries.setData({candles_json});

                // 均线
                const ma5Series = chart.addLineSeries({{ color: '#000000', lineWidth: 1 }});
                ma5Series.setData({ma5_json});

                const ma10Series = chart.addLineSeries({{ color: '#f1c40f', lineWidth: 1 }});
                ma10Series.setData({ma10_json});

                const ma20Series = chart.addLineSeries({{ color: '#9b59b6', lineWidth: 1 }});
                ma20Series.setData({ma20_json});

                chart.timeScale().fitContent();
            </script>
        </body>
        </html>
        """

    return html


# 指数名称到代码的映射（用于跳转链接）
INDEX_CODE_MAP = {
    '上证指数': '000001.SH',
    '创业板指': '399006.SZ',
    '沪深300': '000300.SH',
    '上证50': '000016.SH',
    '中证500': '000905.SH',
    '中证1000': '000852.SH',
}

# 过滤掉深证成指
filtered_index_history = {k: v for k, v in index_history_daily.items() if k != '深证成指'}

# 获取主要指数的技术分析数据（优先沪深300，其次上证）
main_index = None
main_index_name = None
for name in ['沪深300', '上证指数']:
    if name in filtered_index_performance:
        idx_data = filtered_index_performance[name]
        analysis = idx_data.get('analysis', {})
        if analysis and 'daily' in analysis and 'dow_theory' in analysis['daily']:
            main_index = idx_data
            main_index_name = name
            break

PERIODS = [('日线', 'daily'), ('周线', 'weekly'), ('月线', 'monthly')]
trend_emoji = {'BULL': '🟢', 'BEAR': '🔴', 'SIDEWAYS': '🟡', 'UNKNOWN': '⚪'}

# ============= 页面级三周期 Tab =============
st.markdown("### 📈 指数走势 & 技术分析")

tabs = st.tabs([f"📊 {label}" for label, _ in PERIODS])
for tab, (label, key) in zip(tabs, PERIODS):
    with tab:
        # ---------- 指数走势图 ----------
        if filtered_index_history:
            chart_cols = st.columns(2)
            col_idx = 0
            for name in filtered_index_history.keys():
                with chart_cols[col_idx % 2]:
                    index_code = INDEX_CODE_MAP.get(name, '')
                    if index_code:
                        st.markdown(f"**[{name}](/stock_chart?symbol={index_code})**", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{name}**")

                    if label == "日线":
                        hist_data = index_history_daily.get(name, [])
                    elif label == "周线":
                        hist_data = index_history_weekly.get(name, [])
                    else:
                        hist_data = index_history_monthly.get(name, [])

                    if hist_data:
                        chart_html = create_index_chart_html(hist_data, name, is_intraday=False)
                        st.components.v1.html(chart_html, height=280, scrolling=False)
                    else:
                        st.caption(f"{name} 暂无{label}数据")

                    # 极值点信息
                    idx_data = filtered_index_performance.get(name, {})
                    analysis = idx_data.get('analysis', {})
                    period_data = analysis.get(key, {})
                    elliott = period_data.get('elliott_wave', {})
                    structure = elliott.get('structure', {})
                    peaks = structure.get('recent_peaks', [])
                    troughs = structure.get('recent_troughs', [])
                    if peaks or troughs:
                        info_cols = st.columns(2)
                        with info_cols[0]:
                            if peaks:
                                latest_peak = peaks[-1]
                                st.caption(f"📈 最近峰值: {latest_peak[1]:.2f} ({latest_peak[0]})")
                        with info_cols[1]:
                            if troughs:
                                latest_trough = troughs[-1]
                                st.caption(f"📉 最近谷值: {latest_trough[1]:.2f} ({latest_trough[0]})")
                col_idx += 1
                if col_idx == 2:
                    chart_cols = st.columns(2)
        else:
            st.info("暂无指数历史数据")

        st.divider()

        # ---------- 道氏理论概览 ----------
        st.markdown(f"**📊 道氏理论概览 ({label})**")
        dow_data = []
        for name, data in filtered_index_performance.items():
            if name == 'inter_index_validation':
                continue
            analysis = data.get('analysis', {})
            period_data = analysis.get(key, {})
            dow = period_data.get('dow_theory', {})
            if dow and 'primary_trend' in dow:
                primary = dow.get('primary_trend', 'UNKNOWN')
                strength = dow.get('trend_strength', {})
                dow_data.append({
                    '指数': name,
                    '主要趋势': f"{trend_emoji.get(primary, '⚪')} {dow.get('primary_desc', '未知')}",
                    '次要趋势': dow.get('secondary_desc', '未知'),
                    '趋势强度': f"ADX: {strength.get('adx', 0)} ({strength.get('strength', 'weak')})",
                    '区间位置': f"{dow.get('position_in_range', 0):.0%}"
                })
        if dow_data:
            st.dataframe(dow_data, hide_index=True, width="stretch")
        else:
            st.caption("道氏理论数据不足")

        # ---------- 波浪理论概览 ----------
        st.markdown(f"**🌊 波浪理论概览 ({label})**")
        wave_data = []
        for name, data in filtered_index_performance.items():
            if name == 'inter_index_validation':
                continue
            analysis = data.get('analysis', {})
            period_data = analysis.get(key, {})
            wave = period_data.get('elliott_wave', {})
            if wave and 'current_phase' in wave:
                wave_data.append({
                    '指数': name,
                    '当前阶段': wave.get('current_phase', '未知'),
                    '最近峰值': wave.get('last_peak', '-'),
                    '最近谷值': wave.get('last_trough', '-'),
                    '距峰值': f"{wave.get('current_vs_peak', 0):+.1f}%"
                })
        if wave_data:
            st.dataframe(wave_data, hide_index=True, width="stretch")
        else:
            st.caption("波浪理论数据不足")

        # ---------- 主要指数详细分析 ----------
        if main_index:
            st.divider()
            st.markdown(f"**{main_index_name} {label} 详细分析**")

            analysis = main_index.get('analysis', {})
            period_data = analysis.get(key, {})
            dow = period_data.get('dow_theory', {})
            elliott = period_data.get('elliott_wave', {})

            if dow and 'primary_trend' in dow:
                # 道氏理论详情
                st.markdown("**道氏理论**")
                primary_trend = dow.get('primary_trend', 'UNKNOWN')
                st.markdown(f"主要趋势: {trend_emoji.get(primary_trend, '⚪')} {dow.get('primary_desc', '未知')}")
                st.markdown(f"次要趋势: {dow.get('secondary_desc', '未知')}")

                strength = dow.get('trend_strength', {})
                adx = strength.get('adx', 0)
                strength_text = strength.get('strength', 'weak')
                st.progress(min(adx / 100, 1.0), text=f"趋势强度 ADX: {adx} ({strength_text})")

                volume = dow.get('volume_signal', 'neutral')
                vol_emoji = {'confirming': '✅', 'warning': '⚠️', 'neutral': '➖'}
                vol_text = {'confirming': '确认趋势', 'warning': '背离警示', 'neutral': '中性'}
                st.caption(f"成交量信号: {vol_emoji.get(volume, '➖')} {vol_text.get(volume, '中性')}")

                st.divider()

                # 波浪理论详情
                st.markdown("**波浪理论**")
                if elliott and 'current_phase' in elliott:
                    phase = elliott.get('current_phase', '未知')
                    st.markdown(f"当前阶段: {phase}")

                    structure = elliott.get('structure', {})
                    if structure:
                        volatility = structure.get('volatility_pct', 0)
                        st.caption(f"波动率: {volatility:.2f}%")

                        fib_382 = structure.get('fib_382')
                        fib_500 = structure.get('fib_500')
                        fib_618 = structure.get('fib_618')
                        if fib_382 and fib_500 and fib_618:
                            fib_col1, fib_col2, fib_col3 = st.columns(3)
                            with fib_col1:
                                st.metric("38.2%", f"{fib_382:.0f}")
                            with fib_col2:
                                st.metric("50.0%", f"{fib_500:.0f}")
                            with fib_col3:
                                st.metric("61.8%", f"{fib_618:.0f}")
                else:
                    st.caption("波浪分析数据暂不可用")
            else:
                st.caption(f"{label}分析数据不足")
        else:
            st.caption("技术分析数据加载中...")

        # 跨指数验证（只在日线 tab 显示一次）
        if key == 'daily':
            inter_validation = tech_indicators.get('inter_index_validation', {})
            if inter_validation:
                st.divider()
                val_status = inter_validation.get('validation', '')
                consistency = inter_validation.get('consistency', 0)
                note = inter_validation.get('note', '')
                val_emoji = {'CONFIRMED': '✅', 'PARTIAL': '⚠️', 'DIVERGENCE': '❌'}
                st.markdown(f"**指数验证**: {val_emoji.get(val_status, '➖')} {note}")
                st.progress(consistency, text=f"一致性: {consistency*100:.0f}%")

# 生成时间
st.caption(f"数据日期: {date}")

st.divider()

# ============= 信号列表 =============
st.markdown("### 📋 涨停信号列表")

if signals:
    df_signals = pd.DataFrame(signals)
    st.dataframe(df_signals, width="stretch")
else:
    st.info("暂无涨停信号")

# ============= 快捷操作 =============
with st.sidebar:
    st.header("🔥 今日技术面操作")

    if st.button("🔄 刷新数据", type="primary"):
        with st.spinner("正在运行今日技术面扫描..."):
            import subprocess
            try:
                result = subprocess.run(
                    [sys.executable, "run_today_technical.py"],
                    cwd=os.path.join(BASE_DIR, "ShortTerm"),
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if result.returncode == 0:
                    st.success("扫描完成!")
                    st.rerun()
                else:
                    st.error(f"运行失败: {result.stderr[:200]}")
            except Exception as e:
                st.error(f"错误: {e}")


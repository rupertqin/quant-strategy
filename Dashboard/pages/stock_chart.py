"""
股票K线图页面 - 专业级图表展示

使用 Lightweight Charts 实现专业K线图表，包含：
- K线蜡烛图
- 多周期均线 (MA5/10/20/60)
- 成交量副图
- 时间范围切换
- 十字光标和提示框
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import json
import logging
from datetime import datetime, timedelta

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 添加项目路径
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from lib.utils import StockCodeUtil, get_stock_name

# 导入共享格式化工具
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.formatters import format_technicals, format_flat_mas, render_flat_ma_badge, detect_flat_mas_for_symbol, render_signal_card, format_ma_bonding

# 导入底层数据接口（默认前复权）
sys.path.insert(0, str(BASE_DIR))
from DataHub.core.data_reader import load_stock_prices, load_stock_prices_raw

# ============= 配置 =============
st.set_page_config(
    page_title="Stock Chart",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============= 样式 =============
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stock-info {
        display: flex;
        align-items: center;
        gap: 30px;
    }
    .stock-name {
        font-size: 28px;
        font-weight: bold;
    }
    .stock-code {
        font-size: 16px;
        opacity: 0.8;
    }
    .price-main {
        font-size: 36px;
        font-weight: bold;
    }
    .price-change {
        font-size: 18px;
        padding: 4px 12px;
        border-radius: 20px;
    }
    .price-up {
        color: #ff4757;
    }
    .price-up-bg {
        background: rgba(255, 71, 87, 0.15);
        color: #ff4757;
    }
    .price-down {
        color: #2ed573;
    }
    .price-down-bg {
        background: rgba(46, 213, 115, 0.15);
        color: #2ed573;
    }
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 15px;
        margin-top: 15px;
    }
    .metric-box {
        background: rgba(255,255,255,0.1);
        padding: 12px;
        border-radius: 8px;
        text-align: center;
    }
    .metric-label {
        font-size: 12px;
        opacity: 0.7;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 16px;
        font-weight: 600;
    }
    .time-selector {
        display: flex;
        gap: 8px;
        margin: 15px 0;
    }
    .time-btn {
        padding: 8px 16px;
        border-radius: 6px;
        border: 1px solid #e0e0e0;
        background: white;
        cursor: pointer;
        transition: all 0.3s;
    }
    .time-btn:hover {
        background: #f5f5f5;
    }
    .time-btn.active {
        background: #667eea;
        color: white;
        border-color: #667eea;
    }
    .chart-container {
        background: white;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    .legend-item {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        margin-right: 16px;
        font-size: 13px;
    }
    .legend-color {
        width: 12px;
        height: 2px;
        border-radius: 1px;
    }
</style>
""", unsafe_allow_html=True)


# ============= 数据加载函数 =============
def calculate_macd(df: pd.DataFrame, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2
    return dif, dea, macd


def calculate_kdj(df: pd.DataFrame, n=9, m1=3, m2=3):
    """计算KDJ指标"""
    low_list = df['low'].rolling(window=n, min_periods=n).min()
    high_list = df['high'].rolling(window=n, min_periods=n).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    
    k = rsv.ewm(alpha=1/m1, adjust=False).mean()
    d = k.ewm(alpha=1/m2, adjust=False).mean()
    j = 3 * k - 2 * d
    
    return k, d, j


@st.cache_data(ttl=300)
def load_stock_signals(symbol: str) -> list:
    """加载指定股票的信号数据"""
    import json
    from pathlib import Path

    BASE_DIR = Path(__file__).parent.parent.parent
    signals_file = BASE_DIR / "storage" / "outputs" / "signals" / "stock_signals_latest.json"

    if not signals_file.exists():
        return []

    try:
        with open(signals_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if data.get("status") != "success":
            return []

        # 筛选当前股票的信号
        all_signals = data.get("signals", [])
        stock_signals = [s for s in all_signals if s.get('symbol') == symbol]

        # 按日期和评分排序
        stock_signals.sort(key=lambda x: (x.get('trigger_date', ''), x.get('score', 0)), reverse=True)

        return stock_signals
    except Exception:
        return []


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """将日线数据重采样为周线数据"""
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    
    # 按周重采样
    weekly = df.resample('W-FRI').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum'
    }).dropna()
    
    weekly.reset_index(inplace=True)
    
    return weekly


def convert_to_forward_adjusted(df: pd.DataFrame, latest_close: float = None) -> pd.DataFrame:
    """
    将后复权/不复权数据转换为前复权数据
    
    转换公式: 前复权价格 = 后复权价格 × (最新收盘价 / 最新后复权价)
    
    Args:
        df: 包含后复权或不复权价格的DataFrame
        latest_close: 最新的不复权收盘价（用于计算转换系数）
        
    Returns:
        转换后的DataFrame
    """
    if df.empty:
        return df
    
    df_adj = df.copy()
    
    # 如果没有提供最新收盘价，假设当前数据的后复权最新价需要转换
    # 使用前复权公式：以最新交易日为基准，所有历史价格按比例缩放
    latest_adj_close = df_adj['close'].iloc[-1]
    
    # 如果提供了实际最新价（不复权），计算转换系数
    if latest_close is not None and latest_adj_close > 0:
        ratio = latest_close / latest_adj_close
    else:
        # 假设当前是后复权数据，需要获取不复权的最新价作为基准
        # 这里简化处理，使用一个估算值
        # 实际使用时应该从接口获取最新不复权价格
        ratio = 1.0  # 默认不转换，需要外部提供正确的ratio
    
    # 转换价格列
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        if col in df_adj.columns:
            df_adj[col] = df_adj[col] * ratio
    
    # 转换均线
    ma_cols = ['ma5', 'ma10', 'ma20', 'ma60']
    for col in ma_cols:
        if col in df_adj.columns:
            df_adj[col] = df_adj[col] * ratio
    
    return df_adj


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """将日线数据重采样为月线数据"""
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    
    # 按月重采样
    monthly = df.resample('ME').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum'
    }).dropna()
    
    monthly.reset_index(inplace=True)
    
    return monthly


@st.cache_data(ttl=3600)
def load_stock_data(symbol: str, force_adjust: str = 'qfq') -> pd.DataFrame:
    """
    加载股票历史数据（使用底层接口，默认前复权）

    Args:
        symbol: 股票代码
        force_adjust: 强制复权方式 - 'qfq'(前复权), None(不复权)
    """
    try:
        # 使用底层接口，默认前复权
        adjust = "qfq" if force_adjust == 'qfq' else None
        df = load_stock_prices(symbol, adjust=adjust)

        if df.empty:
            logger.warning(f"股票数据为空: {symbol}")
            return pd.DataFrame()

        # 计算均线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()

        # 计算MACD
        df['macd_dif'], df['macd_dea'], df['macd_bar'] = calculate_macd(df)

        # 计算KDJ
        df['kdj_k'], df['kdj_d'], df['kdj_j'] = calculate_kdj(df)

        logger.info(f"加载 {symbol} 数据: {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"加载数据失败 {symbol}: {e}")
        st.error(f"加载数据失败: {e}")
        return pd.DataFrame()


@st.cache_data
def get_stock_list() -> pd.DataFrame:
    """获取股票列表（代码+名称）"""
    stock_csv = BASE_DIR / "storage" / "stock_basic_info.csv"
    etf_csv = BASE_DIR / "storage" / "etf_basic_info.csv"
    
    stocks = []
    
    if stock_csv.exists():
        df = pd.read_csv(stock_csv)
        stocks.extend(df[['symbol', 'name']].to_dict('records'))
    
    if etf_csv.exists():
        df = pd.read_csv(etf_csv)
        stocks.extend(df[['symbol', 'name']].to_dict('records'))
    
    return pd.DataFrame(stocks)


def filter_by_time_range(df: pd.DataFrame, time_range: str) -> pd.DataFrame:
    """根据时间范围过滤数据"""
    if df.empty:
        return df
    
    end_date = df['trade_date'].max()
    
    if time_range == "1月":
        start_date = end_date - pd.Timedelta(days=30)
    elif time_range == "3月":
        start_date = end_date - pd.Timedelta(days=90)
    elif time_range == "6月":
        start_date = end_date - pd.Timedelta(days=180)
    elif time_range == "1年":
        start_date = end_date - pd.Timedelta(days=365)
    elif time_range == "3年":
        start_date = end_date - pd.Timedelta(days=365*3)
    else:  # 全部
        return df
    
    return df[df['trade_date'] >= start_date]


# ============= 图表HTML生成 =============
def create_tradingview_chart(df: pd.DataFrame, symbol: str, name: str, show_macd: bool = True, show_kdj: bool = True) -> str:
    """生成 TradingView Lightweight Charts HTML，包含MACD和KDJ"""
    
    # 准备K线数据
    candles = []
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        candles.append({
            'time': timestamp,
            'open': round(row['open'], 2),
            'high': round(row['high'], 2),
            'low': round(row['low'], 2),
            'close': round(row['close'], 2)
        })
    
    # 准备成交量数据
    volumes = []
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        color = 'rgba(255, 71, 87, 0.5)' if row['close'] >= row['open'] else 'rgba(46, 213, 115, 0.5)'
        volumes.append({
            'time': timestamp,
            'value': int(row['volume']),
            'color': color
        })
    
    # 准备均线数据
    ma5_data = []
    ma10_data = []
    ma20_data = []
    ma60_data = []
    
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        if not pd.isna(row['ma5']):
            ma5_data.append({'time': timestamp, 'value': round(row['ma5'], 2)})
        if not pd.isna(row['ma10']):
            ma10_data.append({'time': timestamp, 'value': round(row['ma10'], 2)})
        if not pd.isna(row['ma20']):
            ma20_data.append({'time': timestamp, 'value': round(row['ma20'], 2)})
        if not pd.isna(row['ma60']):
            ma60_data.append({'time': timestamp, 'value': round(row['ma60'], 2)})
    
    # 准备MACD数据
    macd_dif_data = []
    macd_dea_data = []
    macd_bar_data = []
    
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        if not pd.isna(row['macd_dif']):
            macd_dif_data.append({'time': timestamp, 'value': round(row['macd_dif'], 3)})
        if not pd.isna(row['macd_dea']):
            macd_dea_data.append({'time': timestamp, 'value': round(row['macd_dea'], 3)})
        if not pd.isna(row['macd_bar']):
            color = '#ff4757' if row['macd_bar'] >= 0 else '#2ed573'
            macd_bar_data.append({'time': timestamp, 'value': round(row['macd_bar'], 3), 'color': color})
    
    # 准备KDJ数据
    kdj_k_data = []
    kdj_d_data = []
    kdj_j_data = []
    
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        if not pd.isna(row['kdj_k']):
            kdj_k_data.append({'time': timestamp, 'value': round(row['kdj_k'], 2)})
        if not pd.isna(row['kdj_d']):
            kdj_d_data.append({'time': timestamp, 'value': round(row['kdj_d'], 2)})
        if not pd.isna(row['kdj_j']):
            kdj_j_data.append({'time': timestamp, 'value': round(row['kdj_j'], 2)})
    
    candles_json = json.dumps(candles)
    volumes_json = json.dumps(volumes)
    ma5_json = json.dumps(ma5_data)
    ma10_json = json.dumps(ma10_data)
    ma20_json = json.dumps(ma20_data)
    ma60_json = json.dumps(ma60_data)
    macd_dif_json = json.dumps(macd_dif_data)
    macd_dea_json = json.dumps(macd_dea_data)
    macd_bar_json = json.dumps(macd_bar_data)
    kdj_k_json = json.dumps(kdj_k_data)
    kdj_d_json = json.dumps(kdj_d_data)
    kdj_j_json = json.dumps(kdj_j_data)
    
    # 计算图表总高度和每个pane的高度比例
    total_height = 700
    main_height_pct = 50
    vol_height_pct = 15
    macd_height_pct = 17 if show_macd else 0
    kdj_height_pct = 18 if show_kdj else 0
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            body {{ margin: 0; padding: 0; background: #ffffff; width: 100%; height: {total_height}px; }}
            #chart-container {{ width: 100%; height: 100%; position: relative; }}
            #hover-tooltip {{
                position: absolute;
                top: 10px;
                left: 10px;
                background: rgba(255, 255, 255, 0.98);
                border: 1px solid #ccc;
                border-radius: 4px;
                padding: 10px 15px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                font-size: 13px;
                z-index: 9999;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                pointer-events: none;
                min-width: 320px;
            }}
            .tooltip-row {{ margin-bottom: 4px; }}
            .tooltip-label {{ font-weight: 600; color: #666; margin-right: 8px; }}
            .tooltip-val {{ margin-right: 12px; }}
        </style>
    </head>
    <body>
        <div id="chart-container">
            <div id="hover-tooltip">
                <div class="tooltip-row">
                    <span class="tooltip-label">日期:</span>
                    <span id="tt-date" style="font-weight: 600;">--</span>
                </div>
                <div class="tooltip-row">
                    <span class="tooltip-label">K线:</span>
                    <span class="tooltip-val">开:<span id="tt-open">--</span></span>
                    <span class="tooltip-val">高:<span id="tt-high">--</span></span>
                    <span class="tooltip-val">低:<span id="tt-low">--</span></span>
                    <span class="tooltip-val">收:<span id="tt-close">--</span></span>
                </div>
                <div class="tooltip-row">
                    <span class="tooltip-label">均线:</span>
                    <span class="tooltip-val" style="color:#ff9f43">MA5:<span id="tt-ma5">--</span></span>
                    <span class="tooltip-val" style="color:#00d2d3">MA10:<span id="tt-ma10">--</span></span>
                    <span class="tooltip-val" style="color:#5f27cd">MA20:<span id="tt-ma20">--</span></span>
                    <span class="tooltip-val" style="color:#10ac84">MA60:<span id="tt-ma60">--</span></span>
                </div>
                <div class="tooltip-row">
                    <span class="tooltip-label">MACD:</span>
                    <span class="tooltip-val" style="color:#0066cc">DIF:<span id="tt-dif">--</span></span>
                    <span class="tooltip-val" style="color:#ff9900">DEA:<span id="tt-dea">--</span></span>
                </div>
                <div class="tooltip-row">
                    <span class="tooltip-label">KDJ:</span>
                    <span class="tooltip-val" style="color:#ff6b6b">K:<span id="tt-k">--</span></span>
                    <span class="tooltip-val" style="color:#4ecdc4">D:<span id="tt-d">--</span></span>
                    <span class="tooltip-val" style="color:#45b7d1">J:<span id="tt-j">--</span></span>
                </div>
            </div>
        </div>
        <script>
            // K线数据（用于计算可见范围）
            const candles = {candles_json};
            
            // 创建图表
            const chartContainer = document.getElementById('chart-container');
            chartContainer.style.position = 'relative';
            const chart = LightweightCharts.createChart(chartContainer, {{
                layout: {{
                    background: {{ type: 'solid', color: '#ffffff' }},
                    textColor: '#333333',
                }},
                grid: {{
                    vertLines: {{ color: '#f0f0f0' }},
                    horzLines: {{ color: '#f0f0f0' }},
                }},
                crosshair: {{
                    mode: 1,
                    vertLine: {{
                        color: '#758696',
                        labelBackgroundColor: '#758696',
                        width: 1,
                        style: 2,
                        visible: true,
                        labelVisible: true,
                    }},
                    horzLine: {{
                        color: '#758696',
                        labelBackgroundColor: '#758696',
                        width: 1,
                        style: 2,
                        visible: true,
                        labelVisible: false,
                    }},
                }},
                rightPriceScale: {{
                    borderColor: '#e0e0e0',
                }},
                timeScale: {{
                    borderColor: '#e0e0e0',
                    timeVisible: false,
                    fixLeftEdge: true,
                    fixRightEdge: true,
                }},
            }});
            
            // ========== 主图（K线+均线）==========
            const candleSeries = chart.addCandlestickSeries({{
                upColor: '#ff4757',
                downColor: '#2ed573',
                borderUpColor: '#ff4757',
                borderDownColor: '#2ed573',
                wickUpColor: '#ff4757',
                wickDownColor: '#2ed573',
            }});
            candleSeries.setData({candles_json});
            
            // 均线 - 禁用右侧标签和标题（使用上方自定义图例）
            const ma5Series = chart.addLineSeries({{ 
                color: '#ff9f43', lineWidth: 1,
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            ma5Series.setData({ma5_json});
            
            const ma10Series = chart.addLineSeries({{ 
                color: '#00d2d3', lineWidth: 1,
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            ma10Series.setData({ma10_json});
            
            const ma20Series = chart.addLineSeries({{ 
                color: '#5f27cd', lineWidth: 1,
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            ma20Series.setData({ma20_json});
            
            const ma60Series = chart.addLineSeries({{ 
                color: '#10ac84', lineWidth: 1,
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            ma60Series.setData({ma60_json});
            
            // 设置主图占比
            candleSeries.priceScale().applyOptions({{
                scaleMargins: {{ top: 0.18, bottom: 0.55 }}
            }});
            
            // ========== 成交量（副图1）==========
            const volumeSeries = chart.addHistogramSeries({{
                priceFormat: {{ type: 'volume' }},
                priceScaleId: 'volume',
            }});
            volumeSeries.setData({volumes_json});
            
            chart.priceScale('volume').applyOptions({{
                scaleMargins: {{ top: 0.48, bottom: 0.35 }},
            }});
            
            // ========== MACD（副图2）==========
            const macdDifSeries = chart.addLineSeries({{
                color: '#0066cc', lineWidth: 1,
                priceScaleId: 'macd',
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            macdDifSeries.setData({macd_dif_json});
            
            const macdDeaSeries = chart.addLineSeries({{
                color: '#ff9900', lineWidth: 1,
                priceScaleId: 'macd',
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            macdDeaSeries.setData({macd_dea_json});
            
            const macdBarSeries = chart.addHistogramSeries({{
                priceScaleId: 'macd',
                lastValueVisible: false
            }});
            macdBarSeries.setData({macd_bar_json});
            
            chart.priceScale('macd').applyOptions({{
                scaleMargins: {{ top: 0.68, bottom: 0.18 }},
            }});
            
            // ========== KDJ（副图3）==========
            const kdjKSeries = chart.addLineSeries({{
                color: '#ff6b6b', lineWidth: 1,
                priceScaleId: 'kdj',
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            kdjKSeries.setData({kdj_k_json});
            
            const kdjDSeries = chart.addLineSeries({{
                color: '#4ecdc4', lineWidth: 1,
                priceScaleId: 'kdj',
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            kdjDSeries.setData({kdj_d_json});
            
            const kdjJSeries = chart.addLineSeries({{
                color: '#45b7d1', lineWidth: 1,
                priceScaleId: 'kdj',
                lastValueVisible: false,
                priceLineVisible: false,
                title: ''
            }});
            kdjJSeries.setData({kdj_j_json});
            
            chart.priceScale('kdj').applyOptions({{
                scaleMargins: {{ top: 0.85, bottom: 0.02 }},
            }});
            
            // 默认显示最近约250根K线，用户可缩放和拖拽浏览更多历史
            const totalBars = candles.length;
            if (totalBars > 250) {{
                chart.timeScale().setVisibleLogicalRange({{ from: totalBars - 250, to: totalBars }});
            }} else {{
                chart.timeScale().fitContent();
            }}
            
            // 启用鼠标滚轮缩放
            chart.applyOptions({{
                handleScroll: {{
                    vertTouchDrag: false,
                }},
                handleScale: {{
                    axisPressedMouseMove: {{
                        time: true,
                        price: false,
                    }},
                }},
            }});
            
            // 订阅crosshair移动事件，更新tooltip数据
            chart.subscribeCrosshairMove(function(param) {{
                if (!param.time || !param.seriesData) return;
                
                var data = param.seriesData;
                
                // 更新日期
                var date = new Date(param.time * 1000);
                var dateStr = date.getFullYear() + '-' + 
                             String(date.getMonth() + 1).padStart(2, '0') + '-' + 
                             String(date.getDate()).padStart(2, '0');
                document.getElementById('tt-date').textContent = dateStr;
                
                // 更新K线数据
                var candle = data.get(candleSeries);
                if (candle) {{
                    document.getElementById('tt-open').textContent = candle.open ? candle.open.toFixed(2) : '--';
                    document.getElementById('tt-high').textContent = candle.high ? candle.high.toFixed(2) : '--';
                    document.getElementById('tt-low').textContent = candle.low ? candle.low.toFixed(2) : '--';
                    document.getElementById('tt-close').textContent = candle.close ? candle.close.toFixed(2) : '--';
                }}
                
                // 更新均线
                var ma5 = data.get(ma5Series);
                document.getElementById('tt-ma5').textContent = (ma5 && ma5.value) ? ma5.value.toFixed(2) : '--';
                
                var ma10 = data.get(ma10Series);
                document.getElementById('tt-ma10').textContent = (ma10 && ma10.value) ? ma10.value.toFixed(2) : '--';
                
                var ma20 = data.get(ma20Series);
                document.getElementById('tt-ma20').textContent = (ma20 && ma20.value) ? ma20.value.toFixed(2) : '--';
                
                var ma60 = data.get(ma60Series);
                document.getElementById('tt-ma60').textContent = (ma60 && ma60.value) ? ma60.value.toFixed(2) : '--';
                
                // 更新MACD
                var dif = data.get(macdDifSeries);
                document.getElementById('tt-dif').textContent = (dif && dif.value) ? dif.value.toFixed(3) : '--';
                
                var dea = data.get(macdDeaSeries);
                document.getElementById('tt-dea').textContent = (dea && dea.value) ? dea.value.toFixed(3) : '--';
                
                // 更新KDJ
                var k = data.get(kdjKSeries);
                document.getElementById('tt-k').textContent = (k && k.value) ? k.value.toFixed(2) : '--';
                
                var d = data.get(kdjDSeries);
                document.getElementById('tt-d').textContent = (d && d.value) ? d.value.toFixed(2) : '--';
                
                var j = data.get(kdjJSeries);
                document.getElementById('tt-j').textContent = (j && j.value) ? j.value.toFixed(2) : '--';
            }});
            
            // 窗口大小变化时自适应
            window.addEventListener('resize', () => {{
                chart.applyOptions({{ width: chartContainer.clientWidth }});
            }});
        </script>
    </body>
    </html>
    """
    
    return html


# ============= 页面主函数 =============
def main():
    # 加载股票列表
    stock_list = get_stock_list()
    
    # 优先从 URL 参数获取股票代码 (支持 ?symbol=600519.SH)
    query_params = st.query_params
    if 'symbol' in query_params:
        symbol_from_url = query_params['symbol']
        if symbol_from_url:
            st.session_state['selected_stock'] = symbol_from_url
            st.session_state['selected_name'] = get_stock_name(symbol_from_url)
    # 其次检查 session_state（从其他页面跳转）
    elif 'selected_stock' not in st.session_state:
        # 默认显示茅台
        st.session_state['selected_stock'] = '600519.SH'
        st.session_state['selected_name'] = '贵州茅台'
    
    if stock_list.empty:
        st.error("股票列表为空，请先同步股票基础数据")
        return
    
    # ============= 侧边栏搜索 =============
    with st.sidebar:
        st.header("🔍 股票搜索")
        
        # 复权方式选择
        st.subheader("⚙️ 显示设置")
        adjust_type = st.radio(
            "复权方式",
            options=["前复权", "不复权"],
            index=0,  # 默认前复权
            help="前复权: 以最新价格为基准调整历史价格 | 不复权: 原始价格"
        )
        
        st.divider()
        
        search_query = st.text_input(
            "输入代码或名称",
            placeholder="如: 600519 或 茅台",
            key="stock_search"
        )
        
        # 搜索结果
        if search_query:
            query = search_query.upper()
            code_match = stock_list[stock_list['symbol'].str.contains(query, na=False)]
            name_match = stock_list[stock_list['name'].str.contains(query, na=False)]
            search_results = pd.concat([code_match, name_match]).drop_duplicates().head(20)
            
            if not search_results.empty:
                st.write(f"找到 {len(search_results)} 个结果:")
                for _, row in search_results.iterrows():
                    if st.button(
                        f"{row['symbol']} {row['name']}",
                        key=f"btn_{row['symbol']}",
                        use_container_width=True
                    ):
                        st.session_state['selected_stock'] = row['symbol']
                        st.session_state['selected_name'] = row['name']
                        st.rerun()
        
        # 常用股票快捷选择
        st.divider()
        st.subheader("⭐ 常用股票")
        
        common_stocks = [
            ('600519.SH', '贵州茅台'),
            ('300750.SZ', '宁德时代'),
            ('000858.SZ', '五粮液'),
            ('002594.SZ', '比亚迪'),
        ]
        
        for symbol, name in common_stocks:
            if st.button(f"{symbol} {name}", key=f"common_{symbol}", use_container_width=True):
                st.session_state['selected_stock'] = symbol
                st.session_state['selected_name'] = name
                st.rerun()
    
    # ============= 主内容区 =============
    if 'selected_stock' not in st.session_state:
        # 默认显示茅台
        st.session_state['selected_stock'] = '600519.SH'
        st.session_state['selected_name'] = '贵州茅台'
    
    symbol = st.session_state['selected_stock']
    name = st.session_state.get('selected_name', '')
    
    # 根据选择加载数据
    adjust_map = {"前复权": "qfq", "不复权": None}
    selected_adjust = adjust_map.get(adjust_type, "qfq")
    df = load_stock_data(symbol, force_adjust=selected_adjust)
    
    if df.empty:
        st.error(f"未找到 {symbol} 的数据，请先同步历史数据")
        st.info("运行: python DataHub/services/history_sync.py --symbol " + symbol)
        return
    
    # 最新数据
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    change = latest['close'] - prev['close']
    change_pct = (change / prev['close']) * 100
    
    # 涨跌颜色
    is_up = change >= 0
    price_class = "price-up" if is_up else "price-down"
    bg_class = "price-up-bg" if is_up else "price-down-bg"
    change_symbol = "+" if is_up else ""
    
    # 复权方式标签
    adjust_badge = "前复权" if selected_adjust == "qfq" else "不复权"
    badge_color = "#2ed573" if selected_adjust == "qfq" else "#95a5a6"
    
    # ============= 头部信息区 =============
    st.markdown(f"""
    <div class="main-header">
        <div class="stock-info">
            <div>
                <div class="stock-name">{name} <span style="font-size: 14px; background: {badge_color}; color: white; padding: 2px 8px; border-radius: 4px; margin-left: 8px;">{adjust_badge}</span></div>
                <div class="stock-code">{symbol}</div>
            </div>
            <div style="flex: 1;"></div>
            <div style="text-align: right;">
                <div class="price-main {price_class}">{latest['close']:.2f}</div>
                <span class="price-change {bg_class}">
                    {change_symbol}{change:.2f} ({change_symbol}{change_pct:.2f}%)
                </span>
            </div>
        </div>
        <div class="metric-grid">
            <div class="metric-box">
                <div class="metric-label">今开</div>
                <div class="metric-value {price_class}">{latest['open']:.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">最高</div>
                <div class="metric-value">{latest['high']:.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">最低</div>
                <div class="metric-value">{latest['low']:.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">昨收</div>
                <div class="metric-value">{prev['close']:.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">成交量</div>
                <div class="metric-value">{latest['volume']/10000:.1f}万</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ============= 技术指标区域 =============
    # 计算当前技术指标
    tech = {}
    if not df.empty:
        latest = df.iloc[-1]
        tech = {
            'ma5': round(latest.get('ma5', 0), 2) if not pd.isna(latest.get('ma5')) else None,
            'ma10': round(latest.get('ma10', 0), 2) if not pd.isna(latest.get('ma10')) else None,
            'ma20': round(latest.get('ma20', 0), 2) if not pd.isna(latest.get('ma20')) else None,
            'ma60': round(latest.get('ma60', 0), 2) if not pd.isna(latest.get('ma60')) else None,
        }

        # 计算MACD
        dif, dea, macd = calculate_macd(df)
        if not dif.empty:
            tech['macd_dif'] = round(dif.iloc[-1], 3)
            tech['macd_dea'] = round(dea.iloc[-1], 3)

        # 计算KDJ
        k, d, j = calculate_kdj(df)
        if not k.empty:
            tech['kdj_k'] = round(k.iloc[-1], 2)
            tech['kdj_d'] = round(d.iloc[-1], 2)
            tech['kdj_j'] = round(j.iloc[-1], 2)

    # 检测均线走平
    try:
        from ShortTerm.daily_signal.stock_signal_scanner import StockSignalScanner
        scanner = StockSignalScanner()
        flat_ma_dict = detect_flat_mas_for_symbol(scanner, symbol)
        # 合并所有周期的走平均线
        all_flat_mas = []
        for period, mas in flat_ma_dict.items():
            all_flat_mas.extend(mas)
        tech['flat_mas'] = all_flat_mas
    except Exception:
        tech['flat_mas'] = []

    # ============ 技术指标区域 ============
    tech_html = format_technicals(tech)
    flat_ma_html, has_flat_ma = format_flat_mas(tech)

    # 指标区域样式
    indicator_html = f"""
    <div style="
        background: white;
        border-radius: 12px;
        padding: 15px 20px;
        margin: 15px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    ">
        <div style="font-size: 13px; color: #666; margin-bottom: 8px;">
            <b>📊 实时技术指标</b> (最新)
        </div>
        <div style="font-family: monospace; font-size: 13px; color: #333; line-height: 1.8;">
            {tech_html}
        </div>
    </div>
    """
    st.markdown(indicator_html, unsafe_allow_html=True)

    # 显示均线走平标签（如果有）
    if has_flat_ma:
        st.markdown(render_flat_ma_badge(flat_ma_html), unsafe_allow_html=True)

    # 显示均线粘合（如果有）
    ma_bonding_html = format_ma_bonding(tech)
    if ma_bonding_html:
        st.markdown(f'<div style="margin: 5px 0;"><span style="background: linear-gradient(135deg, #8e44ad 0%, #9b59b6 100%); color: white; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;">🧲 均线粘合 {ma_bonding_html}</span></div>', unsafe_allow_html=True)

    # ============= 周期选择 =============
    periods = [("日线", "D"), ("周线", "W"), ("月线", "M")]
    
    if 'chart_period' not in st.session_state:
        st.session_state['chart_period'] = "D"
    
    cols = st.columns([1, 1, 1, 3])
    for i, (label, value) in enumerate(periods):
        with cols[i]:
            if st.button(
                label,
                key=f"period_{value}",
                type="secondary" if st.session_state['chart_period'] != value else "primary",
                use_container_width=True
            ):
                st.session_state['chart_period'] = value
                st.rerun()
    
    # 根据周期处理数据
    current_period = st.session_state['chart_period']
    if current_period == "W":
        df_processed = resample_to_weekly(df)
    elif current_period == "M":
        df_processed = resample_to_monthly(df)
    else:
        df_processed = df.copy()
    
    # 确保 trade_date 是 datetime 类型
    df_processed['trade_date'] = pd.to_datetime(df_processed['trade_date'])
    
    # 计算指标
    if not df_processed.empty:
        df_processed['ma5'] = df_processed['close'].rolling(window=5).mean()
        df_processed['ma10'] = df_processed['close'].rolling(window=10).mean()
        df_processed['ma20'] = df_processed['close'].rolling(window=20).mean()
        df_processed['ma60'] = df_processed['close'].rolling(window=60).mean()
        df_processed['macd_dif'], df_processed['macd_dea'], df_processed['macd_bar'] = calculate_macd(df_processed)
        df_processed['kdj_k'], df_processed['kdj_d'], df_processed['kdj_j'] = calculate_kdj(df_processed)
    
    # 显示所有可用数据，用户可通过图表缩放和拖拽浏览
    df_display = df_processed
    
    if df_display.empty:
        st.warning("该时间范围内没有数据")
        return
    
    # ============= 图表区域 =============
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    
    # 生成并显示图表（移除内部tooltip）
    chart_html = create_tradingview_chart(df_display, symbol, name)
    st.components.v1.html(chart_html, height=700, scrolling=False)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # ============= 数据摘要 =============
    with st.expander("📊 数据详情"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("数据条数", f"{len(df_display):,}")
        with col2:
            st.metric("日期范围", f"{df_display['trade_date'].min().strftime('%Y-%m-%d')} ~ {df_display['trade_date'].max().strftime('%Y-%m-%d')}")
        with col3:
            days = (df_display['trade_date'].max() - df_display['trade_date'].min()).days
            st.metric("时间跨度", f"{days} 天")
        
        # 显示指标数据
        display_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume', 
                       'ma5', 'ma10', 'ma20', 'macd_dif', 'macd_dea', 'macd_bar',
                       'kdj_k', 'kdj_d', 'kdj_j']
        
        df_display_formatted = df_display[display_cols].copy()
        df_display_formatted['trade_date'] = df_display_formatted['trade_date'].dt.strftime('%Y-%m-%d')
        
        st.dataframe(
            df_display_formatted.tail(20),
            use_container_width=True,
            hide_index=True
        )

    # ============= 信号展示区域 (移到最后) =============
    stock_signals = load_stock_signals(symbol)

    if stock_signals:
        st.markdown("---")
        st.markdown("<div style='font-size: 16px; font-weight: 600; margin: 15px 0 10px 0;'>📡 当前信号</div>", unsafe_allow_html=True)

        # 显示该股票的所有信号（使用模块化卡片）
        for idx, sig in enumerate(stock_signals):
            st.markdown(render_signal_card(sig, idx), unsafe_allow_html=True)


if __name__ == "__main__":
    main()

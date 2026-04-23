"""
资产K线图页面 - 专业级图表展示

使用 Lightweight Charts 实现专业K线图表，支持股票/ETF/指数：
- K线蜡烛图
- 多周期均线 (MA5/10/20/60/120)
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
from DataHub.config import get_storage_path

# 导入共享格式化工具
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.formatters import render_signal_card
from utils.scoring import calculate_stock_score, get_score_label
from utils.signal_components import (
    calculate_risk_score, render_signal_list, render_risk_assessment,
    render_expander_header, get_risk_color_emoji
)

# 导入底层数据接口（默认前复权）
sys.path.insert(0, str(BASE_DIR))
from DataHub.core.data_reader import load_stock_prices, load_stock_prices_raw

# 导入实时数据加载工具（统一数据访问层）
sys.path.insert(0, str(BASE_DIR))
from Dashboard.utils.data_access import get_latest_realtime_data, get_todays_realtime_file

# 测试数据访问层是否正常工作（仅在启动时执行一次）
_test_df, _test_time = get_latest_realtime_data(force_fetch=False, full_format=False)
logger.info(f"[初始化测试] 实时数据: {_test_time if not _test_df.empty else '无'}")

# ============= 配置 =============
st.set_page_config(
    page_title="Asset Chart",
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


@st.cache_data(ttl=300)
def load_stock_risk_info(symbol: str) -> dict:
    """加载指定股票的风险信息（用于只有风险信号的股票）"""
    import json
    from pathlib import Path

    BASE_DIR = Path(__file__).parent.parent.parent
    signals_file = BASE_DIR / "storage" / "outputs" / "signals" / "stock_signals_latest.json"

    if not signals_file.exists():
        return {}

    try:
        with open(signals_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if data.get("status") != "success":
            return {}

        # 从 stocks 字段中查找股票信息
        stocks = data.get("stocks", [])
        for stock in stocks:
            if stock.get('symbol') == symbol:
                return {
                    'has_buy_signal': stock.get('has_buy_signal', False),
                    'risk_score': stock.get('risk_score', 50),
                    'risk_warnings': stock.get('risk_warnings', []),
                    'risk_explanations': stock.get('risk_explanations', []),
                    'health_score': stock.get('health_score', 50),
                    'risk_level': stock.get('risk_level', 'medium')
                }
        return {}
    except Exception:
        return {}


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """将日线数据重采样为周线数据"""
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)

    # 基础聚合字段
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }
    
    # ETF数据可能没有amount列
    if 'amount' in df.columns:
        agg_dict['amount'] = 'sum'

    # 按周重采样
    weekly = df.resample('W-FRI').agg(agg_dict).dropna()

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
    ma_cols = ['ma5', 'ma10', 'ma20', 'ma60', 'ma120']
    for col in ma_cols:
        if col in df_adj.columns:
            df_adj[col] = df_adj[col] * ratio

    return df_adj


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """将日线数据重采样为月线数据"""
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)

    # 基础聚合字段
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }
    
    # ETF数据可能没有amount列
    if 'amount' in df.columns:
        agg_dict['amount'] = 'sum'

    # 按月重采样
    monthly = df.resample('ME').agg(agg_dict).dropna()

    monthly.reset_index(inplace=True)

    return monthly


def merge_realtime_to_df(df: pd.DataFrame, realtime_data: dict, fetch_time_str: str = None) -> pd.DataFrame:
    """
    将实时数据合并到历史DataFrame中

    合并策略：
    - 盘中（< 15:00）：用实时数据更新/添加今天的K线
    - 盘后（>= 15:00）：如果历史数据已有今天数据，优先用历史数据（更完整）；否则用实时数据补充

    Args:
        df: 历史数据DataFrame
        realtime_data: 实时数据字典，包含open/high/low/close/volume/change_pct等
        fetch_time_str: 实时数据获取时间字符串，格式 HH:MM

    Returns:
        合并后的DataFrame
    """
    if df.empty or not realtime_data:
        return df

    today = datetime.now().date()
    current_hour = datetime.now().hour
    is_post_market = current_hour >= 15  # 盘后（15:00后）

    # 确保trade_date是datetime类型
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 获取最后一天日期
    last_date = df['trade_date'].iloc[-1].date()
    has_today_data = last_date == today

    logger.info(f"历史数据最后一天: {last_date}, 今天: {today}, 是否有今天数据: {has_today_data}")
    logger.info(f"当前时间: {current_hour}:00, 是否盘后: {is_post_market}")

    # 检查实时数据是否是今天的
    realtime_date_raw = realtime_data.get('trade_date', today)
    try:
        realtime_date = pd.to_datetime(realtime_date_raw).date() if realtime_date_raw else today
    except:
        realtime_date = today

    if realtime_date != today:
        logger.warning(f"实时数据日期 {realtime_date} 不是今天 {today}，跳过合并")
        return df

    # 盘后且历史数据已有今天数据：跳过（历史日线数据更完整准确）
    if is_post_market and has_today_data:
        logger.info("盘后且历史数据已有今天数据，优先使用历史数据，跳过实时数据合并")
        return df

    # 提取实时数据值
    close = float(realtime_data.get('close', 0))
    if close == 0:
        logger.warning("实时数据 close 为 0，跳过合并")
        return df

    open_price = float(realtime_data.get('open', close))
    high = float(realtime_data.get('high', close))
    low = float(realtime_data.get('low', close))
    volume = float(realtime_data.get('volume', 0))
    amount = float(realtime_data.get('amount', 0))
    change_pct = float(realtime_data.get('change_pct', 0))

    if has_today_data:
        # 更新今天的数据（盘中用实时数据覆盖）
        idx = df.index[-1]
        df.loc[idx, 'close'] = close
        df.loc[idx, 'open'] = open_price
        df.loc[idx, 'high'] = high
        df.loc[idx, 'low'] = low
        df.loc[idx, 'volume'] = volume
        if 'amount' in df.columns:
            df.loc[idx, 'amount'] = amount
        if 'change_pct' in df.columns:
            df.loc[idx, 'change_pct'] = change_pct
        # 记录实时数据时间
        if fetch_time_str:
            df.loc[idx, 'realtime_time'] = fetch_time_str
        logger.info(f"更新今天数据: close={close}, volume={volume}")
    else:
        # 添加新行（今天数据缺失，用实时数据补充）
        new_row = pd.DataFrame([{
            'trade_date': pd.Timestamp(today),
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'amount': amount,
            'change_pct': change_pct,
            'realtime_time': fetch_time_str if fetch_time_str else None
        }])
        df = pd.concat([df, new_row], ignore_index=True)
        logger.info(f"添加今天数据: close={close}, volume={volume}")

    return df


@st.cache_data(ttl=60)
def _load_stock_data_cached(symbol: str, force_adjust: str = 'qfq') -> pd.DataFrame:
    """带缓存的数据加载（生产环境使用）"""
    return _load_stock_data_impl(symbol, force_adjust)


def _load_stock_data_impl(symbol: str, force_adjust: str = 'qfq') -> pd.DataFrame:
    """
    加载股票历史数据（使用底层接口，默认前复权）
    
    如果存在当天的实时数据文件，会自动合并到历史数据中

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
        
        # 尝试加载当天实时数据并合并（简洁格式用于tooltip）
        from Dashboard.utils.data_access import get_latest_realtime_data
        from lib.utils.stock_code import detect_asset_type
        asset_type = detect_asset_type(symbol)
        realtime_df, fetch_time_str = get_latest_realtime_data(force_fetch=False, full_format=False, asset_type=asset_type)
        
        if not realtime_df.empty:
            try:
                logger.info(f"加载实时数据: {len(realtime_df)} 条记录 (时间: {fetch_time_str})")
                logger.info(f"实时数据列: {list(realtime_df.columns)}")
                logger.info(f"前3条symbol: {realtime_df['symbol'].head(3).tolist()}")

                # 查找该股票的实时数据
                stock_realtime = realtime_df[realtime_df['symbol'] == symbol]
                logger.info(f"股票 {symbol} 实时数据: {len(stock_realtime)} 条")
                if not stock_realtime.empty:
                    realtime_row = stock_realtime.iloc[0].to_dict()
                    logger.info(f"实时数据内容: {realtime_row}")
                    df = merge_realtime_to_df(df, realtime_row, fetch_time_str)
                    logger.info(f"{symbol} 已合并实时数据，最新close: {df.iloc[-1]['close']}")
                else:
                    logger.warning(f"{symbol} 在实时数据中未找到")
                    # 显示所有可用的symbol帮助调试
                    available = realtime_df['symbol'].tolist()[:10]
                    logger.warning(f"可用symbol示例: {available}")
            except Exception as e:
                logger.error(f"合并实时数据失败 {symbol}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # 不影响主流程，继续使用历史数据
        else:
            logger.info("当天没有实时数据文件")

        # 计算均线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        df['ma120'] = df['close'].rolling(window=120).mean()

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


def load_stock_data(symbol: str, force_adjust: str = 'qfq') -> pd.DataFrame:
    """
    加载股票数据（开发环境无缓存，生产环境有缓存）
    """
    # 检查是否在开发模式
    try:
        mode = st.secrets.get("environment", {}).get("mode", "prod")
        is_dev = mode == "dev"
    except Exception:
        is_dev = False
    
    # 开发模式直接加载，生产模式使用缓存
    if is_dev:
        return _load_stock_data_impl(symbol, force_adjust)
    else:
        return _load_stock_data_cached(symbol, force_adjust)


@st.cache_data
def get_stock_list() -> pd.DataFrame:
    """获取股票列表（代码+名称），包含股票、ETF、指数"""
    # 使用环境变量配置的 storage 路径
    stock_csv = get_storage_path("stock_basic_info.csv")
    etf_csv = get_storage_path("etf_basic_info.csv")
    index_csv = get_storage_path("official_indices.csv")

    stocks = []

    # 加载股票
    if stock_csv.exists():
        df = pd.read_csv(stock_csv)
        stocks.extend(df[['symbol', 'name']].to_dict('records'))

    # 加载ETF
    if etf_csv.exists():
        df = pd.read_csv(etf_csv)
        stocks.extend(df[['symbol', 'name']].to_dict('records'))

    # 加载指数
    if index_csv.exists():
        df = pd.read_csv(index_csv)
        if 'symbol' in df.columns and 'name' in df.columns:
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
    today = datetime.now().date()
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        candle = {
            'time': timestamp,
            'open': round(row['open'], 2),
            'high': round(row['high'], 2),
            'low': round(row['low'], 2),
            'close': round(row['close'], 2)
        }
        # 如果是今天的数据且有实时时间戳，添加精确时间
        if row['trade_date'].date() == today and 'realtime_time' in row and pd.notna(row['realtime_time']):
            candle['realtimeTime'] = str(row['realtime_time'])
        candles.append(candle)

    # 准备成交量数据
    volumes = []
    for _, row in df.iterrows():
        timestamp = int(row['trade_date'].timestamp())
        color = 'rgba(255, 71, 87, 0.5)' if row['close'] >= row['open'] else 'rgba(46, 213, 115, 0.5)'
        # 处理 NaN 值
        volume_val = row['volume']
        if pd.isna(volume_val):
            volume_val = 0
        volumes.append({
            'time': timestamp,
            'value': int(volume_val),
            'color': color
        })

    # 准备均线数据
    ma5_data = []
    ma10_data = []
    ma20_data = []
    ma60_data = []
    ma120_data = []

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
        if not pd.isna(row['ma120']):
            ma120_data.append({'time': timestamp, 'value': round(row['ma120'], 2)})

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
    ma120_json = json.dumps(ma120_data)
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
                    <span class="tooltip-val" style="color:#000000">MA5:<span id="tt-ma5">--</span></span>
                    <span class="tooltip-val" style="color:#f1c40f">MA10:<span id="tt-ma10">--</span></span>
                    <span class="tooltip-val" style="color:#9b59b6">MA20:<span id="tt-ma20">--</span></span>
                    <span class="tooltip-val" style="color:#fd79a8">MA60:<span id="tt-ma60">--</span></span>
                    <span class="tooltip-val" style="color:#6c5ce7">MA120:<span id="tt-ma120">--</span></span>
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
                    mode: LightweightCharts.CrosshairMode.Magnet,
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
                        labelVisible: true,
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
            // 使用与K线图相同的价格刻度(priceScaleId: 'right')确保对齐
            const ma5Series = chart.addLineSeries({{
                color: '#000000', lineWidth: 1,
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
                title: '',
                crosshairMarkerVisible: false
            }});
            ma5Series.setData({ma5_json});

            const ma10Series = chart.addLineSeries({{
                color: '#f1c40f', lineWidth: 1,
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
                title: '',
                crosshairMarkerVisible: false
            }});
            ma10Series.setData({ma10_json});

            const ma20Series = chart.addLineSeries({{
                color: '#9b59b6', lineWidth: 1,
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
                title: '',
                crosshairMarkerVisible: false
            }});
            ma20Series.setData({ma20_json});

            const ma60Series = chart.addLineSeries({{
                color: '#fd79a8', lineWidth: 1,
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
                title: '',
                crosshairMarkerVisible: false
            }});
            ma60Series.setData({ma60_json});

            const ma120Series = chart.addLineSeries({{
                color: '#6c5ce7', lineWidth: 1,
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
                title: '',
                crosshairMarkerVisible: false
            }});
            ma120Series.setData({ma120_json});

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

            // ========== 初始化tooltip显示最新数据 ==========
            function updateTooltipWithLatest() {{
                if (candles.length === 0) return;
                
                // 获取最新数据
                const latest = candles[candles.length - 1];
                
                // 更新日期
                var date = new Date(latest.time * 1000);
                var dateStr = date.getFullYear() + '-' + 
                             String(date.getMonth() + 1).padStart(2, '0') + '-' + 
                             String(date.getDate()).padStart(2, '0');
                if (latest.realtimeTime) {{
                    dateStr += ' ' + latest.realtimeTime;
                }}
                document.getElementById('tt-date').textContent = dateStr;
                
                // 更新K线数据
                document.getElementById('tt-open').textContent = latest.open ? latest.open.toFixed(2) : '--';
                document.getElementById('tt-high').textContent = latest.high ? latest.high.toFixed(2) : '--';
                document.getElementById('tt-low').textContent = latest.low ? latest.low.toFixed(2) : '--';
                document.getElementById('tt-close').textContent = latest.close ? latest.close.toFixed(2) : '--';
                
                // 更新均线（从series数据中获取最后一个有效值）
                const ma5Data = {ma5_json};
                const ma10Data = {ma10_json};
                const ma20Data = {ma20_json};
                const ma60Data = {ma60_json};
                const ma120Data = {ma120_json};
                const macdDifData = {macd_dif_json};
                const macdDeaData = {macd_dea_json};
                const kdjKData = {kdj_k_json};
                const kdjDData = {kdj_d_json};
                const kdjJData = {kdj_j_json};

                const lastMa5 = ma5Data.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastMa10 = ma10Data.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastMa20 = ma20Data.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastMa60 = ma60Data.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastMa120 = ma120Data.filter(d => d.value !== null && d.value !== undefined).pop();

                document.getElementById('tt-ma5').textContent = lastMa5 ? lastMa5.value.toFixed(2) : '--';
                document.getElementById('tt-ma10').textContent = lastMa10 ? lastMa10.value.toFixed(2) : '--';
                document.getElementById('tt-ma20').textContent = lastMa20 ? lastMa20.value.toFixed(2) : '--';
                document.getElementById('tt-ma60').textContent = lastMa60 ? lastMa60.value.toFixed(2) : '--';
                document.getElementById('tt-ma120').textContent = lastMa120 ? lastMa120.value.toFixed(2) : '--';
                
                // 更新MACD
                const lastDif = macdDifData.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastDea = macdDeaData.filter(d => d.value !== null && d.value !== undefined).pop();
                document.getElementById('tt-dif').textContent = lastDif ? lastDif.value.toFixed(3) : '--';
                document.getElementById('tt-dea').textContent = lastDea ? lastDea.value.toFixed(3) : '--';
                
                // 更新KDJ
                const lastK = kdjKData.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastD = kdjDData.filter(d => d.value !== null && d.value !== undefined).pop();
                const lastJ = kdjJData.filter(d => d.value !== null && d.value !== undefined).pop();
                document.getElementById('tt-k').textContent = lastK ? lastK.value.toFixed(2) : '--';
                document.getElementById('tt-d').textContent = lastD ? lastD.value.toFixed(2) : '--';
                document.getElementById('tt-j').textContent = lastJ ? lastJ.value.toFixed(2) : '--';
            }}
            
            // 页面加载时显示最新数据
            updateTooltipWithLatest();

            // 订阅crosshair移动事件，更新tooltip数据
            chart.subscribeCrosshairMove(function(param) {{
                if (!param.time || !param.seriesData) return;
                
                var data = param.seriesData;
                
                // 更新日期
                var date = new Date(param.time * 1000);
                var dateStr = date.getFullYear() + '-' + 
                             String(date.getMonth() + 1).padStart(2, '0') + '-' + 
                             String(date.getDate()).padStart(2, '0');
                
                // 查找当前悬停的蜡烛数据，检查是否有实时时间
                var candleData = candles.find(c => c.time === param.time);
                if (candleData && candleData.realtimeTime) {{
                    dateStr += ' ' + candleData.realtimeTime;
                }}
                
                document.getElementById('tt-date').textContent = dateStr;
                
                // 更新K线数据
                var candle = data.get(candleSeries);
                if (candle) {{
                    document.getElementById('tt-open').textContent = candle.open ? candle.open.toFixed(2) : '--';
                    document.getElementById('tt-high').textContent = candle.high ? candle.high.toFixed(2) : '--';
                    document.getElementById('tt-low').textContent = candle.low ? candle.low.toFixed(2) : '--';
                    document.getElementById('tt-close').textContent = candle.close ? candle.close.toFixed(2) : '--';
                    
                    // 将crosshair水平线对齐到收盘价
                    chart.setCrosshairPosition(candle.close, param.time, candleSeries);
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

                var ma120 = data.get(ma120Series);
                document.getElementById('tt-ma120').textContent = (ma120 && ma120.value) ? ma120.value.toFixed(2) : '--';

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
        st.error("资产列表为空，请先同步基础数据")
        return

    # ============= 侧边栏搜索 =============
    with st.sidebar:
        st.header("🔍 资产搜索")

        # 复权方式选择
        st.subheader("⚙️ 显示设置")
        adjust_type = st.radio(
            "复权方式",
            options=["前复权", "不复权"],
            index=0,  # 默认前复权
            help="前复权: 以最新价格为基准调整历史价格 | 不复权: 原始价格"
        )

        st.divider()

        # ========== 资产类型筛选 ==========
        st.subheader("📊 资产类型")

        # 获取各类型资产列表（使用环境变量配置的 storage 路径）
        stock_csv = get_storage_path("stock_basic_info.csv")
        etf_csv = get_storage_path("etf_basic_info.csv")
        index_csv = get_storage_path("official_indices.csv")

        asset_type = st.radio(
            "选择监控对象",
            options=["全部", "股票", "ETF", "指数"],
            index=0,
            horizontal=True,
            label_visibility="collapsed"
        )

        # 根据资产类型筛选
        if asset_type == "股票" and stock_csv.exists():
            df = pd.read_csv(stock_csv)
            filtered_options = [f"{row['symbol']} - {row['name']}" for _, row in df.iterrows()]
        elif asset_type == "ETF" and etf_csv.exists():
            df = pd.read_csv(etf_csv)
            filtered_options = [f"{row['symbol']} - {row['name']}" for _, row in df.iterrows()]
        elif asset_type == "指数" and index_csv.exists():
            df = pd.read_csv(index_csv)
            filtered_options = [f"{row['symbol']} - {row['name']}" for _, row in df.iterrows()]
        else:
            # 全部或其他情况
            filtered_options = [f"{row['symbol']} - {row['name']}" for _, row in stock_list.iterrows()]

        st.divider()

        # ========== 资产搜索 ==========
        st.subheader("🔎 搜索资产")

        # 使用 selectbox 搜索（Streamlit 内置搜索功能）
        selected = st.selectbox(
            "选择资产",
            options=filtered_options,
            key="stock_selector",
            index=None,
            placeholder=f"输入代码或名称搜索{asset_type}...",
            label_visibility="collapsed"
        )

        # 处理选择
        if selected:
            symbol = selected.split(' - ')[0]
            name = selected.split(' - ')[1]
            if st.session_state.get('selected_stock') != symbol:
                st.query_params['symbol'] = symbol
                st.session_state['selected_stock'] = symbol
                st.session_state['selected_name'] = name
                st.rerun()

        # 常用资产快捷选择
        st.divider()
        st.subheader("⭐ 常用资产")

        common_assets = [
            ('600519.SH', '贵州茅台'),
            ('300750.SZ', '宁德时代'),
            ('000858.SZ', '五粮液'),
            ('002594.SZ', '比亚迪'),
            ('000001.SH', '上证指数'),
            ('000300.SH', '沪深300'),
            ('399006.SZ', '创业板指'),
        ]

        for symbol, name in common_assets:
            if st.button(f"{symbol} {name}", key=f"common_{symbol}", width="stretch"):
                st.query_params['symbol'] = symbol
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
        # 自动识别资产类型并给出同步建议
        from lib.utils.stock_code import detect_asset_type
        asset_type = detect_asset_type(symbol)
        if asset_type == "index":
            st.info("运行: python -m DataHub.services.sync --daily --symbol index")
        else:
            st.info("运行: python -m DataHub.services.sync --symbol " + symbol)
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

    # 获取最新数据日期
    latest_date = pd.to_datetime(latest['trade_date']).strftime('%Y-%m-%d') if 'trade_date' in latest else '未知'
    
    # 检查是否使用了实时数据（今天数据且有实时时间戳）
    today_str = datetime.now().strftime('%Y-%m-%d')
    is_realtime_data = (latest_date == today_str and 'realtime_time' in latest and pd.notna(latest['realtime_time']))
    realtime_badge = '<span style="font-size: 14px; background: #ff6b6b; color: white; padding: 2px 8px; border-radius: 4px; margin-left: 8px;">● 实时</span>' if is_realtime_data else ''
    
    # 显示日期和时间
    if is_realtime_data:
        date_display = f"{latest_date} {latest['realtime_time']}"
    else:
        date_display = latest_date

    # ============= 信号数据加载 =============
    stock_signals = load_stock_signals(symbol)
    risk_info = load_stock_risk_info(symbol)
    has_buy_signal = len(stock_signals) > 0 or risk_info.get('has_buy_signal', False)

    # 计算组合评分和风险分（用于信号折叠区域显示）
    portfolio_score = 0
    score_label = "无信号"
    risk_score = 50
    risk_explanations = []

    if stock_signals:
        portfolio_score = calculate_stock_score(stock_signals, change_pct)
        score_label = get_score_label(portfolio_score)

        # 计算风险分
        best_signal = max(stock_signals, key=lambda x: x.get("score", 0))
        tech = best_signal.get("technicals", {})
        risk_score, risk_explanations = calculate_risk_score(tech, stock_signals)
    elif risk_info:
        # 只有风险信号的股票
        risk_score = risk_info.get('risk_score', 50)
        risk_explanations = risk_info.get('risk_warnings', risk_info.get('risk_explanations', []))
        score_label = "风险预警"

    # ============= 头部信息区 =============
    st.markdown(f"""
    <div class="main-header">
        <div class="stock-info">
            <div>
                <div class="stock-name">{name} <span style="font-size: 14px; background: {badge_color}; color: white; padding: 2px 8px; border-radius: 4px; margin-left: 8px;">{adjust_badge}</span>{realtime_badge}</div>
                <div class="stock-code">{symbol} <span style="font-size: 12px; color: #aaa; margin-left: 8px;">数据日期: {date_display}</span></div>
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

    # ============= 信号展示区域 (折叠式，包含技术指标) =============
    # 显示条件：有买入信号 或 有风险信号（风险分>=60）
    show_signal_section = stock_signals or risk_score >= 60
    
    if show_signal_section:
        signal_count = len(stock_signals)
        
        # 创建自定义折叠区域，评分在标题右侧
        st.markdown(f"""
        <style>
        .signal-header-container {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: #f0f2f6;
            padding: 10px 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            cursor: pointer;
        }}
        .signal-header-left {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .signal-toggle {{
            font-size: 12px;
            color: #666;
        }}
        </style>
        """, unsafe_allow_html=True)

        # 显示该股票的所有信号（使用模块化卡片）
        expander_title = render_expander_header(signal_count, portfolio_score, score_label, risk_score)

        with st.expander(expander_title, expanded=False):
            # 使用组件渲染信号和风险详情（左右布局）
            sig_col, risk_col = st.columns(2)
            with sig_col:
                if stock_signals:
                    render_signal_list(stock_signals)
                else:
                    # 只有风险信号的股票
                    st.markdown("""
                    <div style="padding: 20px; background: #fdf2f2; border-radius: 8px; border-left: 4px solid #e74c3c;">
                        <div style="font-size: 16px; font-weight: 600; color: #e74c3c; margin-bottom: 8px;">⚠️ 风险预警</div>
                        <div style="font-size: 13px; color: #666;">该股票当前无买入信号，但检测到风险信号，建议关注。</div>
                    </div>
                    """, unsafe_allow_html=True)
            with risk_col:
                render_risk_assessment(risk_score, risk_explanations)

    # ============= 周期选择 =============
    periods = [("日线", "D"), ("周线", "W"), ("月线", "M")]

    if 'chart_period' not in st.session_state:
        st.session_state['chart_period'] = "D"

    cols = st.columns([1, 1, 1, 1, 2])
    for i, (label, value) in enumerate(periods):
        with cols[i]:
            if st.button(
                label,
                key=f"period_{value}",
                type="secondary" if st.session_state['chart_period'] != value else "primary",
                width="stretch"
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
        df_processed['ma120'] = df_processed['close'].rolling(window=120).mean()
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
                       'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'macd_dif', 'macd_dea', 'macd_bar',
                       'kdj_k', 'kdj_d', 'kdj_j']

        df_display_formatted = df_display[display_cols].copy()
        df_display_formatted['trade_date'] = df_display_formatted['trade_date'].dt.strftime('%Y-%m-%d')

        st.dataframe(
            df_display_formatted.tail(20),
            width="stretch",
            hide_index=True
        )

if __name__ == "__main__":
    main()

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
sys.path.insert(0, os.path.join(BASE_DIR, "Dashboard"))

# 导入股票代码工具
from lib.utils import StockCodeUtil, get_stock_name


def enrich_with_names(report: dict) -> dict:
    """为报告中的股票数据添加名称"""
    if not report:
        return report
    
    # 为 all_stocks 添加名称
    if 'all_stocks' in report and report['all_stocks']:
        for item in report['all_stocks']:
            if 'symbol' in item:
                item['name'] = get_stock_name(item['symbol'])
    
    # 为信号列表添加名称
    for key in ['buy_signals', 'sell_signals', 'watch_signals']:
        if key in report and report[key]:
            for item in report[key]:
                if 'symbol' in item:
                    item['name'] = get_stock_name(item['symbol'])
    
    return report

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
    """加载股票池监控报告 - 读取最新的报告文件并添加股票名称"""
    report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "pool_watch")
    
    def _read_json_safe(file_path):
        """安全读取JSON，处理空文件或无效数据"""
        try:
            if os.path.getsize(file_path) == 0:
                return None
            with open(file_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
            # 检查是否有有效数据
            if not report.get('all_stocks') and not report.get('rankings'):
                return None
            report = enrich_with_names(report)
            # 添加文件修改时间
            mtime = os.path.getmtime(file_path)
            report['_generated_at'] = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
            return report
        except Exception as e:
            print(f"读取JSON失败 {file_path}: {e}")
            return None
    
    # 只读取根目录的最新文件
    latest_file = os.path.join(report_dir, "pool_watch_latest.json")
    if os.path.exists(latest_file):
        report = _read_json_safe(latest_file)
        if report is not None:
            return report
    
    # 如果根目录最新文件不存在或无效，检查当天日期文件夹
    today = datetime.now().strftime('%Y-%m-%d')
    today_dir = os.path.join(report_dir, today)
    if os.path.exists(today_dir):
        today_file = os.path.join(today_dir, "pool_watch_latest.json")
        if os.path.exists(today_file):
            report = _read_json_safe(today_file)
            if report is not None:
                return report
    
    return {}


def load_pool_ranking():
    """加载股票池排名CSV - 读取最新的排名文件并添加股票名称"""
    report_dir = os.path.join(BASE_DIR, "storage", "outputs", "shortterm", "pool_watch")
    
    def _read_csv_safe(file_path):
        """安全读取CSV，处理空文件情况"""
        try:
            if os.path.getsize(file_path) == 0:
                print(f"警告: CSV文件为空 - {file_path}")
                return None
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            # 添加名称列
            if 'name' not in df.columns or df['name'].isna().all():
                df['name'] = df['symbol'].apply(get_stock_name)
            return df
        except Exception as e:
            print(f"读取CSV失败 {file_path}: {e}")
            return None
    
    # 只读取根目录的最新文件，不回退到旧文件
    latest_file = os.path.join(report_dir, "pool_ranking_latest.csv")
    if os.path.exists(latest_file):
        df = _read_csv_safe(latest_file)
        if df is not None:
            return df
    
    # 如果根目录最新文件不存在或为空，检查当天日期文件夹
    today = datetime.now().strftime('%Y-%m-%d')
    today_dir = os.path.join(report_dir, today)
    if os.path.exists(today_dir):
        today_file = os.path.join(today_dir, "pool_ranking_latest.csv")
        if os.path.exists(today_file):
            df = _read_csv_safe(today_file)
            if df is not None:
                return df
    
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


# ============= 加载数据 =============
report = load_pool_watch_report()
generated_at = report.get('_generated_at', '未知') if report else '未知'
all_stocks = report.get('all_stocks', [])

# ============= 侧边栏筛选器 =============
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
    
    # 初始化筛选条件
    filter_trend = "全部"
    filter_min_score = 0
    filter_signal = "全部"
    
    if all_stocks:
        df_all = pd.DataFrame(all_stocks)
        
        # 趋势筛选
        if 'trend' in df_all.columns:
            trends = df_all['trend'].dropna().unique().tolist()
            filter_trend = st.selectbox("趋势", ["全部"] + sorted(trends))
        
        # 评分筛选
        if 'score' in df_all.columns:
            filter_min_score = st.slider("最低评分", 0, 100, 0)
        
        # 信号类型筛选
        signal_types = ["全部", "🟢 BUY (评分≥80)", "🟡 WATCH (评分≥65)", "🔴 SELL (趋势向下)"]
        filter_signal = st.selectbox("信号类型", signal_types)
    else:
        st.info("暂无数据用于筛选")
    
    st.divider()
    st.caption("""
    数据来源: ShortTerm/pool_watch
    - 监控LongTerm股票池
    - 技术指标: MA5/10/20/60
    - 量价关系分析
    - 综合评分系统
    """)

# ============= 页面标题 =============
st.title("📊 股票池监控")
st.caption(f"LongTerm股票池短线技术指标 | 均线系统 | 量价分析 | 数据生成时间: {generated_at}")

# ============= 应用筛选到数据 =============
filtered_stocks = all_stocks.copy() if all_stocks else []

# 定义筛选函数
def apply_filters(data_list):
    """应用筛选条件到数据列表"""
    if not data_list:
        return []
    
    df = pd.DataFrame(data_list)
    
    # 应用趋势筛选
    if filter_trend != "全部" and 'trend' in df.columns:
        df = df[df['trend'] == filter_trend]
    
    # 应用评分筛选
    if filter_min_score > 0 and 'score' in df.columns:
        df = df[df['score'] >= filter_min_score]
    
    # 应用信号类型筛选
    if filter_signal != "全部":
        if "BUY" in filter_signal and 'score' in df.columns:
            df = df[df['score'] >= 80]
        elif "WATCH" in filter_signal and 'score' in df.columns:
            df = df[(df['score'] >= 65) & (df['score'] < 80)]
        elif "SELL" in filter_signal and 'trend' in df.columns:
            df = df[df['trend'].isin(['STRONG_DOWN', 'DOWN'])]
    
    return df.to_dict('records') if not df.empty else []

# 应用筛选到各个数据集
if all_stocks:
    filtered_stocks = apply_filters(all_stocks)
    
    # 显示筛选结果统计
    if len(filtered_stocks) != len(all_stocks):
        st.info(f"📊 筛选结果: {len(filtered_stocks)} / {len(all_stocks)} 只股票")

# 筛选信号数据
buy_signals_raw = report.get('buy_signals', [])
sell_signals_raw = report.get('sell_signals', [])
watch_list_raw = report.get('watch_list', [])

# ============= 加载CSV数据 =============
ranking_df = load_pool_ranking()

buy_signals = apply_filters(buy_signals_raw)
sell_signals = apply_filters(sell_signals_raw)
watch_list = apply_filters(watch_list_raw)

# 筛选CSV数据
if not ranking_df.empty:
    ranking_filtered = apply_filters(ranking_df.to_dict('records'))
    ranking_df_filtered = pd.DataFrame(ranking_filtered) if ranking_filtered else pd.DataFrame()
else:
    ranking_df_filtered = pd.DataFrame()

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
    st.metric("监控股票数", len(filtered_stocks))

with col2:
    st.metric("买入信号", len(buy_signals), delta=None)

with col3:
    st.metric("卖出信号", len(sell_signals), delta=None)

with col4:
    st.metric("观察列表", len(watch_list), delta=None)

st.divider()

# ============= 信号区域 =============
col_left, col_right = st.columns([1, 1])

# ========== 左侧: 买入信号 ==========
with col_left:
    st.markdown("### 🟢 买入信号")
    
    if buy_signals:
        for sig in buy_signals[:10]:
            name = get_stock_name(sig.get('symbol', ''))
            name_display = f"<span style='font-size:0.8em; opacity:0.8'>({name})</span>" if name else ""
            with st.container():
                st.markdown(f"""
                <div class="buy-card">
                    <h4>{sig['symbol']} {name_display}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:3])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无买入信号")
    
    st.markdown("### 🔴 卖出信号")
    
    if sell_signals:
        for sig in sell_signals[:5]:
            name = get_stock_name(sig.get('symbol', ''))
            name_display = f"<span style='font-size:0.8em; opacity:0.8'>({name})</span>" if name else ""
            with st.container():
                st.markdown(f"""
                <div class="sell-card">
                    <h4>{sig['symbol']} {name_display}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:3])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无卖出信号")

# ========== 右侧: 观察列表 ==========
with col_right:
    st.markdown("### 🟡 观察列表")
    
    if watch_list:
        for sig in watch_list[:10]:
            name = get_stock_name(sig.get('symbol', ''))
            name_display = f"<span style='font-size:0.8em; opacity:0.8'>({name})</span>" if name else ""
            with st.container():
                st.markdown(f"""
                <div class="watch-card">
                    <h4>{sig['symbol']} {name_display}</h4>
                    <p>评分: <strong>{sig['score']:.0f}</strong> | 价格: ¥{sig['close']:.2f} | 涨跌: {sig['change_pct']:+.2f}%</p>
                    <small>{' | '.join(sig['reasons'][:2])}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("暂无观察股票")

st.divider()

# ============= 全部股票列表 =============
st.subheader(f"📈 股票池全列表 ({len(filtered_stocks)}只)")

if filtered_stocks:
    # 转换为DataFrame显示
    df_data = []
    for i, item in enumerate(filtered_stocks, 1):
        score_class = get_score_class(item['score'])
        trend_class = get_trend_class(item.get('trend', ''))
        
        # 获取信号标签
        signals = item.get('signals', [])
        signal_badge = ""
        if item['score'] >= 80:
            signal_badge = "🟢BUY"
        elif item['score'] >= 65:
            signal_badge = "🟡WATCH"
        elif item.get('trend') in ['STRONG_DOWN', 'DOWN']:
            signal_badge = "🔴SELL"
        
        df_data.append({
            '排名': i,
            '信号': signal_badge,
            '代码': item['symbol'],
            '名称': get_stock_name(item.get('symbol', '')),
            '评分': item['score'],
            '最新价': f"¥{item['close']:.2f}",
            '涨跌幅': f"{item['change_pct']:+.2f}%",
            '趋势': item.get('trend', ''),
            'MA5': item.get('ma5'),
            'MA10': item.get('ma10'),
            'MA20': item.get('ma20'),
            'MA60': item.get('ma60'),
            '量比': item.get('vol_ratio'),
            '主要信号': ' | '.join(signals[:2]) if signals else ''
        })
    
    df = pd.DataFrame(df_data)
    
    # 使用自定义样式 - 显示所有股票
    st.dataframe(
        df,
        column_config={
            "排名": st.column_config.NumberColumn("排名", width="small"),
            "信号": st.column_config.TextColumn("信号", width="small"),
            "代码": st.column_config.TextColumn("代码", width="medium"),
            "名称": st.column_config.TextColumn("名称", width="medium"),
            "评分": st.column_config.NumberColumn(
                "评分",
                help="综合评分 0-100",
                format="%.1f",
                width="small"
            ),
            "最新价": st.column_config.TextColumn("最新价", width="small"),
            "涨跌幅": st.column_config.TextColumn("涨跌幅", width="small"),
            "趋势": st.column_config.TextColumn("趋势", width="small"),
            "MA5": st.column_config.NumberColumn("MA5", format="%.2f", width="small"),
            "MA10": st.column_config.NumberColumn("MA10", format="%.2f", width="small"),
            "MA20": st.column_config.NumberColumn("MA20", format="%.2f", width="small"),
            "MA60": st.column_config.NumberColumn("MA60", format="%.2f", width="small"),
            "量比": st.column_config.NumberColumn("量比", format="%.2f", width="small"),
            "主要信号": st.column_config.TextColumn("主要信号", width="large"),
        },
        hide_index=True,
        use_container_width=True,
        height=600  # 固定高度支持滚动
    )
else:
    st.info("暂无股票数据")

# ============= 详细数据表格（从CSV读取最新数据） =============
if not ranking_df_filtered.empty:
    st.divider()
    st.subheader(f"📋 详细技术指标 (CSV数据) - {len(ranking_df_filtered)}只股票")
    
    # 选择显示的列
    display_cols = ['symbol', 'name', 'score', 'close', 'change_pct', 
                   'ma5', 'ma10', 'ma20', 'ma60', 'vol_ratio', 'trend', 'signals']
    
    available_cols = [c for c in display_cols if c in ranking_df_filtered.columns]
    df_display = ranking_df_filtered[available_cols]
    
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
    
    st.dataframe(df_display, hide_index=True, use_container_width=True, height=400)

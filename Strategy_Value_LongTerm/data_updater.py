"""
数据更新模块
下载并清洗股票数据，用于长线组合优化
"""

import akshare as ak
import baostock as bs  # 备用数据源
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
import yaml
from scipy import stats

# 尝试导入可选库
try:
    import quantdata as qd
    HAS_QUANTDATA = True
except ImportError:
    HAS_QUANTDATA = False


class TrendAnalyzer:
    """趋势分析器 - 计算 MA / RSRS / LLT 等技术指标"""

    def __init__(self):
        pass

    def calculate_ma(self, df: pd.DataFrame, periods: list = [60, 120, 250]) -> pd.DataFrame:
        """计算多条均线"""
        result = df.copy()
        for period in periods:
            result[f'ma_{period}'] = result['close'].rolling(period).mean()
        return result

    def calculate_rsrs(self, df: pd.DataFrame, n: int = 18) -> pd.Series:
        """计算 RSRS 斜率"""
        rsrs_values = []

        for i in range(len(df)):
            if i < n:
                rsrs_values.append(np.nan)
                continue

            highs = df['high'].iloc[i-n:i]
            lows = df['low'].iloc[i-n:i]

            if lows.std() == 0 or highs.std() == 0:
                rsrs_values.append(np.nan)
                continue

            slope, _, _, _, _ = stats.linregress(lows, highs)
            rsrs_values.append(slope)

        return pd.Series(rsrs_values, index=df.index)

    def calculate_rsrs_zscore(self, df: pd.DataFrame, n: int = 18, window: int = 600) -> pd.Series:
        """计算 RSRS Z-Score"""
        rsrs = self.calculate_rsrs(df, n)
        rsrs_mean = rsrs.rolling(window).mean()
        rsrs_std = rsrs.rolling(window).std()

        rsrs_std = rsrs_std.replace(0, np.nan)
        zscore = (rsrs - rsrs_mean) / rsrs_std

        return zscore

    def calculate_llt(self, df: pd.DataFrame, n: int = 10) -> pd.Series:
        """计算 LLT 低延迟趋势线"""
        price = df['close'].values
        llt = np.full(len(price), np.nan)

        alpha = 2 / (n + 1)

        llt[0] = price[0]
        llt[1] = price[1]

        for i in range(2, len(price)):
            llt[i] = alpha * price[i] + (1 - alpha) * llt[i-1]

        return pd.Series(llt, index=df.index)

    def calculate_pe_percentile(self, pe_series: pd.Series, window: int = 2500) -> float:
        """
        计算当前 PE 在历史中的分位数
        约 10 年历史数据
        """
        if len(pe_series) < window:
            return 1.0  # 数据不足，默认为不便宜

        current_pe = pe_series.iloc[-1]
        hist_pe = pe_series.iloc[-window:-1]

        if hist_pe.min() == hist_pe.max():
            return 0.5

        percentile = (current_pe - hist_pe.min()) / (hist_pe.max() - hist_pe.min())
        return min(1.0, max(0.0, percentile))

    def analyze_stock(self, df: pd.DataFrame, pe_percentile: float,
                      ma_period: int = 250, rsrs_threshold: float = 0.7) -> dict:
        """
        综合分析单只股票

        Args:
            df: 价格数据 (包含 high, low, close)
            pe_percentile: PE 分位数
            ma_period: 均线周期
            rsrs_threshold: RSRS Z-Score 阈值

        Returns:
            分析结果字典
        """
        if len(df) < ma_period:
            return {'status': 'INSUFFICIENT_DATA', 'reason': f'数据不足 {len(df)} 天'}

        result = {
            'status': 'IGNORE',
            'value_score': 0,
            'trend_score': 0,
            'details': {}
        }

        # 1. 价值判断
        is_undervalued = pe_percentile < 0.4
        result['value_score'] = 1 if is_undervalued else 0
        result['pe_percentile'] = round(pe_percentile, 2)

        # 2. MA 判断
        df_ma = self.calculate_ma(df, [ma_period])
        current_price = df['close'].iloc[-1]
        ma_value = df_ma[f'ma_{ma_period}'].iloc[-1]
        ma_slope = (df_ma[f'ma_{ma_period}'].iloc[-1] - df_ma[f'ma_{ma_period}'].iloc[-20]) / 20

        ma_ok = current_price > ma_value and ma_slope > 0
        result['details']['ma_price'] = round(current_price, 2)
        result['details']['ma_250'] = round(ma_value, 2)
        result['details']['ma_ok'] = ma_ok

        # 3. RSRS 判断
        rsrs_zscore = self.calculate_rsrs_zscore(df)
        rsrs_ok = rsrs_zscore.iloc[-1] > rsrs_threshold if not rsrs_zscore.empty else False
        result['details']['rsrs_zscore'] = round(rsrs_zscore.iloc[-1], 2) if not rsrs_zscore.empty else None
        result['details']['rsrs_ok'] = rsrs_ok

        # 4. LLT 判断
        llt = self.calculate_llt(df)
        llt_trending_up = llt.iloc[-1] > llt.iloc[-2]
        price_above_llt = current_price > llt.iloc[-1]
        llt_ok = price_above_llt and llt_trending_up
        result['details']['llt_ok'] = llt_ok

        # 5. 综合趋势得分
        trend_signals = sum([ma_ok, rsrs_ok, llt_ok])
        result['trend_score'] = trend_signals / 3

        # 6. 最终状态判断
        if is_undervalued and trend_signals >= 2:
            result['status'] = 'STRONG_BUY'
        elif is_undervalued and trend_signals == 1:
            result['status'] = 'WATCH_LIST'
        elif is_undervalued and trend_signals == 0:
            result['status'] = 'WAIT'
        else:
            result['status'] = 'IGNORE'

        return result


class DataUpdater:
    """股票数据更新器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.data_dir = os.path.join(os.path.dirname(config_path), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        # 初始化 baostock
        bs.login()

    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def get_trend_filter_config(self) -> dict:
        """获取趋势过滤配置"""
        return self.config.get('trend_filter', {
            'enabled': False,
            'min_trend_score': 0.33,
            'ma_period': 250,
            'rsrs_threshold': 0.7,
            'pe_percentile_threshold': 0.4
        })

    def get_stock_data(self, symbol: str, period: str = "2300d", retry: int = 2) -> pd.DataFrame:
        """
        获取单只股票历史数据

        Args:
            symbol: 股票代码，如 "600519.SH"
            period: 数据周期，默认2300天(约9年)
            retry: 重试次数

        Returns:
            包含 OHLCV 数据的 DataFrame
        """
        # 先尝试 akshare
        for i in range(retry):
            try:
                time.sleep(0.5)
                df = ak.stock_zh_a_hist(
                    symbol=symbol.replace(".SH", "").replace(".SZ", ""),
                    period="daily",
                    start_date="20100101",
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust="qfq"
                )
                df.set_index('日期', inplace=True)
                df.sort_index(inplace=True)
                return df
            except Exception as e:
                if i < retry - 1:
                    time.sleep(1)
                else:
                    pass  # 静默，使用备用

        # 使用 baostock 备用
        try:
            code = symbol.replace(".SH", "").replace(".SZ", "")
            if symbol.endswith(".SH"):
                bs_code = f"sh.{code}"
            else:
                bs_code = f"sz.{code}"

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount",
                start_date='2010-01-01',
                end_date=datetime.now().strftime("%Y-%m-%d"),
                frequency="d",
                adjustflag="2"  # 前复权
            )

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                df.set_index('date', inplace=True)
                df = df[['open', 'high', 'low', 'close', 'volume']]
                df = df.astype(float)
                return df
        except Exception as e:
            print(f"baostock 获取 {symbol} 也失败: {e}")

        return pd.DataFrame()

    def get_etf_data(self, symbol: str, retry: int = 3) -> pd.DataFrame:
        """获取ETF数据"""
        for i in range(retry):
            try:
                time.sleep(1)
                df = ak.fund_etf_hist_em(
                    symbol=symbol.replace(".SH", "").replace(".SZ", ""),
                    period="daily",
                    start_date="20100101",
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust="qfq"
                )
                df.set_index('日期', inplace=True)
                df.sort_index(inplace=True)
                return df
            except Exception as e:
                if i < retry - 1:
                    print(f"  重试 {i+1}/{retry-1}...")
                    time.sleep(2)
                else:
                    print(f"获取 {symbol} 数据失败: {e}")
                    return pd.DataFrame()
        return pd.DataFrame()

    def download_all_data(self) -> pd.DataFrame:
        """
        下载所有配置中的股票数据
        返回合并的收盘价序列
        """
        price_data = {}
        symbols = self.config['data_source']['stock_list']

        for symbol in symbols:
            print(f"正在下载 {symbol} ...")

            if symbol.endswith(".SH") or symbol.endswith(".SZ"):
                df = self.get_stock_data(symbol)
            else:
                df = self.get_etf_data(symbol)

            if df.empty:
                print(f"  数据为空，跳过")
                continue

            # 兼容中文和英文列名
            close_col = '收盘' if '收盘' in df.columns else 'close'
            if close_col in df.columns:
                price_data[symbol] = df[close_col]
                print(f"  成功获取 {len(df)} 条数据")
            else:
                print(f"  无收盘价列，可用列: {list(df.columns)}")

        # 合并为 DataFrame
        prices = pd.DataFrame(price_data)

        if prices.empty:
            print("警告: 未获取到任何数据")
            return prices

        prices.index = pd.to_datetime(prices.index)

        # 保存
        prices.to_csv(os.path.join(self.data_dir, "prices.csv"))
        print(f"数据已保存至 {self.data_dir}/prices.csv")

        return prices

    def calculate_returns(self, prices: pd.DataFrame = None) -> pd.DataFrame:
        """计算收益率"""
        if prices is None:
            prices = pd.read_csv(
                os.path.join(self.data_dir, "prices.csv"),
                index_col=0, parse_dates=True
            )

        returns = prices.pct_change().dropna()
        returns.to_csv(os.path.join(self.data_dir, "returns.csv"))
        return returns

    def get_risk_free_rate(self) -> float:
        """获取无风险利率 (10年期国债收益率)"""
        try:
            df = ak.bond_china_yield_curve()
            # 取最新一期数据
            latest = df.iloc[-1]
            # 返回年化收益率 (转为小数)
            return float(latest['中证10年']) / 100
        except:
            # 默认值
            return 0.025

    def analyze_all_stocks(self, prices: pd.DataFrame = None,
                           pe_percentiles: dict = None) -> dict:
        """
        分析所有股票的趋势信号

        Args:
            prices: 价格数据
            pe_percentiles: {symbol: pe_percentile} 字典

        Returns:
            {symbol: analysis_result}
        """
        if prices is None:
            prices = pd.read_csv(
                os.path.join(self.data_dir, "prices.csv"),
                index_col=0, parse_dates=True
            )

        if pe_percentiles is None:
            pe_percentiles = {symbol: 0.3 for symbol in prices.columns}

        config = self.get_trend_filter_config()
        analyzer = TrendAnalyzer()

        results = {}

        print("\n" + "=" * 60)
        print("趋势分析结果")
        print("=" * 60)
        print(f"{'代码':<12} {'状态':<12} {'PE分位':<8} {'MA250':<8} {'RSRS':<8} {'趋势分':<8}")
        print("-" * 60)

        for symbol in prices.columns:
            df = prices[[symbol]].copy()
            df.columns = ['close']

            # 尝试添加 high/low 列
            if 'high' not in df.columns or 'low' not in df.columns:
                # 用 close 近似 (实际应该从原始数据获取)
                df['high'] = df['close'] * 1.02
                df['low'] = df['close'] * 0.98
            else:
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)

            pe_pct = pe_percentiles.get(symbol, 0.5)

            result = analyzer.analyze_stock(
                df,
                pe_percentile=pe_pct,
                ma_period=config['ma_period'],
                rsrs_threshold=config['rsrs_threshold']
            )

            results[symbol] = result

            # 打印结果
            status = result['status']
            pe_p = result.get('pe_percentile', 'N/A')
            ma_r = result['details'].get('ma_250', 'N/A')
            rsrs = result['details'].get('rsrs_zscore', 'N/A')
            trend_s = round(result['trend_score'], 2) if result['trend_score'] else 0

            print(f"{symbol:<12} {status:<12} {pe_p:<8} {ma_r:<8} {rsrs:<8} {trend_s:<8}")

        print("=" * 60)

        # 统计
        strong_buy = [s for s, r in results.items() if r['status'] == 'STRONG_BUY']
        watch_list = [s for s, r in results.items() if r['status'] == 'WATCH_LIST']
        ignore = [s for s, r in results.items() if r['status'] == 'IGNORE']

        print(f"\n买入信号: {len(strong_buy)} 只 -> {strong_buy}")
        print(f"观察名单: {len(watch_list)} 只 -> {watch_list}")
        print(f"忽略: {len(ignore)} 只")

        # 转换 numpy 类型为 Python 原生类型
        def _convert(obj):
            if isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_convert(item) for item in obj]
            elif isinstance(obj, (np.bool_, np.integer)):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            else:
                return obj

        return _convert(results)


if __name__ == "__main__":
    updater = DataUpdater()
    prices = updater.download_all_data()
    returns = updater.calculate_returns()
    rf_rate = updater.get_risk_free_rate()
    print(f"无风险利率: {rf_rate:.2%}")
    print(f"数据范围: {prices.index[0]} ~ {prices.index[-1]}")
    print(f"资产数量: {len(prices.columns)}")

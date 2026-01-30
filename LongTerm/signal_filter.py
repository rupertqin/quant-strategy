"""
趋势过滤模块 - 基本面 + 技术面双重确认

提供三种趋势识别算法：
1. MA 均线过滤 (经典稳健)
2. RSRS 阻力支撑相对强度 (进阶算法)
3. LLT 低延迟趋势线 (快速响应)

用于在价值选股后进行"右侧确认"，避免价值陷阱。
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Tuple, Optional
from enum import Enum


class TrendStatus(Enum):
    """趋势状态"""
    STRONG_UP = "强上升"
    UP = "上升"
    NEUTRAL = "震荡"
    DOWN = "下降"
    STRONG_DOWN = "强下降"


class TrendFilter:
    """趋势过滤器"""

    def __init__(self, price_col: str = 'close', high_col: str = 'high', low_col: str = 'low'):
        """
        初始化趋势过滤器

        Args:
            price_col: 价格列名
            high_col: 最高价列名
            low_col: 最低价列名
        """
        self.price_col = price_col
        self.high_col = high_col
        self.low_col = low_col

    # ==================== MA 均线系统 ====================

    def calculate_ma(self, df: pd.DataFrame, periods: List[int] = [60, 120, 250]) -> pd.DataFrame:
        """
        计算多条均线

        Args:
            df: 价格数据
            periods: 周期列表

        Returns:
            包含均线的 DataFrame
        """
        result = df.copy()
        for period in periods:
            result[f'ma_{period}'] = result[self.price_col].rolling(period).mean()
        return result

    def ma_trend_status(self, df: pd.DataFrame, ma_period: int = 250) -> TrendStatus:
        """
        根据 MA 判断趋势状态

        逻辑：
        - 价格在 MA 上方 + MA 向上 = 上升趋势
        - 价格在 MA 下方 + MA 向下 = 下降趋势
        - 其他情况 = 震荡
        """
        if len(df) < ma_period:
            return TrendStatus.NEUTRAL

        current_price = df[self.price_col].iloc[-1]
        ma_value = df[f'ma_{ma_period}'].iloc[-1]

        # 计算 MA 的斜率 (过去 20 个交易日)
        ma_series = df[f'ma_{ma_period}']
        if len(ma_series) < 20:
            return TrendStatus.NEUTRAL

        ma_slope = (ma_series.iloc[-1] - ma_series.iloc[-20]) / 20

        # 判断趋势
        if current_price > ma_value and ma_slope > 0.001:
            return TrendStatus.STRONG_UP
        elif current_price > ma_value and ma_slope > 0:
            return TrendStatus.UP
        elif current_price < ma_value and ma_slope < -0.001:
            return TrendStatus.STRONG_DOWN
        elif current_price < ma_value and ma_slope < 0:
            return TrendStatus.DOWN
        else:
            return TrendStatus.NEUTRAL

    def ma_filter(self, df: pd.DataFrame, ma_period: int = 250) -> pd.Series:
        """
        MA 过滤：价格在 MA 上方才能买入

        Returns:
            布尔 Series，True 表示通过过滤
        """
        if f'ma_{ma_period}' not in df.columns:
            df = self.calculate_ma(df, [ma_period])

        above_ma = df[self.price_col] > df[f'ma_{ma_period}']
        ma_trending_up = df[f'ma_{ma_period}'].diff(20) > 0

        return above_ma & ma_trending_up

    # ==================== RSRS 阻力支撑相对强度 ====================

    def calculate_rsrs(self, df: pd.DataFrame, n: int = 18) -> pd.Series:
        """
        计算 RSRS 指标

        RSRS 通过线性回归计算最高价与最低价的斜率
        斜率越大，说明上涨阻力越小，支撑越强

        Args:
            df: 包含 high, low 列的数据
            n: 回归周期

        Returns:
            RSRS 斜率序列
        """
        rsrs_values = []

        for i in range(len(df)):
            if i < n:
                rsrs_values.append(np.nan)
                continue

            highs = df[self.high_col].iloc[i-n:i]
            lows = df[self.low_col].iloc[i-n:i]

            # 线性回归: High = Beta * Low + Alpha
            # Beta 就是 RSRS 斜率
            if lows.std() == 0 or highs.std() == 0:
                rsrs_values.append(np.nan)
                continue

            slope, _, _, _, _ = stats.linregress(lows, highs)
            rsrs_values.append(slope)

        return pd.Series(rsrs_values, index=df.index)

    def rsrs_signal(self, df: pd.DataFrame, n: int = 18, threshold: float = 1.0,
                     zscore_window: int = 600) -> Tuple[pd.Series, pd.Series]:
        """
        RSRS 择时信号

        逻辑：
        1. 计算原始 RSRS 斜率
        2. 计算 Z-Score 标准化 (RSRS 的相对位置)
        3. Z-Score > threshold -> 买入信号

        Args:
            df: 价格数据
            n: 回归周期
            threshold: Z-Score 阈值 (通常 0.7 ~ 1.0)
            zscore_window: Z-Score 计算窗口

        Returns:
            (rsrs_series, signal_series)
        """
        # 计算 RSRS 斜率
        rsrs = self.calculate_rsrs(df, n)

        # 计算 Z-Score
        rsrs_mean = rsrs.rolling(zscore_window).mean()
        rsrs_std = rsrs.rolling(zscore_window).std()

        # 避免除零
        rsrs_std = rsrs_std.replace(0, np.nan)
        zscore = (rsrs - rsrs_mean) / rsrs_std

        # 生成信号: Z-Score > threshold
        signal = zscore > threshold

        return rsrs, signal

    def rsrs_trend_status(self, rsrs: pd.Series) -> TrendStatus:
        """
        根据 RSRS 值判断趋势状态
        """
        if rsrs.iloc[-1] > 1.2:
            return TrendStatus.STRONG_UP
        elif rsrs.iloc[-1] > 0.8:
            return TrendStatus.UP
        elif rsrs.iloc[-1] < 0.5:
            return TrendStatus.DOWN
        elif rsrs.iloc[-1] < 0.8:
            return TrendStatus.STRONG_DOWN
        else:
            return TrendStatus.NEUTRAL

    # ==================== LLT 低延迟趋势线 ====================

    def calculate_llt(self, df: pd.DataFrame, n: int = 10, alpha: float = 0.6) -> pd.Series:
        """
        计算 LLT (Low Lag Trend) 低延迟趋势线

        LLT 是二阶滤波器，比普通均线更快反应价格拐点

        公式:
        LLT_t = alpha * 2 * LLT_{t-1} - alpha^2 * LLT_{t-2} + (1 - alpha) * price_t
                - 2 * (1 - alpha) * price_{t-1} + (1 - alpha)^2 * price_{t-2}

        Args:
            df: 价格数据
            n: 平滑参数 (影响滞后性)
            alpha: 平滑因子 (0 < alpha < 1)，越大越灵敏

        Returns:
            LLT 序列
        """
        price = df[self.price_col].values
        llt = np.full(len(price), np.nan)

        # 简化版 LLT (一阶)
        llt[0] = price[0]
        llt[1] = price[1]

        alpha = 2 / (n + 1)  # 根据窗口计算 alpha

        for i in range(2, len(price)):
            llt[i] = alpha * price[i] + (1 - alpha) * llt[i-1]

        return pd.Series(llt, index=df.index)

    def llt_trend_status(self, llt: pd.Series) -> TrendStatus:
        """
        根据 LLT 判断趋势状态
        """
        if len(llt) < 2:
            return TrendStatus.NEUTRAL

        current = llt.iloc[-1]
        prev = llt.iloc[-2]

        # 计算 LLT 斜率
        llt_slope = current - prev

        # 价格与 LLT 的位置关系
        # (需要传入原始价格数据，这里简化处理)

        if llt_slope > 0.5:  # 阈值需要根据价格水平调整
            return TrendStatus.STRONG_UP
        elif llt_slope > 0:
            return TrendStatus.UP
        elif llt_slope < -0.5:
            return TrendStatus.STRONG_DOWN
        elif llt_slope < 0:
            return TrendStatus.DOWN
        else:
            return TrendStatus.NEUTRAL

    def llt_filter(self, df: pd.DataFrame, llt: pd.Series) -> pd.Series:
        """
        LLT 过滤：价格从下方突破 LLT 且 LLT 向上

        Returns:
            布尔 Series
        """
        price = df[self.price_col]
        llt_trending_up = llt.diff() > 0
        price_above_llt = price > llt

        return price_above_llt & llt_trending_up

    # ==================== 综合信号 ====================

    def check_buy_signal(self, df: pd.DataFrame,
                         pe_percentile: float,
                         ma_period: int = 250,
                         rsrs_threshold: float = 0.7) -> Dict:
        """
        综合买入信号检查 (价值 + 趋势共振)

        Args:
            df: 价格数据
            pe_percentile: PE 分位数 (0~1)
            ma_period: 均线周期
            rsrs_threshold: RSRS Z-Score 阈值

        Returns:
            信号结果字典
        """
        result = {
            'status': 'IGNORE',
            'value_score': 0,
            'trend_score': 0,
            'details': {}
        }

        # 1. 价值判断
        is_undervalued = pe_percentile < 0.4  # PE 分位数 < 40%
        result['value_score'] = 1 if is_undervalued else 0

        # 2. 计算技术指标
        df_with_ma = self.calculate_ma(df, [ma_period])
        rsrs, rsrs_signal = self.rsrs_signal(df)
        llt = self.calculate_llt(df)

        # 3. MA 趋势判断
        ma_status = self.ma_trend_status(df_with_ma, ma_period)
        ma_ok = ma_status in [TrendStatus.STRONG_UP, TrendStatus.UP]
        result['details']['ma_status'] = ma_status.value

        # 4. RSRS 判断
        rsrs_ok = rsrs_signal.iloc[-1] if not rsrs_signal.empty else False
        result['details']['rsrs_zscore'] = round(rsrs.iloc[-1], 2) if not rsrs.empty else None

        # 5. LLT 判断
        llt_ok = self.llt_filter(df, llt).iloc[-1] if len(df) > 2 else False
        result['details']['llt_status'] = 'up' if llt_ok else 'down'

        # 6. 趋势综合得分
        trend_signals = sum([ma_ok, rsrs_ok, llt_ok])
        result['trend_score'] = trend_signals / 3

        # 7. 最终判断
        if is_undervalued and trend_signals >= 2:
            result['status'] = 'STRONG_BUY'  # 价值 + 趋势共振
        elif is_undervalued and trend_signals == 1:
            result['status'] = 'WATCH_LIST'  # 便宜但趋势一般
        elif is_undervalued and trend_signals == 0:
            result['status'] = 'WAIT'  # 便宜但趋势向下
        else:
            result['status'] = 'IGNORE'  # 不便宜

        result['details']['ma_ok'] = ma_ok
        result['details']['rsrs_ok'] = rsrs_ok
        result['details']['llt_ok'] = llt_ok

        return result

    def filter_universe(self, prices: Dict[str, pd.DataFrame],
                        pe_percentiles: Dict[str, float],
                        min_trend_score: float = 0.33) -> Dict[str, Dict]:
        """
        对整个股票池进行趋势过滤

        Args:
            prices: {symbol: price_df}
            pe_percentiles: {symbol: pe_percentile}
            min_trend_score: 最小趋势得分

        Returns:
            {symbol: signal_result}
        """
        results = {}

        for symbol, df in prices.items():
            if len(df) < 250:  # 数据不足
                results[symbol] = {
                    'status': 'INSUFFICIENT_DATA',
                    'reason': f'数据不足 {len(df)} 天'
                }
                continue

            pe_pct = pe_percentiles.get(symbol, 1.0)  # 默认不便宜

            signal = self.check_buy_signal(df, pe_pct)
            results[symbol] = signal

        return results


# ==================== 便捷函数 ====================

def ma_cross_signal(df: pd.DataFrame, short_ma: int = 60, long_ma: int = 250) -> pd.Series:
    """
    MA 金叉死叉信号

    Args:
        df: 价格数据
        short_ma: 短期均线周期
        long_ma: 长期均线周期

    Returns:
        1 = 金叉 (买入), -1 = 死叉 (卖出), 0 = 无信号
    """
    ma_short = df['close'].rolling(short_ma).mean()
    ma_long = df['close'].rolling(long_ma).mean()

    # 当前位置
    above = ma_short.iloc[-1] > ma_long.iloc[-1]
    # 之前位置 (5 天前)
    above_prev = ma_short.iloc[-5] > ma_long.iloc[-5] if len(df) > 5 else above

    if above and not above_prev:
        return 1  # 金叉
    elif not above and above_prev:
        return -1  # 死叉
    else:
        return 0


def calculate_momentum(df: pd.DataFrame, periods: List[int] = [60, 120, 250]) -> pd.DataFrame:
    """
    计算各周期动量

    Returns:
        包含动量值的 DataFrame
    """
    result = pd.DataFrame(index=df.index)

    for period in periods:
        result[f'momentum_{period}'] = df['close'] / df['close'].shift(period) - 1

    return result


if __name__ == "__main__":
    # 测试代码
    import yfinance as yf

    # 下载测试数据
    data = yf.download("000001.SS", start="2020-01-01")  # 上证指数
    data.columns = [c.lower() for c in data.columns]

    tf = TrendFilter()

    # 计算 MA
    data = tf.calculate_ma(data, [250])

    # 计算 RSRS
    rsrs, signal = tf.rsrs_signal(data)

    # 测试买入信号
    result = tf.check_buy_signal(data, pe_percentile=0.3)

    print("趋势状态:", result)
    print("MA 状态:", result['details']['ma_status'])
    print("RSRS Z-Score:", result['details']['rsrs_zscore'])

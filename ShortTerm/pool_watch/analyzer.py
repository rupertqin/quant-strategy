"""
技术分析模块 - 股票池短线指标计算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class TrendState(Enum):
    """趋势状态"""
    STRONG_UP = "STRONG_UP"
    UP = "UP"
    SIDEWAYS = "SIDEWAYS"
    DOWN = "DOWN"
    STRONG_DOWN = "STRONG_DOWN"


class VolumeSignal(Enum):
    """量价信号"""
    VOL_UP_PRICE_UP = "放量上涨"
    VOL_DOWN_PRICE_UP = "缩量上涨"
    VOL_FLAT_PRICE_UP = "价涨量平"
    VOL_UP_PRICE_DOWN = "放量下跌"
    VOL_DOWN_PRICE_DOWN = "缩量下跌"
    VOL_FLAT_PRICE_DOWN = "价跌量平"
    NEUTRAL = "量价齐平"


@dataclass
class TechnicalIndicators:
    """技术指标数据类"""
    symbol: str
    name: str = ""  # 股票名称
    
    # 价格数据
    close: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    change_pct: float = 0.0
    
    # 均线
    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    
    # 成交量
    volume: float = 0.0
    vol_ratio: float = 1.0  # 量比
    
    # 分析结果
    trend: TrendState = TrendState.SIDEWAYS
    volume_signal: VolumeSignal = VolumeSignal.NEUTRAL
    composite_score: float = 50.0  # 综合评分 0-100
    
    # 信号
    signals: List[str] = None
    
    def __post_init__(self):
        if self.signals is None:
            self.signals = []


class TechnicalAnalyzer:
    """技术分析器 - 计算短线技术指标"""

    def __init__(self):
        pass

    def calculate_ma(self, df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
        """计算多条移动平均线"""
        if periods is None:
            periods = [5, 10, 20, 60]
        
        result = df.copy()
        for period in periods:
            result[f'ma{period}'] = result['close'].rolling(window=period, min_periods=1).mean()
        return result

    def calculate_vol_ratio(self, df: pd.DataFrame, period: int = 5) -> pd.Series:
        """计算量比 (当前成交量 / 近期平均成交量)"""
        avg_vol = df['volume'].rolling(window=period, min_periods=1).mean()
        vol_ratio = df['volume'] / avg_vol.replace(0, np.nan)
        return vol_ratio.fillna(1.0)

    def analyze_trend(self, row: pd.Series) -> TrendState:
        """分析趋势状态"""
        close = row.get('close', 0)
        ma5 = row.get('ma5', close)
        ma10 = row.get('ma10', close)
        ma20 = row.get('ma20', close)
        ma60 = row.get('ma60', close)
        
        # 多头排列
        if close > ma5 > ma10 > ma20 > ma60:
            return TrendState.STRONG_UP
        # 短期多头
        elif close > ma5 > ma10:
            return TrendState.UP
        # 空头排列
        elif close < ma5 < ma10 < ma20 < ma60:
            return TrendState.STRONG_DOWN
        # 短期空头
        elif close < ma5 < ma10:
            return TrendState.DOWN
        else:
            return TrendState.SIDEWAYS

    def analyze_volume_price(self, row: pd.Series) -> VolumeSignal:
        """分析量价关系"""
        change_pct = row.get('change_pct', 0)
        vol_ratio = row.get('vol_ratio', 1)
        
        if change_pct > 0.5:  # 上涨
            if vol_ratio > 1.5:
                return VolumeSignal.VOL_UP_PRICE_UP
            elif vol_ratio < 0.8:
                return VolumeSignal.VOL_DOWN_PRICE_UP
            else:
                return VolumeSignal.VOL_FLAT_PRICE_UP
        elif change_pct < -0.5:  # 下跌
            if vol_ratio > 1.5:
                return VolumeSignal.VOL_UP_PRICE_DOWN
            elif vol_ratio < 0.8:
                return VolumeSignal.VOL_DOWN_PRICE_DOWN
            else:
                return VolumeSignal.VOL_FLAT_PRICE_DOWN
        else:
            return VolumeSignal.NEUTRAL

    def calculate_composite_score(self, row: pd.Series) -> float:
        """
        计算综合评分 (0-100)
        
        评分维度:
        - 趋势得分 (35%)
        - 均线排列 (25%)
        - 量价关系 (25%)
        - 位置得分 (15%)
        """
        score = 50  # 基础分
        
        # 1. 趋势得分 (35分)
        trend = row.get('trend', TrendState.SIDEWAYS)
        trend_scores = {
            TrendState.STRONG_UP: 35,
            TrendState.UP: 25,
            TrendState.SIDEWAYS: 15,
            TrendState.DOWN: 5,
            TrendState.STRONG_DOWN: 0
        }
        score += trend_scores.get(trend, 15)
        
        # 2. 均线排列得分 (25分)
        close = row.get('close', 0)
        ma5 = row.get('ma5', close)
        ma10 = row.get('ma10', close)
        ma20 = row.get('ma20', close)
        
        ma_score = 0
        if close > ma5:
            ma_score += 10
        if ma5 > ma10:
            ma_score += 8
        if ma10 > ma20:
            ma_score += 7
        score += ma_score
        
        # 3. 量价关系得分 (25分)
        vol_signal = row.get('volume_signal', VolumeSignal.NEUTRAL)
        vol_scores = {
            VolumeSignal.VOL_UP_PRICE_UP: 25,
            VolumeSignal.VOL_FLAT_PRICE_UP: 18,
            VolumeSignal.VOL_DOWN_PRICE_UP: 12,
            VolumeSignal.NEUTRAL: 12,
            VolumeSignal.VOL_DOWN_PRICE_DOWN: 8,
            VolumeSignal.VOL_FLAT_PRICE_DOWN: 5,
            VolumeSignal.VOL_UP_PRICE_DOWN: 0
        }
        score += vol_scores.get(vol_signal, 12)
        
        # 4. 位置得分 (15分) - 相对于MA60的位置
        ma60 = row.get('ma60', close)
        if close > 0 and ma60 > 0:
            if close > ma60:
                # 在MA60上方，偏离度越大得分越高（但不过度偏离）
                deviation = min((close - ma60) / ma60, 0.2)  # 最多20%偏离
                score += deviation / 0.2 * 15
            else:
                # 在MA60下方
                deviation = max((close - ma60) / ma60, -0.2)
                score += (1 + deviation / 0.2) * 5
        
        return min(max(score, 0), 100)

    def generate_signals(self, indicators: TechnicalIndicators) -> List[str]:
        """根据技术指标生成交易信号"""
        signals = []
        
        # 均线信号
        if indicators.close > indicators.ma5 > indicators.ma10 > indicators.ma20:
            signals.append("多头排列")
            if indicators.close > indicators.ma60:
                signals.append("站上60日线")
        elif indicators.close > indicators.ma5 and indicators.ma5 > indicators.ma10:
            signals.append("短期金叉")
        
        # 量能信号
        if indicators.vol_ratio > 2.0:
            signals.append("巨量")
        elif indicators.vol_ratio > 1.5:
            signals.append("明显放量")
        
        # 量价配合
        if indicators.volume_signal == VolumeSignal.VOL_UP_PRICE_UP:
            signals.append("量价齐升")
        elif indicators.volume_signal == VolumeSignal.VOL_DOWN_PRICE_DOWN:
            signals.append("缩量回调")
        
        # 涨跌幅信号
        if indicators.change_pct > 5:
            signals.append("强势上涨")
        elif indicators.change_pct > 9.5:
            signals.append("涨停")
        elif indicators.change_pct < -5:
            signals.append("大幅下跌")
        
        return signals

    def analyze(self, df: pd.DataFrame, symbol: str, name: str = "") -> TechnicalIndicators:
        """
        分析单只股票
        
        Args:
            df: DataFrame with columns: open, high, low, close, volume
            symbol: 股票代码
            name: 股票名称
            
        Returns:
            TechnicalIndicators
        """
        if df.empty or len(df) < 5:
            return TechnicalIndicators(symbol=symbol, name=name)
        
        # 确保列名正确
        required_cols = ['close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")
        
        # 计算指标
        df = self.calculate_ma(df)
        df['vol_ratio'] = self.calculate_vol_ratio(df)
        df['change_pct'] = df['close'].pct_change() * 100
        
        # 取最新数据
        latest = df.iloc[-1]
        
        # 分析趋势和量价
        trend = self.analyze_trend(latest)
        volume_signal = self.analyze_volume_price(latest)
        
        # 创建指标对象
        indicators = TechnicalIndicators(
            symbol=symbol,
            name=name,
            close=latest['close'],
            open=latest.get('open', latest['close']),
            high=latest.get('high', latest['close']),
            low=latest.get('low', latest['close']),
            change_pct=latest.get('change_pct', 0),
            ma5=latest.get('ma5', latest['close']),
            ma10=latest.get('ma10', latest['close']),
            ma20=latest.get('ma20', latest['close']),
            ma60=latest.get('ma60', latest['close']),
            volume=latest['volume'],
            vol_ratio=latest.get('vol_ratio', 1),
            trend=trend,
            volume_signal=volume_signal,
            composite_score=0  # 临时值
        )
        
        # 计算综合评分
        indicators.composite_score = self.calculate_composite_score(latest)
        
        # 生成信号
        indicators.signals = self.generate_signals(indicators)
        
        return indicators

    def batch_analyze(self, data_dict: Dict[str, pd.DataFrame], 
                     names: Dict[str, str] = None) -> List[TechnicalIndicators]:
        """
        批量分析多只股票
        
        Args:
            data_dict: {symbol: DataFrame}
            names: {symbol: name}
            
        Returns:
            List[TechnicalIndicators]
        """
        results = []
        names = names or {}
        
        for symbol, df in data_dict.items():
            try:
                indicators = self.analyze(df, symbol, names.get(symbol, ""))
                results.append(indicators)
            except Exception as e:
                print(f"分析 {symbol} 失败: {e}")
                continue
        
        # 按综合评分排序
        results.sort(key=lambda x: x.composite_score, reverse=True)
        return results

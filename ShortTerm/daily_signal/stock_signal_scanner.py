"""
个股信号扫描器 - 生成左侧/右侧交易信号

扫描全市场股票，基于日线/周线/月线生成交易信号

用法:
    # 使用便捷脚本（推荐）
    python ShortTerm/run_signal_scan.py --all
    python ShortTerm/run_signal_scan.py --left
    python ShortTerm/run_signal_scan.py --right
    python ShortTerm/run_signal_scan.py --symbol 600519.SH
    
    # 或者直接运行扫描器
    python ShortTerm/daily_signal/stock_signal_scanner.py --all
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from lib.utils import get_stock_name
from DataHub.core.data_reader import load_stock_prices, load_stock_prices_raw

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型"""
    LEFT = "left"      # 左侧信号（抄底/反转）
    RIGHT = "right"    # 右侧信号（追涨/确认）


class SignalStrength(Enum):
    """信号强度"""
    WEAK = "weak"
    MEDIUM = "medium"
    STRONG = "strong"


@dataclass
class StockSignal:
    """个股信号数据结构"""
    symbol: str                      # 股票代码
    name: str                        # 股票名称
    signal_type: str                 # left/right
    signal_name: str                 # 信号名称
    strength: str                    # weak/medium/strong
    period: str                      # daily/weekly/monthly
    trigger_date: str                # 触发日期
    close_price: float               # 收盘价
    change_pct: float                # 涨跌幅
    volume_ratio: float              # 量比
    description: str                 # 信号描述
    score: int                       # 综合评分 0-100
    technicals: Dict                 # 技术指标详情
    
    def to_dict(self) -> dict:
        return asdict(self)


class SignalCalculator:
    """信号计算器 - 基于DataFrame计算各种信号"""

    @staticmethod
    def calculate_ma(df: pd.DataFrame) -> pd.DataFrame:
        """计算均线"""
        df = df.copy()
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        return df

    @staticmethod
    def calculate_ma_flatness(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
        """
        计算均线平缓度（走平程度）

        原理：
        1. 变异系数（CV）= 标准差 / 均值，反映相对波动程度
        2. 趋势强度 = |斜率| / 均值，反映趋势明显程度
        3. 平缓度 = 1 / (1 + CV * 10 + 趋势强度 * 100)，值越大越平缓

        Returns:
            flatness: 0-1 之间的值，1 表示完全水平，0 表示剧烈波动
        """
        df = df.copy()

        for ma in ['ma10', 'ma20', 'ma60']:
            if ma in df.columns:
                # 1. 计算均线的标准差和均值
                ma_std = df[ma].rolling(window=window).std()
                ma_mean = df[ma].rolling(window=window).mean()

                # 2. 计算变异系数（Coefficient of Variation）
                cv = ma_std / ma_mean

                # 3. 计算均线的斜率（使用最小二乘法）
                def calc_slope(x):
                    if len(x) < 2:
                        return np.nan
                    # 线性回归：y = slope * x + intercept
                    x_vals = np.arange(len(x))
                    slope = np.polyfit(x_vals, x, 1)[0]
                    return slope

                slope = df[ma].rolling(window=window).apply(calc_slope, raw=True)

                # 4. 计算趋势强度（归一化斜率）
                # 将斜率转换为每期的百分比变化
                trend_strength = np.abs(slope) / ma_mean

                # 5. 计算平缓度得分
                # 公式：flatness = 1 / (1 + CV * 20 + trend_strength * 200)
                # 当 CV=0 且 trend=0 时，flatness=1（完全平缓）
                # 当 CV=0.05 或 trend=0.005 时，flatness≈0.5
                flatness_score = 1 / (1 + cv * 20 + trend_strength * 200)

                df[f'{ma}_flatness'] = flatness_score

        return df
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast=12, slow=26, signal=9) -> pd.DataFrame:
        """计算MACD"""
        df = df.copy()
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        df['macd_dif'] = ema_fast - ema_slow
        df['macd_dea'] = df['macd_dif'].ewm(span=signal, adjust=False).mean()
        df['macd_bar'] = (df['macd_dif'] - df['macd_dea']) * 2
        return df
    
    @staticmethod
    def calculate_kdj(df: pd.DataFrame, n=9, m1=3, m2=3) -> pd.DataFrame:
        """计算KDJ"""
        df = df.copy()
        low_list = df['low'].rolling(window=n, min_periods=n).min()
        high_list = df['high'].rolling(window=n, min_periods=n).max()
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100
        
        df['kdj_k'] = rsv.ewm(alpha=1/m1, adjust=False).mean()
        df['kdj_d'] = df['kdj_k'].ewm(alpha=1/m2, adjust=False).mean()
        df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']
        return df
    
    @staticmethod
    def calculate_bollinger(df: pd.DataFrame, window=20, num_std=2) -> pd.DataFrame:
        """计算布林带"""
        df = df.copy()
        df['boll_mid'] = df['close'].rolling(window=window).mean()
        df['boll_std'] = df['close'].rolling(window=window).std()
        df['boll_up'] = df['boll_mid'] + num_std * df['boll_std']
        df['boll_down'] = df['boll_mid'] - num_std * df['boll_std']
        return df
    
    @staticmethod
    def calculate_volume_ratio(df: pd.DataFrame, window=5) -> pd.DataFrame:
        """计算量比"""
        df = df.copy()
        df['volume_ma5'] = df['volume'].rolling(window=window).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma5']
        return df


class LeftSignalDetector:
    """左侧信号检测器（抄底/反转信号）"""
    
    def __init__(self):
        self.calc = SignalCalculator()
    
    def detect_all(self, df: pd.DataFrame, symbol: str, name: str, period: str = "daily") -> List[StockSignal]:
        """检测所有左侧信号"""
        signals = []
        
        # 不同周期的最小数据要求
        min_bars = {"daily": 30, "weekly": 20, "monthly": 12}
        min_required = min_bars.get(period, 30)
        
        if df.empty or len(df) < min_required:
            return signals
        
        # 计算指标
        df = self.calc.calculate_ma(df)
        df = self.calc.calculate_macd(df)
        df = self.calc.calculate_kdj(df)
        df = self.calc.calculate_bollinger(df)
        df = self.calc.calculate_volume_ratio(df)
        
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        # 1. MACD底背离
        macd_divergence = self._detect_macd_divergence(df)
        if macd_divergence:
            signals.append(self._create_signal(
                symbol, name, "MACD底背离", macd_divergence,
                latest, "价格创新低但MACD未创新低，可能反弹", df, period
            ))
        
        # 2. KDJ底背离
        kdj_divergence = self._detect_kdj_divergence(df)
        if kdj_divergence:
            signals.append(self._create_signal(
                symbol, name, "KDJ底背离", kdj_divergence,
                latest, "价格创新低但KDJ未创新低，超卖反弹", df, period
            ))
        
        # 3. 超跌反弹（股价远离MA60）
        oversold = self._detect_oversold(df, latest)
        if oversold:
            signals.append(self._create_signal(
                symbol, name, "超跌反弹", oversold,
                latest, "股价大幅偏离均线，技术性反弹概率高", df, period
            ))
        
        # 4. 缩量十字星（企稳信号）
        doji = self._detect_doji(df, latest, prev)
        if doji:
            signals.append(self._create_signal(
                symbol, name, "缩量十字星", doji,
                latest, "下跌后出现缩量十字星，可能企稳", df, period
            ))
        
        # 5. 长下影线（支撑信号）
        long_shadow = self._detect_long_lower_shadow(df, latest)
        if long_shadow:
            signals.append(self._create_signal(
                symbol, name, "长下影线", long_shadow,
                latest, "出现长下影线，下方有支撑", df, period
            ))
        
        return signals
    
    def _detect_macd_divergence(self, df: pd.DataFrame, lookback=20) -> Optional[str]:
        """检测MACD底背离"""
        if len(df) < lookback + 10:
            return None
        
        recent = df.tail(lookback)
        
        # 找近期价格低点
        price_low_idx = recent['close'].idxmin()
        price_low = recent.loc[price_low_idx, 'close']
        
        # 找MACD低点
        macd_low_idx = recent['macd_dif'].idxmin()
        macd_low = recent.loc[macd_low_idx, 'macd_dif']
        
        # 检查是否背离：价格创新低但MACD未创新低
        if price_low_idx > macd_low_idx and price_low < recent['close'].quantile(0.3):
            # 当前MACD开始向上
            latest_macd = df['macd_dif'].iloc[-1]
            prev_macd = df['macd_dif'].iloc[-2]
            if latest_macd > prev_macd and macd_low < 0:
                return SignalStrength.STRONG.value if latest_macd < -0.5 else SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_kdj_divergence(self, df: pd.DataFrame, lookback=15) -> Optional[str]:
        """检测KDJ底背离"""
        if len(df) < lookback + 5:
            return None
        
        recent = df.tail(lookback)
        
        # 价格创新低
        price_low_idx = recent['close'].idxmin()
        price_low = recent.loc[price_low_idx, 'close']
        
        # KDJ未创新低
        k_low = recent.loc[price_low_idx, 'kdj_k']
        
        # KDJ超卖区(<20)且开始向上
        latest = df.iloc[-1]
        if k_low < 20 and latest['kdj_k'] > latest['kdj_d'] and price_low < df['close'].tail(lookback*2).quantile(0.2):
            return SignalStrength.STRONG.value if k_low < 10 else SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_oversold(self, df: pd.DataFrame, latest, threshold=0.15) -> Optional[str]:
        """检测超跌（股价远离MA60）"""
        if pd.isna(latest['ma60']) or latest['ma60'] == 0:
            return None
        
        # 计算偏离度
        deviation = (latest['close'] - latest['ma60']) / latest['ma60']
        
        # 股价大幅低于MA60且出现反弹迹象
        if deviation < -threshold:
            # 检查是否有企稳迹象（今日收阳或长下影）
            body = abs(latest['close'] - latest['open'])
            lower_shadow = latest['close'] - latest['low'] if latest['close'] > latest['open'] else latest['open'] - latest['low']
            
            if lower_shadow > body * 1.5 or latest['close'] > latest['open']:
                return SignalStrength.STRONG.value if deviation < -0.20 else SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_doji(self, df: pd.DataFrame, latest, prev, max_body_pct=0.3) -> Optional[str]:
        """检测缩量十字星"""
        # 实体较小
        body = abs(latest['close'] - latest['open'])
        range_total = latest['high'] - latest['low']
        
        if range_total == 0:
            return None
        
        body_pct = body / range_total
        
        # 缩量
        if pd.isna(latest['volume_ratio']) or pd.isna(prev['volume_ratio']):
            return None
        
        volume_shrink = latest['volume_ratio'] < 0.8 and prev['volume_ratio'] < 1.0
        
        # 下跌后出现十字星
        if body_pct < max_body_pct and volume_shrink and latest['close'] < latest['ma20']:
            return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_long_lower_shadow(self, df: pd.DataFrame, latest, min_shadow_ratio=2.0) -> Optional[str]:
        """检测长下影线"""
        body = abs(latest['close'] - latest['open'])
        lower_shadow = min(latest['close'], latest['open']) - latest['low']
        
        if body == 0:
            return None
        
        # 长下影线且收阳
        if lower_shadow > body * min_shadow_ratio and latest['close'] > latest['open']:
            # 且在相对低位
            if latest['close'] < latest['ma20']:
                return SignalStrength.MEDIUM.value
        
        return None
    
    def _create_signal(self, symbol, name, signal_name, strength, latest, 
                       description, df, period: str = "daily") -> StockSignal:
        """创建信号对象"""
        technicals = {
            'ma5': round(latest['ma5'], 2) if not pd.isna(latest['ma5']) else None,
            'ma10': round(latest['ma10'], 2) if not pd.isna(latest['ma10']) else None,
            'ma20': round(latest['ma20'], 2) if not pd.isna(latest['ma20']) else None,
            'ma60': round(latest['ma60'], 2) if not pd.isna(latest['ma60']) else None,
            'macd_dif': round(latest['macd_dif'], 3) if not pd.isna(latest['macd_dif']) else None,
            'macd_dea': round(latest['macd_dea'], 3) if not pd.isna(latest['macd_dea']) else None,
            'kdj_k': round(latest['kdj_k'], 2) if not pd.isna(latest['kdj_k']) else None,
            'kdj_d': round(latest['kdj_d'], 2) if not pd.isna(latest['kdj_d']) else None,
            'kdj_j': round(latest['kdj_j'], 2) if not pd.isna(latest['kdj_j']) else None,
        }
        
        # 计算综合评分
        score = self._calculate_score(signal_name, strength, latest)
        
        return StockSignal(
            symbol=symbol,
            name=name,
            signal_type=SignalType.LEFT.value,
            signal_name=signal_name,
            strength=strength,
            period=period,
            trigger_date=latest['trade_date'].strftime('%Y-%m-%d') if isinstance(latest['trade_date'], pd.Timestamp) else str(latest['trade_date']),
            close_price=round(latest['close'], 2),
            change_pct=round(latest.get('change_pct', 0), 2),
            volume_ratio=round(latest.get('volume_ratio', 1), 2),
            description=description,
            score=score,
            technicals=technicals
        )
    
    def _calculate_score(self, signal_name: str, strength: str, latest) -> int:
        """计算信号评分"""
        base_score = 50
        
        # 根据信号类型加分
        if "底背离" in signal_name:
            base_score += 20
        elif "超跌" in signal_name:
            base_score += 15
        elif "十字星" in signal_name:
            base_score += 10
        
        # 根据强度加分
        if strength == SignalStrength.STRONG.value:
            base_score += 15
        elif strength == SignalStrength.MEDIUM.value:
            base_score += 10
        
        # 根据技术指标加分
        if not pd.isna(latest.get('kdj_j')) and latest['kdj_j'] < 0:
            base_score += 5
        
        return min(base_score, 100)


class RightSignalDetector:
    """右侧信号检测器（追涨/确认信号）"""
    
    def __init__(self):
        self.calc = SignalCalculator()
    
    def detect_all(self, df: pd.DataFrame, symbol: str, name: str, period: str = "daily") -> List[StockSignal]:
        """检测所有右侧信号"""
        signals = []
        
        # 不同周期的最小数据要求
        min_bars = {"daily": 30, "weekly": 20, "monthly": 12}
        min_required = min_bars.get(period, 30)
        
        if df.empty or len(df) < min_required:
            return signals
        
        # 计算指标
        df = self.calc.calculate_ma(df)
        df = self.calc.calculate_macd(df)
        df = self.calc.calculate_kdj(df)
        df = self.calc.calculate_volume_ratio(df)
        
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        prev2 = df.iloc[-3] if len(df) > 2 else prev
        
        # 1. MA5上穿MA10金叉
        ma_cross = self._detect_ma_cross(df, latest, prev)
        if ma_cross:
            signals.append(self._create_signal(
                symbol, name, "MA5金叉MA10", ma_cross,
                latest, "短期均线上穿，短期趋势转强", df, period
            ))
        
        # 2. MA5上穿MA20金叉
        ma_cross_20 = self._detect_ma_cross_20(df, latest, prev)
        if ma_cross_20:
            signals.append(self._create_signal(
                symbol, name, "MA5金叉MA20", ma_cross_20,
                latest, "短期均线上穿中期均线，趋势转强", df, period
            ))
        
        # 3. MACD金叉
        macd_cross = self._detect_macd_cross(latest, prev)
        if macd_cross:
            signals.append(self._create_signal(
                symbol, name, "MACD金叉", macd_cross,
                latest, "DIF上穿DEA，动量转强", df, period
            ))
        
        # 4. KDJ金叉
        kdj_cross = self._detect_kdj_cross(latest, prev)
        if kdj_cross:
            signals.append(self._create_signal(
                symbol, name, "KDJ金叉", kdj_cross,
                latest, "K线上穿D线，短期超买", df, period
            ))
        
        # 5. 量价突破（放量上涨）
        volume_breakout = self._detect_volume_breakout(df, latest, prev)
        if volume_breakout:
            signals.append(self._create_signal(
                symbol, name, "量价突破", volume_breakout,
                latest, "放量上涨，资金入场", df, period
            ))
        
        # 6. 均线多头排列
        ma_bull = self._detect_ma_bullish(df, latest)
        if ma_bull:
            signals.append(self._create_signal(
                symbol, name, "均线多头排列", ma_bull,
                latest, "均线呈多头排列，趋势良好", df, period
            ))
        
        # 7. 突破平台
        platform_break = self._detect_platform_breakout(df, latest)
        if platform_break:
            signals.append(self._create_signal(
                symbol, name, "突破平台", platform_break,
                latest, "放量突破近期整理平台", df, period
            ))
        
        return signals
    
    def _detect_ma_cross(self, df: pd.DataFrame, latest, prev) -> Optional[str]:
        """检测MA5上穿MA10"""
        if pd.isna(latest['ma5']) or pd.isna(latest['ma10']) or pd.isna(prev['ma5']) or pd.isna(prev['ma10']):
            return None
        
        # 今日MA5>MA10，昨日MA5<MA10
        if latest['ma5'] > latest['ma10'] and prev['ma5'] < prev['ma10']:
            # 且在MA20之上
            if latest['close'] > latest['ma20']:
                return SignalStrength.STRONG.value
            return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_ma_cross_20(self, df: pd.DataFrame, latest, prev) -> Optional[str]:
        """检测MA5上穿MA20"""
        if pd.isna(latest['ma5']) or pd.isna(latest['ma20']) or pd.isna(prev['ma5']) or pd.isna(prev['ma20']):
            return None
        
        if latest['ma5'] > latest['ma20'] and prev['ma5'] < prev['ma20']:
            if latest['close'] > latest['ma60']:
                return SignalStrength.STRONG.value
            return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_macd_cross(self, latest, prev) -> Optional[str]:
        """检测MACD金叉"""
        if pd.isna(latest['macd_dif']) or pd.isna(latest['macd_dea']) or pd.isna(prev['macd_dif']) or pd.isna(prev['macd_dea']):
            return None
        
        # DIF上穿DEA
        if latest['macd_dif'] > latest['macd_dea'] and prev['macd_dif'] < prev['macd_dea']:
            # 在零轴上方金叉更强
            if latest['macd_dif'] > 0:
                return SignalStrength.STRONG.value
            return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_kdj_cross(self, latest, prev) -> Optional[str]:
        """检测KDJ金叉"""
        if pd.isna(latest['kdj_k']) or pd.isna(latest['kdj_d']) or pd.isna(prev['kdj_k']) or pd.isna(prev['kdj_d']):
            return None
        
        # K上穿D
        if latest['kdj_k'] > latest['kdj_d'] and prev['kdj_k'] < prev['kdj_d']:
            # 在超卖区金叉更强
            if prev['kdj_k'] < 20:
                return SignalStrength.STRONG.value
            return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_volume_breakout(self, df: pd.DataFrame, latest, prev, min_volume_ratio=1.5) -> Optional[str]:
        """检测量价突破"""
        if pd.isna(latest['volume_ratio']):
            return None
        
        # 放量上涨
        if latest['volume_ratio'] > min_volume_ratio and latest['close'] > latest['open']:
            # 突破MA20
            if latest['close'] > latest['ma20'] and prev['close'] < prev['ma20']:
                return SignalStrength.STRONG.value
            # 大涨
            if latest.get('change_pct', 0) > 5:
                return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_ma_bullish(self, df: pd.DataFrame, latest) -> Optional[str]:
        """检测均线多头排列"""
        if pd.isna(latest['ma5']) or pd.isna(latest['ma10']) or pd.isna(latest['ma20']):
            return None
        
        # MA5 > MA10 > MA20
        if latest['ma5'] > latest['ma10'] > latest['ma20']:
            # 且价格在MA5之上
            if latest['close'] > latest['ma5']:
                if latest['ma20'] > latest['ma60']:
                    return SignalStrength.STRONG.value
                return SignalStrength.MEDIUM.value
        
        return None
    
    def _detect_platform_breakout(self, df: pd.DataFrame, latest, lookback=20) -> Optional[str]:
        """检测突破平台"""
        if len(df) < lookback + 5:
            return None
        
        recent = df.tail(lookback)
        
        # 找近期高点
        platform_high = recent['high'].max()
        platform_low = recent['low'].min()
        
        # 平台整理（波动不大）
        platform_range = (platform_high - platform_low) / platform_low
        if platform_range > 0.15:  # 波动太大不算平台
            return None
        
        # 今日突破平台上沿
        if latest['close'] > platform_high * 0.99 and latest['volume_ratio'] > 1.3:
            return SignalStrength.STRONG.value if latest['volume_ratio'] > 2 else SignalStrength.MEDIUM.value
        
        return None
    
    def _create_signal(self, symbol, name, signal_name, strength, latest,
                       description, df, period: str = "daily") -> StockSignal:
        """创建信号对象"""
        technicals = {
            'ma5': round(latest['ma5'], 2) if not pd.isna(latest['ma5']) else None,
            'ma10': round(latest['ma10'], 2) if not pd.isna(latest['ma10']) else None,
            'ma20': round(latest['ma20'], 2) if not pd.isna(latest['ma20']) else None,
            'ma60': round(latest['ma60'], 2) if not pd.isna(latest['ma60']) else None,
            'macd_dif': round(latest['macd_dif'], 3) if not pd.isna(latest['macd_dif']) else None,
            'macd_dea': round(latest['macd_dea'], 3) if not pd.isna(latest['macd_dea']) else None,
            'kdj_k': round(latest['kdj_k'], 2) if not pd.isna(latest['kdj_k']) else None,
            'kdj_d': round(latest['kdj_d'], 2) if not pd.isna(latest['kdj_d']) else None,
            'kdj_j': round(latest['kdj_j'], 2) if not pd.isna(latest['kdj_j']) else None,
        }

        score = self._calculate_score(signal_name, strength, latest)

        return StockSignal(
            symbol=symbol,
            name=name,
            signal_type=SignalType.RIGHT.value,
            signal_name=signal_name,
            strength=strength,
            period=period,
            trigger_date=latest['trade_date'].strftime('%Y-%m-%d') if isinstance(latest['trade_date'], pd.Timestamp) else str(latest['trade_date']),
            close_price=round(latest['close'], 2),
            change_pct=round(latest.get('change_pct', 0), 2),
            volume_ratio=round(latest.get('volume_ratio', 1), 2),
            description=description,
            score=score,
            technicals=technicals
        )
    
    def _calculate_score(self, signal_name: str, strength: str, latest) -> int:
        """计算信号评分"""
        base_score = 55
        
        if "突破" in signal_name or "多头排列" in signal_name:
            base_score += 20
        elif "金叉" in signal_name:
            base_score += 15
        
        if strength == SignalStrength.STRONG.value:
            base_score += 15
        elif strength == SignalStrength.MEDIUM.value:
            base_score += 10
        
        # 量价配合加分
        if not pd.isna(latest.get('volume_ratio')) and latest['volume_ratio'] > 2:
            base_score += 5
        
        return min(base_score, 100)


class StockSignalScanner:
    """个股信号扫描器主类 - 支持多周期（日线/周线/月线）"""
    
    def __init__(self):
        self.left_detector = LeftSignalDetector()
        self.right_detector = RightSignalDetector()
        self.output_dir = Path(project_root) / "storage" / "outputs" / "signals"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.prices_dir = Path(project_root) / "storage" / "raw" / "prices"
    
    def load_stock_data(self, symbol: str, period: str = "daily", adjust: str = "qfq") -> pd.DataFrame:
        """
        加载股票数据（使用底层接口，默认前复权）

        Args:
            symbol: 股票代码
            period: 周期 - daily(日线)/weekly(周线)/monthly(月线)
            adjust: 复权方式 - "qfq"(前复权)/None(不复权)
        """
        try:
            if period == "daily":
                # 使用底层接口，默认前复权
                return load_stock_prices(symbol, adjust=adjust)
            else:
                # 周线/月线从日线合成
                return self._resample_from_daily(symbol, period, adjust=adjust)
        except Exception as e:
            logger.warning(f"加载 {symbol} {period} 数据失败: {e}")
            return pd.DataFrame()
    
    def _resample_from_daily(self, symbol: str, period: str, adjust: str = "qfq") -> pd.DataFrame:
        """从日线数据合成周线/月线（基于前复权日线）"""
        daily_df = self.load_stock_data(symbol, "daily", adjust=adjust)
        if daily_df.empty or len(daily_df) < 30:
            return pd.DataFrame()
        
        try:
            df = daily_df.copy()
            df.set_index('trade_date', inplace=True)
            
            if period == "weekly":
                # 周线：周五为结束日
                rule = 'W-FRI'
            elif period == "monthly":
                # 月线：月末
                rule = 'ME'
            else:
                return pd.DataFrame()
            
            # 重采样
            resampled = df.resample(rule).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'amount': 'sum' if 'amount' in df.columns else 'sum'
            }).dropna()
            
            # 计算涨跌幅
            resampled['change_pct'] = resampled['close'].pct_change() * 100
            
            # 重置索引
            resampled = resampled.reset_index()
            resampled.rename(columns={'index': 'trade_date'}, inplace=True)
            
            return resampled
            
        except Exception as e:
            logger.warning(f"合成 {symbol} {period} 数据失败: {e}")
            return pd.DataFrame()
    
    def scan_stock(self, symbol: str, name: str, signal_type: str = "all",
                   multi_period: bool = True) -> List[StockSignal]:
        """
        扫描单只股票的信号

        Args:
            symbol: 股票代码
            name: 股票名称
            signal_type: 信号类型 - all/left/right
            multi_period: 是否使用多周期分析
        """
        # 加载多周期数据
        df_daily = self.load_stock_data(symbol, "daily")
        if df_daily.empty:
            return []

        df_weekly = pd.DataFrame()
        df_monthly = pd.DataFrame()

        if multi_period:
            df_weekly = self.load_stock_data(symbol, "weekly")
            df_monthly = self.load_stock_data(symbol, "monthly")

        signals = []

        # 检测平缓均线（在所有周期上）
        flat_ma_info = self._detect_flat_mas(df_daily, df_weekly, df_monthly)

        # 日线信号
        if signal_type in ["all", "left"]:
            left_signals = self.left_detector.detect_all(df_daily, symbol, name, "daily")
            for sig in left_signals:
                sig.technicals['flat_mas'] = flat_ma_info.get('daily', [])
            signals.extend(left_signals)

        if signal_type in ["all", "right"]:
            right_signals = self.right_detector.detect_all(df_daily, symbol, name, "daily")
            for sig in right_signals:
                sig.technicals['flat_mas'] = flat_ma_info.get('daily', [])
            signals.extend(right_signals)

        # 周线信号
        if multi_period and not df_weekly.empty:
            if signal_type in ["all", "left"]:
                weekly_left = self.left_detector.detect_all(df_weekly, symbol, name, "weekly")
                for sig in weekly_left:
                    sig.signal_name = f"周线{sig.signal_name}"
                    sig.score = min(sig.score + 10, 100)  # 周线信号加分
                    sig.technicals['flat_mas'] = flat_ma_info.get('weekly', [])
                signals.extend(weekly_left)

            if signal_type in ["all", "right"]:
                weekly_right = self.right_detector.detect_all(df_weekly, symbol, name, "weekly")
                for sig in weekly_right:
                    sig.signal_name = f"周线{sig.signal_name}"
                    sig.score = min(sig.score + 10, 100)
                    sig.technicals['flat_mas'] = flat_ma_info.get('weekly', [])
                signals.extend(weekly_right)

        # 月线信号
        if multi_period and not df_monthly.empty:
            if signal_type in ["all", "left"]:
                monthly_left = self.left_detector.detect_all(df_monthly, symbol, name, "monthly")
                for sig in monthly_left:
                    sig.signal_name = f"月线{sig.signal_name}"
                    sig.score = min(sig.score + 15, 100)  # 月线信号加分更多
                    sig.technicals['flat_mas'] = flat_ma_info.get('monthly', [])
                signals.extend(monthly_left)

            if signal_type in ["all", "right"]:
                monthly_right = self.right_detector.detect_all(df_monthly, symbol, name, "monthly")
                for sig in monthly_right:
                    sig.signal_name = f"月线{sig.signal_name}"
                    sig.score = min(sig.score + 15, 100)
                    sig.technicals['flat_mas'] = flat_ma_info.get('monthly', [])
                signals.extend(monthly_right)

        # 多周期共振检测
        if multi_period and len(signals) > 1:
            signals = self._detect_multi_period_resonance(signals)

        # 应用信号组合评分（考虑信号数量和质量分布）
        if len(signals) > 0:
            signals = self._apply_signal_portfolio_scoring(signals)

        return signals

    def _detect_flat_mas(self, df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                         df_monthly: pd.DataFrame) -> Dict[str, List[str]]:
        """
        检测各周期的平缓均线（均线走平）

        均线走平意味着价格长期围绕某个中枢稳定波动，这个均线位置就是强支撑/阻力

        Returns:
            {
                'daily': ['MA60走平@1450.50(0.92)', 'MA20走平@1420.30(0.85)'],
                'weekly': ['MA20走平@1430.80(0.88)'],
                'monthly': []
            }
        """
        result = {'daily': [], 'weekly': [], 'monthly': []}

        calc = SignalCalculator()

        def detect_single_period(df: pd.DataFrame, window: int, min_bars: int) -> List[str]:
            """检测单个周期的走平均线"""
            if df.empty or len(df) < min_bars:
                return []

            df = calc.calculate_ma(df)
            df = calc.calculate_ma_flatness(df, window=window)
            latest = df.iloc[-1]

            flat_mas = []
            flatness_threshold = 0.75  # 走平阈值

            for ma in ['ma10', 'ma20', 'ma60']:
                flatness_key = f'{ma}_flatness'
                if flatness_key in latest and not pd.isna(latest[flatness_key]):
                    flatness = latest[flatness_key]
                    if flatness >= flatness_threshold:
                        ma_value = latest.get(ma, 0)
                        if not pd.isna(ma_value):
                            # 格式: MAxx走平@价格(分数)
                            flat_mas.append(f"{ma.upper()}走平@{ma_value:.2f}({flatness:.2f})")

            return flat_mas

        # 检测各周期
        result['daily'] = detect_single_period(df_daily, window=20, min_bars=60)
        result['weekly'] = detect_single_period(df_weekly, window=10, min_bars=20)
        result['monthly'] = detect_single_period(df_monthly, window=6, min_bars=12)

        return result
    
    def _detect_multi_period_resonance(self, signals: List[StockSignal]) -> List[StockSignal]:
        """检测多周期共振，提升共振信号评分"""
        # 按信号名称分组
        signal_groups = {}
        for sig in signals:
            # 去掉周期前缀，获取基础信号名称
            base_name = sig.signal_name.replace("周线", "").replace("月线", "")
            if base_name not in signal_groups:
                signal_groups[base_name] = []
            signal_groups[base_name].append(sig)

        # 检查共振
        for base_name, group in signal_groups.items():
            if len(group) >= 2:  # 至少两个周期有相同信号
                # 给共振信号额外加分
                for sig in group:
                    sig.score = min(sig.score + 15, 100)
                    if "共振" not in sig.description:
                        sig.description += " | 多周期共振"

        return signals

    def _apply_signal_portfolio_scoring(self, signals: List[StockSignal]) -> List[StockSignal]:
        """
        信号组合评分 - 综合考虑信号数量和质量分布

        评分维度：
        1. 基础质量分 (60%): 最高信号的原始评分
        2. 信号集中度 (25%): 高分信号(≥80)占比
        3. 信号数量 (15%): 适中数量(2-4个)最佳

        原则：
        - 信号过多(>6个)会扣分，因为可能是噪音
        - 信号过少(1个)也扣分，缺乏验证
        - 质量集中比数量更重要
        """
        if not signals:
            return signals

        n_signals = len(signals)
        scores = [sig.score for sig in signals]
        max_score = max(scores)
        avg_score = sum(scores) / n_signals

        # 1. 计算维度覆盖率（核心指标 - 多维度交叉验证）
        dimension_coverage = self._calculate_dimension_coverage(signals)

        # 2. 计算高分信号占比 (质量集中度)
        high_quality_count = sum(1 for s in scores if s >= 80)
        quality_concentration = high_quality_count / n_signals  # 0-1

        # 3. 信号数量因子：2-5个信号最佳（放宽上限，更看重质量）
        if n_signals <= 1:
            quantity_factor = 0.8  # 信号太少
        elif 2 <= n_signals <= 5:
            quantity_factor = 1.0  # 最佳区间
        elif 6 <= n_signals <= 8:
            quantity_factor = 0.9  # 稍多
        else:
            quantity_factor = 0.8  # 过多，噪音风险

        # 4. 计算综合评分（维度覆盖率最重要）
        # 基础质量分
        base_quality = max_score * 0.6 + avg_score * 0.4

        # 维度覆盖率奖励（35%权重）- 核心！
        dimension_bonus = dimension_coverage['score'] * 0.35

        # 质量集中度奖励（15%权重）
        quality_bonus = quality_concentration * 15

        # 数量因子
        adjusted_base = base_quality * quantity_factor

        # 最终组合评分
        portfolio_score = min(adjusted_base + dimension_bonus + quality_bonus, 100)

        # 为每个信号添加维度信息
        for sig in signals:
            # 高分且多维度覆盖的组合获得额外加成
            if sig.score >= 80 and dimension_coverage['coverage'] >= 0.6:
                sig.score = min(sig.score + 5, 100)

            # 添加组合评分和维度信息
            sig.technicals['portfolio_score'] = round(portfolio_score, 1)
            sig.technicals['signal_count'] = n_signals
            sig.technicals['quality_concentration'] = round(quality_concentration, 2)
            sig.technicals['dimension_coverage'] = round(dimension_coverage['coverage'], 2)
            sig.technicals['dimension_details'] = dimension_coverage['details']

        return signals

    def _calculate_dimension_coverage(self, signals: List[StockSignal]) -> dict:
        """
        计算多维度覆盖率 - 核心指标

        维度定义：
        1. 交易方向维度: left(左侧) / right(右侧)
        2. 周期维度: daily(日线) / weekly(周线) / monthly(月线)
        3. 指标类型维度:
           - trend(趋势): 均线多头排列、金叉
           - momentum(动量): MACD、KDJ
           - pattern(形态): 十字星、底背离、突破
           - volume(量能): 放量、缩量

        返回: {'score': 维度得分0-100, 'coverage': 覆盖率0-1, 'details': {各维度详情}}
        """
        if not signals:
            return {'score': 0, 'coverage': 0, 'details': {}}

        # 1. 交易方向维度
        directions = set(sig.signal_type for sig in signals)
        direction_score = min(len(directions) * 30, 50)  # 单侧30分，双侧50分

        # 2. 周期维度
        periods = set(sig.period for sig in signals)
        period_score = len(periods) * 15  # 每个周期15分，最高45分

        # 3. 指标类型维度（通过信号名称判断）
        indicator_types = {'trend': 0, 'momentum': 0, 'pattern': 0, 'volume': 0}

        for sig in signals:
            name = sig.signal_name
            if any(kw in name for kw in ['均线多头排列', '金叉', '突破']):
                indicator_types['trend'] = 1
            elif any(kw in name for kw in ['MACD', 'KDJ', 'RSI']):
                indicator_types['momentum'] = 1
            elif any(kw in name for kw in ['底背离', '十字星', '超跌', '长下影线']):
                indicator_types['pattern'] = 1
            elif any(kw in name for kw in ['放量', '缩量']):
                indicator_types['volume'] = 1

        unique_indicators = sum(indicator_types.values())
        indicator_score = unique_indicators * 12  # 每种类型12分，最高48分

        # 计算总维度分（满分100）
        total_score = direction_score + period_score + indicator_score

        # 计算覆盖率（实际覆盖维度 / 理想维度数）
        # 理想：双侧 + 3周期 + 4种指标类型 = 8个维度
        ideal_dimensions = 8
        actual_dimensions = len(directions) + len(periods) + unique_indicators
        coverage = actual_dimensions / ideal_dimensions

        return {
            'score': min(total_score, 100),
            'coverage': coverage,
            'details': {
                'directions': list(directions),
                'periods': list(periods),
                'indicators': {k: v for k, v in indicator_types.items() if v > 0},
                'direction_score': direction_score,
                'period_score': period_score,
                'indicator_score': indicator_score
            }
        }
    
    def scan_all(self, signal_type: str = "all", limit: int = None,
                 multi_period: bool = True) -> Dict:
        """扫描全市场股票"""
        # 获取股票列表
        stock_list = self._get_stock_list()
        if stock_list.empty:
            return {"status": "error", "message": "没有股票列表"}

        if limit:
            stock_list = stock_list.head(limit)

        period_str = "多周期" if multi_period else "日线"
        logger.info(f"开始扫描 {len(stock_list)} 只股票，信号类型: {signal_type}, 周期: {period_str}")

        all_signals = []
        stats = {"left": 0, "right": 0, "by_period": {"daily": 0, "weekly": 0, "monthly": 0}, "by_signal": {}}

        for idx, row in stock_list.iterrows():
            symbol = row['symbol']
            name = row.get('name', '')

            if (idx + 1) % 100 == 0:
                logger.info(f"进度: {idx + 1}/{len(stock_list)}")

            signals = self.scan_stock(symbol, name, signal_type, multi_period)

            for sig in signals:
                all_signals.append(sig.to_dict())
                stats[sig.signal_type] += 1
                stats["by_period"][sig.period] = stats["by_period"].get(sig.period, 0) + 1

                if sig.signal_name not in stats["by_signal"]:
                    stats["by_signal"][sig.signal_name] = 0
                stats["by_signal"][sig.signal_name] += 1

        # 按组合评分排序（优先使用 portfolio_score，回退到 score）
        all_signals.sort(
            key=lambda x: (x.get('technicals', {}).get('portfolio_score', x['score']), x['score']),
            reverse=True
        )

        result = {
            "status": "success",
            "scan_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_stocks": len(stock_list),
            "total_signals": len(all_signals),
            "multi_period": multi_period,
            "stats": stats,
            "signals": all_signals
        }

        # 保存结果
        self._save_result(result, signal_type)

        logger.info(f"扫描完成: {len(all_signals)} 个信号，左侧: {stats['left']}, 右侧: {stats['right']}")
        if multi_period:
            logger.info(f"周期分布: 日线 {stats['by_period'].get('daily', 0)}, "
                       f"周线 {stats['by_period'].get('weekly', 0)}, "
                       f"月线 {stats['by_period'].get('monthly', 0)}")

        return result
    
    def _get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        stock_csv = Path(project_root) / "storage" / "stock_basic_info.csv"
        if stock_csv.exists():
            df = pd.read_csv(stock_csv)
            return df[['symbol', 'name']] if 'name' in df.columns else df[['symbol']]
        return pd.DataFrame()
    
    def _save_result(self, result: Dict, signal_type: str):
        """保存扫描结果"""
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"stock_signals_{signal_type}_{date_str}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 同时保存最新结果（供Dashboard使用）
        latest_path = self.output_dir / f"stock_signals_{signal_type}_latest.json"
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"结果已保存: {filepath}")


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='个股信号扫描器 - 支持多周期（日线/周线/月线）')
    parser.add_argument('--all', action='store_true', help='扫描所有股票')
    parser.add_argument('--symbol', type=str, help='扫描指定股票')
    parser.add_argument('--signal-type', type=str, choices=['left', 'right', 'all'],
                        default='all', help='信号类型')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')

    args = parser.parse_args()

    scanner = StockSignalScanner()
    multi_period = not args.no_multi_period

    if args.symbol:
        # 扫描单只股票
        name = get_stock_name(args.symbol)
        signals = scanner.scan_stock(args.symbol, name, args.signal_type, multi_period)

        print(f"\n{args.symbol} {name} 的信号:")
        print("-" * 60)
        for sig in signals:
            period_tag = f"[{sig.period[:1].upper()}]" if sig.period != "daily" else "[D]"
            print(f"{period_tag} [{sig.signal_type.upper()}] {sig.signal_name} ({sig.strength})")
            print(f"  评分: {sig.score} | 日期: {sig.trigger_date} | 价格: {sig.close_price}")
            print(f"  {sig.description}")
            print()

    elif args.all or not args.symbol:
        # 扫描所有股票（默认行为）
        result = scanner.scan_all(args.signal_type, args.limit, multi_period)

        print(f"\n扫描完成!")
        print(f"总信号数: {result['total_signals']}")
        print(f"左侧信号: {result['stats']['left']}")
        print(f"右侧信号: {result['stats']['right']}")

        if result.get('multi_period'):
            by_period = result['stats'].get('by_period', {})
            print(f"\n周期分布:")
            print(f"  日线: {by_period.get('daily', 0)}")
            print(f"  周线: {by_period.get('weekly', 0)}")
            print(f"  月线: {by_period.get('monthly', 0)}")

        print(f"\n按信号类型分布 (Top 15):")
        for sig_name, count in sorted(result['stats']['by_signal'].items(), key=lambda x: -x[1])[:15]:
            print(f"  {sig_name}: {count}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()

"""
个股信号扫描器 - 生成左侧/右侧交易信号

扫描全市场股票，基于日线/周线/月线生成交易信号

用法:
    # 使用便捷脚本（推荐）
    python ShortTerm/run_signal_scan.py              # 扫描全部信号
    python ShortTerm/run_signal_scan.py --left       # 只扫描左侧信号
    python ShortTerm/run_signal_scan.py --right      # 只扫描右侧信号
    python ShortTerm/run_signal_scan.py --symbol 600519.SH  # 扫描单只股票
    
    # 或者直接运行扫描器
    python ShortTerm/services/stock_signal_scanner.py
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from DataHub.config import RAW_PRICE_DIR

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


def format_date_safe(date_value) -> str:
    """
    安全地格式化日期为字符串
    
    处理各种可能的日期格式：
    - pd.Timestamp -> '2026-04-17'
    - datetime -> '2026-04-17'
    - str (已格式化) -> 原样返回
    - 数字字符串 (异常) -> 尝试解析或返回空
    """
    if pd.isna(date_value):
        return ''
    
    # 已经是 Timestamp 或 datetime
    if isinstance(date_value, (pd.Timestamp, datetime)):
        return date_value.strftime('%Y-%m-%d')
    
    # 字符串类型
    if isinstance(date_value, str):
        # 检查是否已是标准格式
        if len(date_value) == 10 and date_value[4] == '-' and date_value[7] == '-':
            return date_value
        # 尝试解析其他格式
        try:
            dt = pd.to_datetime(date_value)
            return dt.strftime('%Y-%m-%d')
        except:
            # 无法解析，可能是异常数据
            logger.warning(f"无法解析日期: {date_value}")
            return ''
    
    # 数字类型（Excel日期序列号等）
    if isinstance(date_value, (int, float)):
        try:
            # 尝试作为Excel日期序列号解析
            from datetime import timedelta
            base = datetime(1899, 12, 30)  # Excel日期基准
            dt = base + timedelta(days=int(date_value))
            return dt.strftime('%Y-%m-%d')
        except:
            return ''
    
    return ''


# 排除的交易所列表（北交所数据不稳定，暂时排除）
EXCLUDED_EXCHANGES = ['BJ']


def filter_excluded_symbols(symbols_or_df) -> list:
    """
    过滤掉排除的交易所股票（如北交所）
    
    Args:
        symbols_or_df: 股票代码列表或包含symbol列的DataFrame
        
    Returns:
        过滤后的股票代码列表
    """
    if isinstance(symbols_or_df, pd.DataFrame):
        symbols = symbols_or_df['symbol'].tolist()
    else:
        symbols = list(symbols_or_df)
    
    return [s for s in symbols if not any(s.endswith(f'.{ex}') for ex in EXCLUDED_EXCHANGES)]


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
        """转换为字典，自动处理 numpy 类型"""
        import numpy as np

        def convert_value(v):
            if isinstance(v, np.integer):
                return int(v)
            elif isinstance(v, np.floating):
                return float(v)
            elif isinstance(v, np.ndarray):
                return v.tolist()
            elif isinstance(v, dict):
                return {k: convert_value(val) for k, val in v.items()}
            elif isinstance(v, list):
                return [convert_value(item) for item in v]
            return v

        return {k: convert_value(v) for k, v in asdict(self).items()}


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

                # 3. 计算均线的斜率（纯向量化最小二乘法）
                # 线性回归斜率公式：slope = Σ(x-x̄)(y-ȳ) / Σ(x-x̄)²
                # 对于等间距x(0,1,2...n-1)，可以预先计算权重
                n = window
                x_mean = (n - 1) / 2
                # Σ(x-x̄)² = n(n²-1)/12
                denominator = n * (n * n - 1) / 12
                
                # 计算 Σ(x-x̄)(y-ȳ) = Σ(x-x̄)y - ȳΣ(x-x̄) = Σ(x-x̄)y (因为Σ(x-x̄)=0)
                # 创建权重数组：[-(n-1)/2, ..., 0, ..., (n-1)/2]
                weights = np.arange(n) - x_mean
                
                # 使用卷积计算加权移动和
                y = df[ma].values
                # 手动计算加权滚动和（避免apply）
                weighted_sum = np.convolve(y * np.ones_like(y), weights[::-1], mode='valid')
                # 对齐长度（前面填充nan）
                slope = np.concatenate([np.full(n-1, np.nan), weighted_sum]) / denominator

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
    def calculate_trend_health(df: pd.DataFrame) -> Dict:
        """
        计算趋势健康度评分（0-100）
        
        评分逻辑：
        - 50分：中性基准
        - 80-100：强势上涨，健康
        - 60-79：正常震荡
        - 40-59：走弱预警
        - 20-39：趋势走坏
        - 0-19：严重恶化
        
        Returns:
            {
                "health_score": int,      # 0-100健康度分数
                "risk_level": str,        # low/medium/high/extreme
                "warnings": List[str],    # 风险点列表
                "recommendation": str,    # 建议操作
                "details": Dict           # 详细指标
            }
        """
        if df.empty or len(df) < 20:
            return {
                "health_score": 50,
                "risk_level": "unknown",
                "warnings": ["数据不足"],
                "recommendation": "观望",
                "details": {}
            }
        
        # 确保必要指标已计算
        df = SignalCalculator.calculate_ma(df)
        df = SignalCalculator.calculate_macd(df)
        df['volume_ma5'] = df['volume'].rolling(window=5).mean()
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()
        
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        score = 50  # 中性基准
        warnings = []
        details = {}
        
        # 1. 均线系统（权重30%，±15分）
        ma_bull = latest['ma5'] > latest['ma10'] > latest['ma20']
        ma_bear = latest['ma5'] < latest['ma10'] < latest['ma20']
        
        if ma_bull:
            score += 15
            details['ma_trend'] = 'bullish'
        elif ma_bear:
            score -= 15
            details['ma_trend'] = 'bearish'
            warnings.append("均线空头排列")
        else:
            details['ma_trend'] = 'mixed'
        
        # 2. 关键均线破位（权重25%，±12分）
        above_ma60 = latest['close'] > latest['ma60']
        ma60_slope = latest['ma60'] - prev['ma60'] if not pd.isna(prev['ma60']) else 0
        
        if above_ma60:
            score += 12
            details['ma60_status'] = 'above'
        else:
            score -= 12
            details['ma60_status'] = 'below'
            warnings.append("跌破MA60")
            if ma60_slope < 0:
                score -= 8
                warnings.append("MA60拐头向下")
                details['ma60_slope'] = 'falling'
            else:
                details['ma60_slope'] = 'flat_rising'
        
        # 3. MACD状态（权重20%，±10分）
        macd_bull = latest['macd_bar'] > 0 and latest['macd_dif'] > latest['macd_dea']
        macd_bear = latest['macd_bar'] < 0 and latest['macd_dif'] < latest['macd_dea']
        
        if macd_bull:
            score += 10
            details['macd_status'] = 'bullish'
        elif macd_bear:
            score -= 10
            details['macd_status'] = 'bearish'
            warnings.append("MACD死叉/空头")
        else:
            details['macd_status'] = 'mixed'
        
        # 4. 量能趋势（权重15%，±8分）
        volume_trend = latest['volume_ma5'] / latest['volume_ma20'] if latest['volume_ma20'] > 0 else 1
        vol_declining = volume_trend < 0.9  # 量能萎缩超过10%
        
        if volume_trend > 1.1:
            score += 8
            details['volume_trend'] = 'expanding'
        elif vol_declining:
            score -= 8
            details['volume_trend'] = 'contracting'
            if latest['close'] < latest['open']:  # 缩量阴跌
                warnings.append("缩量阴跌")
        else:
            details['volume_trend'] = 'neutral'
        
        details['volume_ratio'] = round(volume_trend, 2)
        
        # 5. 近期动量（权重10%，±10分）
        if len(df) >= 5:
            change_5d = (latest['close'] - df.iloc[-5]['close']) / df.iloc[-5]['close'] * 100
        else:
            change_5d = 0
        
        if change_5d > 5:
            score += 10
            details['momentum_5d'] = 'strong'
        elif change_5d > 0:
            score += 5
            details['momentum_5d'] = 'weak_positive'
        elif change_5d > -5:
            score -= 5
            details['momentum_5d'] = 'weak_negative'
        else:
            score -= 10
            details['momentum_5d'] = 'strong_negative'
            warnings.append(f"5日跌幅{change_5d:.1f}%")
        
        details['change_5d_pct'] = round(change_5d, 2)
        
        # 计算最终分数
        final_score = max(0, min(100, score))
        
        # 确定风险等级和建议
        if final_score >= 80:
            risk_level = "low"
            recommendation = "关注买入机会"
        elif final_score >= 60:
            risk_level = "low"
            recommendation = "持有"
        elif final_score >= 40:
            risk_level = "medium"
            recommendation = "谨慎观察"
        elif final_score >= 20:
            risk_level = "high"
            recommendation = "考虑减仓"
        else:
            risk_level = "extreme"
            recommendation = "建议卖出/回避"
        
        return {
            "health_score": final_score,
            "risk_level": risk_level,
            "warnings": warnings,
            "recommendation": recommendation,
            "details": details
        }
    
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
        # 集成量价信号检测器
        from .volume_price_signals import VolumePriceDetector, VolumePriceAdapter
        self.vp_detector = VolumePriceDetector()
        self.vp_adapter = VolumePriceAdapter
    
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
        prev2 = df.iloc[-3] if len(df) > 2 else prev

        # 1. MACD底背离
        macd_divergence = self._detect_macd_divergence(df)
        if macd_divergence:
            signals.append(self._create_signal(
                symbol, name, "MACD底背离", macd_divergence,
                latest, "价格创新低但MACD未创新低，可能反弹", df, period, prev, prev2
            ))

        # 2. KDJ底背离
        kdj_divergence = self._detect_kdj_divergence(df)
        if kdj_divergence:
            signals.append(self._create_signal(
                symbol, name, "KDJ底背离", kdj_divergence,
                latest, "价格创新低但KDJ未创新低，超卖反弹", df, period, prev, prev2
            ))

        # 3. 超跌反弹（股价远离MA60）
        oversold = self._detect_oversold(df, latest)
        if oversold:
            signals.append(self._create_signal(
                symbol, name, "超跌反弹", oversold,
                latest, "股价大幅偏离均线，技术性反弹概率高", df, period, prev, prev2
            ))

        # 4. 缩量十字星（企稳信号）
        doji = self._detect_doji(df, latest, prev)
        if doji:
            signals.append(self._create_signal(
                symbol, name, "缩量十字星", doji,
                latest, "下跌后出现缩量十字星，可能企稳", df, period, prev, prev2
            ))

        # 5. 长下影线（支撑信号）
        long_shadow = self._detect_long_lower_shadow(df, latest)
        if long_shadow:
            signals.append(self._create_signal(
                symbol, name, "长下影线", long_shadow,
                latest, "出现长下影线，下方有支撑", df, period, prev, prev2
            ))
        
        # 6. 新增：左侧量价信号（缩量整理、量价背离）
        vp_signals = self._detect_volume_price_signals(df, symbol, name, period, latest)
        signals.extend(vp_signals)
        
        return signals
    
    def _detect_volume_price_signals(self, df: pd.DataFrame, symbol: str, name: str, 
                                     period: str, latest: pd.Series) -> List[StockSignal]:
        """
        检测左侧量价信号（回调/背离/缩量类）
        """
        signals = []
        from .volume_price_signals import VolumePricePattern
        
        # 只关注左侧信号（背离、缩量整理）
        left_patterns = [
            VolumePricePattern.VOLUME_PRICE_DIVERGENCE,  # 量价背离
            VolumePricePattern.VOLUME_CONTRACTION,        # 缩量整理
        ]
        
        for pattern in left_patterns:
            vp_signal = self.vp_detector.detect(df, pattern)
            if vp_signal:
                # 获取日期并格式化为 YYYY-MM-DD
                if 'trade_date' in latest:
                    date_val = latest['trade_date']
                    if hasattr(date_val, 'strftime'):
                        trigger_date = date_val.strftime('%Y-%m-%d')
                    else:
                        trigger_date = str(date_val)[:10]
                else:
                    trigger_date = str(latest.name) if hasattr(latest, 'name') else ""
                
                stock_signal = self.vp_adapter.to_stock_signal(
                    vp_signal, symbol, name, period,
                    close_price=latest['close'],
                    change_pct=latest.get('change_pct', 0) * 100,
                    technicals={
                        "date": trigger_date,
                        "ma20": latest.get('ma20'),
                        "volume_ratio": vp_signal.volume_ratio,
                    }
                )
                signals.append(StockSignal(**stock_signal))

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
                       description, df, period: str = "daily", prev=None, prev2=None) -> StockSignal:
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

        # 计算涨停质量评分（策略2）
        zt_quality = self._calculate_zt_quality_score(latest, prev, prev2, df)
        if zt_quality['zt_quality_score'] is not None:
            technicals.update(zt_quality)

        return StockSignal(
            symbol=symbol,
            name=name,
            signal_type=SignalType.LEFT.value,
            signal_name=signal_name,
            strength=strength,
            period=period,
            trigger_date=format_date_safe(latest['trade_date']),
            close_price=round(latest['close'], 2),
            change_pct=round(latest.get('change_pct', 0), 2),
            volume_ratio=round(latest.get('volume_ratio', 1), 2),
            description=description,
            score=score,
            technicals=technicals
        )

    def _calculate_score(self, signal_name: str, strength: str, latest) -> int:
        """
        计算信号评分 - 严格版，确保高分稀缺性
        
        评分逻辑：
        - 基础分30（降低起点）
        - 信号类型加分（最高+25）
        - 强度加分（最高+20）
        - 技术指标加分（最高+15）
        - 负面因素扣分（最多-20）
        - 满分100，90+为高置信度，80+为中等，<80为观察
        """
        score = 30  # 降低基础分
        
        # 1. 信号类型加分（更严格）
        if "底背离" in signal_name:
            score += 20  # MACD/KDJ底背离，较强信号
        elif "超跌" in signal_name:
            score += 15
        elif "十字星" in signal_name:
            score += 10
        elif "长下影线" in signal_name:
            score += 12
        else:
            score += 5  # 其他信号基础加分
        
        # 2. 强度加分（更细分）
        if strength == SignalStrength.STRONG.value:
            score += 20
        elif strength == SignalStrength.MEDIUM.value:
            score += 12
        else:
            score += 5
        
        # 3. 技术指标加分（多指标叠加，但有上限）
        tech_bonus = 0
        
        # KDJ超卖加分（J<0严重超卖，J<20轻度超卖）
        kdj_j = latest.get('kdj_j')
        if not pd.isna(kdj_j):
            if kdj_j < 0:
                tech_bonus += 8
            elif kdj_j < 20:
                tech_bonus += 4
        
        # MACD柱状体改善（绿柱缩短或红柱增长）
        macd_hist = latest.get('macd_hist')
        if not pd.isna(macd_hist) and macd_hist > 0:
            tech_bonus += 5
        
        # 成交量配合（底部放量或缩量企稳）
        vol_ratio = latest.get('volume_ratio')
        if not pd.isna(vol_ratio):
            if 0.8 <= vol_ratio <= 1.5:  # 缩量企稳
                tech_bonus += 4
            elif vol_ratio >= 2.0:  # 底部放量
                tech_bonus += 6
        
        # 技术指标加分上限15分
        score += min(tech_bonus, 15)
        
        # 4. 负面因素扣分（新增）
        penalty = 0
        
        # 趋势恶化扣分（MA5<MA10且向下）
        ma5 = latest.get('ma5')
        ma10 = latest.get('ma10')
        if not pd.isna(ma5) and not pd.isna(ma10):
            if ma5 < ma10 * 0.95:  # MA5明显低于MA10
                penalty += 8
        
        # 连续下跌扣分
        change_pct = latest.get('change_pct')
        if not pd.isna(change_pct) and change_pct < -7:
            penalty += 5  # 单日暴跌，可能有利空
        
        # 流动性差扣分
        if not pd.isna(vol_ratio) and vol_ratio < 0.5:
            penalty += 4  # 极度缩量，流动性差
        
        score -= min(penalty, 20)  # 扣分上限20
        
        # 确保分数在合理范围
        return max(30, min(score, 100))
    
    def _calculate_zt_quality_score(self, latest, prev, prev2, df_history: pd.DataFrame) -> dict:
        """
        计算涨停质量评分（策略2）
        
        用于识别涨停陷阱 vs 优质涨停
        
        Returns:
            {
                'zt_quality_score': int,  # 涨停质量分 0-100
                'zt_quality_level': str,  # A/B/C/D 等级
                'zt_risk_flags': List[str],  # 风险标记
            }
        """
        change_pct = latest.get('change_pct', 0)
        
        # 非涨停直接返回
        if change_pct < 9.9:
            return {
                'zt_quality_score': None,
                'zt_quality_level': None,
                'zt_risk_flags': [],
            }
        
        score = 100
        risk_flags = []
        
        # 1. 前期涨幅检查（避免高位涨停）
        if len(df_history) >= 5:
            recent_changes = df_history['change_pct'].tail(5).tolist()
            recent_sum = sum(recent_changes[:-1])  # 前4天涨幅
            
            if recent_sum > 20:
                score -= 25
                risk_flags.append("前4日已涨>20%，高位风险")
            elif recent_sum > 10:
                score -= 15
                risk_flags.append("前4日已涨>10%，追高风险")
        
        # 2. 成交量健康度
        vol_ratio = latest.get('volume_ratio', 1)
        if vol_ratio > 5:
            score -= 20
            risk_flags.append("异常放量(量比>5)，可能出货")
        elif vol_ratio > 3:
            score -= 10
            risk_flags.append("放量过大(量比>3)")
        elif vol_ratio < 1:
            score -= 15
            risk_flags.append("缩量涨停，封单不足")
        
        # 3. 涨停类型检查
        # 左侧信号涨停 = 矛盾（抄底信号却涨停）
        # 这个在调用处判断，这里只计算基础分
        
        # 4. 趋势健康度
        ma5 = latest.get('ma5')
        ma10 = latest.get('ma10')
        ma20 = latest.get('ma20')
        
        if not pd.isna(ma5) and not pd.isna(ma10) and not pd.isna(ma20):
            if ma5 < ma10:
                score -= 20
                risk_flags.append("MA5<MA10，趋势未确认")
            elif ma10 < ma20:
                score -= 10
                risk_flags.append("MA10<MA20，中期偏弱")
        
        # 5. KDJ超买检查（涨停时KDJ过高有风险）
        kdj_j = latest.get('kdj_j')
        if not pd.isna(kdj_j) and kdj_j > 90:
            score -= 15
            risk_flags.append("KDJ严重超买(J>90)")
        elif not pd.isna(kdj_j) and kdj_j > 80:
            score -= 8
            risk_flags.append("KDJ超买(J>80)")
        
        # 6. 连续涨停检查
        prev_change = prev.get('change_pct', 0) if prev is not None else 0
        prev2_change = prev2.get('change_pct', 0) if prev2 is not None else 0
        
        if prev_change >= 9.9:
            score -= 20
            risk_flags.append("连续涨停，开板风险")
            if prev2_change >= 9.9:
                score -= 15
                risk_flags.append("三连板，高风险")
        
        # 确定等级
        final_score = max(0, min(score, 100))
        if final_score >= 80:
            level = 'A'
        elif final_score >= 65:
            level = 'B'
        elif final_score >= 50:
            level = 'C'
        else:
            level = 'D'
        
        return {
            'zt_quality_score': final_score,
            'zt_quality_level': level,
            'zt_risk_flags': risk_flags,
        }


class RightSignalDetector:
    """右侧信号检测器（追涨/确认信号）"""
    
    def __init__(self):
        self.calc = SignalCalculator()
        # 集成量价信号检测器
        from .volume_price_signals import VolumePriceDetector, VolumePriceAdapter
        self.vp_detector = VolumePriceDetector()
        self.vp_adapter = VolumePriceAdapter
    
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
                latest, "短期均线上穿，短期趋势转强", df, period, prev, prev2
            ))

        # 2. MA5上穿MA20金叉
        ma_cross_20 = self._detect_ma_cross_20(df, latest, prev)
        if ma_cross_20:
            signals.append(self._create_signal(
                symbol, name, "MA5金叉MA20", ma_cross_20,
                latest, "短期均线上穿中期均线，趋势转强", df, period, prev, prev2
            ))

        # 3. MACD金叉
        macd_cross = self._detect_macd_cross(latest, prev)
        if macd_cross:
            signals.append(self._create_signal(
                symbol, name, "MACD金叉", macd_cross,
                latest, "DIF上穿DEA，动量转强", df, period, prev, prev2
            ))

        # 4. KDJ金叉
        kdj_cross = self._detect_kdj_cross(latest, prev)
        if kdj_cross:
            signals.append(self._create_signal(
                symbol, name, "KDJ金叉", kdj_cross,
                latest, "K线上穿D线，短期超买", df, period, prev, prev2
            ))

        # 5. 量价突破（放量上涨）- 保留原有逻辑，与新模块互补
        volume_breakout = self._detect_volume_breakout(df, latest, prev)
        if volume_breakout:
            signals.append(self._create_signal(
                symbol, name, "量价突破", volume_breakout,
                latest, "放量上涨，资金入场", df, period, prev, prev2
            ))

        # 6. 均线多头排列
        ma_bull = self._detect_ma_bullish(df, latest)
        if ma_bull:
            signals.append(self._create_signal(
                symbol, name, "均线多头排列", ma_bull,
                latest, "均线呈多头排列，趋势良好", df, period, prev, prev2
            ))

        # 7. 突破平台
        platform_break = self._detect_platform_breakout(df, latest)
        if platform_break:
            signals.append(self._create_signal(
                symbol, name, "突破平台", platform_break,
                latest, "放量突破近期整理平台", df, period, prev, prev2
            ))
        
        # 8. 新增：右侧量价信号（放量突破、倍量启动）
        vp_signals = self._detect_volume_price_signals(df, symbol, name, period, latest)
        signals.extend(vp_signals)
        
        return signals
    
    def _detect_volume_price_signals(self, df: pd.DataFrame, symbol: str, name: str, 
                                     period: str, latest: pd.Series) -> List[StockSignal]:
        """
        检测右侧量价信号（突破/放量类）
        """
        signals = []
        from .volume_price_signals import VolumePricePattern
        
        # 只关注右侧信号（放量突破、倍量启动）
        right_patterns = [
            VolumePricePattern.BREAKOUT_VOLUME,    # 放量突破
            VolumePricePattern.DOUBLE_VOLUME,       # 倍量启动
            VolumePricePattern.VOLUME_ACCUMULATION, # 量能堆积
        ]
        
        for pattern in right_patterns:
            vp_signal = self.vp_detector.detect(df, pattern)
            if vp_signal:
                # 获取日期并格式化为 YYYY-MM-DD
                if 'trade_date' in latest:
                    date_val = latest['trade_date']
                    if hasattr(date_val, 'strftime'):
                        trigger_date = date_val.strftime('%Y-%m-%d')
                    else:
                        trigger_date = str(date_val)[:10]
                else:
                    trigger_date = str(latest.name) if hasattr(latest, 'name') else ""
                
                stock_signal = self.vp_adapter.to_stock_signal(
                    vp_signal, symbol, name, period,
                    close_price=latest['close'],
                    change_pct=latest.get('change_pct', 0) * 100,
                    technicals={
                        "date": trigger_date,
                        "ma20": latest.get('ma20'),
                        "volume_ratio": vp_signal.volume_ratio,
                    }
                )
                signals.append(StockSignal(**stock_signal))

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
                       description, df, period: str = "daily", prev=None, prev2=None) -> StockSignal:
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

        # 计算涨停质量评分（策略2）
        zt_quality = self._calculate_zt_quality_score(latest, prev, prev2, df)
        if zt_quality['zt_quality_score'] is not None:
            technicals.update(zt_quality)

        return StockSignal(
            symbol=symbol,
            name=name,
            signal_type=SignalType.RIGHT.value,
            signal_name=signal_name,
            strength=strength,
            period=period,
            trigger_date=format_date_safe(latest['trade_date']),
            close_price=round(latest['close'], 2),
            change_pct=round(latest.get('change_pct', 0), 2),
            volume_ratio=round(latest.get('volume_ratio', 1), 2),
            description=description,
            score=score,
            technicals=technicals
        )

    def _calculate_score(self, signal_name: str, strength: str, latest) -> int:
        """
        计算右侧信号评分 - 严格版

        右侧信号要求更严格的量价配合和趋势确认
        """
        score = 35  # 右侧信号基础分稍高（趋势已确认）
        
        # 1. 信号类型加分
        if "突破" in signal_name:
            score += 25  # 突破最重要
        elif "多头排列" in signal_name:
            score += 20
        elif "金叉" in signal_name:
            score += 15
        elif "量价" in signal_name:
            score += 18
        else:
            score += 8
        
        # 2. 强度加分
        if strength == SignalStrength.STRONG.value:
            score += 18
        elif strength == SignalStrength.MEDIUM.value:
            score += 10
        else:
            score += 4
        
        # 3. 技术指标加分（右侧信号关注动能和量能）
        tech_bonus = 0
        
        # MACD红柱且扩大
        macd_hist = latest.get('macd_hist')
        if not pd.isna(macd_hist) and macd_hist > 0:
            tech_bonus += 6
        
        # KDJ金叉区域（K>D且向上）
        kdj_k = latest.get('kdj_k')
        kdj_d = latest.get('kdj_d')
        if not pd.isna(kdj_k) and not pd.isna(kdj_d):
            if kdj_k > kdj_d and kdj_k < 80:  # 金叉且未超买
                tech_bonus += 6
            elif kdj_k > 80:  # 超买区域扣分
                tech_bonus -= 5
        
        # 成交量配合（右侧信号更重视量能）
        vol_ratio = latest.get('volume_ratio')
        if not pd.isna(vol_ratio):
            if vol_ratio >= 2.5:  # 明显放量
                tech_bonus += 10
            elif vol_ratio >= 1.5:  # 温和放量
                tech_bonus += 6
            elif vol_ratio < 0.8:  # 缩量上涨，不健康
                tech_bonus -= 3
        
        # 价格在均线上方
        close = latest.get('close')
        ma20 = latest.get('ma20')
        if not pd.isna(close) and not pd.isna(ma20):
            if close > ma20 * 1.05:  # 明显站上MA20
                tech_bonus += 5
            elif close < ma20:  # 还在MA20下方
                tech_bonus -= 5
        
        score += min(max(tech_bonus, -10), 20)  # 限制在-10到+20
        
        # 4. 负面因素扣分
        penalty = 0
        
        # 涨速过快（可能回调）
        change_pct = latest.get('change_pct')
        if not pd.isna(change_pct):
            if change_pct > 9:  # 涨停或接近涨停，追高风险
                penalty += 8
            elif change_pct < 0:  # 信号日下跌，信号失效
                penalty += 15
        
        # 上方压力（接近前期高点）
        # 注：这里需要历史数据，暂时简化
        
        score -= min(penalty, 20)
        
        return max(35, min(score, 100))

    def _calculate_zt_quality_score(self, latest, prev, prev2, df_history: pd.DataFrame) -> dict:
        """
        计算涨停质量评分（策略2）
        
        用于识别涨停陷阱 vs 优质涨停
        
        Returns:
            {
                'zt_quality_score': int,  # 涨停质量分 0-100
                'zt_quality_level': str,  # A/B/C/D 等级
                'zt_risk_flags': List[str],  # 风险标记
            }
        """
        change_pct = latest.get('change_pct', 0)
        
        # 非涨停直接返回
        if change_pct < 9.9:
            return {
                'zt_quality_score': None,
                'zt_quality_level': None,
                'zt_risk_flags': [],
            }
        
        score = 100
        risk_flags = []
        
        # 1. 前期涨幅检查（避免高位涨停）
        if len(df_history) >= 5:
            recent_changes = df_history['change_pct'].tail(5).tolist()
            recent_sum = sum(recent_changes[:-1])  # 前4天涨幅
            
            if recent_sum > 20:
                score -= 25
                risk_flags.append("前4日已涨>20%，高位风险")
            elif recent_sum > 10:
                score -= 15
                risk_flags.append("前4日已涨>10%，追高风险")
        
        # 2. 成交量健康度
        vol_ratio = latest.get('volume_ratio', 1)
        if vol_ratio > 5:
            score -= 20
            risk_flags.append("异常放量(量比>5)，可能出货")
        elif vol_ratio > 3:
            score -= 10
            risk_flags.append("放量过大(量比>3)")
        elif vol_ratio < 1:
            score -= 15
            risk_flags.append("缩量涨停，封单不足")
        
        # 3. 趋势健康度
        ma5 = latest.get('ma5')
        ma10 = latest.get('ma10')
        ma20 = latest.get('ma20')
        
        if not pd.isna(ma5) and not pd.isna(ma10) and not pd.isna(ma20):
            if ma5 < ma10:
                score -= 20
                risk_flags.append("MA5<MA10，趋势未确认")
            elif ma10 < ma20:
                score -= 10
                risk_flags.append("MA10<MA20，中期偏弱")
        
        # 4. KDJ超买检查（涨停时KDJ过高有风险）
        kdj_j = latest.get('kdj_j')
        if not pd.isna(kdj_j) and kdj_j > 90:
            score -= 15
            risk_flags.append("KDJ严重超买(J>90)")
        elif not pd.isna(kdj_j) and kdj_j > 80:
            score -= 8
            risk_flags.append("KDJ超买(J>80)")
        
        # 5. 连续涨停检查
        prev_change = prev.get('change_pct', 0) if prev is not None else 0
        prev2_change = prev2.get('change_pct', 0) if prev2 is not None else 0
        
        if prev_change >= 9.9:
            score -= 20
            risk_flags.append("连续涨停，开板风险")
            if prev2_change >= 9.9:
                score -= 15
                risk_flags.append("三连板，高风险")
        
        # 确定等级
        final_score = max(0, min(score, 100))
        if final_score >= 80:
            level = 'A'
        elif final_score >= 65:
            level = 'B'
        elif final_score >= 50:
            level = 'C'
        else:
            level = 'D'
        
        return {
            'zt_quality_score': final_score,
            'zt_quality_level': level,
            'zt_risk_flags': risk_flags,
        }


class StockSignalScanner:
    """个股/ETF信号扫描器主类 - 支持多周期（日线/周线/月线）"""
    
    def __init__(self, asset_type: str = "stock"):
        """
        初始化扫描器
        
        Args:
            asset_type: "stock"(股票) 或 "etf"(ETF)
        """
        self.asset_type = asset_type
        self.left_detector = LeftSignalDetector()
        self.right_detector = RightSignalDetector()
        self.output_dir = Path(project_root) / "storage" / "outputs" / "signals"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.price_dir = RAW_PRICE_DIR
    
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

            # 过滤掉未来的日期（重要！）
            from datetime import datetime
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            resampled = resampled[resampled.index <= today]

            if resampled.empty:
                return pd.DataFrame()

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
        
        # 计算趋势健康度并添加到每个信号（便于signal_watch页面展示）
        if not df_daily.empty:
            health = SignalCalculator.calculate_trend_health(df_daily)
            for sig in signals:
                sig.technicals['health_score'] = health['health_score']
                sig.technicals['risk_level'] = health['risk_level']
                sig.technicals['health_warnings'] = health['warnings']
                sig.technicals['health_recommendation'] = health['recommendation']

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
        信号组合评分 - 严格分级制度
        
        分数等级（稀缺性控制）：
        - 95-100分（极品）: <2% - 必须双侧+多周期+多指标
        - 90-94分（强烈推荐）: <5% - 必须双侧或多周期覆盖
        - 85-89分（值得关注）: <15% - 需要较好维度覆盖
        - 80-84分（观察）: <30% - 基础条件满足
        - <80分（普通）: >50% - 一般信号
        
        硬性门槛：
        - 单一信号（无论多强）≤ 85分
        - 单侧信号（只有左或只有右）≤ 90分  
        - 要达到95+必须有：双侧 + 多周期 + 多指标
        """
        if not signals:
            return signals
        
        n_signals = len(signals)
        scores = [sig.score for sig in signals]
        max_score = max(scores)
        avg_score = sum(scores) / n_signals
        
        # 计算维度
        dimension_coverage = self._calculate_dimension_coverage(signals)
        dim_coverage = dimension_coverage['coverage']
        dim_details = dimension_coverage['details']
        
        # 检查关键条件
        has_both_sides = len(set(sig.signal_type for sig in signals)) >= 2
        has_multi_period = len(set(sig.period for sig in signals)) >= 2
        has_multi_indicator = len(dim_details.get('indicators', {})) >= 2
        
        # === 硬性上限控制（更严格）===
        # 基础上限
        if n_signals == 1:
            max_possible = 82  # 单一信号上限82
        elif not has_multi_period:
            max_possible = 85  # 单周期上限85
        elif not has_both_sides:
            max_possible = 88  # 单侧上限88
        elif has_both_sides and has_multi_period and has_multi_indicator and dim_coverage >= 0.7:
            max_possible = 100  # 完美条件可达100
        elif has_both_sides and has_multi_period and has_multi_indicator:
            max_possible = 95   # 较好条件95
        elif has_both_sides and has_multi_period:
            max_possible = 92   # 双侧+多周期92
        elif has_both_sides and has_multi_indicator:
            max_possible = 90   # 双侧+多指标90
        else:
            max_possible = 88
        
        # === 基础分计算（降低）===
        base_quality = max_score * 0.3 + avg_score * 0.4 + min(scores) * 0.3
        
        # === 维度奖励（降低）===
        if dim_coverage >= 0.75:
            dim_bonus = 8
        elif dim_coverage >= 0.5:
            dim_bonus = 5
        elif dim_coverage >= 0.3:
            dim_bonus = 2
        else:
            dim_bonus = 0
        
        # === 共振奖励（降低）===
        resonance_bonus = 0
        if has_both_sides:
            resonance_bonus += 3
        if has_multi_period:
            resonance_bonus += 2
        if has_multi_indicator:
            resonance_bonus += 1
        
        # === 计算最终分 ===
        portfolio_score = base_quality + dim_bonus + resonance_bonus
        portfolio_score = min(portfolio_score, max_possible)
        portfolio_score = max(30, min(portfolio_score, 100))
        
        # === 稀缺性标签 ===
        if portfolio_score >= 95:
            scarcity_label = "极品 ⭐⭐⭐"
        elif portfolio_score >= 90:
            scarcity_label = "强烈推荐 ⭐⭐"
        elif portfolio_score >= 85:
            scarcity_label = "值得关注 ⭐"
        elif portfolio_score >= 80:
            scarcity_label = "观察"
        else:
            scarcity_label = "普通"
        
        for sig in signals:
            sig.technicals['portfolio_score'] = round(portfolio_score, 1)
            sig.technicals['signal_count'] = n_signals
            sig.technicals['dimension_coverage'] = round(dim_coverage, 2)
            sig.technicals['scarcity_label'] = scarcity_label
        
        return signals
    
    def _calculate_consistency(self, signals: List[StockSignal]) -> float:
        """计算信号间一致性（0-1），1表示完全一致，0表示矛盾"""
        if len(signals) <= 1:
            return 1.0
        
        # 检查方向一致性
        directions = [sig.signal_type for sig in signals]
        if len(set(directions)) == 1:
            return 1.0  # 同向
        
        # 左右都有时，检查是否合理（左抄底+右确认是合理的）
        has_left = 'left' in directions
        has_right = 'right' in directions
        
        if has_left and has_right:
            # 检查是否有时间逻辑（先左后右或同时）
            return 0.8  # 合理但有差异
        
        return 0.6  # 一般一致性
    
    def _get_scarcity_label(self, portfolio_score: float, dim_coverage: float) -> str:
        """获取稀缺性标签"""
        if portfolio_score >= 95 and dim_coverage >= 0.7:
            return "极品信号"
        elif portfolio_score >= 90:
            return "强烈推荐"
        elif portfolio_score >= 85:
            return "值得关注"
        elif portfolio_score >= 80:
            return "观察"
        else:
            return "普通"

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
        """
        扫描全市场股票或ETF
        
        Args:
            signal_type: 信号类型 all/left/right
            limit: 限制扫描数量
            multi_period: 是否多周期分析
        """
        date_str = datetime.now().strftime('%Y%m%d')
        
        # 根据资产类型获取列表
        if self.asset_type == "etf":
            asset_list = self._get_etf_list()
            asset_name = "ETF"
        elif self.asset_type == "index":
            asset_list = self._get_index_list()
            asset_name = "指数"
        else:
            asset_list = self._get_stock_list()
            asset_name = "股票"
        
        if asset_list.empty:
            return {"status": "error", "message": f"没有{asset_name}列表"}

        if limit:
            asset_list = asset_list.head(limit)

        period_str = "多周期" if multi_period else "日线"
        logger.info(f"开始扫描 {len(asset_list)} 只{asset_name}，信号类型: {signal_type}, 周期: {period_str}")

        all_signals = []
        all_health_scores = []  # 存储所有股票的健康度
        risk_alerts = []        # 存储风险预警（健康度<40且无买入信号）
        stats = {"left": 0, "right": 0, "by_period": {"daily": 0, "weekly": 0, "monthly": 0}, "by_signal": {}}

        for idx, row in asset_list.iterrows():
            symbol = row['symbol']
            name = row.get('name', '')

            if (idx + 1) % 100 == 0:
                logger.info(f"进度: {idx + 1}/{len(asset_list)}")

            # 扫描买入信号
            signals = self.scan_stock(symbol, name, signal_type, multi_period)
            
            # 计算趋势健康度（无论是否有买入信号）
            df_daily = self.load_stock_data(symbol, "daily")
            if not df_daily.empty:
                health = SignalCalculator.calculate_trend_health(df_daily)
                health_record = {
                    "symbol": symbol,
                    "name": name,
                    "health_score": health["health_score"],
                    "risk_level": health["risk_level"],
                    "warnings": health["warnings"],
                    "recommendation": health["recommendation"],
                    "has_buy_signal": len(signals) > 0
                }
                all_health_scores.append(health_record)
                
                # 识别风险预警：健康度<40 且 无买入信号
                if health["health_score"] < 40 and len(signals) == 0:
                    risk_alerts.append({
                        "symbol": symbol,
                        "name": name,
                        "health_score": health["health_score"],
                        "risk_level": health["risk_level"],
                        "warnings": health["warnings"],
                        "recommendation": health["recommendation"],
                        "details": health["details"]
                    })
            
            # 处理买入信号
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
        
        # 按健康度排序（健康度低的在前，即风险高的在前）
        all_health_scores.sort(key=lambda x: x["health_score"])
        risk_alerts.sort(key=lambda x: x["health_score"])
        
        # 统计健康度分布
        health_distribution = {
            "excellent": len([h for h in all_health_scores if h["health_score"] >= 80]),
            "good": len([h for h in all_health_scores if 60 <= h["health_score"] < 80]),
            "warning": len([h for h in all_health_scores if 40 <= h["health_score"] < 60]),
            "risky": len([h for h in all_health_scores if 20 <= h["health_score"] < 40]),
            "extreme": len([h for h in all_health_scores if h["health_score"] < 20])
        }

        result = {
            "status": "success",
            "scan_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_stocks": len(asset_list),
            "total_signals": len(all_signals),
            "multi_period": multi_period,
            "stats": stats,
            "signals": all_signals,
            "health_scores": {
                "total": len(all_health_scores),
                "distribution": health_distribution,
                "risk_alerts_count": len(risk_alerts),
                "all_scores": all_health_scores,  # 所有股票的健康度
                "risk_alerts": risk_alerts        # 风险预警列表（健康度<40且无买入信号）
            }
        }

        # 保存结果
        self._save_result(result, signal_type)

        # 保存健康度数据到单独文件（便于快速加载）
        self._save_health_scores(result.get("health_scores", {}), date_str)

        logger.info(f"扫描完成: {len(all_signals)} 个信号，左侧: {stats['left']}, 右侧: {stats['right']}")
        if multi_period:
            logger.info(f"周期分布: 日线 {stats['by_period'].get('daily', 0)}, "
                       f"周线 {stats['by_period'].get('weekly', 0)}, "
                       f"月线 {stats['by_period'].get('monthly', 0)}")
        
        # 输出健康度统计
        health = result.get("health_scores", {})
        if health:
            dist = health.get("distribution", {})
            logger.info(f"健康度分布: 优秀{dist.get('excellent', 0)} 良好{dist.get('good', 0)} "
                       f"预警{dist.get('warning', 0)} 风险{dist.get('risky', 0)} 极端{dist.get('extreme', 0)}")
            logger.info(f"风险预警: {health.get('risk_alerts_count', 0)} 只股票趋势走坏（健康度<40且无买入信号）")

        return result
    
    def _get_stock_list(self) -> pd.DataFrame:
        """获取股票列表（排除北交所股票）"""
        stock_csv = Path(project_root) / "storage" / "stock_basic_info.csv"
        if stock_csv.exists():
            df = pd.read_csv(stock_csv)
            # 过滤掉排除的交易所股票
            df = df[df['symbol'].apply(lambda x: not any(x.endswith(f'.{ex}') for ex in EXCLUDED_EXCHANGES))]
            return df[['symbol', 'name']] if 'name' in df.columns else df[['symbol']]
        return pd.DataFrame()
    
    def _get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表"""
        etf_csv = Path(project_root) / "storage" / "etf_basic_info.csv"
        if etf_csv.exists():
            df = pd.read_csv(etf_csv)
            return df[['symbol', 'name']] if 'name' in df.columns else df[['symbol']]
        return pd.DataFrame()

    def _get_index_list(self) -> pd.DataFrame:
        """获取指数列表（从 official_indices.csv）"""
        index_csv = Path(project_root) / "storage" / "official_indices.csv"
        if index_csv.exists():
            df = pd.read_csv(index_csv)
            # 确保列名正确（处理BOM）
            if 'symbol' not in df.columns and '\ufeffsymbol' in df.columns:
                df = df.rename(columns={'\ufeffsymbol': 'symbol'})
            return df[['symbol', 'name']] if 'name' in df.columns else df[['symbol']]
        return pd.DataFrame()
    
    def _save_result(self, result: Dict, signal_type: str):
        """保存扫描结果 - 新格式：按股票组织，包含健康度和信号列表"""
        date_str = datetime.now().strftime('%Y%m%d')
        if self.asset_type == "etf":
            prefix = "etf_signals"
        elif self.asset_type == "index":
            prefix = "index_signals"
        else:
            prefix = "stock_signals"
        
        # 构建统一格式的数据：按股票组织
        unified_data = self._build_unified_data(result)
        
        # 保存带日期的文件
        filename = f"{prefix}_{date_str}.json"
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(unified_data, f, ensure_ascii=False, indent=2)
        
        # 保存latest文件（供Dashboard使用）
        latest_path = self.output_dir / f"{prefix}_latest.json"
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(unified_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"结果已保存: {filepath} ({unified_data['total_signals']} 个信号, {unified_data['total_stocks']} 只股票)")
    
    def _build_unified_data(self, result: Dict) -> Dict:
        """
        将信号列表和健康度数据合并为按股票组织的统一格式
        
        Returns:
            {
                "scan_time": "...",
                "total_stocks": 5000,
                "total_signals": 123,
                "stocks": [
                    {
                        "symbol": "600000.SH",
                        "name": "浦发银行",
                        "health_score": 8,
                        "risk_level": "extreme",
                        "health_recommendation": "建议卖出/回避",
                        "health_warnings": [...],
                        "has_buy_signal": true,
                        "best_signal_score": 55,  # 最高信号分
                        "signals": [...]
                    },
                    ...
                ]
            }
        """
        # 获取健康度数据
        health_data = result.get("health_scores", {})
        all_health_scores = {h["symbol"]: h for h in health_data.get("all_scores", [])}
        
        # 按股票组织信号
        signals_by_stock = {}
        for sig in result.get("signals", []):
            symbol = sig["symbol"]
            if symbol not in signals_by_stock:
                signals_by_stock[symbol] = []
            signals_by_stock[symbol].append(sig)
        
        # 构建股票列表
        stocks = []
        for symbol, health in all_health_scores.items():
            stock_signals = signals_by_stock.get(symbol, [])
            
            # 计算最高信号分
            best_score = max([s["score"] for s in stock_signals]) if stock_signals else 0
            
            # 风险分 = 100 - 健康分（健康分越高，风险越低）
            health_score = health.get("health_score", 50)
            risk_score = 100 - health_score  # 转换：健康80分 = 风险20分
            
            stock_data = {
                "symbol": symbol,
                "name": health.get("name", ""),
                "risk_score": risk_score,  # 风险分（0-100，越高越危险）
                "signal_score": best_score,  # 信号分（0-100，越高越好）
                "has_buy_signal": len(stock_signals) > 0,
                "signal_count": len(stock_signals),
                "signals": stock_signals,
                # 风险详情
                "risk_level": health.get("risk_level", "medium"),
                "risk_warnings": health.get("warnings", []),
                "risk_details": health.get("details", {}),
                "risk_recommendation": health.get("recommendation", ""),
                # 技术指标摘要
                "technicals": health.get("technicals", {})
            }
            stocks.append(stock_data)
        
        return {
            "scan_time": result.get("scan_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            "total_stocks": len(stocks),
            "total_signals": len(result.get("signals", [])),
            "stocks": stocks
        }
    
    def _save_health_scores(self, health_scores: Dict, date_str: str):
        """保留此方法以兼容旧代码，但不再生成独立文件（数据已包含在主结果中）"""
        # 健康度数据现在已统一保存在主结果文件中
        # 此方法保留为空，避免调用出错
        pass


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='个股信号扫描器 - 支持多周期（日线/周线/月线）')
    parser.add_argument('--symbol', type=str, help='扫描指定股票')
    parser.add_argument('--limit', type=int, help='限制扫描数量（测试用）')
    parser.add_argument('--no-multi-period', action='store_true',
                        help='禁用多周期分析，仅使用日线')

    args = parser.parse_args()

    scanner = StockSignalScanner(asset_type="stock")
    multi_period = not args.no_multi_period

    if args.symbol:
        # 扫描单只股票（同时扫描左右侧）
        name = get_stock_name(args.symbol)
        signals = scanner.scan_stock(args.symbol, name, 'all', multi_period)

        print(f"\n{args.symbol} {name} 的信号:")
        print("-" * 60)
        for sig in signals:
            period_tag = f"[{sig.period[:1].upper()}]" if sig.period != "daily" else "[D]"
            print(f"{period_tag} [{sig.signal_type.upper()}] {sig.signal_name} ({sig.strength})")
            print(f"  评分: {sig.score} | 日期: {sig.trigger_date} | 价格: {sig.close_price}")
            print(f"  {sig.description}")
            print()

    else:
        # 扫描所有股票（默认行为，同时扫描左右侧）
        result = scanner.scan_all('all', args.limit, multi_period)

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


if __name__ == "__main__":
    main()

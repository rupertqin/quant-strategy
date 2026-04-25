"""
量价关系信号检测器 - 模块化信号系统

基于成交量和价格行为的信号检测，包括：
- 放量突破
- 缩量回调
- 量价背离
- 倍量启动
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Callable
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class VolumePricePattern(Enum):
    """量价模式类型"""
    BREAKOUT_VOLUME = "breakout_volume"      # 放量突破
    VOLUME_CONTRACTION = "volume_contraction" # 缩量整理
    VOLUME_PRICE_DIVERGENCE = "vp_divergence" # 量价背离
    DOUBLE_VOLUME = "double_volume"          # 倍量启动
    VOLUME_ACCUMULATION = "volume_accum"     # 量能堆积


@dataclass
class VolumePriceSignal:
    """量价信号数据"""
    pattern: VolumePricePattern
    strength: str  # strong/medium/weak
    description: str
    volume_ratio: float      # 量比
    price_change_pct: float  # 价格变化%
    score: int               # 0-100
    metadata: Dict           # 额外数据


class VolumePriceDetector:
    """
    量价关系信号检测器
    
    设计特点：
    1. 每个检测方法独立，可单独调用
    2. 支持通过注册表动态添加新的检测规则
    3. 统一的评分体系
    """

    def __init__(self):
        # 检测方法注册表
        self._detectors: Dict[VolumePricePattern, Callable] = {
            VolumePricePattern.BREAKOUT_VOLUME: self._detect_breakout_volume,
            VolumePricePattern.VOLUME_CONTRACTION: self._detect_volume_contraction,
            VolumePricePattern.VOLUME_PRICE_DIVERGENCE: self._detect_vp_divergence,
            VolumePricePattern.DOUBLE_VOLUME: self._detect_double_volume,
            VolumePricePattern.VOLUME_ACCUMULATION: self._detect_volume_accumulation,
        }

    def detect_all(self, df: pd.DataFrame) -> List[VolumePriceSignal]:
        """
        检测所有量价信号
        
        Args:
            df: 包含 OHLCV 数据的 DataFrame
            
        Returns:
            信号列表
        """
        signals = []
        
        if len(df) < 20:
            return signals
            
        # 预处理数据 - 计算必要的指标
        df = self._prepare_data(df)
        
        # 遍历所有注册的检测器
        for pattern, detector_fn in self._detectors.items():
            try:
                signal = detector_fn(df)
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"{pattern.value} 检测失败: {e}")
                
        return signals

    def detect(self, df: pd.DataFrame, pattern: VolumePricePattern) -> Optional[VolumePriceSignal]:
        """
        检测特定类型的量价信号
        
        Args:
            df: 价格数据
            pattern: 要检测的模式
            
        Returns:
            信号对象或None
        """
        if len(df) < 20:
            return None
            
        df = self._prepare_data(df)
        detector_fn = self._detectors.get(pattern)
        
        if detector_fn:
            return detector_fn(df)
        return None

    def register_detector(self, pattern: VolumePricePattern, 
                         detector_fn: Callable[[pd.DataFrame], Optional[VolumePriceSignal]]):
        """
        注册新的检测器（扩展点）
        
        Args:
            pattern: 信号类型
            detector_fn: 检测函数，接收df返回VolumePriceSignal或None
        """
        self._detectors[pattern] = detector_fn
        logger.info(f"注册新的量价检测器: {pattern.value}")

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """预处理数据，计算必要的量价指标"""
        df = df.copy()
        
        # 基础成交量指标
        df['volume_ma5'] = df['volume'].rolling(5).mean()
        df['volume_ma20'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma20']
        
        # 价格变化
        df['price_change'] = df['close'].pct_change()
        df['price_change_5d'] = df['close'].pct_change(5)
        
        # 量价相关系数（5日）
        df['vp_corr_5'] = df['close'].rolling(5).corr(df['volume'])
        
        # 振幅
        df['amplitude'] = (df['high'] - df['low']) / df['low']
        
        return df

    def _detect_breakout_volume(self, df: pd.DataFrame) -> Optional[VolumePriceSignal]:
        """
        检测放量突破信号
        
        条件：
        1. 当日成交量 > 前20日均量 * 1.5
        2. 当日收盘价 > 前5日最高价（或突破某条均线）
        3. 当日涨幅 > 3%
        """
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        volume_ratio = latest['volume_ratio']
        prev_high_5 = df['high'].tail(6).head(5).max()
        price_change = latest['price_change'] * 100
        
        # 条件判断
        is_volume_surge = volume_ratio >= 1.5
        is_price_breakout = latest['close'] > prev_high_5
        is_price_up = price_change >= 3.0
        
        if is_volume_surge and is_price_breakout and is_price_up:
            # 计算强度
            if volume_ratio >= 2.5 and price_change >= 5.0:
                strength = "strong"
                score = 85
            elif volume_ratio >= 2.0 and price_change >= 4.0:
                strength = "medium"
                score = 70
            else:
                strength = "weak"
                score = 55
                
            return VolumePriceSignal(
                pattern=VolumePricePattern.BREAKOUT_VOLUME,
                strength=strength,
                description=f"放量突破: 量比{volume_ratio:.1f}, 涨幅{price_change:.1f}%",
                volume_ratio=volume_ratio,
                price_change_pct=price_change,
                score=score,
                metadata={
                    "breakout_level": prev_high_5,
                    "volume_ma20": latest['volume_ma20']
                }
            )
        return None

    def _detect_volume_contraction(self, df: pd.DataFrame) -> Optional[VolumePriceSignal]:
        """
        检测缩量整理信号
        
        条件：
        1. 当日成交量 < 前20日均量 * 0.6
        2. 近5日价格波动 < 5%
        3. 整体趋势向上或横盘
        """
        latest = df.iloc[-1]
        recent_5d = df.tail(5)
        
        volume_ratio = latest['volume_ratio']
        price_range_5d = (recent_5d['close'].max() - recent_5d['close'].min()) / recent_5d['close'].mean() * 100
        price_change_5d = abs(latest['price_change_5d'] * 100)
        
        # 条件判断
        is_volume_shrink = volume_ratio <= 0.6
        is_price_stable = price_range_5d < 5.0
        is_not_falling = latest['close'] >= df['close'].tail(20).mean() * 0.95
        
        if is_volume_shrink and is_price_stable and is_not_falling:
            if volume_ratio <= 0.4:
                strength = "strong"
                score = 75
            else:
                strength = "medium"
                score = 60
                
            return VolumePriceSignal(
                pattern=VolumePricePattern.VOLUME_CONTRACTION,
                strength=strength,
                description=f"缩量整理: 量比{volume_ratio:.1f}, 5日波动{price_range_5d:.1f}%",
                volume_ratio=volume_ratio,
                price_change_pct=price_change_5d,
                score=score,
                metadata={
                    "volume_ma20": latest['volume_ma20'],
                    "price_stability": price_range_5d
                }
            )
        return None

    def _detect_vp_divergence(self, df: pd.DataFrame) -> Optional[VolumePriceSignal]:
        """
        检测量价背离信号
        
        底背离（看涨）：
        - 价格创新低，成交量萎缩，可能见底
        
        顶背离（看跌）：
        - 价格创新高，成交量萎缩，可能见顶
        """
        latest = df.iloc[-1]
        recent_10d = df.tail(10)
        recent_20d = df.tail(20)
        
        # 底背离：价格近10日低点，但成交量萎缩
        price_near_low = latest['close'] <= recent_10d['close'].quantile(0.2)
        volume_shrinking = latest['volume_ratio'] < 0.8
        vp_corr_negative = latest['vp_corr_5'] < -0.3 if not pd.isna(latest['vp_corr_5']) else False
        
        if price_near_low and volume_shrinking and vp_corr_negative:
            return VolumePriceSignal(
                pattern=VolumePricePattern.VOLUME_PRICE_DIVERGENCE,
                strength="medium",
                description=f"量价底背离: 价格低位+量能萎缩",
                volume_ratio=latest['volume_ratio'],
                price_change_pct=latest['price_change'] * 100,
                score=65,
                metadata={
                    "divergence_type": "bottom",
                    "vp_correlation": latest['vp_corr_5']
                }
            )
        
        # 顶背离检测
        price_near_high = latest['close'] >= recent_10d['close'].quantile(0.8)
        
        if price_near_high and volume_shrinking:
            return VolumePriceSignal(
                pattern=VolumePricePattern.VOLUME_PRICE_DIVERGENCE,
                strength="weak",
                description=f"量价顶背离: 价格高位+量能萎缩",
                volume_ratio=latest['volume_ratio'],
                price_change_pct=latest['price_change'] * 100,
                score=50,
                metadata={
                    "divergence_type": "top",
                    "vp_correlation": latest['vp_corr_5']
                }
            )
        return None

    def _detect_double_volume(self, df: pd.DataFrame) -> Optional[VolumePriceSignal]:
        """
        检测倍量启动信号
        
        条件：
        1. 当日成交量 >= 前一日 * 2
        2. 且当日成交量 > 前20日均量 * 1.8
        3. 价格上涨
        """
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if prev['volume'] == 0:
            return None
            
        volume_surge_ratio = latest['volume'] / prev['volume']
        vs_ma20 = latest['volume_ratio']
        price_change = latest['price_change'] * 100
        
        is_double_volume = volume_surge_ratio >= 2.0
        is_above_average = vs_ma20 >= 1.8
        is_price_up = price_change > 0
        
        if is_double_volume and is_above_average and is_price_up:
            if volume_surge_ratio >= 3.0 and price_change >= 5:
                strength = "strong"
                score = 90
            elif volume_surge_ratio >= 2.5 and price_change >= 3:
                strength = "medium"
                score = 75
            else:
                strength = "weak"
                score = 60
                
            return VolumePriceSignal(
                pattern=VolumePricePattern.DOUBLE_VOLUME,
                strength=strength,
                description=f"倍量启动: 量增{volume_surge_ratio:.1f}倍, 涨幅{price_change:.1f}%",
                volume_ratio=volume_surge_ratio,
                price_change_pct=price_change,
                score=score,
                metadata={
                    "prev_volume": prev['volume'],
                    "today_volume": latest['volume']
                }
            )
        return None

    def _detect_volume_accumulation(self, df: pd.DataFrame) -> Optional[VolumePriceSignal]:
        """
        检测量能堆积信号

        条件：
        1. 连续3日成交量 > 前20日均量
        2. 价格缓慢上涨或横盘（且最新一天不能暴跌）
        3. 可能是主力资金吸筹
        """
        if len(df) < 5:
            return None

        recent_3d = df.tail(3)
        latest = df.iloc[-1]

        # === 防线2：修正放量逻辑 —— 暴跌日放量不是"资金流入"，是"恐慌出逃" ===
        latest_change = latest.get('change_pct')
        if not pd.isna(latest_change) and latest_change < -4.0:
            # 最新一天暴跌超过4%，即使放量也不产生量能堆积买入信号
            return None

        # 连续3日放量
        all_above_avg = all(recent_3d['volume_ratio'] > 1.0)

        # 价格变化
        price_change_3d = (latest['close'] - df.iloc[-4]['close']) / df.iloc[-4]['close'] * 100

        # 成交量递增
        volume_increasing = recent_3d['volume'].is_monotonic_increasing

        if all_above_avg and abs(price_change_3d) < 5:
            avg_volume_ratio = recent_3d['volume_ratio'].mean()

            if avg_volume_ratio >= 1.5 and volume_increasing:
                strength = "strong"
                score = 70
            else:
                strength = "medium"
                score = 55

            return VolumePriceSignal(
                pattern=VolumePricePattern.VOLUME_ACCUMULATION,
                strength=strength,
                description=f"量能堆积: 3日持续放量, 价格变化{price_change_3d:.1f}%",
                volume_ratio=avg_volume_ratio,
                price_change_pct=price_change_3d,
                score=score,
                metadata={
                    "avg_volume_ratio_3d": avg_volume_ratio,
                    "volume_increasing": volume_increasing
                }
            )
        return None


# ========== 与现有系统的集成适配器 ==========

class VolumePriceAdapter:
    """
    量价信号适配器 - 将 VolumePriceSignal 转换为 StockSignal
    
    用于与现有的信号系统无缝集成
    """
    
    PATTERN_NAMES = {
        VolumePricePattern.BREAKOUT_VOLUME: "放量突破",
        VolumePricePattern.VOLUME_CONTRACTION: "缩量整理",
        VolumePricePattern.VOLUME_PRICE_DIVERGENCE: "量价背离",
        VolumePricePattern.DOUBLE_VOLUME: "倍量启动",
        VolumePricePattern.VOLUME_ACCUMULATION: "量能堆积",
    }
    
    @classmethod
    def to_stock_signal(cls, vp_signal: VolumePriceSignal, 
                       symbol: str, name: str, period: str,
                       close_price: float, change_pct: float,
                       technicals: Dict) -> Dict:
        """
        将量价信号转换为 StockSignal 格式
        
        Returns:
            符合 StockSignal 格式的字典
        """
        signal_name = cls.PATTERN_NAMES.get(vp_signal.pattern, vp_signal.pattern.value)
        
        # 判断左右侧信号
        if vp_signal.pattern in [VolumePricePattern.BREAKOUT_VOLUME,
                                  VolumePricePattern.DOUBLE_VOLUME,
                                  VolumePricePattern.VOLUME_ACCUMULATION]:
            signal_type = "right"  # 追涨/突破/量能堆积类属于右侧（趋势跟随）
        else:
            signal_type = "left"   # 背离/缩量类属于左侧（逆势抄底）
        
        # 构建包含具体数字的信号描述
        pct_str = f"+{change_pct}%" if change_pct > 0 else f"{change_pct}%"
        vol_ratio = round(vp_signal.volume_ratio, 2)
        ma20 = technicals.get("ma20")
        ma20_str = f"MA20=¥{ma20:.2f}" if ma20 else ""

        if signal_name == "放量突破":
            description = f"当前价格¥{close_price:.2f}({pct_str})，成交量{vol_ratio}倍突破，资金主动进攻"
        elif signal_name == "倍量启动":
            description = f"当前价格¥{close_price:.2f}({pct_str})，成交量骤增至{vol_ratio}倍，启动迹象明显"
        elif signal_name == "量能堆积":
            description = f"当前价格¥{close_price:.2f}({pct_str})，连续放量({vol_ratio}倍)，资金持续流入"
        elif signal_name == "量价背离":
            description = f"当前价格¥{close_price:.2f}({pct_str})，价格下行但成交量萎缩({vol_ratio}倍)，抛压减弱"
        elif signal_name == "缩量整理":
            description = f"当前价格¥{close_price:.2f}({pct_str})，成交量缩至{vol_ratio}倍，整理接近尾声"
        else:
            description = f"当前价格¥{close_price:.2f}({pct_str})，{signal_name}"

        return {
            "symbol": symbol,
            "name": name,
            "signal_type": signal_type,
            "signal_name": signal_name,
            "strength": vp_signal.strength,
            "period": period,
            "trigger_date": technicals.get("date", ""),
            "close_price": close_price,
            "change_pct": change_pct,
            "volume_ratio": vp_signal.volume_ratio,
            "description": description,
            "score": vp_signal.score,
            "technicals": {
                **technicals,
                "vp_metadata": vp_signal.metadata,
                "pattern": vp_signal.pattern.value,
            }
        }

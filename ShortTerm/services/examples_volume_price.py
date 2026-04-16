"""
量价信号模块使用示例

展示如何使用新的模块化量价信号系统
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 导入模块
from volume_price_signals import (
    VolumePriceDetector, 
    VolumePricePattern,
    VolumePriceAdapter
)


def generate_sample_data(n_days=60, trend='up') -> pd.DataFrame:
    """
    生成示例K线数据用于测试
    
    Args:
        n_days: 天数
        trend: 'up'(上涨), 'down'(下跌), 'consolidate'(盘整)
    """
    dates = pd.date_range(end=datetime.now(), periods=n_days, freq='D')
    
    # 基础价格
    base_price = 100.0
    
    # 根据趋势生成价格
    if trend == 'up':
        prices = base_price * (1 + np.cumsum(np.random.randn(n_days) * 0.01 + 0.002))
    elif trend == 'down':
        prices = base_price * (1 - np.cumsum(np.random.randn(n_days) * 0.01 + 0.001))
    else:  # consolidate
        prices = base_price + np.cumsum(np.random.randn(n_days) * 0.5)
    
    # 生成OHLCV
    df = pd.DataFrame({
        'open': prices * (1 + np.random.randn(n_days) * 0.005),
        'high': prices * (1 + abs(np.random.randn(n_days)) * 0.015),
        'low': prices * (1 - abs(np.random.randn(n_days)) * 0.015),
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, n_days) * 
                  (1 + np.random.randn(n_days) * 0.3),
    }, index=dates)
    
    # 确保 high >= low
    df['high'] = np.maximum(df['high'], df[['open', 'close']].max(axis=1) * 1.01)
    df['low'] = np.minimum(df['low'], df[['open', 'close']].min(axis=1) * 0.99)
    
    return df


def example_1_basic_usage():
    """示例1: 基础使用 - 检测所有量价信号"""
    print("="*60)
    print("示例1: 检测所有量价信号")
    print("="*60)
    
    # 生成示例数据（放量突破场景）
    df = generate_sample_data(40, trend='up')
    
    # 模拟放量突破：最后几天成交量激增
    df.loc[df.index[-3]:, 'volume'] *= 2.5
    df.loc[df.index[-1], 'close'] *= 1.05  # 大涨
    
    # 创建检测器
    detector = VolumePriceDetector()
    
    # 检测所有信号
    signals = detector.detect_all(df)
    
    print(f"检测到 {len(signals)} 个量价信号:")
    for sig in signals:
        print(f"  - {sig.pattern.value}: {sig.description} (评分: {sig.score})")
    
    return signals


def example_2_specific_pattern():
    """示例2: 检测特定类型的信号"""
    print("\n" + "="*60)
    print("示例2: 只检测倍量启动信号")
    print("="*60)
    
    df = generate_sample_data(40, trend='up')
    
    # 模拟倍量
    df.loc[df.index[-1], 'volume'] = df['volume'].iloc[-2] * 2.5
    df.loc[df.index[-1], 'close'] *= 1.04
    
    detector = VolumePriceDetector()
    
    # 只检测倍量启动
    signal = detector.detect(df, VolumePricePattern.DOUBLE_VOLUME)
    
    if signal:
        print(f"✓ 检测到: {signal.description}")
        print(f"  量比: {signal.volume_ratio:.2f}")
        print(f"  涨幅: {signal.price_change_pct:.2f}%")
        print(f"  评分: {signal.score}")
        print(f"  强度: {signal.strength}")
    else:
        print("✗ 未检测到倍量启动信号")
    
    return signal


def example_3_integration():
    """示例3: 与现有StockSignal系统集成"""
    print("\n" + "="*60)
    print("示例3: 转换为StockSignal格式")
    print("="*60)
    
    df = generate_sample_data(40)
    
    detector = VolumePriceDetector()
    vp_signals = detector.detect_all(df)
    
    if not vp_signals:
        print("未检测到信号，无法演示转换")
        return
    
    # 转换为StockSignal格式
    vp_signal = vp_signals[0]
    latest = df.iloc[-1]
    
    stock_signal = VolumePriceAdapter.to_stock_signal(
        vp_signal=vp_signal,
        symbol="600519.SH",
        name="贵州茅台",
        period="daily",
        close_price=latest['close'],
        change_pct=2.5,
        technicals={
            "date": str(df.index[-1]),
            "ma20": latest['close'] * 0.98,
            "volume_ratio": vp_signal.volume_ratio,
        }
    )
    
    print("转换后的StockSignal格式:")
    for key, value in stock_signal.items():
        if key == 'technicals':
            print(f"  {key}: {{...}}")
        else:
            print(f"  {key}: {value}")
    
    return stock_signal


def example_4_custom_detector():
    """示例4: 注册自定义检测器"""
    print("\n" + "="*60)
    print("示例4: 注册自定义量价检测器")
    print("="*60)
    
    detector = VolumePriceDetector()
    
    # 定义自定义检测函数
    def my_custom_detector(df: pd.DataFrame):
        """示例：检测'地量地价'信号"""
        latest = df.iloc[-1]
        recent = df.tail(20)
        
        # 成交量创20日新低
        is_low_volume = latest['volume'] == recent['volume'].min()
        # 价格也较低
        is_low_price = latest['close'] <= recent['close'].quantile(0.1)
        
        if is_low_volume and is_low_price:
            from volume_price_signals import VolumePriceSignal
            return VolumePriceSignal(
                pattern=VolumePricePattern.VOLUME_CONTRACTION,
                strength="strong",
                description="地量地价: 成交量创20日新低",
                volume_ratio=latest['volume'] / recent['volume'].mean(),
                price_change_pct=0,
                score=80,
                metadata={"custom": True}
            )
        return None
    
    # 注册到检测器（扩展系统）
    detector.register_detector(
        VolumePricePattern.VOLUME_CONTRACTION, 
        my_custom_detector
    )
    
    df = generate_sample_data(40, trend='down')
    # 模拟地量
    df.loc[df.index[-1], 'volume'] = df['volume'].tail(20).min()
    
    signals = detector.detect_all(df)
    print(f"使用自定义检测器发现 {len(signals)} 个信号")
    
    return signals


def example_5_batch_scan():
    """示例5: 批量扫描多只股票"""
    print("\n" + "="*60)
    print("示例5: 批量扫描信号")
    print("="*60)
    
    stocks = [
        ("600519.SH", "茅台", "up"),
        ("000001.SZ", "平安银行", "consolidate"),
        ("300750.SZ", "宁德时代", "up"),
    ]
    
    detector = VolumePriceDetector()
    results = []
    
    for symbol, name, trend in stocks:
        df = generate_sample_data(40, trend=trend)
        
        # 随机添加一些放量
        if np.random.random() > 0.5:
            df.loc[df.index[-1], 'volume'] *= 2.0
            df.loc[df.index[-1], 'close'] *= 1.03
        
        signals = detector.detect_all(df)
        
        if signals:
            best = max(signals, key=lambda x: x.score)
            results.append({
                "symbol": symbol,
                "name": name,
                "signal": best.pattern.value,
                "score": best.score,
                "strength": best.strength
            })
    
    print("扫描结果:")
    for r in results:
        print(f"  {r['symbol']}({r['name']}): {r['signal']} - 评分{r['score']}")
    
    return results


if __name__ == "__main__":
    # 运行所有示例
    example_1_basic_usage()
    example_2_specific_pattern()
    example_3_integration()
    example_4_custom_detector()
    example_5_batch_scan()
    
    print("\n" + "="*60)
    print("所有示例运行完成!")
    print("="*60)

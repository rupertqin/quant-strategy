# 量价信号模块 - 架构设计文档

## 设计目标

1. **模块化**：每个信号检测独立，可单独调用
2. **可扩展**：通过注册表动态添加新的检测规则
3. **优雅集成**：与现有信号系统无缝衔接
4. **类型安全**：使用 Enum 和 Dataclass 定义信号

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    StockSignalScanner                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌──────────────────────┐    ┌──────────────────────┐     │
│   │  LeftSignalDetector  │    │ RightSignalDetector  │     │
│   │  (左侧/抄底信号)      │    │  (右侧/追涨信号)      │     │
│   └──────────┬───────────┘    └──────────┬───────────┘     │
│              │                           │                  │
│              └───────────┬───────────────┘                  │
│                          │                                  │
│              ┌───────────▼───────────┐                      │
│              │ VolumePriceDetector   │                      │
│              │    量价信号检测器      │                      │
│              └───────────┬───────────┘                      │
│                          │                                  │
│        ┌─────────────────┼─────────────────┐               │
│        ▼                 ▼                 ▼               │
│   ┌─────────┐      ┌─────────┐      ┌─────────┐           │
│   │ 放量突破 │      │ 缩量整理 │      │ 量价背离 │ ...       │
│   └─────────┘      └─────────┘      └─────────┘           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. VolumePricePattern (Enum)
定义所有支持的量价模式：
- `BREAKOUT_VOLUME` - 放量突破
- `VOLUME_CONTRACTION` - 缩量整理
- `VOLUME_PRICE_DIVERGENCE` - 量价背离
- `DOUBLE_VOLUME` - 倍量启动
- `VOLUME_ACCUMULATION` - 量能堆积

### 2. VolumePriceSignal (Dataclass)
标准化的信号数据结构：
```python
@dataclass
class VolumePriceSignal:
    pattern: VolumePricePattern    # 信号类型
    strength: str                  # 强度 (strong/medium/weak)
    description: str               # 描述
    volume_ratio: float           # 量比
    price_change_pct: float       # 价格变化%
    score: int                    # 评分 0-100
    metadata: Dict                # 额外数据
```

### 3. VolumePriceDetector (核心类)
信号检测引擎：

```python
detector = VolumePriceDetector()

# 检测所有信号
signals = detector.detect_all(df)

# 检测特定类型
signal = detector.detect(df, VolumePricePattern.BREAKOUT_VOLUME)

# 注册自定义检测器
detector.register_detector(VolumePricePattern.CUSTOM, my_detector)
```

### 4. VolumePriceAdapter (适配器)
将量价信号转换为现有系统的 `StockSignal` 格式，实现无缝集成。

## 信号分类

### 左侧信号 (抄底/反转)
用于 `LeftSignalDetector`：
- **量价背离**：价格新低但量能萎缩，可能见底
- **缩量整理**：缩量横盘，蓄势待发

### 右侧信号 (追涨/确认)
用于 `RightSignalDetector`：
- **放量突破**：放量突破前高，趋势确认
- **倍量启动**：成交量翻倍，资金入场
- **量能堆积**：连续放量，主力吸筹

## 使用示例

### 基础使用
```python
from ShortTerm.daily_signal.volume_price_signals import VolumePriceDetector

detector = VolumePriceDetector()
signals = detector.detect_all(df)

for sig in signals:
    print(f"{sig.pattern.value}: {sig.description} (评分: {sig.score})")
```

### 在扫描器中集成
```python
# LeftSignalDetector 已自动集成
def detect_all(self, df, symbol, name, period):
    # ... 原有信号检测 ...
    
    # 新增量价信号
    vp_signals = self._detect_volume_price_signals(df, symbol, name, period, latest)
    signals.extend(vp_signals)
    
    return signals
```

### 自定义扩展
```python
def my_detector(df: pd.DataFrame) -> Optional[VolumePriceSignal]:
    # 自定义逻辑
    if condition_met:
        return VolumePriceSignal(
            pattern=VolumePricePattern.CUSTOM,
            strength="strong",
            description="自定义信号",
            volume_ratio=2.0,
            price_change_pct=5.0,
            score=75,
            metadata={}
        )
    return None

# 注册到检测器
detector.register_detector(VolumePricePattern.CUSTOM, my_detector)
```

## 评分体系

每个信号都有 0-100 的评分，用于信号排序和筛选：

| 评分范围 | 含义 | 操作建议 |
|---------|------|---------|
| 85-100 | 极强信号 | 高度关注 |
| 70-84  | 强信号   | 重点关注 |
| 55-69  | 中等信号 | 一般关注 |
| <55    | 弱信号   | 可忽略 |

## 与现有系统的关系

```
┌────────────────────────────────────────┐
│         原有信号系统                    │
│  - MACD/KDJ 背离                       │
│  - 均线金叉/死叉                        │
│  - 平台突破                            │
└────────────┬───────────────────────────┘
             │
             │ 集成
             ▼
┌────────────────────────────────────────┐
│      VolumePriceDetector               │
│  - 放量突破 (右侧)                      │
│  - 缩量整理 (左侧)                      │
│  - 量价背离 (左侧)                      │
│  - 倍量启动 (右侧)                      │
└────────────────────────────────────────┘
```

## 优势

1. **解耦**：量价逻辑独立于其他信号
2. **可测试**：每个检测器可独立单元测试
3. **可配置**：通过注册表动态添加/移除信号
4. **类型安全**：IDE友好，减少运行时错误
5. **文档化**：Dataclass自带文档

## 未来扩展方向

1. **组合信号**：多个量价模式组合判断
2. **机器学习**：用ML识别更复杂的量价模式
3. **多周期**：支持日线/周线/月线不同周期的量价关系
4. **板块量能**：板块级别的量价分析

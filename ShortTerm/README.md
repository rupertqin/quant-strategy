# ShortTerm - 短线事件驱动

涨停板分析模块，扫描板块热度，生成交易信号。

## 功能特性

- **涨停池获取**: 通过 AkShare 获取每日涨停板数据
- **板块热度统计**: 统计各行业涨停家数，识别热点板块
- **市场状态判断**: 基于汇率、北向资金、黄金价格判断市场环境
- **交易信号生成**: 根据热点强度生成 "关注" / "观望" 信号
- **事件回测**: 验证板块热度与次日表现的相关性

## 快速开始

```bash
cd ShortTerm
python run_scanner.py
```

## 配置 (config.yaml)

```yaml
# 策略参数
event_params:
  min_zt_count: 5            # 最小涨停家数触发热点板块
  lookback_days: 20          # 回看历史天数
  min_industry_stocks: 10    # 行业最小股票数量

# 数据缓存
cache:
  dir: "cache"
  expire_hours: 4

# 输出配置
output:
  signals_file: "../storage/outputs/shortterm/signals/daily_signals.json"
  history_file: "../storage/outputs/shortterm/history/sector_heat_history.csv"
  database_file: "../storage/outputs/shortterm/database/signals.db"

# 分析参数
analysis:
  correlation_window: 10     # 相关性计算窗口
  win_rate_threshold: 0.6    # 胜率阈值
  avg_return_threshold: 0.005 # 平均收益阈值
```

## 模块说明

| 文件 | 说明 |
|------|------|
| `run_scanner.py` | 主入口 |
| `scanner.py` | 涨停板扫描器 |
| `market_regime.py` | 市场状态判断 |
| `data_manager.py` | 缓存管理 |
| `backtest_event.py` | 事件回测分析 |

## 输出文件

输出统一保存到 `storage/outputs/shortterm/`:

| 文件 | 路径 |
|------|------|
| 每日信号 | `signals/daily_signals.json` |
| 热度历史 | `history/sector_heat_history.csv` |
| 信号数据库 | `database/signals.db` |
| 事件分析图表 | `charts/event_study_analysis.png` |
| 涨停池缓存 | `cache/zt_pool/` |

## 信号规则

| 条件 | 信号 |
|------|------|
| 涨停家数 ≥ 5 | 板块进入热点监控 |
| 强度评分 ≥ 0.5 | "关注" |
| 强度评分 < 0.5 | "观望" |

**强度评分公式:**
```
strength = 0.5 * min(涨停数/10, 1.0) + 0.3 * (龙头是否硬板) + 0.2 * (近期是否上涨)
```

## 市场状态

| 状态 | 风险评分 | 建议仓位 |
|------|----------|----------|
| AGGRESSIVE | 0-2 | 100% |
| NEUTRAL | 3-5 | 70% |
| DEFENSIVE | 6-10 | 40% |

## 板块偏好

| 市场状态 | 推荐板块 |
|----------|----------|
| AGGRESSIVE | 科技、新能源、消费、券商 |
| NEUTRAL | 中特估、高股息、半导体 |
| DEFENSIVE | 黄金、军工、医药、公用事业 |

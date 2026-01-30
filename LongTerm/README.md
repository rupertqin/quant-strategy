# LongTerm - 长线组合优化

均值-方差优化模块，计算最优资产配置权重。

## 功能特性

- **数据获取**: 通过 AkShare/baostock 获取股票/ETF 历史数据
- **收益率计算**: 自动计算日收益率，支持前复权
- **组合优化**: 基于 scipy 的均值-方差优化 (最小方差组合)
- **绩效指标**: 年化收益、年化波动、夏普比率、最大回撤
- **报告生成**: 支持 Markdown 和 HTML 格式报告

## 快速开始

```bash
cd LongTerm
python run_optimization.py
```

## 配置 (config.yaml)

```yaml
# 策略参数
risk_aversion: 0.05          # 风险厌恶系数
target_return: null          # 目标收益 (null = 最大化夏普)

constraints:
  max_weight: 0.20           # 单只股票最大权重
  min_weight: 0.02           # 单只股票最小权重
  sector_limits:             # 行业限制
    银行: 0.25
    消费: 0.25
    科技: 0.30

# 回测参数
benchmark: "000300.SH"       # 沪深300
lookback_period: 252         # 回看天数 (交易日)
rebalance_freq: "Q"          # 再平衡频率: M/月, Q/季

# 输出配置
output:
  weights_file: "../storage/outputs/longterm/weights/output_weights.csv"
  reports_dir: "../storage/outputs/longterm/reports"

# 数据源
data_source:
  stock_list:
    - "600519.SH"            # 茅台
    - "601398.SH"            # 工商银行
    - "600036.SH"            # 招商银行
    - "510300.SH"            # 沪深300ETF
```

## 模块说明

| 文件 | 说明 |
|------|------|
| `run_optimization.py` | 主入口 |
| `optimizer.py` | 组合优化器 |
| `data_updater.py` | 数据下载与更新 |
| `data_manager.py` | Parquet/SQLite 数据管理 |
| `report.py` | 报告生成器 |

## 输出文件

输出统一保存到 `storage/outputs/longterm/` 目录:

| 子目录 | 文件 | 说明 |
|--------|------|------|
| `data/` | prices.csv | 处理后的价格数据 |
| `data/` | returns.csv | 收益率数据 |
| `data/` | trend_analysis.json | 趋势分析结果 |
| `weights/` | output_weights.csv | 最优权重配置 |
| `reports/` | portfolio_report.md | 绩效报告 (Markdown) |
| `reports/` | portfolio_report.html | 绩效报告 (HTML) |
| `reports/charts/` | *.png | 图表 |

## 优化目标

默认优化目标: **最小方差组合**

```python
minimize: w' Σ w
subject to:
    Σ w = 1
    w_min ≤ w ≤ w_max
```

其中:
- w: 资产权重向量
- Σ: 协方差矩阵
- w_min/w_max: 权重上下限

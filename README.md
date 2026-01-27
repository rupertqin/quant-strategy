# Quant Strategies - 量化交易系统

模块化量化交易系统，长短策略结合：

- **长线** (Strategy_Value_LongTerm): 均值-方差优化，月度调仓
- **短线** (Strategy_Event_ShortTerm): 涨停板分析，事件驱动
- **看板** (Dashboard_Streamlit): 可视化展示

## 项目结构

```
quant-strategies/
├── README.md                      # 主说明文档
├── Strategy_Value_LongTerm/       # 长线策略 (战略配置)
├── Strategy_Event_ShortTerm/      # 短线策略 (战术扫描)
└── Dashboard_Streamlit/           # 可视化看板
```

## 核心理念

**上帝的归上帝，凯撒的归凯撒**

| 组件 | 职责 | 技术栈 |
|------|------|--------|
| 长线策略 | 均值-方差优化，计算最优资产配置 | scipy, numpy, pandas |
| 短线策略 | 涨停板分析，板块热度扫描 | akshare, pandas |
| 看板 | 整合展示，信号汇总 | streamlit, plotly |

## 快速开始

### 1. 安装依赖

```bash
# 长线库
pip install numpy pandas scipy pyyaml jinja2 matplotlib

# 短线库
pip install akshare pandas numpy seaborn matplotlib

# 看板
pip install streamlit plotly
```

### 2. 运行策略

**长线优化 (每月/季度运行)**
```bash
cd Strategy_Value_LongTerm
python run_optimization.py
```

**短线扫描 (每日收盘后运行)**
```bash
cd Strategy_Event_ShortTerm
python run_scanner.py
```

**启动看板**
```bash
cd Dashboard_Streamlit
streamlit run app.py
```

## 输出文件

| 目录 | 输出文件 | 说明 |
|------|----------|------|
| Strategy_Value_LongTerm | output_weights.csv | 最优权重配置 |
| Strategy_Value_LongTerm/reports | portfolio_report.md | 绩效报告 (Markdown) |
| Strategy_Value_LongTerm/reports | portfolio_report.html | 绩效报告 (HTML) |
| Strategy_Event_ShortTerm | daily_signals.json | 每日热点信号 |
| Dashboard_Streamlit | - | 实时看板 |

## 注意事项

- 本系统仅供学习研究，不构成投资建议
- 短线策略风险较高，请谨慎使用
- 建议先回测验证，再实盘操作

# Quant Strategy - 量化交易系统

模块化量化交易系统，长短策略结合：

- **长线** (LongTerm): 均值-方差优化，月度调仓
- **短线** (ShortTerm): 涨停板分析，事件驱动
- **看板** (Dashboard): 可视化展示
- **中台** (DataHub): 统一数据管理

## 项目结构

```
quant-strategy/
├── README.md              # 主说明文档
├── DataHub/               # 数据中台 (统一数据管理)
│   ├── core/              # 数据提供者 + 存储引擎
│   ├── models/            # 数据模型
│   ├── services/          # 统一服务接口
│   ├── scripts/           # 迁移/刷新脚本
│   └── crontab.txt        # crontab 配置
├── storage/               # 数据存储
│   ├── raw/prices/        # 价格数据 (Parquet)
│   ├── raw/zt_pool/       # 涨停池数据
│   ├── processed/returns/ # 收益率数据
│   ├── database/          # SQLite 元数据
│   ├── outputs/           # 策略输出 (统一管理)
│   │   ├── longterm/      # 长线策略输出
│   │   │   ├── weights/       # 权重配置
│   │   │   └── reports/       # 绩效报告
│   │   └── shortterm/     # 短线策略输出
│   │       ├── signals/       # 每日信号
│   │       ├── history/       # 热度历史
│   │       └── database/      # 信号数据库
│   └── logs/              # 日志
├── LongTerm/              # 长线策略 (战略配置)
├── ShortTerm/             # 短线策略 (战术扫描)
└── Dashboard/             # 可视化看板
```

## 核心理念

**上帝的归上帝，凯撒的归凯撒**

| 组件     | 职责                            | 技术栈               |
| -------- | ------------------------------- | -------------------- |
| DataHub  | 统一数据管理，akshare + baostock | akshare, baostock    |
| 长线策略 | 均值-方差优化，计算最优资产配置 | scipy, numpy, pandas |
| 短线策略 | 涨停板分析，板块热度扫描        | akshare, pandas      |
| 看板     | 整合展示，信号汇总              | streamlit, plotly    |

## DataHub 数据中台

### 存储结构

```
storage/
├── raw/prices/prices.parquet          # 价格数据
├── raw/zt_pool/zt_pool_YYYYMMDD.parquet  # 涨停池
├── processed/returns/returns.parquet  # 收益率
├── database/datahub.db                # SQLite 元数据
└── outputs/                           # 策略输出 (统一管理)
    ├── longterm/
    │   ├── weights/output_weights.csv     # 最优权重配置
    │   └── reports/
    │       ├── portfolio_report.md        # 绩效报告 (Markdown)
    │       ├── portfolio_report.html      # 绩效报告 (HTML)
    │       └── charts/*.png               # 图表
    └── shortterm/
        ├── signals/daily_signals.json     # 每日热点信号
        ├── history/sector_heat_history.csv # 热度历史
        └── database/signals.db            # 信号数据库
```

### 数据刷新

```bash
# 查看数据状态
python DataHub/scripts/refresh_data.py status

# 刷新价格数据
python DataHub/scripts/refresh_data.py prices

# 刷新涨停池
python DataHub/scripts/refresh_data.py zt_pool

# 清理旧数据
python DataHub/scripts/refresh_data.py cleanup --days 90
```

### Crontab 配置

```bash
# 安装 crontab
crontab DataHub/crontab.txt

# 每天 16:30 更新价格数据 (周一至周五)
# 每天 15:15 更新涨停池 (周一至周五)
# 每周日 17:00 清理旧数据
```

## 快速开始

### 1. 安装依赖

```bash
# 核心库
pip install numpy pandas scipy pyyaml jinja2 matplotlib pyarrow

# 数据源
pip install akshare baostock

# 看板
pip install streamlit plotly
```

### 2. 运行策略

**长线优化 (每月/季度运行)**

```bash
cd LongTerm
python run_optimization.py
```

**短线扫描 (每日收盘后运行)**

```bash
cd ShortTerm
python run_scanner.py
```

**启动看板**

```bash
cd Dashboard
streamlit run app.py
```

### 3. 数据迁移

从旧数据迁移到 DataHub：

```bash
# 预览迁移
python DataHub/scripts/migrate_data.py --dry-run

# 执行迁移
python DataHub/scripts/migrate_data.py
```

## 输出文件

| 目录                         | 输出文件                      | 说明                |
| ---------------------------- | ----------------------------- | ------------------- |
| storage/outputs/longterm/    | weights/output_weights.csv    | 最优权重配置        |
| storage/outputs/longterm/    | reports/portfolio_report.md   | 绩效报告 (Markdown) |
| storage/outputs/longterm/    | reports/portfolio_report.html | 绩效报告 (HTML)     |
| storage/outputs/longterm/    | reports/charts/*.png          | 图表                |
| storage/outputs/shortterm/   | signals/daily_signals.json    | 每日热点信号        |
| storage/outputs/shortterm/   | history/sector_heat_history.csv | 热度历史          |
| storage/outputs/shortterm/   | database/signals.db           | 信号数据库          |
| Dashboard                    | -                             | 实时看板            |

## 注意事项

- 本系统仅供学习研究，不构成投资建议
- 短线策略风险较高，请谨慎使用
- 建议先回测验证，再实盘操作

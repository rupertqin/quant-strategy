# Quant Strategy - 量化交易系统

模块化量化交易系统，长短策略结合：

- **长线** (LongTerm): 均值-方差优化，月度调仓
- **短线** (ShortTerm): 双模块 - 今日技术面 + 股票池监控
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
│   ├── raw/stocks/        # 股票数据
│   │   ├── prices/            # 价格数据 (Parquet)
│   │   └── adjust_factors/    # 复权因子
│   ├── raw/zt_pool/       # 涨停池数据
│   ├── processed/returns/ # 收益率数据
│   ├── database/          # SQLite 元数据
│   ├── outputs/           # 策略输出 (统一管理)
│   │   ├── longterm/      # 长线策略输出
│   │   │   ├── weights/       # 权重配置
│   │   │   └── reports/       # 绩效报告
│   │   └── shortterm/     # 短线策略输出
│   │       ├── services/  # 今日技术面输出
│   │       │   ├── signals/daily_signals.json
│   │       │   └── history/sector_heat_history.csv
│   │       └── pool_watch/    # 股票池监控输出
│   │           ├── pool_watch_YYYYMMDD.json
│   │           └── pool_ranking_YYYYMMDD.csv
│   └── logs/              # 日志
├── LongTerm/              # 长线策略 (战略配置)
├── ShortTerm/             # 短线策略 (战术扫描)
│   ├── services/          # 今日技术面服务模块
│   │   ├── scanner.py         # 涨停扫描
│   │   ├── market_regime.py   # 市场状态
│   │   └── backtest_event.py  # 事件回测
│   ├── pool_watch/        # 股票池监控模块
│   │   ├── monitor.py         # 监控主模块
│   │   └── analyzer.py        # 技术分析
│   └── run_today_technical.py  # 今日技术面扫描入口
└── Dashboard/             # 可视化看板
```

## 核心理念

**上帝的归上帝，凯撒的归凯撒**

| 组件     | 职责                             | 技术栈               |
| -------- | -------------------------------- | -------------------- |
| DataHub  | 统一数据管理，akshare + baostock | akshare, baostock    |
| 长线策略 | 均值-方差优化，计算最优资产配置  | scipy, numpy, pandas |
| 短线策略 | 双模块：今日技术面 + 股票池监控  | pandas, numpy        |
| 看板     | 整合展示，信号汇总               | Astro, React, Tailwind |

## ShortTerm 短线策略 (双模块)

### 模块 1: Daily Signal (今日技术面)

**功能**: 全市场涨停板扫描、板块热度分析

**指标**:

- 每日涨停家数统计
- 板块热度排名
- 市场状态判断 (汇率/北向资金/黄金)
- 事件研究回测

**运行**:

```bash
cd ShortTerm
python run_today_technical.py
```

### 模块 2: Pool Watch (股票池监控) ⭐ 新增

**功能**: 监控 LongTerm 股票池的短线技术指标

**指标**:

- **均线系统**: MA5, MA10, MA20, MA60(周线)
- **价格**: 最新价、涨跌幅、涨跌额
- **量能**: 成交量、量比(vol_ratio)
- **趋势**: 多头/空头/震荡判断
- **量价**: 放量上涨、缩量回调等信号
- **综合评分**: 0-100分量化评分

**信号生成**:
| 信号类型 | 条件 | 说明 |
|----------|------|------|
| BUY | 评分≥80 | 强烈关注 |
| WATCH | 评分≥65 或 趋势变化 | 加入观察 |
| SELL | 空头排列 或 跌破60日线 | 风险提示 |
| HOLD | 其他 | 持有观望 |

**运行**:

```bash
# 今日技术面扫描（涨停、板块热度）
python ShortTerm/run_today_technical.py

# 个股信号扫描（用于股票池和信号监控页面）
python ShortTerm/run_signal_scan.py
```

**输出文件**:

```
storage/outputs/shortterm/pool_watch/
├── pool_watch_YYYYMMDD.json      # 完整报告
└── pool_ranking_YYYYMMDD.csv     # 排名数据
```

### 模块 3: 个股信号扫描 (Stock Signal) ⭐ 新增

**功能**: 全市场个股技术面信号扫描，生成左侧（抄底/反转）和右侧（追涨/确认）信号

**左侧信号**（抄底/反转）:

- MACD底背离、KDJ底背离
- 超跌反弹、缩量十字星、长下影线

**右侧信号**（追涨/确认）:

- MA5/MA10/MA20金叉、MACD金叉、KDJ金叉
- 量价突破、均线多头排列、突破平台

**多周期分析**（日线/周线/月线共振）:

- 日线信号：基础信号（默认）
- 周线信号：信号名称前缀"周线"，评分+10
- 月线信号：信号名称前缀"月线"，评分+15
- 多周期共振：同类型信号在多个周期同时出现，额外加分

**📏 均线走平指标**（新增）:
检测单根均线（MA10/MA20/MA60）是否趋于水平直线。均线走平意味着价格长期围绕某个中枢稳定波动，这个"走平"的价格位置就是强支撑/阻力位。

显示格式：`MAxx@价格`

- 🔴红色：强烈走平（分数≥0.90）- 长期稳定中枢
- 🟡黄色：较走平（分数≥0.80）- 中期稳定区间
- 🔵蓝色：走平（分数≥0.75）- 短期参考位

**📊 均线粘合指标**（新增）:
检测多根均线是否纠缠在一起。均线粘合意味着市场在选择方向，一旦突破往往有大行情。

| 标记   | 含义     | 分数  |
| ------ | -------- | ----- |
| 🔴MAxx | 强烈粘合 | ≥0.90 |
| 🟡MAxx | 较平缓   | ≥0.80 |
| 🟢MAxx | 平缓     | ≥0.70 |

**运行**:

```bash
# 扫描全部信号（全量扫描，默认多周期）
python ShortTerm/run_signal_scan.py

# 只扫描左侧信号
python ShortTerm/run_signal_scan.py --left

# 只扫描右侧信号
python ShortTerm/run_signal_scan.py --right

# 扫描单只股票
python ShortTerm/run_signal_scan.py --symbol 600519.SH

# 限制扫描数量（测试用）
python ShortTerm/run_signal_scan.py --limit 100

# 仅使用日线（禁用多周期，扫描更快）
python ShortTerm/run_signal_scan.py --no-multi-period
```

**输出文件**:

```
DataStorage/outputs/shortterm/signals/
├── signal_latest.json      # 最新全部信号
└── signal_YYYYMMDD.json    # 历史归档
```

**构建看板**:

```bash
cd Dashboard
npm install
npm run build
```

## DataHub 数据中台

### 存储结构

```
storage/
├── raw/                                    # 原始数据 (DataHub)
│   ├── prices/prices.parquet              # 价格数据
│   └── zt_pool/zt_pool_YYYYMMDD.parquet   # 涨停池
├── database/
│   └── datahub.db                         # SQLite 元数据
└── outputs/                                # 策略输出 (统一管理)
    ├── longterm/
    │   ├── data/                          # 处理数据
    │   │   ├── prices.csv                # 价格数据
    │   │   ├── returns.csv               # 收益率数据
    │   │   └── trend_analysis.json       # 趋势分析
    │   ├── weights/                       # 权重输出
    │   │   └── output_weights.csv
    │   └── reports/                       # 绩效报告
    │       ├── portfolio_report.md
    │       ├── portfolio_report.html
    │       └── charts/*.png
    └── shortterm/
        ├── services/                      # 今日技术面输出
        │   ├── signals/daily_signals.json
        │   └── history/sector_heat_history.csv
        └── pool_watch/                    # 股票池监控输出 ⭐
            ├── pool_watch_YYYYMMDD.json
            └── pool_ranking_YYYYMMDD.csv
```

### 股票基本信息数据库

从 baostock/akshare 获取所有A股公司基本信息，保存为CSV。用于Dashboard显示股票中文名称。

**构建数据库（不定期执行）**：

```bash
# 直接运行模块
python -m DataHub.run_build_stock_db

# 强制重新构建（忽略缓存）
python -m DataHub.run_build_stock_db --force
```

**查询数据库**：

```bash
# 查看数据库信息
python -m DataHub.run_build_stock_db --info

# 搜索股票
python -m DataHub.run_build_stock_db --search 茅台
python -m DataHub.run_build_stock_db --search 600519
```

**在代码中使用**：

```python
from lib.utils import get_stock_name

# 获取股票名称
name = get_stock_name('600519.SH')  # 返回 '贵州茅台'

# 格式化显示（代码+名称）
from lib.utils import format_stock
display = format_stock('600519.SH')  # 返回 '600519.SH(贵州茅台)'
```

**数据库文件**：`storage/stock_basic_info.csv`

- 数据来源：baostock（优先）/ akshare（备用）
- 股票数量：约5000+只A股
- 更新频率：建议季度/半年更新一次
- 日常任务不需要执行此命令

**数据字段**：
| 字段 | 说明 | 示例 |
|------|------|------|
| `symbol` | 股票代码 | 600519.SH |
| `name` | 股票名称 | 贵州茅台 |
| `exchange` | 交易所 | SH/SZ |
| `industry` | 所属行业 | C15酒、饮料和精制茶制造业 |
| `industry_classification` | 行业分类标准 | 证监会行业分类 |
| `ipo_date` | 上市日期 | 2001-08-27 |
| `out_date` | 退市日期 | （空表示未退市） |
| `status` | 上市状态 | 1=上市 |
| `security_type` | 证券类型 | 1=股票 |
| `industry_update_date` | 行业更新日期 | 2026-03-30 |
| `update_time` | 数据更新时间 | 2026-04-02 17:35:32 |
| `data_source` | 数据来源 | baostock |

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
# DataHub (必须先安装)
pip install -r DataHub/requirements.txt

# 长线策略
pip install -r LongTerm/requirements.txt

# 短线策略
pip install -r ShortTerm/requirements.txt

# 看板 (Astro 静态站点)
cd Dashboard && npm install
```

### 2. 运行策略

**长线优化 (每月/季度运行)**

```bash
cd LongTerm
python run_optimization.py
```

**短线策略 (每日收盘后运行)**

```bash
cd ShortTerm

# 今日技术面扫描
python ShortTerm/run_today_technical.py daily

# 个股信号扫描
python ShortTerm/run_signal_scan.py
```

**构建看板**

```bash
cd Dashboard
npm run build
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

| 目录                                     | 输出文件                        | 说明                |
| ---------------------------------------- | ------------------------------- | ------------------- |
| storage/outputs/longterm/                | weights/output_weights.csv      | 最优权重配置        |
| storage/outputs/longterm/                | reports/portfolio_report.md     | 绩效报告 (Markdown) |
| storage/outputs/longterm/                | reports/portfolio_report.html   | 绩效报告 (HTML)     |
| storage/outputs/longterm/                | reports/charts/\*.png           | 图表                |
| storage/outputs/shortterm/services/      | signals/daily_signals.json      | 每日热点信号        |
| storage/outputs/shortterm/services/      | history/sector_heat_history.csv | 热度历史            |
| storage/outputs/shortterm/pool_watch/ ⭐ | pool_watch_YYYYMMDD.json        | 股票池监控报告      |
| storage/outputs/shortterm/pool_watch/ ⭐ | pool_ranking_YYYYMMDD.csv       | 股票池排名          |
| Dashboard                                | -                               | 实时看板            |

## 版本历史

### v2.0 (2026-04-02)

- **重构**: ShortTerm 拆分为双模块
  - `services`: 原涨停扫描功能
  - `pool_watch`: 新增股票池短线监控
- **新增**: PoolWatch 模块
  - 监控 LongTerm 股票池的 MA5/10/20/60
  - 量价关系分析
  - 综合评分系统 (0-100)
  - 自动生成 BUY/SELL/WATCH 信号

## 注意事项

- 本系统仅供学习研究，不构成投资建议
- 短线策略风险较高，请谨慎使用
- 建议先回测验证，再实盘操作

### 常用命令

```
# 实时数据服务与
python DataHub/services/realtime_service.py

# 每日数据入库
python -m DataHub.services.sync --today && python -m DataHub.services.sync --daily --symbol etf --workers 1 && python -m DataHub.services.sync --daily --symbol index --workers 1

#短线信号扫描
python ShortTerm/run_signal_scan.py && python ShortTerm/run_today_technical.py
```

## TODO

- [ ] 每周日 17:00 再次全量拉取数据 以纠偏
- [ ] 指数、ETF配置化，且进入代码库
- [ ] 信号详情带上价格和百分比和日期，不然我怕弄错了时间。
- [ ] 代码编辑器、UI 做回测工具

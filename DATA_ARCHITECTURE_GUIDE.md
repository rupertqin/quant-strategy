# DataHub 数据架构快速指南

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│  应用层 (Application)                                    │
│  Dashboard / Scanner / Strategy                         │
└────────────────────┬────────────────────────────────────┘
                     │ 调用
                     ▼
┌─────────────────────────────────────────────────────────┐
│  数据服务层 (Data Service)                               │
│  DataSyncService / Repository                           │
└────────────────────┬────────────────────────────────────┘
                     │ 读写
                     ▼
┌─────────────────────────────────────────────────────────┐
│  存储层 (Storage)                                        │
│  SQLite / Parquet / CSV                                 │
└─────────────────────────────────────────────────────────┘
```

## 目录结构

```
DataHub/
├── database/
│   └── init_db.py              # 数据库初始化脚本
├── crawlers/
│   ├── __init__.py
│   ├── base_crawler.py         # 爬虫基类
│   ├── stock_price_crawler.py  # 股票价格爬虫
│   └── etf_crawler.py          # ETF爬虫
├── repositories/
│   ├── __init__.py
│   ├── base_repository.py      # 仓库基类
│   └── stock_repository.py     # 股票数据仓库
├── processors/
│   └── __init__.py             # 数据处理（指标、指数计算）
├── core/
│   └── index_calculator.py     # 指数计算器
└── services/
    └── sync_service.py         # 数据同步服务
```

## 快速开始

### 1. 初始化数据库

```bash
python DataHub/database/init_db.py
```

这会：
- 创建 SQLite 数据库: `storage/database/quant.db`
- 创建所有必要的表
- 导入股票和 ETF 基础信息

### 2. 同步数据

```bash
# 同步全部数据
python DataHub/services/sync_service.py --all

# 只同步股票最新数据
python DataHub/services/sync_service.py --latest

# 只同步ETF数据
python DataHub/services/sync_service.py --etfs

# 查看同步状态
python DataHub/services/sync_service.py --status
```

### 3. 在代码中使用

```python
from DataHub.repositories import StockRepository
from DataHub.processors import IndexCalculator

# 初始化仓库
repo = StockRepository()

# 获取股票列表
stocks = repo.get_all_stocks()

# 获取某只股票历史价格
prices = repo.get_daily_price('600519.SH', start_date='2024-01-01')

# 计算指数
calc = IndexCalculator()
index_value = calc.calculate_index_value(prices, '自定义指数')
```

## 核心规则

### 规则1: 数据获取必须通过仓库

❌ **禁止** - 直接调用接口:
```python
import akshare as ak
df = ak.stock_zh_a_hist(symbol='600519')  # 禁止！
```

✅ **正确** - 从数据库读取:
```python
from DataHub.repositories import StockRepository
repo = StockRepository()
df = repo.get_daily_price('600519.SH')  # 正确
```

### 规则2: 原始数据必须持久化

爬虫获取的数据必须保存到数据库:
```python
from DataHub.crawlers import StockPriceCrawler

crawler = StockPriceCrawler(repository=repo)
result = crawler.sync(symbols=['600519.SH'])
# 自动保存到数据库
```

### 规则3: 衍生数据实时计算

指标和指数基于原始数据实时计算:
```python
from DataHub.processors import IndexCalculator

prices = repo.get_multiple_prices(symbols)
index_value = calc.calculate_index_value(prices, '创业板')
```

## 数据库表结构

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| stock_basic | 股票基础信息 | symbol, name, exchange, industry |
| stock_daily_price | 股票日线价格 | symbol, trade_date, open, high, low, close |
| etf_basic | ETF基础信息 | symbol, name, exchange |
| etf_daily_price | ETF日线价格 | symbol, trade_date, close, nav |
| stock_fundamental | 基本面数据 | symbol, report_date, eps, roe, pe_ttm |
| index_daily_value | 计算后的指数 | index_name, trade_date, close |
| data_update_log | 同步日志 | data_type, status, records_count |

## 分层职责

### Crawlers (数据爬取层)
- 职责: 从外部API获取原始数据
- 禁止: 不处理业务逻辑
- 输出: 清洗后的 DataFrame

### Repositories (数据持久层)
- 职责: 封装所有数据库操作
- 提供: CRUD接口、查询方法
- 屏蔽: 底层SQL细节

### Processors (数据处理层)
- 职责: 计算衍生数据
- 输入: 原始数据 (来自 Repository)
- 输出: 指标、指数等计算结果

### Services (业务服务层)
- 职责: 协调各层完成业务流程
- 例如: 数据同步调度、批量更新

## 开发新功能流程

1. **添加新的数据类型**
   - 在 `init_db.py` 中添加表结构
   - 创建对应的 Crawler
   - 创建 Repository 方法

2. **添加新的计算逻辑**
   - 在 `processors/` 中添加计算类
   - 从 Repository 读取原始数据
   - 返回计算结果（不保存，除非必要）

3. **在业务中使用**
   - 通过 Repository 获取数据
   - 通过 Processors 计算指标
   - 整合到业务逻辑中

## 命令行工具

### 数据库初始化

```bash
# 初始化数据库（首次运行必须执行）
python DataHub/database/init_db.py
```

### 实时数据同步（SQLite）

```bash
# 同步全部数据（股票+ETF）
python DataHub/services/sync_service.py --all

# 只同步股票最新数据（增量）
python DataHub/services/sync_service.py --latest

# 只同步ETF数据
python DataHub/services/sync_service.py --etfs

# 查看同步状态
python DataHub/services/sync_service.py --status
```

### 历史数据同步（Parquet）

```bash
# ========== 每日增量更新（推荐日常使用）==========
# 同步所有股票，只获取最新数据（从已有数据的最新日期开始）
python DataHub/services/history_sync.py --daily

# 每日增量更新（测试模式，只同步前10只）
python DataHub/services/history_sync.py --daily --limit 10


# ========== 全量更新（首次同步或数据损坏）==========
# 单只股票全量同步（从上市日期开始）
python DataHub/services/history_sync.py --symbol 600519.SH --full

# 所有股票全量同步（耗时较长）
python DataHub/services/history_sync.py --all --full


# ========== 指定日期范围（补数据）==========
# 单只股票指定日期范围
python DataHub/services/history_sync.py --symbol 600519.SH --start-date 20240101 --end-date 20240331

# 所有股票指定日期范围
python DataHub/services/history_sync.py --all --start-date 20240101 --end-date 20240331


# ========== 查看同步状态 ==========
# 查看已同步的股票列表和数据摘要
python DataHub/services/history_sync.py --summary
```

### 定时任务配置

**每日自动更新（添加到 crontab）：**

```bash
# 编辑 crontab
crontab -e

# 添加以下行（每个交易日 15:30 执行增量更新）
30 15 * * 1-5 cd /Users/rupert/code/quant-strategy && python DataHub/services/history_sync.py --daily >> logs/daily_sync.log 2>&1
```

### 演示脚本

```bash
# 数据流演示
python examples/data_flow_demo.py

# 指数计算演示
python examples/index_calculation_example.py
```

## 注意事项

1. **数据库位置**: `storage/database/quant.db`
2. **Parquet 位置**: `storage/raw/prices/*.parquet`（每只股票一个文件）
3. **首次运行**: 必须先执行 `init_db.py` 初始化数据库
4. **历史数据**: 首次使用需执行 `history_sync.py --all --full` 同步全量历史数据
5. **日常更新**: 使用 `history_sync.py --daily` 进行每日增量更新
6. **离线可用**: 数据一旦入库，无需网络即可查询
7. **备份建议**: 定期备份 `quant.db` 和 `storage/raw/prices/` 目录

## 数据同步工作流

### 首次部署

```bash
# 1. 初始化数据库
python DataHub/database/init_db.py

# 2. 同步历史数据（全量，耗时较长，建议分批或后台运行）
python DataHub/services/history_sync.py --all --full

# 3. 验证数据
python DataHub/services/history_sync.py --summary
```

### 日常维护

```bash
# 每个交易日收盘后执行（15:30后）
python DataHub/services/history_sync.py --daily

# 查看同步状态
python DataHub/services/history_sync.py --summary
```

### 补数据场景

```bash
# 场景1: 某只股票数据缺失
python DataHub/services/history_sync.py --symbol 600519.SH --full

# 场景2: 某段时间数据缺失（如2024年1月）
python DataHub/services/history_sync.py --all --start-date 20240101 --end-date 20240131 --full

# 场景3: 新上市股票
python DataHub/services/history_sync.py --symbol 688XXX.SH
```

## 下一步

- 运行完整数据同步填充历史数据
- 将现有代码迁移到新的 Repository 接口
- 实现更多技术指标计算
- 添加数据质量检查和修复功能

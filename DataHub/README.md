# DataHub

DataHub 是量化交易系统的数据基础设施层，负责数据的采集、存储、计算与分发。

## 目录结构

```
DataHub/
├── core/                          # 核心计算模块
│   ├── data_client.py             # 数据客户端（akshare/baostock 封装）
│   ├── data_provider.py           # 数据提供者接口
│   ├── data_reader.py             # 数据读取器（Parquet/SQLite）
│   ├── index_calculator.py        # 指数计算器（基于成分股等权法）
│   └── storage_engine.py          # 存储引擎
├── database/
│   └── init_db.py                 # 数据库初始化脚本
├── models/
│   ├── price.py                   # 价格数据模型
│   └── zt_pool.py                 # 涨停池模型
├── repositories/
│   ├── base_repository.py         # 仓库基类
│   └── stock_repository.py        # 股票数据仓库
├── scripts/
│   ├── asset_manager.py           # 资产管理脚本
│   ├── download_etf.py            # ETF 下载脚本
│   ├── save_official_index_basic.py # 官方指数基础信息保存
│   └── stock_list_manager.py      # 股票列表管理
├── services/
│   ├── realtime_service.py        # 实时数据服务
│   ├── sync_service.py            # 同步服务
│   └── sync/                      # 数据同步服务
│       ├── base.py                # 同步基类
│       ├── stock_sync.py          # 股票价格同步
│       ├── factor_sync.py         # 复权因子同步
│       ├── sync_manager.py        # 统一调度管理器
│       └── cli.py                 # 命令行接口
└── tests/                         # 单元测试
    ├── test_data_reader.py
    ├── test_index_calculator.py
    └── services/
        └── sync/                  # 镜像 services/sync/ 结构
            ├── test_base.py
            ├── test_cli.py
            ├── test_stock_sync.py
            ├── test_etf_sync.py
            ├── test_index_sync.py
            ├── test_factor_sync.py
            └── test_sync_manager.py
```

## 快速开始

### 1. 初始化数据库

```bash
python DataHub/database/init_db.py
```

这会创建 SQLite 数据库 `storage/database/quant.db`，并建立所有必要的表结构（股票基础信息、ETF 基础信息、日线价格、分钟线、基本面等）。

### 2. 同步数据

**收盘后快速同步当天数据（推荐日常使用）：**
```bash
python -m DataHub.services.sync --today
```

**每日增量更新：**
```bash
# 同步全部股票
python -m DataHub.services.sync --daily

# 同步全部 ETF
python -m DataHub.services.sync --daily --symbol etf

# 同步全部指数
python -m DataHub.services.sync --daily --symbol index

# 指定日期补数据
python -m DataHub.services.sync --daily 2026-04-13

# 指定日期范围
python -m DataHub.services.sync --daily 2026-04-13~2026-04-14

# 指定单只或多只股票
python -m DataHub.services.sync --daily --symbol 600519
python -m DataHub.services.sync --daily --symbol 600519,300750,000858
```

**首次全量同步（断点续传）：**
```bash
python -m DataHub.services.sync --all --skip-existing
```

**同步复权因子：**
```bash
python -m DataHub.services.sync --sync-factors
```

**查看同步状态：**
```bash
python -m DataHub.services.sync --summary
```

### 3. 新模块化命令（推荐）

```bash
# 同步当天数据（默认不同步复权因子）
python -m DataHub.services.sync --today

# 同步当天数据，同时同步复权因子
python -m DataHub.services.sync --today --sync-factors

# 只同步复权因子（全量覆盖）
python -m DataHub.services.sync --sync-factors

# 同步指定股票
python -m DataHub.services.sync --symbol 600519.SH

# 使用多线程
python -m DataHub.services.sync --today --workers 3
```

### 4. 定时任务（crontab）

```bash
# 每个交易日 15:30 执行数据同步
30 15 * * 1-5 cd /Users/rupert/code/quant-strategy && python DataHub/services/history_sync.py --daily >> logs/daily_sync.log 2>&1
```

## 单元测试

测试文件镜像 `DataHub/services/sync/` 的模块结构，统一放在 `DataHub/tests/` 下。

### 运行全部测试

```bash
# 从项目根目录运行
cd /Users/rupert/code/quant-strategy
python -m unittest discover -s DataHub/tests
```

### 运行单个模块

```bash
# 使用模块路径（Pythonic，推荐）
python -m unittest DataHub.tests.services.sync.test_base
python -m unittest DataHub.tests.services.sync.test_stock_sync
python -m unittest DataHub.tests.services.sync.test_factor_sync
python -m unittest DataHub.tests.services.sync.test_sync_manager
```

### 运行单个测试类或方法

```bash
python -m unittest DataHub.tests.services.sync.test_base.TestBaseSyncService
python -m unittest DataHub.tests.services.sync.test_base.TestBaseSyncService.test_init
```

### 运行顶层测试（非 sync 模块）

```bash
python -m unittest DataHub.tests.test_data_reader
python -m unittest DataHub.tests.test_index_calculator
```

## 架构原则

1. **前后端分离的数据流**：Dashboard 只读取预生成的 JSON/Parquet，不直接调接口；数据获取在后台脚本完成
2. **数据源优先级**：baostock > akshare > 东财接口
3. **原始数据只读**：`storage/raw/prices/` 下的原始价格数据永不修改，复权在使用时实时计算
4. **双轨存储**：
   - 实时/当日数据 → SQLite
   - 历史/回测数据 → DuckDB + Parquet
5. **指数计算基于成分股**：不直接调用指数接口，从个股数据等权计算指数点位
6. **禁止自动重试**：爬虫失败一次即跳过，避免对数据源造成压力
7. **默认不限量**：扫描/同步类命令默认处理全部数据，`--limit` 仅用于测试

## 常用命令速查

| 命令 | 说明 |
|------|------|
| `python DataHub/database/init_db.py` | 初始化数据库 |
| `python -m DataHub.services.sync --today` | 收盘后快速同步 |
| `python -m DataHub.services.sync --daily` | 每日增量更新 |
| `python -m DataHub.services.sync --all --skip-existing` | 首次全量同步（断点续传） |
| `python -m DataHub.services.sync --sync-factors` | 同步复权因子 |
| `python -m DataHub.services.sync --summary` | 查看同步状态 |
| `python -m unittest discover -s DataHub/tests` | 运行全部单元测试 |

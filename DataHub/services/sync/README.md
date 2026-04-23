# 数据同步服务（重构版）

## 设计原则

1. **模块化**：每个同步类型独立模块
2. **模板方法**：基类定义通用流程，子类实现具体逻辑
3. **简化复权因子**：全量覆盖，无需区分增量/全量

## 目录结构

```
DataHub/services/sync/
├── __init__.py          # 包导出
├── __main__.py          # 模块入口
├── base.py              # 同步基类（模板方法模式）
├── stock_sync.py        # 股票价格同步
├── factor_sync.py       # 复权因子同步（简化版全量覆盖）
├── sync_manager.py      # 统一调度管理器
├── cli.py               # 命令行接口
└── README.md            # 本文档
```

## 使用方式

### 命令行

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

### Python API

```python
from DataHub.services.sync import SyncManager

# 创建管理器
manager = SyncManager(max_workers=3)

# 同步当天数据
result = manager.sync_today_data(skip_factors=False)

# 只同步复权因子
result = manager.sync_factors_only()

# 同步指定股票
result = manager.sync_stock_list(['600519.SH', '300750.SZ'])

# 清理资源
manager.close()
```

## 关键设计

### 复权因子同步（factor_sync.py）

**简化原则：**
- 每次全量获取（从1990到今天）
- 直接覆盖本地文件（无需合并）
- 过滤 factor=1.0 的记录（节省空间）
- 从未分红的股票不创建文件

**优势：**
- 代码简单，不易出错
- 无数据缺失风险
- 复权因子数据量小，全量获取无性能问题

### 股票价格同步（stock_sync.py）

**保持原有功能：**
- 支持增量/全量同步
- 支持数据源选择（baostock/akshare）
- 支持并发同步
- 自动合并新旧数据

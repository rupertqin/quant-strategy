---
description: 
alwaysApply: true
enabled: true
updatedAt: 2026-04-03T12:42:18.466Z
provider: 
---

# 量化交易系统 - 数据架构规则

## 核心原则：前后端分离的数据流

## 数据源优先级规则

**核心原则：baostock > akshare > 东财接口**

### 1. 接口优先级定义

| 优先级 | 数据源 | 适用场景 | 说明 |
|-------|--------|---------|------|
| **1** | baostock | 历史数据、复权因子 | 稳定、支持前复权计算 |
| **2** | akshare | 实时数据、备用方案 | 功能丰富、接口多样 |
| **3** | 东财接口 | 最后备选 | 可能有频率限制 |

### 2. 实现方式

```python
# 正确：按优先级尝试
try:
    # 1. 优先使用 baostock
    df = self._fetch_from_baostock()
except Exception as e1:
    logger.warning(f"baostock 失败: {e1}")
    try:
        # 2. 尝试 akshare
        df = ak.stock_zh_a_daily(...)
    except Exception as e2:
        logger.warning(f"akshare 失败: {e2}")
        # 3. 最后尝试东财
        df = ak.stock_zh_a_spot_em()
```

### 3. 禁止行为

❌ **严禁以下操作**：
```python
# 禁止：优先使用东财接口
df = ak.stock_zh_a_spot_em()  # 错误！东财应该是最后备选

# 禁止：只使用单一数据源而不提供降级方案
try:
    df = ak.some_interface()  # 错误！没有备选方案
except:
    raise  # 直接抛出，没有降级
```

✅ **正确做法**：
```python
# 正确：按优先级尝试多个数据源
def fetch_data_with_fallback():
    errors = []
    
    # 优先级 1: baostock
    try:
        return fetch_from_baostock()
    except Exception as e:
        errors.append(f"baostock: {e}")
    
    # 优先级 2: akshare
    try:
        return fetch_from_akshare()
    except Exception as e:
        errors.append(f"akshare: {e}")
    
    # 优先级 3: 东财
    try:
        return fetch_from_em()
    except Exception as e:
        errors.append(f"em: {e}")
    
    # 全部失败
    raise RuntimeError(f"所有数据源都失败: {errors}")
```

---

## 核心原则：前后端分离的数据流

### 1. Dashboard 页面（前端展示层）
- **只读操作**：Dashboard 页面只允许从 JSON/CSV 文件读取数据
- **禁止实时获取**：严禁在 Dashboard 页面直接调用 akshare、baostock 等数据接口
- **数据格式**：统一从 `storage/outputs/` 目录下的 JSON 文件获取数据

### 2. 后台脚本（数据生产层）
- **定时任务**：所有数据获取任务必须在后台脚本中执行
- **数据生成**：脚本负责调用数据接口、处理数据、生成 JSON 文件
- **存储位置**：生成的数据文件统一存放到 `storage/outputs/` 目录

### 3. 数据流示意图
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   数据源接口     │────▶│   后台脚本       │────▶│  storage/       │
│  (akshare等)    │     │  (Scanner等)    │     │  outputs/*.json │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   用户浏览器     │◀────│   Dashboard     │◀────│  预生成数据文件  │
│                 │     │   (Streamlit)   │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### 4. 禁止行为
❌ **严禁以下操作**：
```python
# 禁止：在 Dashboard 页面直接获取实时数据
@st.cache_data
def get_realtime_data():
    import akshare as ak
    return ak.stock_zh_a_spot_em()  # 禁止！

# 禁止：页面启动时调用外部API
if st.button("刷新"):
    data = fetch_from_api()  # 禁止！
```

✅ **正确做法**：
```python
# 正确：只读取预生成的 JSON 文件
def load_data():
    with open('storage/outputs/xxx.json', 'r') as f:
        return json.load(f)
```

### 5. 后台脚本规范
- 所有数据获取逻辑写在 `ShortTerm/`、`LongTerm/`、`DataHub/` 等模块
- 脚本输出使用 `logging` 而不是 `print`（除 scanner 主脚本外）
- 生成时间戳格式：`datetime.now().strftime('%Y%m%d_%H%M%S')`

### 6. 新增数据字段流程
当 Dashboard 需要新数据时：
1. **先改后台脚本**：在 `scanner.py` 或对应模块中添加数据获取逻辑
2. **生成JSON**：运行后台脚本，确保新字段写入 JSON
3. **再改Dashboard**：修改页面代码，从 JSON 读取新字段
4. **禁止反向操作**：永远不要先改页面再改脚本

### 7. 股票代码与名称映射规则
**核心原则：JSON 只存代码，显示时才映射名称**

❌ **禁止**（后端脚本中）：
```python
# 不要在 JSON 中存储股票名称
detail = {
    'code': '300001',
    'name': '特锐德',  # 禁止！会增加 JSON 体积
}
```

✅ **正确做法**：
```python
# 后端：JSON 只保存代码
detail = {
    'code': '300001',  # 仅代码
    'stocks': ['300001', '300002']  # 代码列表
}

# 前端：Dashboard 页面按需映射
@lru_cache(maxsize=1)
def get_stock_name_mapper() -> dict:
    """获取代码到名称的映射表"""
    df = ak.stock_zh_a_spot_em()
    return dict(zip(df['代码'], df['名称']))

def get_stock_name(code: str) -> str:
    """代码转名称，按需调用"""
    mapper = get_stock_name_mapper()
    return mapper.get(code, '')

# 显示时使用
st.write(f"{code}({get_stock_name(code)})")
```

**理由**：
- 减少 JSON 文件体积
- 名称可能变化，代码是唯一标识
- 映射逻辑集中管理，便于维护

---

### 8. 指数计算规则（重构原则）

**核心原则：基于个股数据计算指数，而非直接调用指数接口**

#### 8.1 背景与动机
传统方式直接调用指数接口（如 `ak.index_zh_a_hist()`）存在以下问题：
- **数据源不一致**：指数数据与个股数据可能来自不同源，时间戳不一致
- **接口限制**：部分指数接口不稳定或频次受限
- **无法自定义**：无法计算"我的股票池指数"等自定义指数
- **离线不可用**：没有网络时无法获取指数数据

#### 8.2 新方案：基于成分股计算
从最根本的个股数据出发，通过成分股计算指数表现：

| 指数名称 | 代码前缀规则 | 交易所 |
|---------|-------------|--------|
| 创业板 | 300、301 开头 | SZ |
| 科创板 | 688、689 开头 | SH |
| 上证指数 | 600、601、603、605 开头 | SH |
| 深证成指 | 000、001、002、003 开头 | SZ |
| 沪深300 | 主板大市值股票（简化版） | SH+SZ |
| 中证1000 | 小市值股票（排除沪深300） | SH+SZ |
| 北交所 | 43、8、82、83、87、88 开头 | BJ |

#### 8.3 数据流示意

```
旧流程（不推荐）：
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   akshare   │────▶│ 指数接口     │────▶│  分析模块    │
│             │     │ (如000300)  │     │             │
└─────────────┘     └─────────────┘     └─────────────┘

新流程（推荐）：
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   akshare   │────▶│  全市场个股  │────▶│ IndexCalculator │────▶│  分析模块    │
│             │     │  价格数据    │     │  (成分股计算)   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │  指数点位    │
                                        │  涨跌幅      │
                                        │  技术指标    │
                                        └─────────────┘
```

#### 8.4 实现方式

**使用 IndexCalculator 类**（位于 `DataHub/core/index_calculator.py`）：

```python
from DataHub.core.index_calculator import IndexCalculator, get_index_summary

# 1. 获取全市场个股价格数据（日线）
price_df = load_all_stock_prices()  # DataFrame: index=date, columns=symbols

# 2. 计算所有指数
calc = IndexCalculator()
indices = calc.calculate_all_indices(price_df)

# 3. 获取指数表现摘要
summary = calc.get_index_performance_summary(price_df)
# 返回: {
#     '创业板': {
#         'current_value': 2150.50,
#         'change_pct': 1.25,
#         'ma5': 2140.30,
#         'trend': 'UP'
#     }
# }
```

#### 8.5 计算方法

**等权法**（默认）：
```python
# 1. 计算每只成分股的日收益率
returns = prices.pct_change()

# 2. 取平均收益率（等权）
avg_return = returns.mean(axis=1)

# 3. 累积计算指数点位
index_value = (1 + avg_return).cumprod() * 1000  # 基准1000点
```

**优势**：
- 简单直观，不需要市值数据
- 避免大市值股票过度影响指数
- 计算速度快

#### 8.6 使用场景

**场景1: 替代指数接口**
```python
# ❌ 旧方式 - 直接调用接口
import akshare as ak
df = ak.index_zh_a_hist(symbol="000300", period="daily")

# ✅ 新方式 - 从个股计算
from DataHub.core.index_calculator import IndexCalculator
calc = IndexCalculator()
hs300 = calc.calculate_index_value(price_df, '沪深300')
```

**场景2: 计算自定义指数**
```python
# 我的股票池
my_stocks = ['600519.SH', '300750.SZ', '000858.SZ']
my_pool_prices = price_df[my_stocks]

# 计算"我的指数"
my_index = calc.calculate_index_value(my_pool_prices, '我的股票池')
```

**场景3: 离线分析**
```python
# 加载本地历史数据（无需网络）
price_df = pd.read_parquet('storage/raw/prices/history.parquet')

# 完全离线计算
indices = calc.calculate_all_indices(price_df)
```

#### 8.7 集成步骤

1. **建立个股数据存储**：每日收盘后保存全市场个股价格
   - 存储位置：`storage/raw/prices/YYYYMMDD.parquet`
   
2. **替换 MarketRegime**：
   - 删除 `_get_index_history()` 的接口调用
   - 改用 `IndexCalculator.calculate_index_value()`

3. **数据验证**：
   - 对比计算的指数与官方指数，确保误差在可接受范围（通常<0.5%）
   - 等权法与市值加权有差异，但在趋势判断上一致

#### 8.8 注意事项

- **数据完整性**：确保成分股价格数据完整，缺失数据会影响计算精度
- **新股处理**：新上市股票历史数据不足时，从上市首日开始参与计算
- **停牌处理**：停牌股票使用最后有效价格，或从前一交易日 carry forward

**下一步行动**：
在 `MarketRegime` 类中逐步实现指数计算替换，先并行运行新旧两种方案，验证一致性后完全切换到新方案。

---

### 9. 数据持久化与数据库架构规则

**核心原则：所有原始数据必须持久化到数据库，数据获取必须从数据库读取**

#### 9.1 架构分层

```
┌─────────────────────────────────────────────────────────────────┐
│                        应用层 (Application)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Dashboard  │  │   Scanner    │  │  Strategy    │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
└─────────┼─────────────────┼─────────────────┼──────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        数据服务层 (Data Service)                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              DataService / Repository Pattern           │    │
│  │  - 统一数据访问接口                                      │    │
│  │  - 屏蔽底层存储细节                                      │    │
│  └────────────────────┬────────────────────────────────────┘    │
└───────────────────────┼─────────────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
┌──────────────┐ ┌──────────┐ ┌──────────────┐
│  关系数据库   │ │  时序DB  │ │   文件存储   │
│  (SQLite/    │ │ (可选)   │ │  (CSV/JSON)  │
│   PostgreSQL)│ │          │ │              │
└──────────────┘ └──────────┘ └──────────────┘
```

#### 9.2 数据分类与存储策略

| 数据类型 | 存储方式 | 说明 |
|---------|---------|------|
| **原始数据** | SQLite/PostgreSQL | 股票日线、分钟线、基本面等原始数据 |
| **衍生数据** | SQLite/PostgreSQL | 计算指标、信号、指数等 |
| **配置数据** | SQLite | 股票列表、策略参数等 |
| **缓存数据** | Parquet/CSV | 大规模计算中间结果 |
| **输出数据** | JSON | Dashboard展示数据 |

#### 9.3 数据库表结构设计

**核心表定义**：

```sql
-- 1. 股票基础信息表
CREATE TABLE stock_basic (
    symbol VARCHAR(20) PRIMARY KEY,     -- 600519.SH
    code VARCHAR(10),                    -- 600519
    name VARCHAR(100),                   -- 贵州茅台
    exchange VARCHAR(10),                -- SH/SZ/BJ
    industry VARCHAR(100),               -- 白酒
    list_date DATE,                      -- 上市日期
    is_active BOOLEAN DEFAULT TRUE,      -- 是否在售
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 股票价格数据表（日线）
CREATE TABLE stock_daily_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    volume BIGINT,
    amount DECIMAL(20, 4),
    change_pct DECIMAL(8, 4),
    turnover_ratio DECIMAL(8, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);
CREATE INDEX idx_daily_symbol_date ON stock_daily_price(symbol, trade_date);

-- 3. 股票分钟线数据表（可选）
CREATE TABLE stock_minute_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    trade_time TIME NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    volume BIGINT,
    UNIQUE(symbol, trade_date, trade_time)
);
CREATE INDEX idx_minute_symbol_date ON stock_minute_price(symbol, trade_date);

-- 4. 基本面数据表
CREATE TABLE stock_fundamental (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,           -- 报告期
    report_type VARCHAR(20),             -- 年报/季报
    -- 主要财务指标
    eps DECIMAL(10, 4),                  -- 每股收益
    bps DECIMAL(10, 4),                  -- 每股净资产
    roe DECIMAL(8, 4),                   -- 净资产收益率
    revenue DECIMAL(20, 4),              -- 营业收入
    net_profit DECIMAL(20, 4),           -- 净利润
    -- 估值指标
    pe_ttm DECIMAL(10, 4),               -- 市盈率TTM
    pb DECIMAL(10, 4),                   -- 市净率
    ps_ttm DECIMAL(10, 4),               -- 市销率TTM
    market_cap DECIMAL(20, 4),           -- 总市值
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, report_date, report_type)
);

-- 5. ETF基础信息表
CREATE TABLE etf_basic (
    symbol VARCHAR(20) PRIMARY KEY,
    code VARCHAR(10),
    name VARCHAR(100),
    exchange VARCHAR(10),
    etf_type VARCHAR(50),                -- 股票型/债券型/商品型
    tracking_index VARCHAR(100),         -- 跟踪指数
    management_fee DECIMAL(6, 4),        -- 管理费率
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. ETF净值数据表
CREATE TABLE etf_daily_nav (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    nav DECIMAL(10, 4),                  -- 单位净值
    nav_cum DECIMAL(10, 4),              -- 累计净值
    premium_ratio DECIMAL(8, 4),         -- 溢价率
    volume BIGINT,
    UNIQUE(symbol, trade_date)
);

-- 7. 指数成分股权重表（用于计算自定义指数）
CREATE TABLE index_constituent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name VARCHAR(50) NOT NULL,     -- 指数名称，如'创业板'
    symbol VARCHAR(20) NOT NULL,
    weight DECIMAL(8, 4),                -- 权重（等权为NULL）
    effective_date DATE NOT NULL,        -- 生效日期
    expire_date DATE,                    -- 失效日期（NULL表示当前有效）
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. 数据更新日志表
CREATE TABLE data_update_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type VARCHAR(50) NOT NULL,      -- daily_price/fundamental/etf等
    status VARCHAR(20),                  -- success/failed/partial
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_count INTEGER,
    error_message TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 9.4 数据获取规范

**❌ 禁止直接调用接口获取数据**：
```python
# 禁止 - 直接调用接口
def get_stock_price(symbol):
    import akshare as ak
    return ak.stock_zh_a_hist(symbol=symbol)  # 禁止！
```

**✅ 正确做法 - 从数据库获取**：
```python
# 正确 - 通过 Repository 从数据库获取
from DataHub.repositories import StockRepository

class StockRepository:
    """股票数据仓库"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_daily_price(
        self, 
        symbol: str, 
        start_date: str = None, 
        end_date: str = None
    ) -> pd.DataFrame:
        """从数据库获取日线数据"""
        query = """
            SELECT trade_date, open, high, low, close, volume, amount
            FROM stock_daily_price
            WHERE symbol = ?
        """
        params = [symbol]
        
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY trade_date"
        
        return pd.read_sql_query(query, self.db_path, params=params)
    
    def save_daily_price(self, df: pd.DataFrame):
        """保存日线数据到数据库"""
        df.to_sql('stock_daily_price', self.db_path, 
                  if_exists='append', index=False)
```

#### 9.5 数据爬取与同步模块

**分层架构**：

```
DataHub/
├── crawlers/                    # 数据爬取层
│   ├── __init__.py
│   ├── base_crawler.py          # 爬虫基类
│   ├── stock_price_crawler.py   # 股价爬虫
│   ├── fundamental_crawler.py   # 基本面爬虫
│   ├── etf_crawler.py           # ETF爬虫
│   └── scheduler.py             # 调度器
├── repositories/                # 数据仓库层
│   ├── __init__.py
│   ├── base_repository.py       # 仓库基类
│   ├── stock_repository.py      # 股票仓库
│   ├── etf_repository.py        # ETF仓库
│   └── index_repository.py      # 指数仓库
├── processors/                  # 数据处理层
│   ├── __init__.py
│   ├── index_calculator.py      # 指数计算
│   ├── technical_indicators.py  # 技术指标
│   └── fundamental_analysis.py  # 基本面分析
└── services/                    # 业务服务层
    ├── data_service.py          # 统一数据服务
    └── sync_service.py          # 数据同步服务
```

**爬虫基类设计**：
```python
# DataHub/crawlers/base_crawler.py
from abc import ABC, abstractmethod
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """数据爬虫基类"""
    
    def __init__(self, db_repository):
        self.repository = db_repository
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def fetch(self, **kwargs) -> Optional[pd.DataFrame]:
        """从数据源获取数据"""
        pass
    
    def sync(self, **kwargs) -> dict:
        """
        同步数据：获取 -> 清洗 -> 保存
        
        Returns:
            {'status': 'success/failed', 'records': int, 'message': str}
        """
        try:
            # 1. 从接口获取
            df = self.fetch(**kwargs)
            if df is None or df.empty:
                return {'status': 'failed', 'records': 0, 'message': 'No data fetched'}
            
            # 2. 数据清洗
            df = self._clean(df)
            
            # 3. 保存到数据库
            count = self._save(df)
            
            return {
                'status': 'success', 
                'records': count, 
                'message': f'Synced {count} records'
            }
            
        except Exception as e:
            self.logger.error(f"Sync failed: {e}")
            return {'status': 'failed', 'records': 0, 'message': str(e)}
    
    @abstractmethod
    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗"""
        pass
    
    @abstractmethod
    def _save(self, df: pd.DataFrame) -> int:
        """保存到数据库，返回保存记录数"""
        pass
```

**股价爬虫实现**：
```python
# DataHub/crawlers/stock_price_crawler.py
import akshare as ak
from datetime import datetime


class StockPriceCrawler(BaseCrawler):
    """股票价格爬虫"""
    
    def fetch(
        self, 
        symbol: str, 
        start_date: str = None, 
        end_date: str = None
    ) -> pd.DataFrame:
        """获取单只股票历史价格"""
        # 转换代码格式 600519.SH -> sh600519
        code = self._format_code(symbol)
        
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date or "19700101",
            end_date=end_date or datetime.now().strftime("%Y%m%d"),
            adjust="qfq"  # 前复权
        )
        
        # 添加symbol列
        df['symbol'] = symbol
        return df
    
    def fetch_batch(self, symbols: list, **kwargs) -> pd.DataFrame:
        """批量获取多只股票"""
        all_data = []
        for symbol in symbols:
            try:
                df = self.fetch(symbol, **kwargs)
                all_data.append(df)
            except Exception as e:
                self.logger.warning(f"Failed to fetch {symbol}: {e}")
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def _format_code(self, symbol: str) -> str:
        """转换代码格式 600519.SH -> sh600519"""
        if '.SH' in symbol:
            return symbol.replace('.SH', '').lower() + 'sh'
        elif '.SZ' in symbol:
            return symbol.replace('.SZ', '').lower() + 'sz'
        return symbol
    
    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗数据"""
        # 重命名列以匹配数据库
        column_map = {
            '日期': 'trade_date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '换手率': 'turnover_ratio'
        }
        df = df.rename(columns=column_map)
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 
                     'close', 'volume', 'amount', 'change_pct', 'turnover_ratio']
        return df[[c for c in keep_cols if c in df.columns]]
    
    def _save(self, df: pd.DataFrame) -> int:
        """保存到数据库"""
        return self.repository.save_daily_price(df)
```

#### 9.6 数据衍生与计算层

**原始数据 vs 衍生数据**：

| 类型 | 示例 | 存储位置 | 计算方式 |
|------|------|---------|---------|
| **原始数据** | 日线价格、成交量、基本面 | 数据库 | 从接口获取 |
| **衍生数据** | MA均线、MACD、RSI | 实时计算 | 基于原始数据 |
| **聚合数据** | 指数点位、板块涨跌幅 | 数据库/JSON | IndexCalculator |

**衍生数据计算规范**：
```python
# DataHub/processors/technical_indicators.py
import pandas as pd
import numpy as np


class TechnicalIndicators:
    """技术指标计算"""
    
    @staticmethod
    def ma(prices: pd.Series, window: int) -> pd.Series:
        """简单移动平均线"""
        return prices.rolling(window=window).mean()
    
    @staticmethod
    def macd(prices: pd.Series, fast=12, slow=26, signal=9) -> dict:
        """MACD指标"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal).mean()
        macd = (dif - dea) * 2
        
        return {'DIF': dif, 'DEA': dea, 'MACD': macd}
    
    @staticmethod
    def rsi(prices: pd.Series, window: int = 14) -> pd.Series:
        """RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))


# DataHub/processors/index_calculator.py
class IndexCalculator:
    """指数计算（基于成分股）"""
    
    def __init__(self, repository: StockRepository):
        self.repository = repository
    
    def calculate_index(
        self, 
        index_name: str, 
        date: str = None
    ) -> pd.DataFrame:
        """
        计算指数表现
        
        Args:
            index_name: 指数名称，如'创业板'
            date: 计算日期，None表示最新
        """
        # 1. 获取成分股列表
        constituents = self._get_constituents(index_name, date)
        
        # 2. 获取成分股价格数据
        prices = self.repository.get_prices(
            symbols=constituents['symbol'].tolist(),
            end_date=date
        )
        
        # 3. 计算指数点位（等权法）
        returns = prices.pct_change()
        avg_return = returns.mean(axis=1)
        index_value = (1 + avg_return).cumprod() * 1000
        
        return index_value
```

#### 9.7 数据同步调度

**每日数据更新流程**：
```python
# DataHub/services/sync_service.py
from datetime import datetime
import schedule
import time


class DataSyncService:
    """数据同步服务"""
    
    def __init__(self):
        self.stock_repo = StockRepository()
        self.stock_crawler = StockPriceCrawler(self.stock_repo)
    
    def sync_daily_prices(self):
        """同步日线数据"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 获取所有股票列表
        stocks = self.stock_repo.get_all_stocks()
        
        # 2. 批量爬取
        result = self.stock_crawler.fetch_batch(
            symbols=stocks['symbol'].tolist(),
            start_date=today,
            end_date=today
        )
        
        # 3. 保存
        if not result.empty:
            self.stock_repo.save_daily_price(result)
            print(f"Synced {len(result)} records for {today}")
    
    def run_scheduler(self):
        """运行定时任务"""
        # 每个交易日 15:30 更新
        schedule.every().monday.to.friday.at("15:30").do(self.sync_daily_prices)
        
        while True:
            schedule.run_pending()
            time.sleep(60)
```

#### 9.8 使用示例

**完整数据获取流程**：
```python
# 1. 初始化仓库
from DataHub.repositories import StockRepository
repo = StockRepository('storage/database/quant.db')

# 2. 从数据库获取数据（而非接口）
prices = repo.get_daily_price(
    symbol='600519.SH',
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# 3. 计算技术指标（实时计算）
from DataHub.processors import TechnicalIndicators
ti = TechnicalIndicators()
prices['MA20'] = ti.ma(prices['close'], 20)
prices['RSI'] = ti.rsi(prices['close'], 14)

# 4. 计算指数表现
from DataHub.processors import IndexCalculator
calc = IndexCalculator(repo)
chuangye_index = calc.calculate_index('创业板', date='2024-12-31')

# 5. 保存计算结果（可选）
repo.save_index_value('创业板', chuangye_index)
```

#### 9.9 关键要点总结

| 原则 | 说明 |
|------|------|
| **原始数据必须持久化** | 所有从接口获取的原始数据必须存入数据库 |
| **数据获取走数据库** | 业务代码只能从数据库读取，禁止直接调接口 |
| **爬虫与业务分离** | 爬虫只负责获取和存储原始数据 |
| **衍生数据实时计算** | 指标、指数等基于原始数据实时计算 |
| **分层架构** | Crawler -> Repository -> Processor -> Service |
| **禁止重试机制** | 爬虫遇到网络错误时不自动重试，避免对数据源造成压力 |

---

## 10. 数据存储架构：双轨制设计

### 10.1 核心原则

根据数据使用场景的不同，采用两种不同的存储方案：

| 数据类型 | 存储方案 | 使用场景 | 特点 |
|---------|---------|---------|------|
| **实时/当日数据** | SQLite | 短线交易、Dashboard展示 | 轻量、快速、支持SQL |
| **历史/回测数据** | DuckDB + Parquet | 长线/中线策略回测 | 高性能、列式存储、适合分析 |

### 10.2 为什么用 DuckDB + Parquet？

**Parquet 优势：**
- **列式存储** - 回测时通常只需要读取特定列（如close价格），Parquet只读取需要的列，IO效率高
- **压缩率高** - 相同数据量，Parquet文件比CSV小70-80%
- **类型安全** - 保留数据类型，避免CSV的字符串解析问题
- **分区友好** - 可以按日期/股票分区存储，查询时只读相关分区

**DuckDB 优势：**
- **本地分析型数据库** - 零配置，无需服务器
- **直接查询Parquet** - 无需导入，直接 `SELECT * FROM 'file.parquet'`
- **兼容SQL** - 标准SQL语法，学习成本低
- **高性能** - 向量化执行，比Pandas快10-100倍
- **与Python生态集成** - 结果直接返回DataFrame

### 10.3 数据流示意图

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据源 (akshare)                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌─────────────────────┐         ┌─────────────────────┐
│   实时数据爬虫        │         │   历史数据爬虫       │
│   (当日数据)          │         │   (批量下载)         │
└──────────┬──────────┘         └──────────┬──────────┘
           │                                │
           ▼                                ▼
┌─────────────────────┐         ┌─────────────────────┐
│   SQLite            │         │   Parquet文件       │
│   storage/database/ │         │   storage/raw/      │
│   quant.db          │         │   prices/           │
│                     │         │                     │
│   - 股票日线价格      │         │   - 全市场历史价格    │
│   - ETF日线价格      │         │   - 按年/月分区      │
│   - 当日信号数据      │         │   - 压缩存储         │
└──────────┬──────────┘         └──────────┬──────────┘
           │                                │
           ▼                                ▼
┌─────────────────────┐         ┌─────────────────────┐
│   Dashboard/短线     │         │   DuckDB            │
│   (Streamlit)       │         │   (分析引擎)         │
│                     │         │                     │
│   读取JSON/SQLite   │         │   直接查询Parquet   │
│   展示实时数据        │         │   高性能回测计算     │
└─────────────────────┘         └──────────┬──────────┘
                                           │
                                           ▼
                                ┌─────────────────────┐
                                │   长线/中线策略      │
                                │   (回测引擎)         │
                                │                     │
                                │   读取Parquet       │
                                │   批量历史计算       │
                                └─────────────────────┘
```

### 10.4 存储路径规范

```
storage/
├── database/
│   └── quant.db              # SQLite: 实时数据、当日数据
├── raw/
│   ├── prices/
│   │   ├── 2024/
│   │   │   ├── 202401.parquet    # 按月分区
│   │   │   ├── 202402.parquet
│   │   │   └── ...
│   │   └── 2025/
│   │       └── ...
│   └── zt_pool/              # 涨停池数据（Parquet）
├── processed/
│   └── returns/              # 收益计算结果（Parquet）
└── outputs/                  # Dashboard展示数据（JSON）
```

### 10.5 使用示例

**场景1: 实时数据查询（SQLite）**
```python
from DataHub.repositories import StockRepository

# 获取当日最新价格
repo = StockRepository()
prices = repo.get_daily_price('600519.SH', start_date='2024-01-01')
```

**场景2: 回测数据查询（DuckDB + Parquet）**
```python
import duckdb

# 直接查询Parquet文件
con = duckdb.connect()

# 读取特定时间段的全市场数据
df = con.execute("""
    SELECT symbol, trade_date, close, volume
    FROM 'storage/raw/prices/2024/*.parquet'
    WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31'
      AND symbol IN ('600519.SH', '300750.SZ')
""").fetchdf()

# 计算均线
con.execute("""
    SELECT 
        symbol,
        trade_date,
        close,
        AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) as ma20
    FROM df
""").fetchdf()
```

**场景3: 批量回测（长线策略）**
```python
import duckdb

con = duckdb.connect()

# 读取多年数据进行分析
result = con.execute("""
    SELECT 
        year,
        symbol,
        COUNT(*) as trading_days,
        (MAX(close) - MIN(close)) / MIN(close) * 100 as max_drawdown
    FROM read_parquet('storage/raw/prices/*/*.parquet')
    GROUP BY year, symbol
    HAVING COUNT(*) > 200  -- 排除停牌股票
""").fetchdf()
```

### 10.6 数据同步策略

| 数据 | 更新频率 | 存储位置 | 工具 |
|------|---------|---------|------|
| 当日价格 | 收盘后 | SQLite | sync_service.py |
| 历史价格 | 首次/补数据 | Parquet | history_sync.py |
| 涨停池 | 每日 | Parquet | zt_pool_crawler.py |
| 基本面 | 季度 | SQLite | fundamental_sync.py |

### 10.7 关键要点总结

| 原则 | 说明 |
|------|------|
| **实时用SQLite** | 当日数据、Dashboard展示、短线信号 |
| **回测用Parquet** | 历史数据、批量分析、长线/中线策略 |
| **DuckDB桥接** | 用SQL查询Parquet，无需导入，高性能分析 |
| **分区存储** | Parquet按年/月分区，查询只读必要文件 |
| **双轨不重复** | 同一份数据不同时存两种格式，根据用途选择 |

---

## 11. 复权数据使用规则

### 11.1 核心原则

**原始价格数据永不修改，复权在使用时实时计算。**

### 11.2 存储层规则

| 数据类型 | 存储位置 | 复权状态 | 说明 |
|---------|---------|---------|------|
| 原始价格 | `storage/raw/prices/*.parquet` | **不复权** | 已下载完成，**禁止修改** |
| 复权因子 | `storage/raw/adjust_factors/*.parquet` | - | 用于前复权转换计算 |

### 11.3 使用层规则

**默认使用前复权**，通过 `convert_to_qfq()` 实时转换：

```python
from Dashboard.utils.adjustment import convert_to_qfq

# 加载原始价格
df = pd.read_parquet(f'storage/raw/prices/{symbol}.parquet')

# 实时转换为前复权
df_qfq = convert_to_qfq(df, symbol=symbol)
```

**转换时机：**
- **图表展示** → 加载时转换
- **信号计算** → 扫描时转换
- **价格显示** → 展示时转换

### 11.4 不复权数据用途

不复权价格保留用于：
- 真实成交计算（模拟撮合）
- 分红送股事件分析
- 特殊策略需求

### 11.5 禁止行为

❌ **严禁以下操作**：
```python
# 禁止：修改原始价格数据为前复权
prices_df['close'] = prices_df['close'] * adjust_ratio  # 禁止！
prices_df.to_parquet('storage/raw/prices/xxx.parquet')  # 禁止覆盖原始数据！

# 禁止：重复存储前复权价格
qfq_df.to_parquet('storage/raw/prices_qfq/xxx.parquet')  # 禁止！浪费空间
```

✅ **正确做法**：
```python
# 正确：使用时实时转换
df = pd.read_parquet('storage/raw/prices/xxx.parquet')
df_qfq = convert_to_qfq(df, symbol='xxx')
```

### 11.6 关键要点总结

| 原则 | 说明 |
|------|------|
| **原始数据只读** | `storage/raw/prices/` 目录下的数据永不修改 |
| **实时转换** | 前复权在使用时通过 `convert_to_qfq()` 计算 |
| **默认前复权** | 图表、信号、展示默认使用前复权价格 |
| **保留不复权** | 原始数据保留用于特殊场景 |

---

## 12. 开发者快速指南

### 12.1 初始化数据库

```bash
python DataHub/database/init_db.py
```

这会：
- 创建 SQLite 数据库: `storage/database/quant.db`
- 创建所有必要的表
- 导入股票和 ETF 基础信息

### 12.2 同步数据

```bash
# 每日增量更新（推荐日常使用）
python DataHub/services/history_sync.py --daily

# 首次全量同步（断点续传）
python DataHub/services/history_sync.py --all --skip-existing

# 单只股票全量同步
python DataHub/services/history_sync.py --symbol 600519.SH --full

# 只同步复权因子
python DataHub/services/history_sync.py --sync-factors

# 查看同步状态
python DataHub/services/history_sync.py --summary
```

### 12.3 在代码中使用

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

### 12.4 目录结构

```
DataHub/
├── database/
│   └── init_db.py              # 数据库初始化脚本
├── crawlers/
│   ├── base_crawler.py         # 爬虫基类
│   ├── stock_price_crawler.py  # 股票价格爬虫
│   └── etf_crawler.py          # ETF爬虫
├── repositories/
│   ├── base_repository.py      # 仓库基类
│   └── stock_repository.py     # 股票数据仓库
├── processors/
│   └── index_calculator.py     # 指数计算器
└── services/
    └── sync_service.py         # 数据同步服务
```

### 12.5 常用命令速查

| 命令 | 说明 |
|------|------|
| `history_sync.py --daily` | 每日增量更新 |
| `history_sync.py --all --skip-existing` | 首次同步（断点续传） |
| `history_sync.py --symbol 600519.SH --full` | 单只全量 |
| `history_sync.py --sync-factors` | 同步复权因子 |
| `history_sync.py --summary` | 查看状态 |
| `init_db.py` | 初始化数据库 |

### 12.6 首次部署工作流

```bash
# 1. 初始化数据库
python DataHub/database/init_db.py

# 2. 同步历史数据（全量，建议后台运行）
python DataHub/services/history_sync.py --all --skip-existing

# 3. 验证数据
python DataHub/services/history_sync.py --summary
```

### 12.7 日常维护

```bash
# 每个交易日收盘后执行（15:30后）
python DataHub/services/history_sync.py --daily
```

**定时任务配置（crontab）：**
```bash
30 15 * * 1-5 cd /Users/rupert/code/quant-strategy && python DataHub/services/history_sync.py --daily >> logs/daily_sync.log 2>&1
```

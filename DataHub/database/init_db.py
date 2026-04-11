#!/usr/bin/env python3
"""
数据库初始化脚本
创建所有必要的表结构
"""

import sqlite3
import os
from pathlib import Path


# 数据库路径
BASE_DIR = Path(__file__).parent.parent.parent
DB_DIR = BASE_DIR / "storage" / "database"
DB_PATH = DB_DIR / "quant.db"

# 确保目录存在
DB_DIR.mkdir(parents=True, exist_ok=True)


# 建表SQL
CREATE_TABLES_SQL = """
-- 1. 股票基础信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    symbol VARCHAR(20) PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    industry VARCHAR(100),
    list_date DATE,
    is_active BOOLEAN DEFAULT 1,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. ETF基础信息表
CREATE TABLE IF NOT EXISTS etf_basic (
    symbol VARCHAR(20) PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    etf_type VARCHAR(50),
    tracking_index VARCHAR(100),
    management_fee DECIMAL(6, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 股票价格数据表（日线）
CREATE TABLE IF NOT EXISTS stock_daily_price (
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

-- 4. ETF价格数据表
CREATE TABLE IF NOT EXISTS etf_daily_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    volume BIGINT,
    amount DECIMAL(20, 4),
    nav DECIMAL(10, 4),
    premium_ratio DECIMAL(8, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);

-- 5. 基本面数据表
CREATE TABLE IF NOT EXISTS stock_fundamental (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    report_type VARCHAR(20),
    eps DECIMAL(10, 4),
    bps DECIMAL(10, 4),
    roe DECIMAL(8, 4),
    revenue DECIMAL(20, 4),
    net_profit DECIMAL(20, 4),
    pe_ttm DECIMAL(10, 4),
    pb DECIMAL(10, 4),
    ps_ttm DECIMAL(10, 4),
    market_cap DECIMAL(20, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, report_date, report_type)
);

-- 6. 指数成分股权重表
CREATE TABLE IF NOT EXISTS index_constituent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    weight DECIMAL(8, 4),
    effective_date DATE NOT NULL,
    expire_date DATE,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. 计算后的指数数据表
CREATE TABLE IF NOT EXISTS index_daily_value (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name VARCHAR(50) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    change_pct DECIMAL(8, 4),
    volume BIGINT,
    constituent_count INTEGER,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(index_name, trade_date)
);

-- 8. 数据更新日志表
CREATE TABLE IF NOT EXISTS data_update_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type VARCHAR(50) NOT NULL,
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_count INTEGER,
    error_message TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. 涨停股票池表
CREATE TABLE IF NOT EXISTS zt_pool (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    trade_date DATE NOT NULL,
    close_price DECIMAL(10, 4),
    change_pct DECIMAL(8, 4),
    turnover_ratio DECIMAL(8, 4),
    zt_reason TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_daily_price_symbol_date ON stock_daily_price(symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_etf_price_symbol_date ON etf_daily_price(symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_fundamental_symbol ON stock_fundamental(symbol);
CREATE INDEX IF NOT EXISTS idx_index_value_name_date ON index_daily_value(index_name, trade_date);
CREATE INDEX IF NOT EXISTS idx_zt_pool_date ON zt_pool(trade_date);
CREATE INDEX IF NOT EXISTS idx_constituent_index ON index_constituent(index_name, effective_date);
"""


def init_database():
    """初始化数据库"""
    print(f"初始化数据库: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 执行建表SQL
        cursor.executescript(CREATE_TABLES_SQL)
        conn.commit()
        
        print("✅ 数据库表创建成功")
        
        # 显示创建的表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        print(f"\n共创建 {len(tables)} 张表:")
        for table in tables:
            print(f"  - {table[0]}")
            
        return True
        
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        return False
        
    finally:
        conn.close()


def import_stock_basic():
    """从CSV导入股票基础信息"""
    import pandas as pd
    
    csv_path = BASE_DIR / "storage" / "stock_basic_info.csv"
    if not csv_path.exists():
        print(f"⚠️ 股票基础信息CSV不存在: {csv_path}")
        return False
    
    print(f"\n导入股票基础信息...")
    
    try:
        df = pd.read_csv(csv_path)
        
        # 标准化列名
        if 'symbol' not in df.columns and '代码' in df.columns:
            df = df.rename(columns={'代码': 'symbol', '名称': 'name', '交易所': 'exchange'})
        
        # 确保必要列存在
        required_cols = ['symbol', 'name', 'exchange']
        for col in required_cols:
            if col not in df.columns:
                print(f"❌ 缺少必要列: {col}")
                return False
        
        # 提取code
        df['code'] = df['symbol'].str.extract(r'(\d{6})')
        
        # 选择需要的列
        keep_cols = ['symbol', 'code', 'name', 'exchange', 'industry', 'ipo_date']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        # 导入数据库
        conn = sqlite3.connect(DB_PATH)
        df.to_sql('stock_basic', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✅ 成功导入 {len(df)} 只股票基础信息")
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return False


def import_etf_basic():
    """从CSV导入ETF基础信息"""
    import pandas as pd
    
    csv_path = BASE_DIR / "storage" / "etf_basic_info.csv"
    if not csv_path.exists():
        print(f"⚠️ ETF基础信息CSV不存在: {csv_path}")
        return False
    
    print(f"\n导入ETF基础信息...")
    
    try:
        df = pd.read_csv(csv_path)
        
        # 确保列名正确
        if 'symbol' not in df.columns and '代码' in df.columns:
            df = df.rename(columns={'代码': 'code', '名称': 'name'})
        
        if 'symbol' not in df.columns:
            df['symbol'] = df['code'] + '.' + df['exchange']
        
        # 选择需要的列
        keep_cols = ['symbol', 'code', 'name', 'exchange', 'type', 'tracking_index']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        if 'type' in df.columns:
            df = df.rename(columns={'type': 'etf_type'})
        
        # 导入数据库
        conn = sqlite3.connect(DB_PATH)
        df.to_sql('etf_basic', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✅ 成功导入 {len(df)} 只ETF基础信息")
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("量化交易系统 - 数据库初始化")
    print("=" * 60)
    
    # 1. 创建表结构
    if init_database():
        # 2. 导入股票基础信息
        import_stock_basic()
        
        # 3. 导入ETF基础信息
        import_etf_basic()
        
        print("\n" + "=" * 60)
        print("数据库初始化完成!")
        print("=" * 60)

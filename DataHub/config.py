"""DataHub Configuration"""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent.parent

# Storage paths
STORAGE_DIR = BASE_DIR / "storage"
RAW_PRICES_DIR = STORAGE_DIR / "raw" / "prices"
RAW_ZT_POOL_DIR = STORAGE_DIR / "raw" / "zt_pool"
PROCESSED_RETURNS_DIR = STORAGE_DIR / "processed" / "returns"
DATABASE_DIR = STORAGE_DIR / "database"
DATABASE_PATH = DATABASE_DIR / "datahub.db"

# Strategy outputs directory
OUTPUTS_DIR = STORAGE_DIR / "outputs"
LONGTERM_DIR = OUTPUTS_DIR / "longterm"
SHORTTERM_DIR = OUTPUTS_DIR / "shortterm"

# LongTerm subdirectories
LONGTERM_WEIGHTS_DIR = LONGTERM_DIR / "weights"
LONGTERM_REPORTS_DIR = LONGTERM_DIR / "reports"
LONGTERM_CHARTS_DIR = LONGTERM_REPORTS_DIR / "charts"
LONGTERM_DATA_DIR = LONGTERM_DIR / "data"

# ShortTerm subdirectories
SHORTTERM_SIGNALS_DIR = SHORTTERM_DIR / "signals"
SHORTTERM_HISTORY_DIR = SHORTTERM_DIR / "history"
SHORTTERM_DATABASE_DIR = SHORTTERM_DIR / "database"
SHORTTERM_CHARTS_DIR = SHORTTERM_DIR / "charts"
SHORTTERM_CACHE_DIR = SHORTTERM_DIR / "cache"

# Ensure all directories exist
for _dir in [
    STORAGE_DIR, RAW_PRICES_DIR, RAW_ZT_POOL_DIR, PROCESSED_RETURNS_DIR, DATABASE_DIR,
    OUTPUTS_DIR, LONGTERM_DIR, SHORTTERM_DIR,
    LONGTERM_WEIGHTS_DIR, LONGTERM_REPORTS_DIR, LONGTERM_CHARTS_DIR, LONGTERM_DATA_DIR,
    SHORTTERM_SIGNALS_DIR, SHORTTERM_HISTORY_DIR, SHORTTERM_DATABASE_DIR, SHORTTERM_CHARTS_DIR, SHORTTERM_CACHE_DIR
]:
    _dir.mkdir(parents=True, exist_ok=True)

# Stock list from LongTerm
STOCK_LIST = [
    # 原策略股票
    "600519.SH",   # 茅台
    "601398.SH",   # 工商银行
    "513310.SH",   # 中韩半导体ETF
    "588080.SH",   # 科创50ETF
    "159949.SH",   # 创业板50ETF
    "510720.SH",   # 红利国企ETF
    "510050.SH",   # 上证50ETF
    "510300.SH",   # 沪深300ETF
    "000858.SZ",   # 五粮液
    "002563.SZ",   # 森马服饰
    "600438.SH",   # 通威股份
    # 用户自选股 - A股
    "000428.SZ",   # 华天酒店
    "603618.SH",   # 杭电股份
    "600060.SH",   # 海信视像
    "603777.SH",   # 来伊份
    "002415.SZ",   # 海康威视
    "300750.SZ",   # 宁德时代
    "002594.SZ",   # 比亚迪
    "601318.SH",   # 中国平安
    "002696.SZ",   # 百洋股份
    "000651.SZ",   # 格力电器
    "300502.SZ",   # 新易盛
    "000063.SZ",   # 中兴通讯
    "000333.SZ",   # 美的集团
    # 用户自选股 - 港股
    "01810.HK",    # 小米集团
    "00175.HK",    # 吉利汽车
    "09992.HK",    # 泡泡玛特
    "00700.HK",    # 腾讯控股
]

# Data source settings
DATA_SOURCE = {
    "primary": "akshare",
    "fallback": ["akshare", "baostock"],
}

# Update settings
UPDATE_CONFIG = {
    "prices": {
        "schedule": "30 16 * * 1-5",  # Every weekday at 16:30
        "days_before_today": 0,
        "retry_times": 3,
        "retry_delay": 5,
    },
    "zt_pool": {
        "schedule": "15 15 * * 1-5",  # Every weekday at 15:15
        "retry_times": 3,
        "retry_delay": 5,
    },
}

# Data retention
RETENTION = {
    "zt_pool_days": 90,  # Keep 90 days of ZT pool data
}

# Logging
LOG_LEVEL = "INFO"

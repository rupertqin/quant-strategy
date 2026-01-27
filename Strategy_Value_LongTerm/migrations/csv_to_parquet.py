"""
CSV → Parquet 迁移脚本

使用方法:
    cd Strategy_Value_LongTerm
    python migrations/csv_to_parquet.py

迁移内容:
    - data/prices.csv → data/prices.parquet
    - data/returns.csv → data/returns.parquet
    - 初始化 signals.db (SQLite)
"""

import os
import sys

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_manager import DataManager
import pandas as pd


def migrate_csv_to_parquet():
    """执行迁移"""
    print("=" * 50)
    print("数据层迁移: CSV → Parquet + SQLite")
    print("=" * 50)

    dm = DataManager()

    # 1. 迁移 prices.csv
    print("\n[1/3] 迁移 prices.csv → prices.parquet...")
    prices_csv = os.path.join(dm.data_dir, "prices.csv")
    if os.path.exists(prices_csv):
        df = pd.read_csv(prices_csv, index_col=0, parse_dates=True)
        print(f"    读取 {len(df)} 行数据")
        dm.save_prices(df)
        print("    完成!")
    else:
        print("    文件不存在，跳过")

    # 2. 迁移 returns.csv
    print("\n[2/3] 迁移 returns.csv → returns.parquet...")
    returns_csv = os.path.join(dm.data_dir, "returns.csv")
    if os.path.exists(returns_csv):
        df = pd.read_csv(returns_csv, index_col=0, parse_dates=True)
        print(f"    读取 {len(df)} 行数据")
        dm.save_returns(df)
        print("    完成!")
    else:
        print("    文件不存在，跳过")

    # 3. 初始化数据库
    print("\n[3/3] 初始化 SQLite 数据库...")
    versions = dm.get_all_versions()
    print(f"    数据库路径: {dm.db_path}")
    print(f"    已记录版本: {list(versions.keys())}")
    print("    完成!")

    # 验证
    print("\n" + "=" * 50)
    print("验证结果:")
    print("=" * 50)

    prices = dm.get_prices()
    print(f"  Prices: {len(prices)} 行")

    returns = dm.get_returns()
    print(f"  Returns: {len(returns)} 行")

    all_versions = dm.get_all_versions()
    print(f"  数据版本: {list(all_versions.keys())}")

    print("\n迁移成功!")
    return True


def verify_migration():
    """验证迁移结果"""
    dm = DataManager()

    print("\n" + "=" * 50)
    print("迁移验证")
    print("=" * 50)

    # 测试读取
    prices = dm.get_prices()
    returns = dm.get_returns()

    print(f"\nPrices 数据:")
    print(f"  - 行数: {len(prices)}")
    print(f"  - 列数: {len(prices.columns)}")
    print(f"  - 日期范围: {prices.index.min()} ~ {prices.index.max()}")

    print(f"\nReturns 数据:")
    print(f"  - 行数: {len(returns)}")
    print(f"  - 列数: {len(returns.columns)}")
    print(f"  - 日期范围: {returns.index.min()} ~ {returns.index.max()}")

    # 测试 SQLite
    print(f"\nSQLite 数据库:")
    print(f"  - 路径: {dm.db_path}")
    print(f"  - 存在: {os.path.exists(dm.db_path)}")

    print("\n验证通过!")


if __name__ == "__main__":
    migrate_csv_to_parquet()
    verify_migration()

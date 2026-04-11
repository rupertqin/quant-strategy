#!/usr/bin/env python3
"""
数据流架构演示

展示完整的数据持久化流程：
1. 初始化数据库
2. 通过爬虫获取数据并保存
3. 从数据库读取数据进行分析
4. 计算指数和指标
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 导入各层组件
from DataHub.repositories import StockRepository
from DataHub.crawlers import StockPriceCrawler
from DataHub.processors import IndexCalculator


def demo_1_database_init():
    """演示1: 数据库状态"""
    print("=" * 60)
    print("演示1: 数据库初始化状态")
    print("=" * 60)
    
    repo = StockRepository()
    
    # 获取表统计
    stats = repo.get_table_stats()
    print("\n数据库表统计:")
    for table, count in stats.items():
        if count > 0:
            print(f"  ✅ {table}: {count:,} 条记录")
        elif count == 0:
            print(f"  ⏳ {table}: 空表")
        else:
            print(f"  ❌ {table}: 表不存在")
    
    # 获取股票和ETF数量
    stocks = repo.get_all_stocks()
    etfs = repo.get_all_etfs()
    
    print(f"\n基础数据:")
    print(f"  股票数量: {len(stocks):,} 只")
    print(f"  ETF数量: {len(etfs):,} 只")


def demo_2_crawler_and_save():
    """演示2: 爬虫获取数据并保存"""
    print("\n" + "=" * 60)
    print("演示2: 爬虫获取数据并持久化")
    print("=" * 60)
    
    # 选择几只股票进行演示
    demo_stocks = ['600519.SH', '300750.SZ', '000001.SZ', '510050.SH']
    
    print(f"\n将同步以下股票最近30天数据:")
    for s in demo_stocks:
        print(f"  - {s}")
    
    # 初始化爬虫和仓库
    repo = StockRepository()
    crawler = StockPriceCrawler(repository=repo)
    
    # 执行同步
    print("\n开始同步...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    result = crawler.sync(
        symbols=demo_stocks,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
    
    print(f"\n同步结果:")
    print(f"  状态: {result['status']}")
    print(f"  记录数: {result.get('records', 0)}")
    print(f"  消息: {result.get('message', 'N/A')}")


def demo_3_read_from_db():
    """演示3: 从数据库读取数据"""
    print("\n" + "=" * 60)
    print("演示3: 从数据库读取数据进行分析")
    print("=" * 60)
    
    repo = StockRepository()
    
    # 获取某只股票的历史数据
    symbol = '600519.SH'
    print(f"\n获取 {symbol} 的历史价格数据:")
    
    prices = repo.get_daily_price(symbol, fields=['close', 'volume', 'change_pct'])
    
    if not prices.empty:
        print(f"  数据条数: {len(prices)}")
        print(f"  日期范围: {prices.index[0]} 至 {prices.index[-1]}")
        print(f"\n  最近5日数据:")
        print(prices.tail().to_string())
        
        # 计算简单统计
        print(f"\n  价格统计:")
        print(f"    最新收盘价: {prices['close'].iloc[-1]:.2f}")
        print(f"    30日均价: {prices['close'].mean():.2f}")
        print(f"    30日波动: {prices['change_pct'].std():.2f}%")
    else:
        print("  ⚠️ 暂无数据，请先运行演示2")


def demo_4_index_calculation():
    """演示4: 基于个股数据计算指数"""
    print("\n" + "=" * 60)
    print("演示4: 基于成分股计算指数")
    print("=" * 60)
    
    repo = StockRepository()
    calc = IndexCalculator()
    
    # 尝试获取股票池
    print("\n从数据库获取股票价格数据...")
    
    # 创业板股票
    chuangye_stocks = ['300750.SZ', '300001.SZ', '300002.SZ']
    
    # 获取价格数据
    prices = repo.get_multiple_prices(
        chuangye_stocks,
        start_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    )
    
    if not prices.empty:
        print(f"\n获取到 {len(prices.columns)} 只股票, {len(prices)} 个交易日数据")
        
        # 计算指数
        index_value = calc.calculate_index_value(prices, '创业板')
        
        if not index_value.empty:
            print(f"\n创业板指数计算结果:")
            print(f"  基准点位: 1000.00")
            print(f"  最新点位: {index_value.iloc[-1]:.2f}")
            print(f"  涨跌幅: {(index_value.iloc[-1]/1000-1)*100:+.2f}%")
            print(f"  期间最高: {index_value.max():.2f}")
            print(f"  期间最低: {index_value.min():.2f}")
    else:
        print("  ⚠️ 暂无价格数据，无法计算指数")


def demo_5_data_flow_summary():
    """演示5: 数据流总结"""
    print("\n" + "=" * 60)
    print("数据流架构总结")
    print("=" * 60)
    
    print("""
完整数据流:
┌──────────────────────────────────────────────────────────┐
│  1. 数据爬取层 (Crawlers)                                 │
│     - StockPriceCrawler: 获取股票日线数据                  │
│     - ETFCrawler: 获取ETF数据                             │
└──────────────────────┬───────────────────────────────────┘
                       │ 获取原始数据
                       ▼
┌──────────────────────────────────────────────────────────┐
│  2. 数据持久层 (Repository)                               │
│     - StockRepository: 保存到 SQLite 数据库               │
│     - 表: stock_daily_price, etf_daily_price              │
└──────────────────────┬───────────────────────────────────┘
                       │ 查询历史数据
                       ▼
┌──────────────────────────────────────────────────────────┐
│  3. 数据处理层 (Processors)                               │
│     - IndexCalculator: 基于成分股计算指数                 │
│     - TechnicalIndicators: 计算技术指标                   │
└──────────────────────┬───────────────────────────────────┘
                       │ 分析结果
                       ▼
┌──────────────────────────────────────────────────────────┐
│  4. 业务层 (Services/Strategies)                          │
│     - Scanner: 生成交易信号                               │
│     - Dashboard: 可视化展示                               │
└──────────────────────────────────────────────────────────┘

关键规则:
1. 所有原始数据必须存入数据库
2. 业务代码只从数据库读取，禁止直接调接口
3. 爬虫与业务分离，爬虫只负责获取和存储
4. 衍生数据（指数、指标）基于原始数据实时计算
    """)


def main():
    """运行所有演示"""
    print("\n" + "=" * 60)
    print("DataHub 数据持久化架构演示")
    print("=" * 60)
    
    demo_1_database_init()
    demo_2_crawler_and_save()
    demo_3_read_from_db()
    demo_4_index_calculation()
    demo_5_data_flow_summary()
    
    print("\n" + "=" * 60)
    print("演示完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()

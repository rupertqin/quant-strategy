#!/usr/bin/env python3
"""
指数计算示例 - 展示如何使用 IndexCalculator

核心思想：基于个股数据计算指数表现，而非直接调用指数接口
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from DataHub.core.index_calculator import IndexCalculator, get_index_summary


def demo_with_mock_data():
    """使用模拟数据演示指数计算"""
    print("=" * 60)
    print("指数计算演示 - 基于个股数据计算指数")
    print("=" * 60)
    
    # 1. 创建模拟的全市场个股数据
    print("\n1. 创建模拟个股数据...")
    
    dates = pd.date_range(end=datetime.now(), periods=20, freq='D')
    
    # 各板块代表性股票
    stocks = {
        # 创业板
        '300001.SZ': '特锐德',
        '300750.SZ': '宁德时代',
        '300002.SZ': '神州泰岳',
        # 科创板
        '688001.SH': '华兴源创',
        '688002.SH': '睿创微纳',
        # 上海主板
        '600519.SH': '贵州茅台',
        '600000.SH': '浦发银行',
        # 深圳主板
        '000001.SZ': '平安银行',
        '000858.SZ': '五粮液',
    }
    
    # 生成价格数据
    np.random.seed(42)
    price_data = {}
    
    for symbol, name in stocks.items():
        base_price = np.random.uniform(10, 1000)
        returns = np.random.normal(0.0005, 0.015, len(dates))
        prices = base_price * (1 + returns).cumprod()
        price_data[symbol] = prices
    
    price_df = pd.DataFrame(price_data, index=dates)
    
    print(f"   股票数量: {len(stocks)}")
    print(f"   日期范围: {dates[0].date()} 到 {dates[-1].date()}")
    
    # 2. 计算指数
    print("\n2. 计算各指数表现...")
    
    calc = IndexCalculator()
    
    # 计算所有指数
    indices = calc.calculate_all_indices(price_df)
    
    for name, series in indices.items():
        if not series.empty:
            current = series.iloc[-1]
            start = series.iloc[0]
            change = (current / start - 1) * 100
            print(f"   {name:10s}: {current:8.2f}点 ({change:+6.2f}%)")
    
    # 3. 获取详细摘要
    print("\n3. 指数表现摘要...")
    
    summary = calc.get_index_performance_summary(price_df)
    
    for name, data in summary.items():
        print(f"\n   {name}:")
        print(f"     当前点位: {data['current_value']}")
        print(f"     日涨跌:   {data['change_pct']:+.2f}%")
        print(f"     趋势:     {data['trend']}")
        print(f"     成分股数: {data['constituent_count']}")
    
    print("\n" + "=" * 60)
    print("演示完成!")
    print("=" * 60)


def demo_real_world_scenario():
    """展示实际应用场景"""
    print("\n" + "=" * 60)
    print("实际应用场景")
    print("=" * 60)
    
    print("""
场景1: 计算自定义指数
---------------------
假设你有一个股票池，想计算这个池子的"自定义指数":

    from DataHub.core.index_calculator import IndexCalculator
    
    calc = IndexCalculator()
    
    # 你的股票池
    my_stocks = ['600519.SH', '300750.SZ', '000858.SZ']
    
    # 获取这些股票的价格数据
    price_df = load_prices(my_stocks)  # 你自己的数据加载函数
    
    # 计算自定义指数（等权）
    custom_index = calc.calculate_index_value(price_df, '自定义指数')


场景2: 对比不同板块表现
----------------------
对比创业板 vs 科创板的表现:

    calc = IndexCalculator()
    
    # 全市场数据
    all_stocks = load_all_stocks()
    
    # 计算各板块指数
    chuangye = calc.filter_constituents(all_stocks, '创业板')
    kechuang = calc.filter_constituents(all_stocks, '科创板')
    
    print(f"创业板成分股: {len(chuangye)}只")
    print(f"科创板成分股: {len(kechuang)}只")


场景3: 离线分析
--------------
只需要历史个股数据就能计算指数，不需要实时接口:

    # 加载本地历史数据
    price_df = pd.read_parquet('storage/raw/prices/history.parquet')
    
    # 计算指数（完全离线）
    indices = calc.calculate_all_indices(price_df)
    """
    )


def main():
    """主函数"""
    demo_with_mock_data()
    demo_real_world_scenario()


if __name__ == "__main__":
    main()

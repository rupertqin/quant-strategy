"""
指数计算器单元测试
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from DataHub.core.index_calculator import IndexCalculator, IndexDefinition


def create_mock_stock_data():
    """创建模拟股票数据用于测试"""
    # 创建30天的日期索引
    dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    # 模拟不同板块的股票
    stocks = {
        # 创业板 (300开头)
        '300001.SZ': {'name': '特锐德', 'exchange': 'SZ', 'base_price': 20},
        '300002.SZ': {'name': '神州泰岳', 'exchange': 'SZ', 'base_price': 15},
        '300750.SZ': {'name': '宁德时代', 'exchange': 'SZ', 'base_price': 200},
        
        # 科创板 (688开头)
        '688001.SH': {'name': '华兴源创', 'exchange': 'SH', 'base_price': 30},
        '688002.SH': {'name': '睿创微纳', 'exchange': 'SH', 'base_price': 45},
        
        # 上海主板 (600开头)
        '600000.SH': {'name': '浦发银行', 'exchange': 'SH', 'base_price': 8},
        '600519.SH': {'name': '贵州茅台', 'exchange': 'SH', 'base_price': 1500},
        
        # 深圳主板 (000开头)
        '000001.SZ': {'name': '平安银行', 'exchange': 'SZ', 'base_price': 12},
        '000858.SZ': {'name': '五粮液', 'exchange': 'SZ', 'base_price': 150},
    }
    
    # 生成价格数据（带随机波动）
    np.random.seed(42)
    price_data = {}
    
    for symbol, info in stocks.items():
        base = info['base_price']
        # 生成随机收益率序列
        returns = np.random.normal(0.001, 0.02, len(dates))
        prices = base * (1 + returns).cumprod()
        price_data[symbol] = prices
    
    df = pd.DataFrame(price_data, index=dates)
    return df


def test_index_definition():
    """测试指数定义"""
    print("\n=== 测试 IndexDefinition ===")
    
    indices = IndexDefinition.list_indices()
    print(f"支持的指数: {indices}")
    
    # 测试创业板规则
    rule = IndexDefinition.get_rule('创业板')
    print(f"\n创业板规则: {rule}")
    
    # 验证代码匹配
    test_codes = ['300001', '300750', '688001', '600519', '000001']
    import re
    for code in test_codes:
        matches = bool(re.match(rule['code_pattern'], code))
        print(f"  {code}: {'✅' if matches else '❌'}")


def test_filter_constituents():
    """测试成分股筛选"""
    print("\n=== 测试成分股筛选 ===")
    
    calc = IndexCalculator()
    
    # 创建模拟股票基础数据
    stocks_data = [
        {'symbol': '300001.SZ', 'name': '特锐德', 'exchange': 'SZ'},
        {'symbol': '300750.SZ', 'name': '宁德时代', 'exchange': 'SZ'},
        {'symbol': '688001.SH', 'name': '华兴源创', 'exchange': 'SH'},
        {'symbol': '600519.SH', 'name': '贵州茅台', 'exchange': 'SH'},
        {'symbol': '000001.SZ', 'name': '平安银行', 'exchange': 'SZ'},
    ]
    df = pd.DataFrame(stocks_data)
    
    # 测试各指数成分股筛选
    for index_name in ['创业板', '科创板', '上证指数', '深证成指']:
        constituents = calc.filter_constituents(df, index_name)
        print(f"\n{index_name}: {len(constituents)} 只成分股")
        if not constituents.empty:
            print(f"  成分股: {constituents['name'].tolist()}")


def test_calculate_index():
    """测试指数计算"""
    print("\n=== 测试指数计算 ===")
    
    calc = IndexCalculator()
    price_df = create_mock_stock_data()
    
    print(f"价格数据维度: {price_df.shape}")
    print(f"股票列表: {list(price_df.columns)}")
    
    # 计算各指数
    for index_name in ['创业板', '科创板', '上证指数']:
        try:
            # 筛选该指数的成分股
            code_col = 'symbol'
            stocks_df = pd.DataFrame({'symbol': price_df.columns})
            constituents = calc.filter_constituents(stocks_df, index_name)
            
            if constituents.empty:
                print(f"\n{index_name}: 没有成分股")
                continue
            
            # 获取成分股的价格数据
            constituent_symbols = constituents['symbol'].tolist()
            available_symbols = [s for s in constituent_symbols if s in price_df.columns]
            
            if not available_symbols:
                print(f"\n{index_name}: 没有可用的价格数据")
                continue
            
            constituent_prices = price_df[available_symbols]
            
            # 计算指数
            index_value = calc.calculate_index_value(constituent_prices, index_name)
            
            if not index_value.empty:
                print(f"\n{index_name}:")
                print(f"  成分股数量: {len(available_symbols)}")
                print(f"  最新点位: {index_value.iloc[-1]:.2f}")
                print(f"  首日点位: {index_value.iloc[0]:.2f}")
                change_pct = (index_value.iloc[-1] / index_value.iloc[0] - 1) * 100
                print(f"  期间涨跌: {change_pct:+.2f}%")
            
        except Exception as e:
            print(f"\n{index_name}: 计算失败 - {e}")


def test_index_summary():
    """测试指数表现摘要"""
    print("\n=== 测试指数表现摘要 ===")
    
    calc = IndexCalculator()
    price_df = create_mock_stock_data()
    
    # 计算摘要
    summary = calc.get_index_performance_summary(price_df)
    
    for name, data in summary.items():
        print(f"\n{name}:")
        for key, value in data.items():
            print(f"  {key}: {value}")


def main():
    """运行所有测试"""
    print("=" * 60)
    print("指数计算器单元测试")
    print("=" * 60)
    
    test_index_definition()
    test_filter_constituents()
    test_calculate_index()
    test_index_summary()
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()

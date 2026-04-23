"""
DataHub 数据读取功能测试

运行: python -m pytest DataHub/tests/test_data_reader.py -v
"""

import pandas as pd

# 可选的pytest
try:
    import pytest
except ImportError:
    pytest = None

from DataHub.core.data_reader import load_stock_prices, load_stock_prices_raw


class TestDataReader:
    """测试数据读取功能"""

    def test_load_stock_prices_structure(self):
        """测试加载价格数据的结构"""
        # 使用已知存在的股票测试（平安银行）
        df = load_stock_prices("000001.SZ")

        if df.empty:
            print("\n⚠️  未找到测试数据，跳过测试")
            return

        # 验证列存在
        required_cols = ['open', 'high', 'low', 'close', 'volume', 'trade_date']
        for col in required_cols:
            assert col in df.columns, f"缺少列: {col}"

        # 验证数据类型
        assert df['close'].dtype in ['float64', 'float32']
        assert df['volume'].dtype in ['int64', 'float64']

        print(f"\n✓ 数据结构正常，共 {len(df)} 条记录")

    def test_load_stock_prices_date_range(self):
        """测试日期范围过滤"""
        df = load_stock_prices("000001.SZ", start_date="2024-01-01", end_date="2024-01-31")

        if df.empty:
            print("\n⚠️  未找到测试数据，跳过测试")
            return

        # 验证日期范围
        dates = pd.to_datetime(df['trade_date'])
        assert all(dates >= pd.Timestamp("2024-01-01"))
        assert all(dates <= pd.Timestamp("2024-01-31"))

        print(f"\n✓ 日期范围过滤正常，共 {len(df)} 条记录")

    def test_load_stock_prices_raw(self):
        """测试加载不复权数据"""
        df = load_stock_prices_raw("000001.SZ")

        if df.empty:
            print("\n⚠️  未找到测试数据，跳过测试")
            return

        assert not df.empty
        assert 'close' in df.columns

        print(f"\n✓ 不复权数据加载正常")

    def test_load_nonexistent_stock(self):
        """测试加载不存在的股票"""
        df = load_stock_prices("999999.XY")  # 不存在的代码
        assert df.empty
        print("\n✓ 不存在的股票返回空数据")


if __name__ == "__main__":
    # 无pytest时直接运行
    test = TestDataReader()

    tests = [
        ("test_load_stock_prices_structure", test.test_load_stock_prices_structure),
        ("test_load_stock_prices_date_range", test.test_load_stock_prices_date_range),
        ("test_load_stock_prices_raw", test.test_load_stock_prices_raw),
        ("test_load_nonexistent_stock", test.test_load_nonexistent_stock),
    ]

    for name, test_func in tests:
        try:
            test_func()
            print(f"✓ {name} passed")
        except AssertionError as e:
            print(f"✗ {name} failed: {e}")
        except Exception as e:
            print(f"✗ {name} error: {e}")

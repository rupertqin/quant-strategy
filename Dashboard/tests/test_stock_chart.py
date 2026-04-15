"""
Dashboard 股票图表功能测试

运行: python -m pytest Dashboard/tests/test_stock_chart.py -v
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# 添加项目路径
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# 可选的pytest
try:
    import pytest
except ImportError:
    pytest = None

from Dashboard.pages.stock_chart import merge_realtime_to_df


class TestRealtimeMerge:
    """测试实时数据合并到历史数据"""

    def create_sample_history(self):
        """创建示例历史数据"""
        today = datetime.now().date()
        dates = pd.date_range(end=today, periods=10, freq='D')

        df = pd.DataFrame({
            'trade_date': dates,
            'open': [100.0] * 10,
            'high': [105.0] * 10,
            'low': [95.0] * 10,
            'close': [102.0] * 10,
            'volume': [10000] * 10
        })
        return df

    def test_merge_realtime_update_today(self):
        """测试更新今天的数据"""
        df = self.create_sample_history()
        today = datetime.now().date()

        # 确保最后一天是今天（替换最后一行的日期）
        df = df.copy()
        df.iloc[-1, df.columns.get_loc('trade_date')] = pd.Timestamp(today)

        realtime_data = {
            'trade_date': today.strftime('%Y-%m-%d'),
            'open': 101.0,
            'high': 108.0,  # 比历史数据更高
            'low': 99.0,    # 比历史数据更低
            'close': 106.0, # 新收盘价
            'volume': 50000,
            'change_pct': 3.92
        }

        result = merge_realtime_to_df(df, realtime_data, "14:30")

        # 验证更新
        last_row = result.iloc[-1]
        assert last_row['close'] == 106.0, f"close 应为 106.0, 实际是 {last_row['close']}"
        assert last_row['high'] == 108.0, f"high 应为 108.0, 实际是 {last_row['high']}"
        assert last_row['low'] == 99.0, f"low 应为 99.0, 实际是 {last_row['low']}"
        assert last_row['volume'] == 50000
        assert last_row['realtime_time'] == "14:30"

        print(f"\n合并后最后一条数据:\n{last_row}")

    def test_merge_realtime_add_new_day(self):
        """测试添加新的一天数据"""
        df = self.create_sample_history()
        today = datetime.now().date()
        yesterday = today - pd.Timedelta(days=1)

        # 最后一天是昨天
        df.loc[len(df)-1, 'trade_date'] = pd.Timestamp(yesterday)

        realtime_data = {
            'trade_date': today.strftime('%Y-%m-%d'),
            'open': 100.0,
            'high': 105.0,
            'low': 98.0,
            'close': 103.0,
            'volume': 20000,
            'change_pct': 3.0
        }

        result = merge_realtime_to_df(df, realtime_data, "10:30")

        # 验证新增了一条
        assert len(result) == len(df) + 1

        # 验证新数据
        last_row = result.iloc[-1]
        assert last_row['trade_date'].date() == today
        assert last_row['close'] == 103.0
        assert last_row['realtime_time'] == "10:30"

        print(f"\n新增数据后共 {len(result)} 条, 最后一条:\n{last_row}")

    def test_merge_realtime_empty_data(self):
        """测试空数据处理"""
        df = self.create_sample_history()

        # 空实时数据
        result = merge_realtime_to_df(df, {}, "10:30")
        assert len(result) == len(df)  # 数据条数不变

        # None 实时数据
        result = merge_realtime_to_df(df, None, "10:30")
        assert len(result) == len(df)

        print("\n✓ 空数据处理正常")

    def test_merge_realtime_invalid_data(self):
        """测试无效数据处理"""
        df = self.create_sample_history()
        today = datetime.now().date()

        # 收盘价为0（无效）
        realtime_data = {
            'trade_date': today.strftime('%Y-%m-%d'),
            'close': 0,
            'volume': 10000
        }

        result = merge_realtime_to_df(df, realtime_data, "10:30")
        # 无效数据应被忽略
        assert len(result) == len(df)

        print("\n✓ 无效数据被正确忽略")


if __name__ == "__main__":
    # 无pytest时直接运行
    test = TestRealtimeMerge()

    tests = [
        ("test_merge_realtime_update_today", test.test_merge_realtime_update_today),
        ("test_merge_realtime_add_new_day", test.test_merge_realtime_add_new_day),
        ("test_merge_realtime_empty_data", test.test_merge_realtime_empty_data),
        ("test_merge_realtime_invalid_data", test.test_merge_realtime_invalid_data),
    ]

    for name, test_func in tests:
        try:
            test_func()
            print(f"✓ {name} passed")
        except AssertionError as e:
            print(f"✗ {name} failed: {e}")

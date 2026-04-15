"""
Dashboard 数据访问层测试

运行: python -m pytest Dashboard/tests/test_data_access.py -v
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

from Dashboard.utils.data_access import (
    get_todays_realtime_file,
    load_realtime_data,
    get_realtime_price_time,
    has_realtime_data,
    REALTIME_DIR
)


class TestRealtimeDataAccess:
    """测试实时数据访问层"""

    def test_realtime_dir_exists(self):
        """测试实时数据目录存在"""
        print(f"\n实时数据目录: {REALTIME_DIR}")
        assert REALTIME_DIR.exists(), f"目录不存在: {REALTIME_DIR}"

    def test_get_todays_realtime_file(self):
        """测试获取当天实时数据文件"""
        filepath = get_todays_realtime_file()
        print(f"\n当天实时数据文件: {filepath}")

        if filepath:
            assert Path(filepath).exists(), f"文件不存在: {filepath}"
            # 验证文件名格式
            assert 'realtime_' in filepath
            assert '.json' in filepath

    def test_load_realtime_data(self):
        """测试加载实时数据"""
        df = load_realtime_data()
        print(f"\n加载实时数据: {len(df)} 条记录")

        if not df.empty:
            # 验证必要的列存在
            required_cols = ['symbol', 'close', 'open', 'high', 'low', 'volume']
            for col in required_cols:
                assert col in df.columns, f"缺少列: {col}"

            # 验证数据类型
            assert df['close'].dtype in ['float64', 'float32', 'int64']
            assert len(df) > 0

            print(f"列名: {list(df.columns)}")
            print(f"示例数据:\n{df.head(2)}")

    def test_get_realtime_price_time(self):
        """测试获取实时数据时间"""
        time_str = get_realtime_price_time()
        print(f"\n实时数据时间: {time_str}")

        if time_str:
            # 验证格式: YYYY-MM-DD HH:MM
            assert len(time_str) == 16
            assert time_str[4] == '-'
            assert time_str[7] == '-'
            assert time_str[10] == ' '
            assert time_str[13] == ':'

    def test_has_realtime_data(self):
        """测试检查实时数据存在"""
        has_data = has_realtime_data()
        filepath = get_todays_realtime_file()
        print(f"\n有实时数据: {has_data}, 文件: {filepath}")

        # 一致性检查
        if filepath:
            assert has_data is True
        else:
            assert has_data is False


if __name__ == "__main__":
    # 无pytest时直接运行
    test = TestRealtimeDataAccess()
    try:
        test.test_realtime_dir_exists()
        print("✓ test_realtime_dir_exists passed")
    except AssertionError as e:
        print(f"✗ test_realtime_dir_exists failed: {e}")

    try:
        test.test_get_todays_realtime_file()
        print("✓ test_get_todays_realtime_file passed")
    except AssertionError as e:
        print(f"✗ test_get_todays_realtime_file failed: {e}")

    try:
        test.test_load_realtime_data()
        print("✓ test_load_realtime_data passed")
    except AssertionError as e:
        print(f"✗ test_load_realtime_data failed: {e}")

    try:
        test.test_get_realtime_price_time()
        print("✓ test_get_realtime_price_time passed")
    except AssertionError as e:
        print(f"✗ test_get_realtime_price_time failed: {e}")

    try:
        test.test_has_realtime_data()
        print("✓ test_has_realtime_data passed")
    except AssertionError as e:
        print(f"✗ test_has_realtime_data failed: {e}")

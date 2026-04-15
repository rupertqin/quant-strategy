"""
实时数据功能测试用例

运行: python -m pytest tests/test_realtime_data.py -v
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# 添加项目路径
BASE_DIR = Path(__file__).parent.parent
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
from Dashboard.pages.stock_chart import merge_realtime_to_df


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
        result = merge_realtime_to_df(df, {}, "10:00")
        assert len(result) == len(df)
        
        # None实时数据
        result = merge_realtime_to_df(df, None, "10:00")
        assert len(result) == len(df)
    
    def test_merge_realtime_wrong_date(self):
        """测试非今天数据不合并"""
        df = self.create_sample_history()
        today = datetime.now().date()
        
        # 昨天的实时数据
        yesterday = (today - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        realtime_data = {
            'trade_date': yesterday,
            'close': 200.0
        }
        
        result = merge_realtime_to_df(df, realtime_data, "10:00")
        
        # 不应该合并
        assert len(result) == len(df)


class TestDataConsistency:
    """测试数据一致性"""
    
    def test_realtime_file_structure(self):
        """测试实时数据文件结构"""
        filepath = get_todays_realtime_file()
        if not filepath:
            print("跳过: 没有实时数据文件")
            return
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 验证顶层字段
        assert 'fetch_time' in data, "缺少 fetch_time"
        assert 'data' in data, "缺少 data"
        
        # 验证 fetch_time 格式
        fetch_time = data['fetch_time']
        assert len(fetch_time) == 15  # YYYYMMDD_HHMMSS
        assert fetch_time[8] == '_'
        
        # 验证 data 是数组
        assert isinstance(data['data'], list)
        assert len(data['data']) > 0
        
        # 验证第一条记录结构
        first = data['data'][0]
        assert 'symbol' in first, "记录缺少 symbol"
        assert 'close' in first, "记录缺少 close"
        
        print(f"\n文件结构验证通过:")
        print(f"  fetch_time: {fetch_time}")
        print(f"  记录数: {len(data['data'])}")
        print(f"  第一条记录symbol: {first.get('symbol')}")
    
    def test_symbol_format_consistency(self):
        """测试symbol格式一致性"""
        df = load_realtime_data()
        if df.empty:
            print("跳过: 没有实时数据")
            return
        
        # 检查symbol格式（应该包含 .SH/.SZ/.BJ）
        symbols = df['symbol'].tolist()
        valid_suffixes = ['.SH', '.SZ', '.BJ']
        
        for sym in symbols[:10]:  # 检查前10个
            assert any(sym.endswith(s) for s in valid_suffixes), \
                f"symbol格式错误: {sym}"
        
        print(f"\nSymbol格式检查通过，示例: {symbols[:3]}")


def run_tests():
    """直接运行测试（不依赖pytest）"""
    print("=" * 60)
    print("实时数据功能测试")
    print("=" * 60)
    
    test_classes = [
        TestRealtimeDataAccess(),
        TestRealtimeMerge(),
        TestDataConsistency()
    ]
    
    for cls in test_classes:
        print(f"\n{'='*60}")
        print(f"测试类: {cls.__class__.__name__}")
        print('='*60)
        
        for method_name in dir(cls):
            if method_name.startswith('test_'):
                print(f"\n  运行: {method_name}...")
                try:
                    getattr(cls, method_name)()
                    print(f"  ✓ 通过")
                except AssertionError as e:
                    print(f"  ✗ 失败: {e}")
                except Exception as e:
                    print(f"  ✗ 错误: {type(e).__name__}: {e}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    run_tests()

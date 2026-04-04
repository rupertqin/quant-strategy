"""
股票代码工具类离线单元测试（不依赖网络）
"""

import sys
import os

# 添加项目根路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib.utils.stock_code import StockCodeUtil


def test_with_mock_mapper():
    """使用模拟数据测试名称获取"""
    print("\n=== 使用模拟数据测试 ===")
    
    # 创建模拟映射表
    mock_mapper = {
        '600519': '贵州茅台',
        '300001': '特锐德',
        '603618': '杭电股份',
        '688001': '测试科创板',
    }
    
    # 临时替换映射表
    original_mapper = StockCodeUtil.get_name_mapper
    StockCodeUtil.get_name_mapper = lambda: mock_mapper
    
    try:
        # 测试 get_name
        test_cases = [
            ('600519.SH', '贵州茅台'),
            ('600519', '贵州茅台'),
            ('300001.SZ', '特锐德'),
            ('603618.SH', '杭电股份'),
            ('999999.SH', ''),  # 不存在
        ]
        
        passed = 0
        failed = 0
        
        for input_val, expected in test_cases:
            result = StockCodeUtil.get_name(input_val)
            status = "✅" if result == expected else "❌"
            if result == expected:
                passed += 1
            else:
                failed += 1
            print(f"{status} get_name('{input_val}') = '{result}' (期望: '{expected}')")
        
        print(f"\n结果: {passed} 通过, {failed} 失败")
        
        # 测试 format_display
        print("\n=== 测试 format_display ===")
        format_tests = [
            ('600519.SH', True, '600519.SH(贵州茅台)'),
            ('300001', True, '300001.SZ(特锐德)'),
            ('603618.SH', False, '603618.SH'),  # 不包含名称
        ]
        
        for input_val, include_name, expected in format_tests:
            result = StockCodeUtil.format_display(input_val, include_name)
            status = "✅" if result == expected else "❌"
            print(f"{status} format_display('{input_val}', {include_name}) = '{result}'")
            if result == expected:
                passed += 1
            else:
                failed += 1
        
        print(f"\n总计: {passed} 通过, {failed} 失败")
        return failed == 0
        
    finally:
        # 恢复原始方法
        StockCodeUtil.get_name_mapper = original_mapper


if __name__ == '__main__':
    print("=" * 60)
    print("股票代码工具类离线单元测试")
    print("=" * 60)
    
    success = test_with_mock_mapper()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 所有测试通过！")
        sys.exit(0)
    else:
        print("⚠️ 有测试失败")
        sys.exit(1)

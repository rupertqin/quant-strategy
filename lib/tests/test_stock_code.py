"""
股票代码工具类单元测试
"""

import sys
import os

# 添加项目根路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib.utils.stock_code import StockCodeUtil, get_stock_name, format_stock, normalize_code


def test_extract():
    """测试代码提取功能"""
    print("\n=== 测试 extract() ===")
    
    test_cases = [
        ('600519.SH', '600519'),
        ('600519', '600519'),
        ('贵州茅台(600519)', '600519'),
        ('sh600519', '600519'),
        ('sz300001', '300001'),
        ('300001.SZ', '300001'),
        ('invalid', None),
        ('', None),
        (None, None),
        ('603618.SH', '603618'),
        ('688001.SH', '688001'),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected in test_cases:
        result = StockCodeUtil.extract(input_val)
        status = "✅" if result == expected else "❌"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} extract('{input_val}') = '{result}' (期望: '{expected}')")
    
    print(f"\n结果: {passed} 通过, {failed} 失败")
    return failed == 0


def test_get_exchange():
    """测试交易所判断"""
    print("\n=== 测试 get_exchange() ===")
    
    test_cases = [
        ('600519.SH', 'SH'),
        ('600519', 'SH'),
        ('300001.SZ', 'SZ'),
        ('300001', 'SZ'),
        ('688001.SH', 'SH'),
        ('430001.BJ', 'BJ'),
        ('invalid', None),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected in test_cases:
        result = StockCodeUtil.get_exchange(input_val)
        status = "✅" if result == expected else "❌"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} get_exchange('{input_val}') = '{result}' (期望: '{expected}')")
    
    print(f"\n结果: {passed} 通过, {failed} 失败")
    return failed == 0


def test_with_suffix():
    """测试添加后缀"""
    print("\n=== 测试 with_suffix() ===")
    
    test_cases = [
        ('600519', '600519.SH'),
        ('600519.SH', '600519.SH'),
        ('300001', '300001.SZ'),
        ('300001.SZ', '300001.SZ'),
        ('688001', '688001.SH'),
        ('invalid', None),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected in test_cases:
        result = StockCodeUtil.with_suffix(input_val)
        status = "✅" if result == expected else "❌"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} with_suffix('{input_val}') = '{result}' (期望: '{expected}')")
    
    print(f"\n结果: {passed} 通过, {failed} 失败")
    return failed == 0


def test_get_name():
    """测试获取股票名称（需要网络）"""
    print("\n=== 测试 get_name() ===")
    
    # 先获取映射表
    mapper = StockCodeUtil.get_name_mapper()
    print(f"映射表大小: {len(mapper)}")
    
    if len(mapper) == 0:
        print("⚠️ 无法获取映射表（可能是网络问题），跳过名称测试")
        return True
    
    # 显示前5条映射
    print("\n前5条映射示例:")
    for i, (code, name) in enumerate(list(mapper.items())[:5]):
        print(f"  {code} -> {name}")
    
    # 测试茅台
    test_cases = [
        ('600519.SH', '贵州茅台'),
        ('600519', '贵州茅台'),
    ]
    
    passed = 0
    failed = 0
    
    for input_val, expected in test_cases:
        result = StockCodeUtil.get_name(input_val)
        # 模糊匹配：检查是否包含关键字
        is_match = expected in result if result else False
        status = "✅" if is_match else "❌"
        if is_match:
            passed += 1
        else:
            failed += 1
        print(f"{status} get_name('{input_val}') = '{result}' (期望包含: '{expected}')")
    
    print(f"\n结果: {passed} 通过, {failed} 失败")
    return failed == 0


def test_format_display():
    """测试格式化显示"""
    print("\n=== 测试 format_display() ===")
    
    # 需要网络获取名称
    mapper = StockCodeUtil.get_name_mapper()
    
    if len(mapper) == 0:
        print("⚠️ 无法获取映射表，跳过格式化测试")
        return True
    
    result = StockCodeUtil.format_display('600519.SH')
    print(f"format_display('600519.SH') = '{result}'")
    
    # 检查是否包含代码和名称
    has_code = '600519' in result
    has_name = '茅台' in result  # 贵州茅台
    
    status = "✅" if has_code and has_name else "❌"
    print(f"{status} 包含代码: {has_code}, 包含名称: {has_name}")
    
    return has_code and has_name


def test_is_same():
    """测试代码比较"""
    print("\n=== 测试 is_same() ===")
    
    test_cases = [
        (('600519.SH', '600519'), True),
        (('600519', '600519.SH'), True),
        (('600519.SH', '600519.SH'), True),
        (('600519.SH', '300001.SZ'), False),
        (('invalid', '600519'), False),
    ]
    
    passed = 0
    failed = 0
    
    for (code1, code2), expected in test_cases:
        result = StockCodeUtil.is_same(code1, code2)
        status = "✅" if result == expected else "❌"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} is_same('{code1}', '{code2}') = {result} (期望: {expected})")
    
    print(f"\n结果: {passed} 通过, {failed} 失败")
    return failed == 0


def test_convenience_functions():
    """测试便捷函数"""
    print("\n=== 测试便捷函数 ===")
    
    # normalize_code
    result = normalize_code('600519.SH')
    print(f"normalize_code('600519.SH') = '{result}'")
    assert result == '600519', "normalize_code 失败"
    
    print("✅ 便捷函数测试通过")
    return True


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("股票代码工具类单元测试")
    print("=" * 60)
    
    results = []
    
    results.append(test_extract())
    results.append(test_get_exchange())
    results.append(test_with_suffix())
    results.append(test_is_same())
    results.append(test_convenience_functions())
    results.append(test_get_name())
    results.append(test_format_display())
    
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"总共: {total} 个测试组")
    print(f"通过: {passed} 个")
    print(f"失败: {total - passed} 个")
    
    if all(results):
        print("\n🎉 所有测试通过！")
        return 0
    else:
        print("\n⚠️ 有测试失败")
        return 1


if __name__ == '__main__':
    exit_code = run_all_tests()
    sys.exit(exit_code)

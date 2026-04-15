#!/usr/bin/env python3
"""
回测框架测试
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime, timedelta


def test_scoring_functions():
    """测试评分函数"""
    sys.path.insert(0, str(project_root / 'Dashboard'))
    from utils.scoring import calculate_stock_score, get_score_color, get_score_label
    
    # 测试评分计算
    signals = [
        {'strength': 'strong', 'score': 85, 'period': 'daily', 'signal_type': 'left', 'signal_name': 'MACD底背离'},
        {'strength': 'medium', 'score': 70, 'period': 'daily', 'signal_type': 'left', 'signal_name': 'KDJ底背离'},
    ]
    
    score = calculate_stock_score(signals)
    assert isinstance(score, int), f"评分应为整数，实际是 {type(score)}"
    assert 0 <= score <= 100, f"评分应在0-100之间，实际是 {score}"
    print(f"✓ 评分计算测试通过: {score}")
    
    # 测试颜色函数
    color = get_score_color(90)
    assert isinstance(color, str), "颜色应为字符串"
    print(f"✓ 颜色函数测试通过: {color}")
    
    # 测试标签函数
    label = get_score_label(90)
    assert isinstance(label, str), "标签应为字符串"
    print(f"✓ 标签函数测试通过: {label}")


def test_load_signals():
    """测试信号加载"""
    # 支持直接运行和包内导入
    try:
        from .scoring_validator import ScoringValidator
    except ImportError:
        from scoring_validator import ScoringValidator
    
    validator = ScoringValidator()
    
    # 测试加载最新信号
    signals = validator.load_signals()
    assert isinstance(signals, list), "信号应为列表"
    print(f"✓ 信号加载测试通过: 加载到 {len(signals)} 个信号")
    
    # 检查数据质量
    valid_dates = 0
    invalid_dates = 0
    for s in signals:
        date = s.get('trigger_date', '')
        if date and len(str(date)) == 10 and str(date)[4] == '-':
            valid_dates += 1
        else:
            invalid_dates += 1
    
    print(f"  数据质量: {valid_dates} 个有效日期, {invalid_dates} 个异常日期")
    
    if invalid_dates > 0:
        print(f"  ⚠ 警告: 有 {invalid_dates/len(signals)*100:.1f}% 的信号日期格式异常")
        print(f"    这会导致这些信号无法计算未来收益")
        print(f"    建议: 重新运行信号扫描器生成正确的日期")
    
    return len(signals) > 0


def test_calculate_future_returns():
    """测试未来收益计算"""
    try:
        from .scoring_validator import ScoringValidator
    except ImportError:
        from scoring_validator import ScoringValidator
    
    validator = ScoringValidator()
    
    # 找一个有数据的日期
    signals = validator.load_signals()
    if not signals:
        print("⚠ 没有信号数据，跳过收益计算测试")
        return False
    
    # 取第一个信号测试
    test_signal = signals[0]
    symbol = test_signal['symbol']
    signal_date = test_signal['trigger_date']
    
    print(f"测试股票: {symbol}, 信号日期: {signal_date}")
    
    returns = validator.calculate_future_returns(symbol, signal_date)
    
    assert isinstance(returns, dict), "收益应为字典"
    print(f"✓ 收益计算测试通过: {returns}")
    
    return True


def test_full_backtest():
    """测试完整回测流程"""
    try:
        from .scoring_validator import ScoringValidator
    except ImportError:
        from scoring_validator import ScoringValidator
    
    validator = ScoringValidator()
    
    # 运行回测
    results = validator.run_backtest()
    
    if not results:
        print("⚠ 回测无结果，可能缺少数据")
        return False
    
    assert len(results) > 0, "回测应有结果"
    print(f"✓ 回测运行测试通过: {len(results)} 只股票")
    
    # 测试分桶分析
    analysis = validator.analyze_by_score_bucket()
    assert isinstance(analysis, pd.DataFrame), "分析结果应为DataFrame"
    print(f"✓ 分桶分析测试通过: {len(analysis)} 个分桶")
    
    # 测试生成报告
    report = validator.generate_report()
    assert isinstance(report, str), "报告应为字符串"
    assert "回测验证报告" in report, "报告应包含标题"
    print(f"✓ 报告生成测试通过")
    
    return True


def main():
    """运行所有测试"""
    print("=" * 60)
    print("回测框架测试")
    print("=" * 60)
    
    try:
        print("\n1. 测试评分函数...")
        test_scoring_functions()
        
        print("\n2. 测试信号加载...")
        has_signals = test_load_signals()
        
        if has_signals:
            print("\n3. 测试收益计算...")
            test_calculate_future_returns()
            
            print("\n4. 测试完整回测...")
            test_full_backtest()
        else:
            print("\n⚠ 跳过需要信号的测试")
        
        print("\n" + "=" * 60)
        print("✓ 所有测试通过!")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

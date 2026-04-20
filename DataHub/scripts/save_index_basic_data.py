#!/usr/bin/env python3
"""
保存指数基本数据到 CSV

生成两个文件：
1. index_definitions.csv - 指数定义规则
2. index_constituents.csv - 各指数成分股列表
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from DataHub.core.index_calculator import IndexCalculator, IndexDefinition


def save_index_definitions(output_dir: Path):
    """保存指数定义规则到 CSV"""
    print("=" * 60)
    print("保存指数定义规则...")
    
    rules = IndexDefinition.RULES
    data = []
    
    for index_name, rule in rules.items():
        data.append({
            'index_name': index_name,
            'code_pattern': rule.get('code_pattern', ''),
            'exchange': rule.get('exchange', ''),
            'top_n': rule.get('top_n', ''),
            'exclude_top_n': rule.get('exclude_top_n', ''),
            'description': rule.get('description', '')
        })
    
    df = pd.DataFrame(data)
    output_path = output_dir / 'index_definitions.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✓ 已保存: {output_path}")
    print(f"  包含 {len(df)} 个指数定义")
    return df


def save_index_constituents(output_dir: Path):
    """保存各指数成分股列表到 CSV"""
    print("\n" + "=" * 60)
    print("保存指数成分股列表...")
    
    # 加载股票基础信息
    stock_basic_path = project_root / "storage" / "stock_basic_info.csv"
    if not stock_basic_path.exists():
        print(f"✗ 股票基础信息文件不存在: {stock_basic_path}")
        return None
    
    stocks_df = pd.read_csv(stock_basic_path)
    print(f"  已加载 {len(stocks_df)} 只股票基础信息")
    
    # 初始化指数计算器
    calc = IndexCalculator()
    
    all_constituents = []
    
    for index_name in IndexDefinition.list_indices():
        # 筛选成分股
        constituents = calc.filter_constituents(stocks_df, index_name)
        
        if constituents.empty:
            print(f"  ⚠ {index_name}: 无成分股")
            continue
        
        # 获取代码列名
        code_col = 'symbol' if 'symbol' in constituents.columns else constituents.columns[0]
        
        for _, row in constituents.iterrows():
            all_constituents.append({
                'index_name': index_name,
                'symbol': row.get(code_col, ''),
                'name': row.get('name', ''),
                'exchange': row.get('exchange', ''),
                'industry': row.get('industry', ''),
            })
        
        print(f"  ✓ {index_name}: {len(constituents)} 只成分股")
    
    df = pd.DataFrame(all_constituents)
    output_path = output_dir / 'index_constituents.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✓ 已保存: {output_path}")
    print(f"  共 {len(df)} 条成分股记录")
    return df


def main():
    """主函数"""
    # 输出目录
    output_dir = project_root / "storage" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("保存指数基本数据到 CSV")
    print("=" * 60)
    
    # 保存指数定义
    definitions_df = save_index_definitions(output_dir)
    
    # 保存成分股列表
    constituents_df = save_index_constituents(output_dir)
    
    # 打印统计信息
    print("\n" + "=" * 60)
    print("统计信息:")
    print("=" * 60)
    
    if definitions_df is not None:
        print("\n指数定义:")
        for _, row in definitions_df.iterrows():
            print(f"  • {row['index_name']}: {row['description']}")
    
    if constituents_df is not None:
        print("\n成分股统计:")
        stats = constituents_df.groupby('index_name').size()
        for index_name, count in stats.items():
            print(f"  • {index_name}: {count} 只")
    
    print("\n" + "=" * 60)
    print(f"文件已保存到: {output_dir}")
    print("  - index_definitions.csv")
    print("  - index_constituents.csv")
    print("=" * 60)


if __name__ == "__main__":
    main()

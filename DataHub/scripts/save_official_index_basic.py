#!/usr/bin/env python3
"""
保存官方指数基本数据到 CSV

通过 akshare 接口获取 00、99 开头的官方指数列表
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import akshare as ak


def get_index_list_from_sina():
    """从新浪获取指数列表"""
    print("从新浪获取指数列表...")
    try:
        df = ak.stock_zh_index_spot_sina()
        print(f"  获取到 {len(df)} 条指数数据")
        return df
    except Exception as e:
        print(f"  获取失败: {e}")
        return pd.DataFrame()


def get_index_list_from_em():
    """从东方财富获取指数列表"""
    print("从东方财富获取指数列表...")
    try:
        df = ak.stock_zh_index_spot_em()
        print(f"  获取到 {len(df)} 条指数数据")
        return df
    except Exception as e:
        print(f"  获取失败: {e}")
        return pd.DataFrame()


def process_and_save_indices(output_dir: Path):
    """处理并保存指数数据"""
    
    # 尝试多个数据源
    df = get_index_list_from_sina()
    
    if df.empty:
        df = get_index_list_from_em()
    
    if df.empty:
        print("✗ 无法从任何数据源获取指数数据")
        return None
    
    print(f"\n处理数据...")
    
    # 标准化列名
    column_mapping = {
        '代码': 'code',
        '名称': 'name',
        '最新价': 'price',
        '涨跌额': 'change_amount',
        '涨跌幅': 'change_pct',
        '昨收': 'prev_close',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
    }
    
    # 重命名存在的列
    for old, new in column_mapping.items():
        if old in df.columns:
            df[new] = df[old]
    
    # 确定代码列
    code_col = None
    for col in ['code', 'symbol', '代码']:
        if col in df.columns:
            code_col = col
            break
    
    if not code_col:
        print("✗ 无法找到代码列")
        return None
    
    # 提取标准格式的 symbol
    df['symbol'] = df[code_col].astype(str).str.strip()
    
    # 新浪返回的代码格式是 sh000001 / sz399001
    # 需要转换为标准格式 000001.SH / 399001.SZ
    def normalize_symbol(code):
        code = str(code).strip().lower()
        if code.startswith('sh'):
            return f"{code[2:]}.SH"
        elif code.startswith('sz'):
            return f"{code[2:]}.SZ"
        # 如果已经是纯数字，根据规则添加后缀
        if code.isdigit():
            if code.startswith('000') or code.startswith('6'):
                return f"{code}.SH"
            else:
                return f"{code}.SZ"
        return code
    
    df['symbol'] = df['symbol'].apply(normalize_symbol)
    
    # 提取纯数字代码
    df['code'] = df['symbol'].str.extract(r'(\d+)')[0]
    
    # 确定市场
    def get_market(symbol):
        if symbol.endswith('.SH'):
            return '上海'
        elif symbol.endswith('.SZ'):
            return '深圳'
        return '其他'
    
    df['market'] = df['symbol'].apply(get_market)
    
    # 分类指数
    def classify_index(row):
        code = str(row['code'])
        name = str(row.get('name', ''))
        
        # 上证指数系列
        if code.startswith('000'):
            if code in ['000001', '000002', '000003']:
                return '上证综指系列'
            elif code in ['000016', '000010', '000009', '000133']:
                return '上证规模指数'
            else:
                return '中证规模指数'
        
        # 深证规模指数
        if code.startswith('3990') or code in ['399001', '399002', '399003', '399004', '399005', '399006']:
            return '深证规模指数'
        
        # 国证行业指数
        if code.startswith('3996'):
            return '国证行业指数'
        
        # 深证主题/行业指数
        if code.startswith('399'):
            return '深证主题指数'
        
        return '其他'
    
    df['category'] = df.apply(classify_index, axis=1)
    
    # 选择需要的列
    output_columns = ['symbol', 'code', 'name', 'market', 'category']
    
    # 添加价格相关列（如果存在）
    for col in ['price', 'change_pct', 'volume', 'amount']:
        if col in df.columns:
            output_columns.append(col)
    
    df_output = df[output_columns].copy()
    
    # 保存到 CSV
    output_path = output_dir / 'official_indices.csv'
    df_output.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✓ 已保存: {output_path}")
    print(f"  共 {len(df_output)} 个指数")
    
    # 统计
    print("\n按分类统计:")
    stats = df_output.groupby('category').size().sort_values(ascending=False)
    for category, count in stats.items():
        print(f"  • {category}: {count} 个")
    
    print("\n按市场统计:")
    market_stats = df_output.groupby('market').size()
    for market, count in market_stats.items():
        print(f"  • {market}: {count} 个")
    
    return df_output


def main():
    """主函数"""
    from DataHub.config import get_storage_path
    output_dir = get_storage_path()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("保存官方指数基本数据到 CSV")
    print("=" * 60)
    
    df = process_and_save_indices(output_dir)
    
    if df is not None:
        print("\n" + "=" * 60)
        print("前10个指数示例:")
        print(df.head(10).to_string(index=False))
        print("=" * 60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ETF列表管理器 - 管理ETF基础信息

功能：
1. 从 akshare 获取ETF列表
2. 生成 etf_basic_info.csv

使用方式:
    python -m DataHub.scripts.etf_list_manager           # 更新ETF列表
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from DataHub.config import STORAGE_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_etf_list() -> pd.DataFrame:
    """
    从 akshare 获取ETF列表
    
    Returns:
        DataFrame with columns: symbol, name, exchange
    """
    logger.info("从 akshare 获取ETF列表...")
    
    try:
        import akshare as ak
        
        # 获取ETF实时行情作为列表
        df = ak.fund_etf_spot_em()
        
        # 添加symbol列（带后缀）
        df['symbol'] = df['代码'].apply(
            lambda x: f"{x}.SH" if str(x).startswith('5') else f"{x}.SZ"
        )
        
        # 选择需要的列
        result = pd.DataFrame({
            'symbol': df['symbol'],
            'name': df['名称'],
            'exchange': df['代码'].apply(lambda x: 'SH' if str(x).startswith('5') else 'SZ'),
        })
        
        logger.info(f"获取到 {len(result)} 只ETF")
        return result
        
    except Exception as e:
        logger.error(f"获取ETF列表失败: {e}")
        # 返回默认ETF列表
        default_etfs = [
            ('510300.SH', '华泰柏瑞沪深300ETF', 'SH'),
            ('510050.SH', '华夏上证50ETF', 'SH'),
            ('510500.SH', '南方中证500ETF', 'SH'),
            ('588080.SH', '易方达上证科创板50ETF', 'SH'),
            ('588000.SH', '华夏上证科创板50ETF', 'SH'),
            ('159915.SZ', '易方达创业板ETF', 'SZ'),
            ('159949.SZ', '华安创业板50ETF', 'SZ'),
            ('159998.SZ', '汇添富中证芯片ETF', 'SZ'),
            ('512880.SH', '国泰中证全指证券公司ETF', 'SH'),
            ('515050.SH', '华夏中证5G通信主题ETF', 'SH'),
            ('512690.SH', '鹏华中证酒ETF', 'SH'),
            ('512000.SH', '华宝中证医疗ETF', 'SH'),
            ('513100.SH', '国泰纳斯达克100ETF', 'SH'),
            ('513310.SH', '华泰柏瑞中韩半导体ETF', 'SH'),
            ('510720.SH', '汇添富上证综合ETF', 'SH'),
            ('159919.SZ', '嘉实沪深300ETF', 'SZ'),
            ('159901.SZ', '易方达深证100ETF', 'SZ'),
            ('159905.SZ', '深红利ETF', 'SZ'),
        ]
        return pd.DataFrame(default_etfs, columns=['symbol', 'name', 'exchange'])


def save_etf_list(df: pd.DataFrame) -> bool:
    """保存ETF列表到CSV"""
    try:
        csv_path = STORAGE_DIR / "etf_basic_info.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"ETF列表已保存: {csv_path} ({len(df)} 只)")
        return True
    except Exception as e:
        logger.error(f"保存ETF列表失败: {e}")
        return False


def main():
    """主函数"""
    # 获取ETF列表
    df = fetch_etf_list()
    
    if df.empty:
        logger.error("获取ETF列表失败")
        return
    
    # 保存
    if save_etf_list(df):
        print(f"\n✅ ETF列表更新完成: {len(df)} 只")
        print("\n前10只ETF:")
        print(df.head(10).to_string(index=False))
    else:
        print("\n❌ ETF列表更新失败")


if __name__ == '__main__':
    main()

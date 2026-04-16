#!/usr/bin/env python3
"""
股票列表管理器 - 统一管理新增和退市股票

功能：
1. 从 akshare 获取最新A股列表
2. 对比本地 stock_basic_info.csv 找出新增、退市、更名股票
3. 更新 stock_basic_info.csv
4. 清理退市股票的价格数据文件
5. 为新增股票准备数据文件结构

数据源规范：
- 使用 ak.stock_zh_a_spot() (sina源) 获取实时列表
- 避免使用 stock_zh_a_spot_em() (东财源)，防止IP限制

执行频率：建议每周运行一次，或每日收盘后运行

使用方式:
    python -m DataHub.scripts.stock_list_manager           # 查看变化但不执行
    python -m DataHub.scripts.stock_list_manager --apply   # 执行更新和清理
    python -m DataHub.scripts.stock_list_manager --cleanup-only  # 仅清理退市股票数据
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Set
import pandas as pd

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.utils.stock_code import StockCodeUtil
from DataHub.config import RAW_PRICES_DIR, RAW_ADJUST_FACTORS_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockListManager:
    """股票列表管理器"""
    
    def __init__(self, apply_changes: bool = False):
        """
        初始化管理器
        
        Args:
            apply_changes: 是否执行实际变更，False 仅预览
        """
        self.apply_changes = apply_changes
        self.base_dir = project_root
        self.storage_dir = self.base_dir / "storage"
        self.csv_path = self.storage_dir / "stock_basic_info.csv"
        self.prices_dir = RAW_PRICES_DIR
        self.adjust_dir = RAW_ADJUST_FACTORS_DIR
        
        # 确保目录存在
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.prices_dir.mkdir(parents=True, exist_ok=True)
        self.adjust_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载本地数据
        self.local_df = self._load_local_stock_list()
        
    def _load_local_stock_list(self) -> pd.DataFrame:
        """加载本地股票列表"""
        if not self.csv_path.exists():
            logger.warning(f"本地股票列表不存在: {self.csv_path}")
            return pd.DataFrame(columns=[
                'symbol', 'name', 'exchange', 'industry', 'industry_classification',
                'ipo_date', 'out_date', 'status', 'security_type', 
                'industry_update_date', 'update_time', 'data_source'
            ])
        
        df = pd.read_csv(self.csv_path, dtype={'symbol': str})
        logger.info(f"加载本地股票列表: {len(df)} 只")
        return df
    
    def fetch_latest_stock_list(self) -> pd.DataFrame:
        """
        从 akshare 获取最新股票列表
        
        Returns:
            DataFrame with columns: symbol, name, exchange, ipo_date, status
        """
        logger.info("从 akshare 获取最新股票列表...")
        
        try:
            import akshare as ak
            
            # 获取A股列表（使用 sina 源，更稳定）
            df = ak.stock_zh_a_spot()
            
            # 调试：打印列名
            logger.debug(f"akshare 返回列名: {df.columns.tolist()}")
            
            # 转换为标准格式
            stocks = []
            
            for _, row in df.iterrows():
                # sina源使用'代码'列，格式如'bj920000', 'sh600000'
                raw_code = str(row.get('代码', ''))
                name = row.get('名称', '')
                
                if not raw_code or raw_code == 'nan':
                    continue
                
                # 使用 StockCodeUtil 统一解析带前缀的代码
                code, exchange = StockCodeUtil.parse_prefixed_code(raw_code)
                
                if not code or not exchange:
                    continue
                
                symbol = StockCodeUtil.with_suffix(code)
                
                stocks.append({
                    'symbol': symbol,
                    'code': code,
                    'name': name,
                    'exchange': exchange,
                    'status': 1  # 默认正常状态
                })
            
            result_df = pd.DataFrame(stocks)
            logger.info(f"获取到最新股票列表: {len(result_df)} 只")
            return result_df
            
        except Exception as e:
            logger.error(f"获取最新股票列表失败: {e}")
            raise
    
    def analyze_changes(self, latest_df: pd.DataFrame) -> Dict:
        """
        分析股票列表变化
        
        Args:
            latest_df: 最新股票列表
            
        Returns:
            Dict with keys: new_listings, delisted, renamed
        """
        local_symbols = set(self.local_df['symbol'].tolist()) if not self.local_df.empty else set()
        latest_symbols = set(latest_df['symbol'].tolist())
        
        # 1. 新增股票（在 latest 中但不在 local 中）
        new_symbols = latest_symbols - local_symbols
        new_listings = latest_df[latest_df['symbol'].isin(new_symbols)].copy()
        
        # 2. 退市股票（在 local 中但不在 latest 中，且 status=1）
        if not self.local_df.empty:
            active_local = self.local_df[self.local_df['status'] == 1]
            active_local_symbols = set(active_local['symbol'].tolist())
            delisted_symbols = active_local_symbols - latest_symbols
            delisted = self.local_df[self.local_df['symbol'].isin(delisted_symbols)].copy()
        else:
            delisted = pd.DataFrame()
        
        # 3. 更名股票（symbol 相同但 name 不同）
        renamed = []
        if not self.local_df.empty:
            merged = latest_df.merge(
                self.local_df[['symbol', 'name']], 
                on='symbol', 
                how='inner',
                suffixes=('_new', '_old')
            )
            renamed = merged[merged['name_new'] != merged['name_old']]
        
        return {
            'new_listings': new_listings,
            'delisted': delisted,
            'renamed': renamed
        }
    
    def update_stock_list(self, changes: Dict) -> None:
        """
        更新股票列表 CSV
        
        Args:
            changes: analyze_changes 返回的变化字典
        """
        today_str = datetime.now().strftime('%Y-%m-%d')
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        updated_df = self.local_df.copy() if not self.local_df.empty else pd.DataFrame()
        
        # 1. 处理新增股票
        new_listings = changes['new_listings']
        if not new_listings.empty:
            logger.info(f"\n新增股票 ({len(new_listings)} 只):")
            for _, row in new_listings.iterrows():
                logger.info(f"  + {row['symbol']} {row['name']}")
            
            if self.apply_changes:
                # 创建新记录
                new_records = []
                for _, row in new_listings.iterrows():
                    new_records.append({
                        'symbol': row['symbol'],
                        'name': row['name'],
                        'exchange': row['exchange'],
                        'industry': '',
                        'industry_classification': '',
                        'ipo_date': today_str,  # 使用今天作为IPO日期（实际应该查询详细信息）
                        'out_date': '',
                        'status': 1,
                        'security_type': 1,
                        'industry_update_date': '',
                        'update_time': now_str,
                        'data_source': 'akshare'
                    })
                
                new_df = pd.DataFrame(new_records)
                updated_df = pd.concat([updated_df, new_df], ignore_index=True)
        
        # 2. 处理退市股票
        delisted = changes['delisted']
        if not delisted.empty:
            logger.info(f"\n退市股票 ({len(delisted)} 只):")
            for _, row in delisted.iterrows():
                logger.info(f"  - {row['symbol']} {row['name']}")
            
            if self.apply_changes:
                # 更新状态
                for symbol in delisted['symbol']:
                    mask = updated_df['symbol'] == symbol
                    updated_df.loc[mask, 'status'] = 0
                    updated_df.loc[mask, 'out_date'] = today_str
                    updated_df.loc[mask, 'update_time'] = now_str
        
        # 3. 处理更名股票
        renamed = changes['renamed']
        if not renamed.empty:
            logger.info(f"\n更名股票 ({len(renamed)} 只):")
            for _, row in renamed.iterrows():
                logger.info(f"  ~ {row['symbol']}: {row['name_old']} -> {row['name_new']}")
            
            if self.apply_changes:
                for _, row in renamed.iterrows():
                    mask = updated_df['symbol'] == row['symbol']
                    updated_df.loc[mask, 'name'] = row['name_new']
                    updated_df.loc[mask, 'update_time'] = now_str
        
        # 保存更新后的列表
        if self.apply_changes:
            updated_df.to_csv(self.csv_path, index=False)
            logger.info(f"\n已更新股票列表: {self.csv_path}")
            logger.info(f"  总计: {len(updated_df)} 只 (正常: {len(updated_df[updated_df['status']==1])}, 退市: {len(updated_df[updated_df['status']==0])})")
    
    def cleanup_delisted_data(self, delisted_df: pd.DataFrame) -> None:
        """
        清理退市股票的数据文件
        
        Args:
            delisted_df: 退市股票 DataFrame
        """
        if delisted_df.empty:
            return
        
        logger.info(f"\n清理退市股票数据文件...")
        
        cleanup_count = 0
        for _, row in delisted_df.iterrows():
            symbol = row['symbol']
            
            # 清理价格数据
            price_file = self.prices_dir / f"{symbol}.parquet"
            if price_file.exists():
                if self.apply_changes:
                    price_file.unlink()
                    logger.info(f"  删除价格文件: {price_file}")
                else:
                    logger.info(f"  [预览] 将删除价格文件: {price_file}")
                cleanup_count += 1
            
            # 清理复权因子
            adjust_file = self.adjust_dir / f"{symbol}.parquet"
            if adjust_file.exists():
                if self.apply_changes:
                    adjust_file.unlink()
                    logger.info(f"  删除复权因子: {adjust_file}")
                else:
                    logger.info(f"  [预览] 将删除复权因子: {adjust_file}")
        
        if cleanup_count == 0:
            logger.info("  没有需要清理的数据文件")
        else:
            action = "已清理" if self.apply_changes else "预览完成"
            logger.info(f"  {action}: {cleanup_count} 个文件")
    
    def run(self) -> Dict:
        """
        执行完整的股票列表管理流程
        
        Returns:
            变化统计字典
        """
        logger.info("=" * 60)
        logger.info("股票列表管理器")
        logger.info(f"模式: {'执行变更' if self.apply_changes else '仅预览'}")
        logger.info("=" * 60)
        
        # 1. 获取最新列表
        latest_df = self.fetch_latest_stock_list()
        
        # 2. 分析变化
        changes = self.analyze_changes(latest_df)
        
        # 3. 更新股票列表
        self.update_stock_list(changes)
        
        # 4. 清理退市股票数据
        self.cleanup_delisted_data(changes['delisted'])
        
        # 统计
        stats = {
            'total_local': len(self.local_df),
            'total_latest': len(latest_df),
            'new_listings': len(changes['new_listings']),
            'delisted': len(changes['delisted']),
            'renamed': len(changes['renamed'])
        }
        
        logger.info("\n" + "=" * 60)
        logger.info("处理完成")
        logger.info(f"  本地原有: {stats['total_local']} 只")
        logger.info(f"  最新市场: {stats['total_latest']} 只")
        logger.info(f"  新增上市: {stats['new_listings']} 只")
        logger.info(f"  退市股票: {stats['delisted']} 只")
        logger.info(f"  更名股票: {stats['renamed']} 只")
        logger.info("=" * 60)
        
        if not self.apply_changes and (stats['new_listings'] > 0 or stats['delisted'] > 0):
            logger.info("\n提示: 使用 --apply 参数执行实际变更")
        
        return stats


def main():
    parser = argparse.ArgumentParser(
        description='股票列表管理器 - 管理新增和退市股票',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 仅预览变化（默认）
  python -m DataHub.scripts.stock_list_manager
  
  # 执行更新和清理
  python -m DataHub.scripts.stock_list_manager --apply
  
  # 仅清理退市股票数据（不移除CSV中的记录）
  python -m DataHub.scripts.stock_list_manager --cleanup-only
        """
    )
    
    parser.add_argument(
        '--apply', 
        action='store_true',
        help='执行实际变更（默认仅预览）'
    )
    parser.add_argument(
        '--cleanup-only',
        action='store_true',
        help='仅清理退市股票数据文件，不更新股票列表'
    )
    
    args = parser.parse_args()
    
    if args.cleanup_only:
        # 仅清理模式
        manager = StockListManager(apply_changes=True)
        
        # 找出已标记为退市的股票
        if manager.local_df.empty:
            logger.warning("本地股票列表为空")
            return
        
        delisted = manager.local_df[manager.local_df['status'] == 0]
        logger.info(f"发现 {len(delisted)} 只已标记退市的股票")
        
        manager.cleanup_delisted_data(delisted)
    else:
        # 正常运行模式
        manager = StockListManager(apply_changes=args.apply)
        manager.run()


if __name__ == '__main__':
    main()

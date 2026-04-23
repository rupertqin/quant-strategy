"""
数据同步服务 - 统一的数据更新入口

提供定时任务和手动触发两种方式同步数据

注意：价格数据统一由 DataHub.services.sync 管理，本模块只做调度入口
"""

import sys
from pathlib import Path
# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from DataHub.repositories.stock_repository import StockRepository
from DataHub.services.sync.sync_manager import SyncManager

logger = logging.getLogger(__name__)


class DataSyncService:
    """
    数据同步服务
    
    协调各种爬虫，统一管理和调度数据同步任务
    
    注意：股票价格数据统一由 SyncManager 管理（Parquet格式）
    """
    
    def __init__(self, db_path: str = None):
        """
        初始化同步服务
        
        Args:
            db_path: 数据库路径
        """
        self.repository = StockRepository(db_path)
        self.sync_manager = SyncManager(max_workers=1)
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def sync_stock_prices(
        self,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        batch_size: int = 50
    ) -> Dict[str, Any]:
        """
        同步股票价格数据
        
        统一使用 SyncManager（Parquet格式），不再重复实现
        
        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 每批处理的股票数量（暂不使用，由SyncManager内部处理）
            
        Returns:
            同步结果统计
        """
        self.logger.info("开始同步股票价格数据（使用SyncManager）...")
        
        # 如果没有指定股票，获取全部
        if symbols is None:
            symbols = self.sync_manager._get_stock_list(include_bj=False)
            self.logger.info(f"将同步全部 {len(symbols)} 只股票")
        
        if not symbols:
            return {'status': 'failed', 'message': '没有股票需要同步', 'records': 0}
        
        # 设置默认日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        # 使用 SyncManager 底层服务批量同步
        result = self.sync_manager.stock_sync.sync(
            symbols=symbols,
            incremental=True,
            start_date=start_date,
            end_date=end_date
        )
        
        total_records = result.get('success', 0)
        failed_count = result.get('failed', 0)
        status = 'success' if failed_count == 0 else 'partial'
        
        # 记录同步日志
        self.repository.log_sync(
            'stock_daily_price',
            status,
            total_records,
            f"成功: {total_records}, 失败: {failed_count}"
        )
        
        result = {
            'status': status,
            'records': total_records,
            'success': total_records,
            'failed': failed_count,
            'start_date': start_date,
            'end_date': end_date
        }
        
        self.logger.info(f"股票数据同步完成: {result}")
        return result
    
    def sync_latest_prices(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        同步最新价格数据（增量更新）
        
        统一使用 SyncManager 的 sync_daily 增量模式
        """
        self.logger.info("开始同步最新价格数据（使用SyncManager）...")
        
        if symbols:
            result = self.sync_manager.stock_sync.sync(
                symbols=symbols,
                incremental=True
            )
        else:
            result = self.sync_manager.sync_daily(asset_type='stock')
        
        return result
    
    # TODO: ETF数据同步待实现（当前版本暂不启用）
    # def sync_etf_prices(self, symbols: List[str] = None) -> Dict[str, Any]:
    #     pass
    
    def sync_all(self) -> Dict[str, Any]:
        """
        执行全量同步
        
        同步所有类型的数据
        """
        self.logger.info("=" * 60)
        self.logger.info("开始全量数据同步")
        self.logger.info("=" * 60)
        
        results = {}
        
        # 同步股票价格
        self.logger.info("\n同步股票价格...")
        results['stock_prices'] = self.sync_latest_prices()
        
        # 统计
        total_records = results['stock_prices'].get('new_records', 0)
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"全量同步完成，共 {total_records} 条记录")
        self.logger.info("=" * 60)
        
        return {
            'status': 'success',
            'total_records': total_records,
            'details': results
        }
    
    def get_sync_status(self) -> Dict[str, Any]:
        """
        获取同步状态概览
        """
        # 获取表统计
        table_stats = self.repository.get_table_stats()
        
        # 获取最近同步日志
        recent_logs = self.repository.get_sync_logs(limit=5)
        
        return {
            'table_stats': table_stats,
            'recent_logs': recent_logs.to_dict('records') if not recent_logs.empty else [],
            'timestamp': datetime.now().isoformat()
        }


# ==================== 命令行入口 ====================

def main():
    """
    命令行入口
    
    用法:
        python -m DataHub.services.sync_service --help
        python -m DataHub.services.sync_service --all
        python -m DataHub.services.sync_service --latest
        python -m DataHub.services.sync_service --status
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据同步服务')
    parser.add_argument('--all', action='store_true', help='执行全量同步')
    parser.add_argument('--latest', action='store_true', help='同步最新数据（增量）')
    parser.add_argument('--stocks', action='store_true', help='同步股票数据')
    parser.add_argument('--status', action='store_true', help='查看同步状态')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    service = DataSyncService()
    
    if args.all:
        result = service.sync_all()
        print("\n同步结果:")
        print(f"  总记录数: {result['total_records']}")
        print(f"  股票价格: {result['details']['stock_prices'].get('new_records', 0)}")
    
    elif args.latest:
        result = service.sync_latest_prices()
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  记录数: {result.get('records', 0)}")
    
    elif args.stocks:
        result = service.sync_stock_prices()
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  记录数: {result.get('records', 0)}")
    
    elif args.status:
        status = service.get_sync_status()
        print("\n数据库统计:")
        for table, count in status['table_stats'].items():
            print(f"  {table}: {count} 条")
        
        print("\n最近同步记录:")
        for log in status['recent_logs'][:5]:
            print(f"  {log['data_type']}: {log['status']} ({log['records_count']} 条)")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

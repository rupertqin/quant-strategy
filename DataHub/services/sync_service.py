"""
数据同步服务 - 统一的数据更新入口

提供定时任务和手动触发两种方式同步数据
"""

import sys
from pathlib import Path
# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time

from DataHub.repositories.stock_repository import StockRepository
from DataHub.crawlers.stock_price_crawler import StockPriceCrawler
from DataHub.crawlers.etf_crawler import ETFCrawler
from DataHub.config import CRAWLER_REQUEST_DELAY

logger = logging.getLogger(__name__)


class DataSyncService:
    """
    数据同步服务
    
    协调各种爬虫，统一管理和调度数据同步任务
    """
    
    def __init__(self, db_path: str = None):
        """
        初始化同步服务
        
        Args:
            db_path: 数据库路径
        """
        self.repository = StockRepository(db_path)
        self.stock_crawler = StockPriceCrawler(self.repository)
        self.etf_crawler = ETFCrawler(self.repository)
        
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
        
        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 每批处理的股票数量
            
        Returns:
            同步结果统计
        """
        self.logger.info("开始同步股票价格数据...")
        
        # 如果没有指定股票，获取全部
        if symbols is None:
            stocks_df = self.repository.get_all_stocks()
            symbols = stocks_df['symbol'].tolist()
            self.logger.info(f"将同步全部 {len(symbols)} 只股票")
        
        if not symbols:
            return {'status': 'failed', 'message': '没有股票需要同步', 'records': 0}
        
        # 设置默认日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            # 默认同步最近30天
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        total_records = 0
        success_count = 0
        failed_symbols = []
        
        # 分批处理
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            self.logger.info(f"处理批次 {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}, {len(batch)} 只股票")
            
            try:
                result = self.stock_crawler.sync(
                    symbols=batch,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if result['status'] == 'success':
                    total_records += result['records']
                    success_count += len(batch)
                else:
                    failed_symbols.extend(batch)
                    
            except Exception as e:
                self.logger.error(f"批次处理失败: {e}")
                failed_symbols.extend(batch)
            
            # 请求间隔，避免请求过快
            time.sleep(CRAWLER_REQUEST_DELAY)
        
        # 记录同步日志
        status = 'success' if not failed_symbols else 'partial'
        self.repository.log_sync(
            'stock_daily_price',
            status,
            total_records,
            f"成功: {success_count}, 失败: {len(failed_symbols)}"
        )
        
        result = {
            'status': status,
            'records': total_records,
            'success': success_count,
            'failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'start_date': start_date,
            'end_date': end_date
        }
        
        self.logger.info(f"股票数据同步完成: {result}")
        return result
    
    def sync_latest_prices(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        同步最新价格数据（增量更新）
        
        只获取从上次更新到今天的数据
        """
        self.logger.info("开始同步最新价格数据...")
        
        # 获取所有股票
        if symbols is None:
            stocks_df = self.repository.get_all_stocks()
            symbols = stocks_df['symbol'].tolist()
        
        # 确定每只股票需要同步的日期范围
        sync_tasks = []
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        for symbol in symbols:
            latest_date = self.repository.get_latest_price_date(symbol)
            
            if latest_date:
                # 从最新日期的下一天开始
                next_date = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                if next_date <= end_date:
                    sync_tasks.append({
                        'symbol': symbol,
                        'start_date': next_date,
                        'end_date': end_date
                    })
            else:
                # 没有历史数据，同步最近30天
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                sync_tasks.append({
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date
                })
        
        if not sync_tasks:
            self.logger.info("所有股票数据已是最新")
            return {'status': 'success', 'records': 0, 'message': 'Already up to date'}
        
        self.logger.info(f"需要同步 {len(sync_tasks)} 只股票")
        
        # 执行同步
        total_records = 0
        for task in sync_tasks:
            try:
                result = self.stock_crawler.sync(
                    symbol=task['symbol'],
                    start_date=task['start_date'],
                    end_date=task['end_date']
                )
                if result['status'] == 'success':
                    total_records += result['records']
                time.sleep(CRAWLER_REQUEST_DELAY)  # 请求间隔，避免请求过快
            except Exception as e:
                self.logger.error(f"同步 {task['symbol']} 失败: {e}")
        
        return {
            'status': 'success',
            'records': total_records,
            'synced_stocks': len(sync_tasks)
        }
    
    def sync_etf_prices(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        同步ETF价格数据
        """
        self.logger.info("开始同步ETF价格数据...")
        
        if symbols is None:
            etfs_df = self.repository.get_all_etfs()
            symbols = etfs_df['symbol'].tolist()
        
        if not symbols:
            return {'status': 'failed', 'message': '没有ETF需要同步', 'records': 0}
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        result = self.etf_crawler.sync(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date
        )
        
        # 记录日志
        self.repository.log_sync(
            'etf_daily_price',
            result['status'],
            result.get('records', 0),
            result.get('message', '')
        )
        
        return result
    
    def sync_all(self) -> Dict[str, Any]:
        """
        执行全量同步
        
        同步所有类型的数据
        """
        self.logger.info("=" * 60)
        self.logger.info("开始全量数据同步")
        self.logger.info("=" * 60)
        
        results = {}
        
        # 1. 同步股票价格
        self.logger.info("\n[1/2] 同步股票价格...")
        results['stock_prices'] = self.sync_latest_prices()
        
        # 2. 同步ETF价格
        self.logger.info("\n[2/2] 同步ETF价格...")
        results['etf_prices'] = self.sync_etf_prices()
        
        # 统计
        total_records = sum(r.get('records', 0) for r in results.values())
        
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
    parser.add_argument('--stocks', action='store_true', help='只同步股票数据')
    parser.add_argument('--etfs', action='store_true', help='只同步ETF数据')
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
        print(f"  股票价格: {result['details']['stock_prices'].get('records', 0)}")
        print(f"  ETF价格: {result['details']['etf_prices'].get('records', 0)}")
    
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
    
    elif args.etfs:
        result = service.sync_etf_prices()
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

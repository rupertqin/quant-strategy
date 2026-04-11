"""
爬虫基类 - 定义所有爬虫的通用接口
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """
    数据爬虫基类
    
    所有具体爬虫必须继承此类，实现 fetch 和 _save 方法
    """
    
    def __init__(self, repository=None):
        """
        初始化爬虫
        
        Args:
            repository: 数据仓库实例，用于保存数据
        """
        self.repository = repository
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def fetch(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从数据源获取原始数据
        
        Args:
            **kwargs: 不同爬虫需要的参数不同
            
        Returns:
            DataFrame 格式的原始数据，获取失败返回 None
        """
        pass
    
    def sync(self, **kwargs) -> Dict[str, Any]:
        """
        同步数据：获取 -> 清洗 -> 保存
        
        这是主入口方法，封装了完整的数据同步流程
        
        Args:
            **kwargs: 传递给 fetch 方法的参数
            
        Returns:
            {
                'status': 'success' | 'failed' | 'partial',
                'records': int,
                'message': str,
                'start_time': datetime,
                'end_time': datetime
            }
        """
        start_time = datetime.now()
        result = {
            'status': 'failed',
            'records': 0,
            'message': '',
            'start_time': start_time,
            'end_time': None
        }
        
        try:
            # 1. 从接口获取原始数据
            self.logger.info(f"开始获取数据: {kwargs}")
            df = self.fetch(**kwargs)
            
            if df is None or df.empty:
                result['message'] = 'No data fetched from source'
                self.logger.warning(result['message'])
                return result
            
            self.logger.info(f"获取到 {len(df)} 条原始数据")
            
            # 2. 数据清洗
            df = self._clean(df)
            self.logger.info(f"清洗后剩余 {len(df)} 条数据")
            
            # 3. 保存到数据库
            if self.repository:
                count = self._save(df)
                result['records'] = count
                result['status'] = 'success' if count > 0 else 'failed'
                result['message'] = f'Successfully synced {count} records'
                self.logger.info(result['message'])
            else:
                result['status'] = 'success'
                result['records'] = len(df)
                result['message'] = 'Data fetched and cleaned (no repository to save)'
            
        except Exception as e:
            result['message'] = f'Sync failed: {str(e)}'
            self.logger.error(result['message'], exc_info=True)
        
        finally:
            result['end_time'] = datetime.now()
            duration = (result['end_time'] - start_time).total_seconds()
            self.logger.info(f"同步完成，耗时 {duration:.2f} 秒")
        
        return result
    
    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据清洗 - 子类可覆盖
        
        默认实现：
        - 删除完全重复的行
        - 处理明显的异常值
        
        Args:
            df: 原始数据
            
        Returns:
            清洗后的数据
        """
        # 删除完全重复的行
        df = df.drop_duplicates()
        
        # 删除所有值为空的行
        df = df.dropna(how='all')
        
        return df
    
    @abstractmethod
    def _save(self, df: pd.DataFrame) -> int:
        """
        保存数据到数据库 - 子类必须实现
        
        Args:
            df: 清洗后的数据
            
        Returns:
            保存的记录数
        """
        pass
    
    def _log_sync(self, data_type: str, status: str, records: int, message: str = ''):
        """
        记录同步日志到数据库
        
        Args:
            data_type: 数据类型，如 'daily_price', 'fundamental'
            status: 同步状态
            records: 记录数
            message: 附加信息
        """
        if self.repository and hasattr(self.repository, 'log_sync'):
            self.repository.log_sync(data_type, status, records, message)


class SyncResult:
    """同步结果封装类"""
    
    def __init__(self, status: str, records: int, message: str = ''):
        self.status = status
        self.records = records
        self.message = message
        self.timestamp = datetime.now()
    
    def __repr__(self):
        return f"SyncResult(status='{self.status}', records={self.records}, message='{self.message}')"
    
    def is_success(self) -> bool:
        return self.status == 'success'
    
    def is_failed(self) -> bool:
        return self.status == 'failed'

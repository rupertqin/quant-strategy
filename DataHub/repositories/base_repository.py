"""
数据仓库基类 - 定义所有仓库的通用接口
"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class BaseRepository:
    """
    数据仓库基类
    
    所有具体仓库必须继承此类
    提供统一的数据库连接管理和基础CRUD操作
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        初始化仓库
        
        Args:
            db_path: 数据库文件路径，默认使用项目标准路径
        """
        if db_path is None:
            from DataHub.config import get_storage_path
            db_path = get_storage_path("database", "quant.db")
        
        self.db_path = str(db_path)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 确保数据库文件存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        获取数据库连接
        
        Returns:
            sqlite3连接对象
        """
        conn = sqlite3.connect(self.db_path)
        # 设置行工厂，使查询结果可以通过列名访问
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute(
        self,
        sql: str,
        params: tuple = (),
        fetch: bool = False
    ) -> Optional[List[Dict]]:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: SQL参数
            fetch: 是否获取结果
            
        Returns:
            如果fetch=True，返回查询结果列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql, params)
            
            if fetch:
                rows = cursor.fetchall()
                # 转换为字典列表
                result = [dict(row) for row in rows]
                return result
            else:
                conn.commit()
                return None
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"SQL执行失败: {sql}, 参数: {params}, 错误: {e}")
            raise
        finally:
            conn.close()
    
    def execute_many(self, sql: str, params_list: List[tuple]) -> int:
        """
        批量执行SQL语句
        
        Args:
            sql: SQL语句
            params_list: 参数列表
            
        Returns:
            影响的行数
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.executemany(sql, params_list)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            self.logger.error(f"批量SQL执行失败: {e}")
            raise
        finally:
            conn.close()
    
    def read_sql(
        self,
        sql: str,
        params: tuple = (),
        parse_dates: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        执行查询并返回DataFrame
        
        Args:
            sql: 查询SQL
            params: SQL参数
            parse_dates: 需要解析为日期的列
            
        Returns:
            查询结果DataFrame
        """
        conn = None
        try:
            conn = self._get_connection()
            df = pd.read_sql_query(sql, conn, params=params, parse_dates=parse_dates)
            return df
        except Exception as e:
            self.logger.error(f"查询失败: {sql}, 错误: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        index: bool = False
    ) -> int:
        """
        保存DataFrame到数据库
        
        Args:
            df: 要保存的数据
            table_name: 表名
            if_exists: 'fail'|'replace'|'append'
            index: 是否保存索引
            
        Returns:
            保存的行数
        """
        if df.empty:
            self.logger.warning(f"DataFrame为空，未保存到 {table_name}")
            return 0
        
        conn = self._get_connection()
        
        try:
            df.to_sql(table_name, conn, if_exists=if_exists, index=index)
            self.logger.info(f"成功保存 {len(df)} 条记录到 {table_name}")
            return len(df)
        except Exception as e:
            self.logger.error(f"保存DataFrame失败: {e}")
            raise
        finally:
            conn.close()
    
    def log_sync(
        self,
        data_type: str,
        status: str,
        records_count: int,
        message: str = ''
    ):
        """
        记录数据同步日志
        
        Args:
            data_type: 数据类型
            status: 同步状态
            records_count: 记录数
            message: 附加信息
        """
        sql = """
            INSERT INTO data_update_log (data_type, status, records_count, message, end_time)
            VALUES (?, ?, ?, ?, ?)
        """
        try:
            self.execute(sql, (data_type, status, records_count, message, datetime.now()))
        except Exception as e:
            self.logger.error(f"记录同步日志失败: {e}")
    
    def get_sync_logs(
        self,
        data_type: str = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        获取同步日志
        
        Args:
            data_type: 数据类型筛选
            limit: 返回条数
            
        Returns:
            日志DataFrame
        """
        sql = "SELECT * FROM data_update_log"
        params = []
        
        if data_type:
            sql += " WHERE data_type = ?"
            params.append(data_type)
        
        sql += " ORDER BY create_time DESC LIMIT ?"
        params.append(limit)
        
        return self.read_sql(sql, tuple(params))
    
    def get_table_stats(self) -> Dict[str, int]:
        """
        获取各表统计信息
        
        Returns:
            {表名: 记录数}
        """
        tables = [
            'stock_basic', 'stock_daily_price', 'stock_fundamental',
            'etf_basic', 'etf_daily_price',
            'index_daily_value', 'zt_pool'
        ]
        
        stats = {}
        for table in tables:
            try:
                result = self.execute(f"SELECT COUNT(*) as count FROM {table}", fetch=True)
                stats[table] = result[0]['count'] if result else 0
            except:
                stats[table] = -1  # 表不存在
        
        return stats

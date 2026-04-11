"""
DataHub Repositories - 数据仓库层

提供统一的数据库访问接口，屏蔽底层存储细节
"""

from .base_repository import BaseRepository
from .stock_repository import StockRepository

__all__ = [
    'BaseRepository',
    'StockRepository',
]

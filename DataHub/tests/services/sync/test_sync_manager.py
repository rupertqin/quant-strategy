"""
同步管理器单元测试

运行: python -m unittest DataHub.tests.test_sync_manager -v
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import pandas as pd

import unittest

from DataHub.services.sync.sync_manager import SyncManager


class TestSyncManager(unittest.TestCase):
    """测试同步管理器"""
    
    def setUp(self):
        """每个测试前初始化"""
        self.manager = SyncManager(max_workers=1)
    
    def tearDown(self):
        """每个测试后清理"""
        self.manager.close()
    
    def test_init(self):
        """测试初始化"""
        self.assertEqual(self.manager.max_workers, 1)
        self.assertIsNotNone(self.manager.stock_sync)
        self.assertIsNotNone(self.manager.etf_sync)
        self.assertIsNotNone(self.manager.index_sync)
        self.assertIsNotNone(self.manager.factor_sync)
    
    def test_is_etf(self):
        """测试ETF判断"""
        # ETF代码
        self.assertTrue(self.manager._is_etf('510300.SH'))
        self.assertTrue(self.manager._is_etf('159949.SZ'))
        self.assertTrue(self.manager._is_etf('588080.SH'))
        
        # 非ETF代码
        self.assertFalse(self.manager._is_etf('600519.SH'))
        self.assertFalse(self.manager._is_etf('000858.SZ'))
        self.assertFalse(self.manager._is_etf('300750.SZ'))
    
    @patch('akshare.stock_zh_a_spot')
    def test_sync_today_data(self, mock_spot):
        """测试同步当天数据"""
        # Mock 全市场数据
        mock_spot.return_value = pd.DataFrame({
            '代码': ['600519', '000858'],
            '名称': ['贵州茅台', '五粮液'],
            '最新价': [1700.0, 150.0],
            '今开': [1690.0, 149.0],
            '最高': [1710.0, 151.0],
            '最低': [1680.0, 148.0],
            '成交量': [10000, 20000],
            '成交额': [17000000, 3000000],
            '涨跌幅': [1.5, 0.8]
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_price_dir = Path(temp_dir) / "price"
            mock_price_dir.mkdir(parents=True, exist_ok=True)
            
            with patch('DataHub.services.sync.sync_manager.RAW_PRICE_DIR', mock_price_dir):
                result = self.manager.sync_today_data(sync_factors=False)
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['updated'], 2)
            self.assertEqual(result['failed'], 0)
            self.assertIn('trade_date', result)
    
    @patch('akshare.stock_zh_a_spot')
    def test_sync_today_data_with_factors(self, mock_spot):
        """测试同步当天数据并同步复权因子"""
        mock_spot.return_value = pd.DataFrame({
            '代码': ['600519'],
            '名称': ['贵州茅台'],
            '最新价': [1700.0],
            '今开': [1690.0],
            '最高': [1710.0],
            '最低': [1680.0],
            '成交量': [10000],
            '成交额': [17000000],
            '涨跌幅': [1.5]
        })
        
        # Mock 复权因子同步
        with patch.object(self.manager.factor_sync, 'sync', return_value={
            'status': 'success',
            'success': 1,
            'failed': 0
        }):
            with tempfile.TemporaryDirectory() as temp_dir:
                mock_price_dir = Path(temp_dir) / "price"
                mock_price_dir.mkdir(parents=True, exist_ok=True)
                
                with patch('DataHub.services.sync.sync_manager.RAW_PRICE_DIR', mock_price_dir):
                    result = self.manager.sync_today_data(sync_factors=True)
                
                self.assertEqual(result['status'], 'success')
                self.assertEqual(result['factor_updated'], 1)
    
    def test_sync_daily_stock(self):
        """测试每日股票同步"""
        # Mock 股票列表
        self.manager._stock_list = ['600519.SH', '000858.SZ']
        
        # Mock stock_sync.sync
        with patch.object(self.manager.stock_sync, 'sync', return_value={
            'status': 'success',
            'total': 2,
            'success': 2,
            'failed': 0
        }):
            result = self.manager.sync_stock_daily(
                start_date='20240101',
                end_date='20240102'
            )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['asset_type'], 'stock')
        self.assertEqual(result['total'], 2)
    
    def test_sync_daily_etf(self):
        """测试每日ETF同步"""
        # Mock ETF列表
        self.manager._etf_list = ['510300.SH', '159949.SZ']
        
        with patch.object(self.manager.etf_sync, 'sync', return_value={
            'status': 'success',
            'total': 2,
            'success': 2,
            'failed': 0
        }):
            result = self.manager.sync_etf_daily(
                start_date='20240101',
                end_date='20240102'
            )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['asset_type'], 'etf')
    
    def test_sync_daily_index(self):
        """测试每日指数同步"""
        # Mock 指数列表
        self.manager._index_list = ['000001.SH', '399006.SZ']
        
        with patch.object(self.manager.index_sync, 'sync', return_value={
            'status': 'success',
            'total': 2,
            'success': 2,
            'failed': 0
        }):
            result = self.manager.sync_index_daily(
                start_date='20240101',
                end_date='20240102'
            )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['asset_type'], 'index')
    
    def test_sync_factors_only(self):
        """测试只同步复权因子"""
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_price_dir = Path(temp_dir) / "price"
            mock_price_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建模拟价格文件
            (mock_price_dir / "600519.SH.parquet").touch()
            (mock_price_dir / "000858.SZ.parquet").touch()
            
            with patch('DataHub.services.sync.sync_manager.RAW_PRICE_DIR', mock_price_dir):
                with patch.object(self.manager.factor_sync, 'sync', return_value={
                    'status': 'success',
                    'total': 2,
                    'success': 2,
                    'failed': 0
                }):
                    result = self.manager.sync_factors_only()
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['total'], 2)
            self.assertEqual(result['mode'], 'full_overwrite')


class TestSyncManagerHelpers(unittest.TestCase):
    """测试辅助方法"""
    
    def test_get_stock_list(self):
        """测试获取股票列表"""
        manager = SyncManager()
        
        # Mock CSV文件
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_storage = Path(temp_dir)
            mock_csv = mock_storage / "stock_basic_info.csv"
            pd.DataFrame({
                'symbol': ['600519.SH', '000858.SZ', '920000.BJ']
            }).to_csv(mock_csv, index=False)
            
            with patch('DataHub.services.sync.sync_manager.STORAGE_DIR', mock_storage):
                manager._stock_list = None  # 重置缓存
                
                # 默认排除北交所
                stocks = manager._get_stock_list(include_bj=False)
                self.assertNotIn('920000.BJ', stocks)
                self.assertEqual(len(stocks), 2)
                
                # 包含北交所
                stocks = manager._get_stock_list(include_bj=True)
                self.assertIn('920000.BJ', stocks)
                self.assertEqual(len(stocks), 3)
    
    def test_get_etf_list(self):
        """测试获取ETF列表"""
        manager = SyncManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_storage = Path(temp_dir)
            mock_csv = mock_storage / "etf_basic_info.csv"
            pd.DataFrame({
                'symbol': ['510300.SH', '159949.SZ']
            }).to_csv(mock_csv, index=False)
            
            with patch('DataHub.services.sync.sync_manager.STORAGE_DIR', mock_storage):
                manager._etf_list = None  # 重置缓存
                etfs = manager._get_etf_list()
                self.assertEqual(len(etfs), 2)
    
    def test_get_index_list_default(self):
        """测试获取默认指数列表"""
        manager = SyncManager()
        
        indices = manager._get_index_list()
        self.assertGreater(len(indices), 0)
        self.assertIn('000001.SH', indices)


if __name__ == '__main__':
    unittest.main()

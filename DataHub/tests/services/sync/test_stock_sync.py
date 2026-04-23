"""
股票同步服务单元测试

运行: python -m unittest DataHub.tests.test_sync_stock -v
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import pandas as pd

import unittest

from DataHub.services.sync.stock_sync import StockPriceSync


class TestStockPriceSync(unittest.TestCase):
    """测试股票同步服务"""
    
    def setUp(self):
        """每个测试前创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_raw_price_dir = Path(self.temp_dir) / "raw" / "stocks" / "price"
        self.mock_raw_price_dir.mkdir(parents=True, exist_ok=True)
    
    def tearDown(self):
        """每个测试后清理临时目录"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_sync_single_new_stock(self):
        """测试同步新股票（无本地文件）"""
        with patch('DataHub.services.sync.stock_sync.RAW_PRICE_DIR', self.mock_raw_price_dir):
            service = StockPriceSync(max_workers=1)
            
            # Mock 数据获取（与 _fetch_from_baostock 返回格式一致）
            mock_df = pd.DataFrame({
                'trade_date': [pd.Timestamp('2024-01-01').date(), pd.Timestamp('2024-01-02').date()],
                'open': [100.0, 101.0],
                'high': [102.0, 103.0],
                'low': [99.0, 100.0],
                'close': [101.0, 102.0],
                'volume': [10000, 20000],
                'amount': [1000000, 2000000],
                'turn': [1.5, 2.0],
                'change_pct': [1.0, 0.99]
            })
            
            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                service.sync_options = {
                    'incremental': True,
                    'start_date': '20240101',
                    'end_date': '20240102'
                }
                result = service._sync_single('600519.SH')
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['symbol'], '600519.SH')
            self.assertEqual(result['new_records'], 2)
            
            # 验证文件已创建
            file_path = self.mock_raw_price_dir / "600519.SH.parquet"
            self.assertTrue(file_path.exists())
    
    def test_sync_single_incremental(self):
        """测试增量同步"""
        with patch('DataHub.services.sync.stock_sync.RAW_PRICE_DIR', self.mock_raw_price_dir):
            # 先创建已有数据
            existing_df = pd.DataFrame({
                'trade_date': [pd.Timestamp('2024-01-01').date()],
                'open': [100.0],
                'high': [102.0],
                'low': [99.0],
                'close': [101.0],
                'volume': [10000],
                'amount': [1000000],
                'turn': [1.5],
                'change_pct': [1.0]
            })
            file_path = self.mock_raw_price_dir / "600519.SH.parquet"
            existing_df.to_parquet(file_path, index=False)
            
            service = StockPriceSync(max_workers=1)
            
            # Mock 新数据（与 _fetch_from_baostock 返回格式一致）
            mock_df = pd.DataFrame({
                'trade_date': [pd.Timestamp('2024-01-02').date()],
                'open': [101.0],
                'high': [103.0],
                'low': [100.0],
                'close': [102.0],
                'volume': [20000],
                'amount': [2000000],
                'turn': [2.0],
                'change_pct': [0.99]
            })
            
            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                service.sync_options = {
                    'incremental': True,
                    'start_date': '20240102',
                    'end_date': '20240102'
                }
                result = service._sync_single('600519.SH')
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['new_records'], 1)
            self.assertEqual(result['total_records'], 2)
    
    def test_sync_single_up_to_date(self):
        """测试已是最新数据的情况"""
        with patch('DataHub.services.sync.stock_sync.RAW_PRICE_DIR', self.mock_raw_price_dir):
            service = StockPriceSync(max_workers=1)
            
            # start_date > end_date
            service.sync_options = {
                'incremental': True,
                'start_date': '20240103',
                'end_date': '20240102'
            }
            result = service._sync_single('600519.SH')
            
            self.assertEqual(result['status'], 'skipped')
            self.assertEqual(result['message'], 'Already up to date')
    
    def test_sync_single_no_data(self):
        """测试无数据返回的情况"""
        with patch('DataHub.services.sync.stock_sync.RAW_PRICE_DIR', self.mock_raw_price_dir):
            service = StockPriceSync(max_workers=1)
            
            with patch.object(service, '_fetch_from_baostock', return_value=pd.DataFrame()):
                service.sync_options = {
                    'incremental': False,
                    'start_date': '20240101',
                    'end_date': '20240102'
                }
                result = service._sync_single('600519.SH')
            
            self.assertEqual(result['status'], 'skipped')
            self.assertEqual(result['message'], 'No data')
    
    def test_format_code(self):
        """测试代码格式转换"""
        service = StockPriceSync()
        
        self.assertEqual(service._format_code('600519.SH'), 'sh.600519')
        self.assertEqual(service._format_code('000858.SZ'), 'sz.000858')
        self.assertEqual(service._format_code('UNKNOWN'), 'UNKNOWN')
    
    def test_fetch_from_akshare(self):
        """测试 akshare 数据转换"""
        service = StockPriceSync()
        
        # Mock akshare 返回数据
        mock_df = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02'],
            '开盘': [100.0, 101.0],
            '收盘': [101.0, 102.0],
            '最高': [102.0, 103.0],
            '最低': [99.0, 100.0],
            '成交量': [10000, 20000],
            '成交额': [1000000, 2000000],
            '换手率': [1.5, 2.0]
        })
        
        with patch('DataHub.services.sync.stock_sync.ak.stock_zh_a_hist', return_value=mock_df):
            result = service._fetch_from_akshare('600519.SH', '20240101', '20240102')
        
        self.assertFalse(result.empty)
        self.assertIn('trade_date', result.columns)
        self.assertIn('open', result.columns)
        self.assertIn('close', result.columns)
        self.assertEqual(len(result), 2)


class TestStockPriceSyncIntegration(unittest.TestCase):
    """集成测试（使用真实数据获取，但可跳过）"""
    
    def test_baostock_login_logout(self):
        """测试 baostock 登录登出"""
        service = StockPriceSync(data_source='baostock')
        
        # Mock baostock
        with patch('DataHub.services.sync.stock_sync.bs.login') as mock_login:
            with patch('DataHub.services.sync.stock_sync.bs.logout') as mock_logout:
                service._ensure_login()
                self.assertTrue(service._baostock_logged_in)
                
                service.logout()
                mock_logout.assert_called_once()


if __name__ == '__main__':
    unittest.main()

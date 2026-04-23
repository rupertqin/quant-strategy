"""
ETF同步服务单元测试

运行: python -m unittest DataHub.tests.test_sync_etf -v
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import unittest

from DataHub.services.sync.etf_sync import ETFSync


class TestETFSync(unittest.TestCase):
    """测试ETF同步服务"""
    
    def setUp(self):
        """每个测试前创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_etf_dir = Path(self.temp_dir) / "raw" / "etf" / "price"
        self.mock_etf_dir.mkdir(parents=True, exist_ok=True)
    
    def tearDown(self):
        """每个测试后清理"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_sync_single_new_etf(self):
        """测试同步新ETF"""
        with patch('DataHub.services.sync.etf_sync.RAW_ETF_PRICE_DIR', self.mock_etf_dir):
            # 使用 baostock 模式测试，保持与原测试一致
            service = ETFSync(max_workers=1, data_source="baostock")
            
            # Mock baostock 返回数据
            mock_df = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-02'],
                'open': [3.5, 3.6],
                'high': [3.6, 3.7],
                'low': [3.4, 3.5],
                'close': [3.6, 3.65],
                'volume': [100000, 200000],
                'amount': [350000, 720000],
                'turn': [5.0, 8.0],
                'pctChg': [2.0, 1.39]
            })
            
            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                service.sync_options = {
                    'incremental': True,
                    'start_date': '20240101',
                    'end_date': '20240102'
                }
                result = service._sync_single('510300.SH')
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['symbol'], '510300.SH')
            self.assertEqual(result['new_records'], 2)
    
    def test_sync_single_fallback_to_em(self):
        """测试数据源回退到东财"""
        with patch('DataHub.services.sync.etf_sync.RAW_ETF_PRICE_DIR', self.mock_etf_dir):
            service = ETFSync(max_workers=1)
            
            # Mock baostock 失败
            with patch.object(service, '_fetch_from_baostock', return_value=pd.DataFrame()):
                # Mock yfinance 失败
                with patch.object(service, '_fetch_from_yfinance', return_value=pd.DataFrame()):
                    # Mock 东财成功
                    mock_em_df = pd.DataFrame({
                        '日期': ['2024-01-01'],
                        '开盘': [3.5],
                        '收盘': [3.6],
                        '最高': [3.6],
                        '最低': [3.4],
                        '成交量': [100000],
                        '成交额': [350000],
                        '涨跌幅': [2.0]
                    })
                    
                    with patch('DataHub.services.sync.etf_sync.ak.fund_etf_hist_em', return_value=mock_em_df):
                        service.sync_options = {
                            'incremental': False,
                            'start_date': '20240101',
                            'end_date': '20240101'
                        }
                        result = service._sync_single('510300.SH')
                    
                    self.assertEqual(result['status'], 'success')
    
    def test_format_code(self):
        """测试代码格式转换"""
        service = ETFSync()
        
        self.assertEqual(service._format_code('510300.SH'), 'sh.510300')
        self.assertEqual(service._format_code('159949.SZ'), 'sz.159949')
        self.assertEqual(service._format_code('UNKNOWN'), 'UNKNOWN')


class TestETFSyncDataSources(unittest.TestCase):
    """测试多数据源"""
    
    def test_fetch_from_yfinance(self):
        """测试 Yahoo Finance 数据获取（未安装时跳过）"""
        service = ETFSync()
        
        # 模拟 yfinance 未安装
        with patch.dict('sys.modules', {'yfinance': None}):
            result = service._fetch_from_yfinance('510300.SH', '20240101', '20240102')
            self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()

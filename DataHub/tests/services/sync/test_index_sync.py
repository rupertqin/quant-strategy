"""
指数同步服务单元测试

运行: python -m unittest DataHub.tests.test_sync_index -v
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import unittest

from DataHub.services.sync.index_sync import IndexSync


class TestIndexSync(unittest.TestCase):
    """测试指数同步服务"""

    def setUp(self):
        """每个测试前创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_index_dir = Path(self.temp_dir) / "raw" / "index" / "price"
        self.mock_index_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """每个测试后清理"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sync_single_new_index(self):
        """测试同步新指数"""
        with patch('DataHub.services.sync.index_sync.RAW_INDEX_PRICE_DIR', self.mock_index_dir):
            service = IndexSync(max_workers=1)

            # Mock 新浪接口返回数据
            mock_df = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [3000.0, 3010.0],
                '收盘': [3010.0, 3020.0],
                '最高': [3020.0, 3030.0],
                '最低': [2990.0, 3000.0],
                '成交量': [1000000, 2000000],
                '成交额': [3000000000, 6000000000],
                '涨跌幅': [0.5, 0.33]
            })

            with patch.object(service, '_fetch_from_sina', return_value=mock_df):
                service.sync_options = {
                    'incremental': True,
                    'start_date': '20240101',
                    'end_date': '20240102'
                }
                result = service._sync_single('000001.SH')

            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['symbol'], '000001.SH')
            self.assertEqual(result['new_records'], 2)

    def test_sync_single_sina_failed(self):
        """测试新浪失败时不 fallback，直接返回 failed"""
        with patch('DataHub.services.sync.index_sync.RAW_INDEX_PRICE_DIR', self.mock_index_dir):
            service = IndexSync(max_workers=1)

            # 新浪失败 -> 直接返回 failed（与旧版 history_sync 一致，A股指数不 fallback）
            with patch.object(service, '_fetch_from_sina', return_value=None):
                service.sync_options = {
                    'incremental': False,
                    'start_date': '20240101',
                    'end_date': '20240101'
                }
                result = service._sync_single('000001.SH')

            self.assertEqual(result['status'], 'failed')

    def test_format_code_sina(self):
        """测试新浪代码格式转换"""
        service = IndexSync()

        self.assertEqual(service._format_code_sina('000001.SH'), 'sh000001')
        self.assertEqual(service._format_code_sina('399006.SZ'), 'sz399006')
        self.assertEqual(service._format_code_sina('UNKNOWN'), 'UNKNOWN')

    def test_fetch_from_yfinance_not_installed(self):
        """测试 Yahoo Finance 未安装时返回 None"""
        service = IndexSync()

        with patch.dict('sys.modules', {'yfinance': None}):
            result = service._fetch_from_yfinance('000001.SH', '20240101', '20240102')
            self.assertIsNone(result)


class TestIndexSyncHK(unittest.TestCase):
    """测试港股指数"""

    def test_fetch_hk_index_mapping(self):
        """测试港股指数代码映射 - yfinance 优先路径"""
        service = IndexSync()

        # Mock yfinance 返回数据
        mock_yf_df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01']),
            'Open': [17000.0],
            'High': [17200.0],
            'Low': [16900.0],
            'Close': [17100.0],
            'Volume': [1000000],
        })

        mock_ticker = Mock()
        mock_ticker.history = Mock(return_value=mock_yf_df)

        with patch('yfinance.Ticker', return_value=mock_ticker):
            result = service._fetch_hk_index('HSI.HK', '20240101', '20240101')
            self.assertIsNotNone(result)
            self.assertFalse(result.empty)
            self.assertEqual(result.iloc[0]['close'], 17100.0)

    def test_fetch_hk_index_unknown(self):
        """测试未知港股指数"""
        service = IndexSync()

        result = service._fetch_hk_index('UNKNOWN.HK', '20240101', '20240101')
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()

"""
命令行接口单元测试

运行: python -m unittest DataHub.tests.test_sync_cli -v
"""

from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import unittest

from DataHub.services.sync.cli import parse_symbol_arg, parse_date_arg, main


class TestParseSymbolArg(unittest.TestCase):
    """测试解析 symbol 参数"""

    def test_empty_string(self):
        """测试空字符串"""
        symbols, asset_type = parse_symbol_arg('')
        self.assertEqual(symbols, [])
        self.assertEqual(asset_type, 'stock')

    def test_type_alias_etf(self):
        """测试 ETF 类型简写"""
        symbols, asset_type = parse_symbol_arg('etf')
        self.assertEqual(symbols, [])
        self.assertEqual(asset_type, 'etf')

    def test_type_alias_index(self):
        """测试指数类型简写"""
        symbols, asset_type = parse_symbol_arg('index')
        self.assertEqual(symbols, [])
        self.assertEqual(asset_type, 'index')

    def test_type_alias_stock(self):
        """测试股票类型简写"""
        symbols, asset_type = parse_symbol_arg('stock')
        self.assertEqual(symbols, [])
        self.assertEqual(asset_type, 'stock')

    def test_specific_codes(self):
        """测试具体代码列表"""
        symbols, asset_type = parse_symbol_arg('600519.SH,000858.SZ')
        self.assertEqual(symbols, ['600519.SH', '000858.SZ'])
        self.assertIsNone(asset_type)

    def test_single_code(self):
        """测试单只代码"""
        symbols, asset_type = parse_symbol_arg('600519.SH')
        self.assertEqual(symbols, ['600519.SH'])
        self.assertIsNone(asset_type)


class TestParseDateArg(unittest.TestCase):
    """测试解析日期参数"""

    def test_single_date(self):
        """测试单日格式"""
        start, end = parse_date_arg('20260413')
        self.assertEqual(start, '20260413')
        self.assertEqual(end, '20260413')

    def test_date_with_dash(self):
        """测试带横线的日期"""
        start, end = parse_date_arg('2026-04-13')
        self.assertEqual(start, '20260413')
        self.assertEqual(end, '20260413')

    def test_date_range(self):
        """测试日期范围"""
        start, end = parse_date_arg('20260413~20260414')
        self.assertEqual(start, '20260413')
        self.assertEqual(end, '20260414')

    def test_invalid_format(self):
        """测试无效格式"""
        with self.assertRaises(ValueError):
            parse_date_arg('invalid')

    def test_invalid_range(self):
        """测试无效范围"""
        with self.assertRaises(ValueError):
            parse_date_arg('20260413~')


class TestCLI(unittest.TestCase):
    """测试命令行接口"""

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_today_command(self, mock_manager_class):
        """测试 --today 命令"""
        mock_manager = MagicMock()
        mock_manager.sync_today_data.return_value = {
            'status': 'success',
            'trade_date': '20260423',
            'updated': 5507,
            'skipped': 0,
            'failed': 0,
            'factor_updated': 0,
            'factor_skipped': 0
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--today']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_today_data.assert_called_once_with(sync_factors=False)
        mock_manager.close.assert_called_once()

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_today_with_factors(self, mock_manager_class):
        """测试 --today --sync-factors 命令"""
        mock_manager = MagicMock()
        mock_manager.sync_today_data.return_value = {
            'status': 'success',
            'trade_date': '20260423',
            'updated': 5507,
            'skipped': 0,
            'failed': 0,
            'factor_updated': 100,
            'factor_skipped': 0
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--today', '--sync-factors']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_today_data.assert_called_once_with(sync_factors=True)

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_sync_factors_only(self, mock_manager_class):
        """测试只同步复权因子"""
        mock_manager = MagicMock()
        mock_manager.sync_factors_only.return_value = {
            'status': 'success',
            'total': 5000,
            'success': 4800,
            'failed': 200
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--sync-factors']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_factors_only.assert_called_once()

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_daily_etf(self, mock_manager_class):
        """测试 --daily --symbol etf"""
        mock_manager = MagicMock()
        mock_manager.sync_etf_daily.return_value = {
            'status': 'success',
            'asset_type': 'etf',
            'total': 50,
            'success': 48,
            'failed': 2,
            'date_range': '20260423~20260423'
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--daily', '--symbol', 'etf', '--workers', '3']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_etf_daily.assert_called_once()

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_daily_index(self, mock_manager_class):
        """测试 --daily --symbol index"""
        mock_manager = MagicMock()
        mock_manager.sync_index_daily.return_value = {
            'status': 'success',
            'asset_type': 'index',
            'total': 14,
            'success': 14,
            'failed': 0,
            'date_range': '20260423~20260423'
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--daily', '--symbol', 'index']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_index_daily.assert_called_once()

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_daily_with_date(self, mock_manager_class):
        """测试指定日期的 --daily"""
        mock_manager = MagicMock()
        mock_manager.sync_stock_daily.return_value = {
            'status': 'success',
            'asset_type': 'stock',
            'total': 5000,
            'success': 5000,
            'failed': 0,
            'date_range': '20260413~20260413'
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--daily', '20260413']):
            with patch('sys.exit') as mock_exit:
                main()

        mock_manager.sync_stock_daily.assert_called_once()

    @patch('DataHub.services.sync.cli.SyncManager')
    def test_daily_with_limit(self, mock_manager_class):
        """测试 --limit 参数"""
        mock_manager = MagicMock()
        mock_manager.sync_stock_daily.return_value = {
            'status': 'success',
            'asset_type': 'stock',
            'total': 10,
            'success': 10,
            'failed': 0
        }
        mock_manager_class.return_value = mock_manager

        with patch('sys.argv', ['cli.py', '--daily', '--limit', '10']):
            with patch('sys.exit') as mock_exit:
                main()

        # 验证 limit 被传递
        call_kwargs = mock_manager.sync_stock_daily.call_args[1]
        self.assertEqual(call_kwargs.get('limit'), 10)


if __name__ == '__main__':
    unittest.main()

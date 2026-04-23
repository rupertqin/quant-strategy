"""
复权因子同步服务单元测试

运行: python -m unittest DataHub.tests.test_sync_factor -v
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import unittest

from DataHub.services.sync.factor_sync import AdjustFactorSync


class TestAdjustFactorSync(unittest.TestCase):
    """测试复权因子同步服务"""

    def setUp(self):
        """每个测试前创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_factor_dir = Path(self.temp_dir) / "raw" / "stocks" / "adjust_factor"
        self.mock_factor_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """每个测试后清理"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sync_single_with_factors(self):
        """测试同步有复权因子的股票"""
        with patch('DataHub.services.sync.factor_sync.RAW_ADJUST_FACTOR_DIR', self.mock_factor_dir):
            service = AdjustFactorSync(max_workers=1)

            # Mock baostock 返回有复权因子的数据
            mock_df = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02', '2024-06-01'],
                'adjust_factor': [1.0, 1.05, 1.10]  # 有分红送股
            })

            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                result = service._sync_single('600519.SH')

            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['symbol'], '600519.SH')
            self.assertEqual(result['records'], 2)  # 过滤掉 factor=1.0 的

            # 验证文件已创建
            file_path = self.mock_factor_dir / "600519.SH.parquet"
            self.assertTrue(file_path.exists())

    def test_sync_single_no_factors(self):
        """测试同步无复权因子的股票（从未分红）"""
        with patch('DataHub.services.sync.factor_sync.RAW_ADJUST_FACTOR_DIR', self.mock_factor_dir):
            service = AdjustFactorSync(max_workers=1)

            # Mock baostock 返回无复权因子的数据（全部 factor=1.0）
            mock_df = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02'],
                'adjust_factor': [1.0, 1.0]
            })

            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                result = service._sync_single('300750.SZ')

            self.assertEqual(result['status'], 'skipped')
            self.assertEqual(result['symbol'], '300750.SZ')

            # 验证文件未创建（或已删除）
            file_path = self.mock_factor_dir / "300750.SZ.parquet"
            self.assertFalse(file_path.exists())

    def test_sync_single_empty_response(self):
        """测试空响应"""
        with patch('DataHub.services.sync.factor_sync.RAW_ADJUST_FACTOR_DIR', self.mock_factor_dir):
            service = AdjustFactorSync(max_workers=1)

            with patch.object(service, '_fetch_from_baostock', return_value=pd.DataFrame()):
                result = service._sync_single('600519.SH')

            self.assertEqual(result['status'], 'skipped')
            self.assertEqual(result['symbol'], '600519.SH')

    def test_sync_single_delete_existing(self):
        """测试删除已有文件（当股票不再有复权因子时）"""
        with patch('DataHub.services.sync.factor_sync.RAW_ADJUST_FACTOR_DIR', self.mock_factor_dir):
            # 先创建已有文件
            existing_df = pd.DataFrame({
                'trade_date': ['2023-01-01'],
                'adjust_factor': [1.5]
            })
            file_path = self.mock_factor_dir / "600519.SH.parquet"
            existing_df.to_parquet(file_path, index=False)
            self.assertTrue(file_path.exists())

            service = AdjustFactorSync(max_workers=1)

            # Mock 返回无复权因子数据
            mock_df = pd.DataFrame({
                'trade_date': ['2024-01-01'],
                'adjust_factor': [1.0]
            })

            with patch.object(service, '_fetch_from_baostock', return_value=mock_df):
                result = service._sync_single('600519.SH')

            self.assertEqual(result['status'], 'skipped')
            # 验证旧文件已被删除
            self.assertFalse(file_path.exists())

    def test_format_code(self):
        """测试代码格式转换（与旧版 history_sync StockCodeUtil.to_baostock 一致）"""
        service = AdjustFactorSync()

        self.assertEqual(service._format_code('600519.SH'), 'sh.600519')
        self.assertEqual(service._format_code('000858.SZ'), 'sz.000858')
        self.assertEqual(service._format_code('UNKNOWN'), 'UNKNOWN')


class TestAdjustFactorSyncBatch(unittest.TestCase):
    """测试批量同步"""

    def test_sync_multiple_symbols(self):
        """测试批量同步多只股票的复权因子"""
        temp_dir = tempfile.mkdtemp()
        mock_factor_dir = Path(temp_dir) / "raw" / "stocks" / "adjust_factor"
        mock_factor_dir.mkdir(parents=True, exist_ok=True)

        with patch('DataHub.services.sync.factor_sync.RAW_ADJUST_FACTOR_DIR', mock_factor_dir):
            service = AdjustFactorSync(max_workers=1)

            # Mock 返回不同结果
            def mock_fetch(symbol):
                if symbol == '600519.SH':
                    return pd.DataFrame({
                        'trade_date': ['2024-01-01'],
                        'adjust_factor': [1.5]
                    })
                else:
                    return pd.DataFrame({
                        'trade_date': ['2024-01-01'],
                        'adjust_factor': [1.0]
                    })

            with patch('DataHub.services.sync.factor_sync.bs.login'):
                with patch.object(service, '_fetch_from_baostock', side_effect=mock_fetch):
                    result = service.sync(['600519.SH', '300750.SZ'])

            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['total'], 2)
            self.assertEqual(result['success'], 1)  # 600519 有复权因子
            self.assertEqual(result['skipped'], 1)  # 300750 无复权因子

        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

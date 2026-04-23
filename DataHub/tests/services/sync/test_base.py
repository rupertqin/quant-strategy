"""
同步服务基类单元测试

运行: python -m unittest DataHub.tests.test_sync_base -v
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime

from DataHub.services.sync.base import BaseSyncService


class MockSyncService(BaseSyncService):
    """模拟同步服务，用于测试"""
    
    def __init__(self, max_workers=1, request_delay=0):
        super().__init__(max_workers=max_workers, request_delay=request_delay)
        self.sync_calls = []
    
    def _do_sync(self, symbols, **kwargs):
        """模拟同步逻辑"""
        self.sync_calls.append(symbols)
        results = []
        for symbol in symbols:
            results.append({
                'status': 'success',
                'symbol': symbol
            })
            self._update_stats('success')
        return results


class TestBaseSyncService(unittest.TestCase):
    """测试同步服务基类"""
    
    def test_init(self):
        """测试初始化"""
        service = MockSyncService(max_workers=3, request_delay=1.0)
        self.assertEqual(service.max_workers, 3)
        self.assertEqual(service.request_delay, 1.0)
        self.assertEqual(service.stats, {'success': 0, 'failed': 0, 'skipped': 0})
    
    def test_sync_template_method(self):
        """测试模板方法流程"""
        service = MockSyncService()
        symbols = ['600519.SH', '000858.SZ']
        
        result = service.sync(symbols)
        
        # 验证结果结构
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['success'], 2)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(result['skipped'], 0)
        self.assertIn('elapsed_seconds', result)
        
        # 验证调用了 _do_sync
        self.assertEqual(len(service.sync_calls), 1)
        self.assertEqual(service.sync_calls[0], symbols)
    
    def test_sync_with_failed_items(self):
        """测试有失败项的情况"""
        service = MockSyncService()
        
        # 模拟部分失败
        def mock_do_sync(symbols, **kwargs):
            results = []
            for i, symbol in enumerate(symbols):
                if i == 0:
                    results.append({'status': 'success', 'symbol': symbol})
                    service._update_stats('success')
                else:
                    results.append({'status': 'failed', 'symbol': symbol})
                    service._update_stats('failed')
            return results
        
        service._do_sync = mock_do_sync
        
        result = service.sync(['A', 'B', 'C'])
        
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['success'], 1)
        self.assertEqual(result['failed'], 2)
    
    def test_update_stats_thread_safety(self):
        """测试统计更新的线程安全"""
        service = MockSyncService()
        
        # 模拟多线程更新
        import threading
        
        def update_stats():
            for _ in range(100):
                service._update_stats('success')
        
        threads = [threading.Thread(target=update_stats) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(service.stats['success'], 500)


class TestBaseSyncServiceHooks(unittest.TestCase):
    """测试钩子方法"""
    
    def test_before_sync_hook(self):
        """测试前置钩子"""
        service = MockSyncService()
        
        with patch.object(service, '_before_sync') as mock_before:
            service.sync(['600519.SH'])
            mock_before.assert_called_once()
    
    def test_after_sync_hook(self):
        """测试后置钩子"""
        service = MockSyncService()
        
        with patch.object(service, '_after_sync') as mock_after:
            service.sync(['600519.SH'])
            mock_after.assert_called_once()


if __name__ == '__main__':
    unittest.main()

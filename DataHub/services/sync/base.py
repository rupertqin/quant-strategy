"""
同步服务基类 - 模板方法模式

定义通用同步流程，子类只需实现具体同步逻辑
"""

import logging
import random
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union
from threading import Lock

logger = logging.getLogger(__name__)


def get_effective_end_date(now: datetime = None) -> str:
    """
    获取有效的同步结束日期，处理跨日期行为。

    - 收盘后（>= 15:00）：当天数据已可获取，返回今天
    - 凌晨/盘中（< 15:00）：当天数据还未产生或未收盘，返回上一个交易日
    """
    if now is None:
        now = datetime.now()
    today = now.date()

    if now.hour >= 15:
        return today.strftime('%Y%m%d')

    # 未收盘，回退到上一个交易日（跳过周末）
    prev = today - timedelta(days=1)
    while prev.weekday() >= 5:  # 周六=5, 周日=6
        prev -= timedelta(days=1)
    return prev.strftime('%Y%m%d')


class Colors:
    """ANSI 颜色码"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'


def color_log(level: str, message: str) -> str:
    """为日志消息添加颜色"""
    color_map = {
        'error': Colors.RED + Colors.BOLD,
        'warning': Colors.YELLOW + Colors.BOLD,
        'success': Colors.GREEN,
        'info': Colors.CYAN,
    }
    color = color_map.get(level, '')
    return f"{color}{message}{Colors.END}"


class BaseSyncService(ABC):
    """数据同步基类"""

    def __init__(self, max_workers: int = 1, request_delay: Union[float, tuple] = (0.5, 2.0)):
        """
        初始化同步服务

        Args:
            max_workers: 并发线程数
            request_delay: 请求间隔（秒），支持固定值或随机范围 (min, max)
        """
        self.max_workers = max_workers
        self.request_delay = request_delay
        self.stats = {'success': 0, 'failed': 0, 'skipped': 0}
        self._lock = Lock()
    
    def sync(self, symbols: List[str], **kwargs) -> Dict[str, Any]:
        """
        模板方法 - 通用同步流程
        
        Args:
            symbols: 要同步的代码列表
            **kwargs: 额外参数
            
        Returns:
            同步结果统计
        """
        # 重置统计
        self.stats = {'success': 0, 'failed': 0, 'skipped': 0}
        
        # 1. 前置检查
        self._before_sync(symbols, **kwargs)
        
        # 2. 执行同步
        start_time = time.time()
        results = self._do_sync(symbols, **kwargs)
        elapsed = time.time() - start_time
        
        # 3. 后置处理
        self._after_sync(results, **kwargs)
        
        return {
            'status': 'success' if self.stats['failed'] == 0 else 'partial',
            'total': len(symbols),
            'success': self.stats['success'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'elapsed_seconds': round(elapsed, 2)
        }
    
    @abstractmethod
    def _do_sync(self, symbols: List[str], **kwargs) -> List[Dict]:
        """
        子类实现具体同步逻辑
        
        Args:
            symbols: 代码列表
            **kwargs: 额外参数
            
        Returns:
            每条记录的同步结果列表
        """
        pass
    
    def _before_sync(self, symbols: List[str], **kwargs):
        """钩子 - 同步前准备"""
        logger.info(f"开始同步 {len(symbols)} 条记录")
    
    def _after_sync(self, results: List[Dict], **kwargs):
        """钩子 - 同步后处理"""
        logger.info(f"同步完成: 成功 {self.stats['success']}, 失败 {self.stats['failed']}, 跳过 {self.stats['skipped']}")
    
    def _sync_parallel(self, symbols: List[str], sync_func, **kwargs) -> List[Dict]:
        """
        并行同步包装器

        Args:
            symbols: 代码列表
            sync_func: 单条同步函数
            **kwargs: 传递给 sync_func 的参数

        Returns:
            结果列表
        """
        results = []
        total = len(symbols)
        completed = 0
        success_count = 0
        lock = Lock()

        def _update_progress(status: str):
            nonlocal completed, success_count
            with lock:
                completed += 1
                if status == 'success':
                    success_count += 1
                # 每10只或完成时打印进度
                if completed % 10 == 0 or completed == total:
                    logger.info(color_log('info', f"进度: {completed}/{total} ({completed/total*100:.1f}%) - 成功: {success_count}"))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {
                executor.submit(self._wrap_sync, sync_func, symbol, **kwargs): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=60)
                    results.append(result)
                    self._update_stats(result.get('status', 'failed'))
                    _update_progress(result.get('status', 'failed'))
                except Exception as e:
                    logger.error(color_log('error', f"❌ 同步 {symbol} 异常: {e}"))
                    results.append({'status': 'failed', 'symbol': symbol, 'error': str(e)})
                    self._update_stats('failed')
                    _update_progress('failed')

        return results

    def _wrap_sync(self, sync_func, symbol: str, **kwargs) -> Dict:
        """包装单条同步，添加随机延迟"""
        if isinstance(self.request_delay, (tuple, list)) and len(self.request_delay) == 2:
            time.sleep(random.uniform(self.request_delay[0], self.request_delay[1]))
        else:
            time.sleep(self.request_delay)
        return sync_func(symbol, **kwargs)
    
    def _update_stats(self, status: str):
        """线程安全更新统计"""
        with self._lock:
            if status in self.stats:
                self.stats[status] += 1
            else:
                self.stats['failed'] += 1

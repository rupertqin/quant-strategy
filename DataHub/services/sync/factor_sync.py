"""
复权因子同步服务 - 简化版

设计原则：
1. 全量获取，直接覆盖（不区分增量/全量）
2. 数据量小，无需复杂逻辑
3. 简单可靠，避免数据缺失
"""

import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd
import baostock as bs

from DataHub.config import RAW_ADJUST_FACTOR_DIR
from .base import BaseSyncService

logger = logging.getLogger(__name__)


class AdjustFactorSync(BaseSyncService):
    """复权因子同步 - 全量覆盖模式"""
    
    def __init__(self, max_workers: int = 1):
        super().__init__(max_workers=max_workers, request_delay=0.5)
        self._baostock_logged_in = False
    
    def _ensure_login(self):
        """确保 baostock 已登录"""
        if not self._baostock_logged_in:
            try:
                bs.login()
                self._baostock_logged_in = True
                logger.debug("baostock 登录成功")
            except Exception as e:
                logger.error(f"baostock 登录失败: {e}")
                raise
    
    def _do_sync(self, symbols: List[str], **kwargs) -> List[Dict]:
        """
        批量同步复权因子
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            同步结果列表
        """
        RAW_ADJUST_FACTOR_DIR.mkdir(parents=True, exist_ok=True)
        
        # 确保登录
        self._ensure_login()
        
        # 使用并行同步
        return self._sync_parallel(symbols, self._sync_single)
    
    def _sync_single(self, symbol: str) -> Dict:
        """
        同步单只股票的复权因子（全量覆盖）
        
        Args:
            symbol: 股票代码，如 '600519.SH'
            
        Returns:
            同步结果
        """
        file_path = RAW_ADJUST_FACTOR_DIR / f"{symbol}.parquet"
        
        try:
            # 1. 从 baostock 获取全量数据
            df = self._fetch_from_baostock(symbol)
            
            if df is None or df.empty:
                # 从未分红送股，删除已有文件（如果存在）
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(f"{symbol} 删除复权因子文件（从未分红）")
                return {
                    'status': 'skipped',
                    'symbol': symbol,
                    'message': 'Never distributed dividend'
                }
            
            # 2. 过滤 factor=1 的记录（无效值）
            df = df[df['adjust_factor'] != 1.0]
            
            if df.empty:
                if file_path.exists():
                    file_path.unlink()
                return {
                    'status': 'skipped',
                    'symbol': symbol,
                    'message': 'No valid adjust factors'
                }
            
            # 3. 直接覆盖保存（简化：无需合并）
            df.to_parquet(file_path, index=False, compression='zstd')
            
            logger.info(f"✓ {symbol} 复权因子同步完成: {len(df)} 条")
            
            return {
                'status': 'success',
                'symbol': symbol,
                'records': len(df)
            }
            
        except Exception as e:
            logger.error(f"❌ {symbol} 复权因子同步失败: {e}")
            return {
                'status': 'failed',
                'symbol': symbol,
                'error': str(e)
            }
    
    def _fetch_from_baostock(self, symbol: str) -> pd.DataFrame:
        """
        从 baostock 获取复权因子
        
        Args:
            symbol: 股票代码
            
        Returns:
            DataFrame with columns: trade_date, adjust_factor
        """
        # 转换代码格式
        code = self._format_code(symbol)
        
        # 获取全量数据（从1990到今天）
        start_date = "1990-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        rs = bs.query_adjust_factor(code=code, start_date=start_date, end_date=end_date)
        
        if rs is None:
            logger.warning(f"baostock 返回 None（可能未登录或网络异常）: {symbol}")
            return pd.DataFrame()
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return pd.DataFrame()
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 标准化列名
        column_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['dividoperatedate', 'date']:
                column_map[col] = 'trade_date'
            elif col_lower == 'foreadjustfactor':
                column_map[col] = 'adjust_factor'
        
        df = df.rename(columns=column_map)
        
        # 转换数据类型
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        df['adjust_factor'] = pd.to_numeric(df['adjust_factor'], errors='coerce')
        
        return df[['trade_date', 'adjust_factor']].dropna()
    
    def _format_code(self, symbol: str) -> str:
        """转换代码格式 600519.SH -> sh.600519"""
        if '.SH' in symbol:
            return 'sh.' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return 'sz.' + symbol.replace('.SZ', '')
        elif '.BJ' in symbol:
            return 'bj.' + symbol.replace('.BJ', '')
        return symbol
    
    def logout(self):
        """登出 baostock"""
        if self._baostock_logged_in:
            try:
                bs.logout()
                self._baostock_logged_in = False
                logger.debug("baostock 登出成功")
            except:
                pass

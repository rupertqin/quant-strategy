"""
股票价格同步服务

保持原有功能：
- 支持增量/全量同步
- 支持日线数据获取
- 支持数据源选择（baostock/akshare）
- 支持并发同步
"""

import logging
import socket
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import random

import numpy as np
import pandas as pd
import akshare as ak
import threading

from DataHub.config import RAW_PRICE_DIR, STORAGE_DIR
from .base import BaseSyncService, color_log, get_effective_end_date

logger = logging.getLogger(__name__)

# 设置全局 socket 超时（防止网络请求无限等待）
socket.setdefaulttimeout(30)

# akshare 的 py-mini-racer 非线程安全，必须加锁串行调用
_akshare_lock = threading.Lock()


class StockPriceSync(BaseSyncService):
    """股票价格同步 - akshare 新浪接口（前复权）"""

    def __init__(self, max_workers: int = 1, data_source: str = "akshare"):
        super().__init__(max_workers=max_workers, request_delay=(0.5, 2.0))
        self.data_source = data_source
        self.pending_symbols = []

    def _is_bj_stock(self, symbol: str) -> bool:
        """判断是否为北交所股票"""
        return '.BJ' in symbol

    def _add_to_pending_list(self, symbol: str, reason: str = 'unknown'):
        """添加到待处理列表"""
        self.pending_symbols.append({
            'symbol': symbol,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        })

    def save_pending_list(self, output_path: Optional[str] = None):
        """保存待处理列表到文件"""
        if not self.pending_symbols:
            return
        if output_path is None:
            output_dir = STORAGE_DIR / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"pending_symbols_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'count': len(self.pending_symbols),
                'symbols': self.pending_symbols
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"待处理列表已保存: {output_path} ({len(self.pending_symbols)} 只)")

    def sync(self, symbols: List[str], incremental: bool = True,
             start_date: str = None, end_date: str = None, **kwargs) -> Dict:
        """
        同步股票价格（保持原有接口）
        
        Args:
            symbols: 股票代码列表
            incremental: 是否增量更新
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            
        Returns:
            同步结果
        """
        self.sync_options = {
            'incremental': incremental,
            'start_date': start_date,
            'end_date': end_date or get_effective_end_date(),
            'override': kwargs.get('override', False)
        }
        return super().sync(symbols, **kwargs)
    
    def _do_sync(self, symbols: List[str], **kwargs) -> List[Dict]:
        """执行同步"""
        RAW_PRICE_DIR.mkdir(parents=True, exist_ok=True)
        return self._sync_parallel(symbols, self._sync_single)
    
    def _sync_single(self, symbol: str) -> Dict:
        """
        同步单只股票（保持原有逻辑）

        支持增量更新：检查本地文件，从最新日期开始同步
        """
        file_path = RAW_PRICE_DIR / f"{symbol}.parquet"

        try:
            # 确定日期范围
            start_date = self.sync_options.get('start_date')
            end_date = self.sync_options.get('end_date')
            incremental = self.sync_options.get('incremental', True)

            # 限制结束日期为实际可用的最新数据日期
            current_year = datetime.now().year
            max_available_date = f"{current_year + 1}1231"
            if end_date > max_available_date:
                logger.debug(f"限制结束日期从 {end_date} 到 {max_available_date}")
                end_date = max_available_date

            # 快速跳过检查：如果文件已包含目标日期范围
            if start_date and end_date and incremental and file_path.exists():
                try:
                    df_quick = pd.read_parquet(file_path)
                    if not df_quick.empty and 'trade_date' in df_quick.columns:
                        latest_date = df_quick['trade_date'].max()
                        if hasattr(latest_date, 'strftime'):
                            latest_date_str = latest_date.strftime('%Y%m%d')
                        else:
                            latest_date_str = str(latest_date).replace('-', '')
                        if latest_date_str >= end_date:
                            return {
                                'status': 'success',
                                'symbol': symbol,
                                'records': 0,
                                'message': f'Skipped (already up to {latest_date_str})'
                            }
                except Exception:
                    pass

            # 增量模式：检查本地文件
            existing_df = None
            if incremental and file_path.exists():
                try:
                    existing_df = pd.read_parquet(file_path)
                    if not existing_df.empty and 'trade_date' in existing_df.columns:
                        latest_date = pd.to_datetime(existing_df['trade_date']).max()
                        start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
                except Exception as e:
                    logger.warning(f"读取已有数据失败: {e}")

            if start_date is None:
                start_date = "19900101"

            # 日期检查
            if start_date > end_date:
                return {'status': 'skipped', 'symbol': symbol, 'message': 'Already up to date'}

            # 检查日期范围内是否包含工作日
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            has_weekday = False
            current_dt = start_dt
            while current_dt <= end_dt:
                if current_dt.weekday() < 5:
                    has_weekday = True
                    break
                current_dt += timedelta(days=1)
            if not has_weekday:
                logger.info(f"{symbol} 增量日期范围内无交易日，跳过")
                return {
                    'status': 'success',
                    'symbol': symbol,
                    'records': 0,
                    'message': 'No trading days in range'
                }

            if incremental and file_path.exists() and existing_df is not None:
                logger.info(f"{symbol} 增量更新: {start_date} ~ {end_date}")
            else:
                logger.info(f"{symbol} 全量同步: {start_date} ~ {end_date}")

            # 获取数据
            new_df = self._fetch_stock_history(symbol, start_date, end_date)

            if new_df is None or new_df.empty:
                logger.warning(color_log('warning', f"⚠️  {symbol} 没有获取到新数据"))
                return {'status': 'skipped', 'symbol': symbol, 'message': 'No data'}

            # override 模式：合并前删除已有数据中对应日期范围的记录
            override = self.sync_options.get('override', False)
            if override and existing_df is not None and not existing_df.empty:
                start_dt = pd.to_datetime(start_date).date()
                end_dt = pd.to_datetime(end_date).date()
                existing_dates = pd.to_datetime(existing_df['trade_date']).dt.date
                mask = (existing_dates >= start_dt) & (existing_dates <= end_dt)
                if mask.any():
                    existing_df = existing_df[~mask].reset_index(drop=True)
                    logger.info(f"{symbol} 覆盖模式: 已删除 {mask.sum()} 条 [{start_date}~{end_date}] 范围内的已有记录")
                    if existing_df.empty:
                        existing_df = None

            # 合并数据（增量模式）
            if existing_df is not None and not existing_df.empty:
                # 统一 trade_date 类型，避免 datetime64 与 datetime.date 混合
                for df_tmp in (existing_df, new_df):
                    if 'trade_date' in df_tmp.columns:
                        df_tmp['trade_date'] = pd.to_datetime(df_tmp['trade_date']).dt.date
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['trade_date'], keep='last')
                combined = combined.sort_values('trade_date').reset_index(drop=True)
            else:
                combined = new_df

            # 保存
            combined.to_parquet(file_path, index=False, compression='snappy')

            logger.info(color_log('success', f"✓ {symbol} 同步完成: {len(new_df)} 条新数据，共 {len(combined)} 条"))

            return {
                'status': 'success',
                'symbol': symbol,
                'new_records': len(new_df),
                'total_records': len(combined),
                'date_range': f"{combined['trade_date'].min()} ~ {combined['trade_date'].max()}",
                'file_path': str(file_path)
            }

        except Exception as e:
            logger.error(color_log('error', f"❌ {symbol} 同步失败: {e}"))
            return {'status': 'failed', 'symbol': symbol, 'error': str(e)}

    def _fetch_stock_history(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取单只股票历史数据（akshare 新浪接口，前复权）"""
        # 北交所跳过
        if self._is_bj_stock(symbol):
            self._add_to_pending_list(symbol, 'bj_not_supported')
            return None
        return self._fetch_from_akshare(symbol, start_date, end_date)

    def _fetch_bj_stock_history(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """使用 akshare 获取北交所股票历史数据"""
        try:
            code = symbol.replace('.BJ', '')
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )
            if df.empty:
                return None

            df['symbol'] = symbol
            column_map = {
                '日期': 'trade_date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'change_pct'
            }
            df = df.rename(columns=column_map)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
            df = df[[c for c in keep_cols if c in df.columns]]

            logger.info(f"获取 {symbol} 数据: {len(df)} 条 (akshare/北交所)")
            return df

        except Exception as e:
            error_msg = str(e)
            if 'Connection' in error_msg or 'RemoteDisconnected' in error_msg:
                logger.warning(color_log('warning', f"⚠️  {symbol} 跳过: 东财接口限制，北交所数据暂无法自动获取"))
            else:
                logger.warning(color_log('warning', f"⚠️  akshare 获取 {symbol} 失败: {e}"))
            return None
    
    def _fetch_from_akshare(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """使用 akshare 东财接口获取股票历史数据（前复权），支持日期范围"""
        try:
            # 转换代码格式: 600519.SH -> 600519
            code = symbol.replace('.SH', '').replace('.SZ', '')

            # py-mini-racer 非线程安全，必须加锁串行调用
            with _akshare_lock:
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"  # 前复权
                )

            if df is None or df.empty:
                logger.warning(f"{symbol} 东财接口未获取到数据")
                return None

            # 重命名列
            df['symbol'] = symbol
            column_map = {
                '日期': 'trade_date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'change_pct',
            }
            df = df.rename(columns=column_map)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            # 转换数值类型
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 选择需要的列
            keep_cols = [
                'symbol', 'trade_date', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'change_pct'
            ]
            df = df[[c for c in keep_cols if c in df.columns]]

            # 标记数据来源
            df['data_source'] = 'em'

            logger.info(f"获取 {symbol} 数据: {len(df)} 条 (东财, 前复权)")
            return df

        except Exception as e:
            logger.error(color_log('error', f"❌ 东财获取 {symbol} 历史数据失败: {e}"))
            return None

    def sync_all(self, symbols: List[str], incremental: bool = True,
                 max_workers: int = 1, **kwargs) -> Dict:
        """
        批量同步接口（保持兼容）

        Args:
            symbols: 股票代码列表
            incremental: 是否增量
            max_workers: 并发数

        Returns:
            同步结果
        """
        self.max_workers = max_workers
        return self.sync(symbols, incremental=incremental, **kwargs)

    def logout(self):
        """空方法保持兼容（已不再使用 baostock）"""
        pass

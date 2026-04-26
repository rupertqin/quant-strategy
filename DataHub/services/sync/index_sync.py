"""
指数价格同步服务

保持原有功能：
- 支持增量/全量同步
- 支持日线数据获取
- 支持多数据源（新浪优先，备选Yahoo、akshare）
- 支持A股指数和港股指数
- 支持并发同步
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
import akshare as ak

from DataHub.config import RAW_INDEX_PRICE_DIR
from .base import BaseSyncService, color_log, get_effective_end_date

logger = logging.getLogger(__name__)


class IndexSync(BaseSyncService):
    """指数价格同步 - 保持原有功能"""
    
    def __init__(self, max_workers: int = 1, data_source: str = "sina"):
        super().__init__(max_workers=max_workers, request_delay=0.5)
        self.data_source = data_source
    
    def sync(self, symbols: List[str], incremental: bool = True,
             start_date: str = None, end_date: str = None, **kwargs) -> Dict:
        """
        同步指数价格（保持原有接口）
        
        Args:
            symbols: 指数代码列表
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
        RAW_INDEX_PRICE_DIR.mkdir(parents=True, exist_ok=True)
        return self._sync_parallel(symbols, self._sync_single)
    
    def _sync_single(self, symbol: str) -> Dict:
        """
        同步单个指数（保持原有逻辑）
        
        支持增量更新：检查本地文件，从最新日期开始同步
        """
        file_path = RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
        
        try:
            # 确定日期范围
            start_date = self.sync_options.get('start_date')
            end_date = self.sync_options.get('end_date')
            incremental = self.sync_options.get('incremental', True)
            
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
                logger.info(f"{symbol} 数据已是最新，无需更新")
                return {'status': 'success', 'symbol': symbol, 'records': 0, 'message': 'Already up to date'}
            
            # 检查 start_date 到 end_date 之间是否包含工作日（排除全是周末的情况）
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            has_weekday = False
            current_dt = start_dt
            while current_dt <= end_dt:
                if current_dt.weekday() < 5:  # 0-4 是周一到周五
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
            
            # 获取数据 - 根据指数类型选择数据源（与旧版 history_sync 保持一致）
            if symbol.endswith('.HK'):
                new_df = self._fetch_hk_index(symbol, start_date, end_date)
            else:
                # A股指数只使用新浪接口（旧版不 fallback）
                new_df = self._fetch_from_sina(symbol, start_date, end_date)
            
            if new_df is None or new_df.empty:
                logger.warning(color_log('warning', f"⚠️  {symbol} 没有获取到新数据"))
                return {'status': 'failed', 'symbol': symbol, 'records': 0, 'message': 'No new data'}
            
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
                # 对齐 dtypes，避免 FutureWarning
                for col in new_df.columns:
                    if col in existing_df.columns and existing_df[col].dtype != new_df[col].dtype:
                        new_df[col] = new_df[col].astype(existing_df[col].dtype, errors='ignore')
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['trade_date'], keep='last')
                combined = combined.sort_values('trade_date')
            else:
                combined = new_df

            # 基于完整合并后的数据重新计算 change_pct，避免增量时新数据首行 NaN
            if 'close' in combined.columns and len(combined) > 1:
                combined['change_pct'] = combined['close'].pct_change(fill_method=None) * 100
            elif 'close' in combined.columns and len(combined) == 1:
                combined['change_pct'] = np.nan

            # 保存
            combined.to_parquet(file_path, index=False, compression='snappy')
            
            logger.info(color_log('success', f"✓ {symbol} 同步完成: {len(new_df)} 条新数据，共 {len(combined)} 条"))
            
            return {
                'status': 'success',
                'symbol': symbol,
                'new_records': len(new_df),
                'total_records': len(combined)
            }
            
        except Exception as e:
            logger.error(color_log('error', f"❌ {symbol} 同步失败: {e}"))
            return {'status': 'failed', 'symbol': symbol, 'error': str(e)}
    
    def _fetch_from_sina(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从新浪接口获取指数数据（首选）- 与旧版 history_sync 保持一致"""
        # 转换代码格式: 000001.SH -> sh000001
        sina_symbol = self._format_code_sina(symbol)
        
        # 使用 akshare 的新浪接口获取指数日线数据（全量获取 + 本地筛选）
        df = ak.stock_zh_index_daily(symbol=sina_symbol)
        
        if df is None or df.empty:
            logger.warning(f"{symbol} 未获取到数据")
            return None
        
        # 转换日期格式并筛选日期范围
        df['date'] = pd.to_datetime(df['date']).dt.date
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
        
        if df.empty:
            logger.warning(f"{symbol} 日期范围内无数据")
            return None
        
        # 标准化列名
        df['symbol'] = symbol
        df = df.rename(columns={
            'date': 'trade_date',
            'close': 'close',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'volume': 'volume'
        })
        
        # 计算涨跌幅
        df['change_pct'] = df['close'].pct_change(fill_method=None) * 100

        # 新浪接口没有 amount 列，设为 NaN
        df['amount'] = np.nan

        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]

        return df
    
    def _fetch_from_yfinance(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 Yahoo Finance 获取指数数据（备选）"""
        try:
            import yfinance as yf
        except ImportError:
            logger.warning("yfinance 未安装，跳过 Yahoo Finance 数据源")
            return None
        
        # Yahoo Finance 指数代码映射
        yf_mapping = {
            '000001.SH': '^SSEC',      # 上证指数
            '000300.SH': '000300.SS',  # 沪深300
            '000016.SH': '000016.SS',  # 上证50
            '000688.SH': '000688.SS',  # 科创50
            '399001.SZ': '399001.SZ',  # 深证成指
            '399006.SZ': '399006.SZ',  # 创业板指
        }
        
        yf_symbol = yf_mapping.get(symbol)
        if not yf_symbol:
            logger.warning(f"未找到 {symbol} 的 Yahoo Finance 映射")
            return None
        
        # 转换日期格式
        start = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(start=start, end=end)
        
        if df.empty:
            return pd.DataFrame()
        
        # 标准化列名
        df = df.reset_index()
        df['symbol'] = symbol
        df['trade_date'] = pd.to_datetime(df['Date']).dt.date
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # 盘中过滤今天的实时数据（与旧版 history_sync 保持一致）
        now = datetime.now()
        today = now.date()
        market_closed = now.hour >= 15
        
        if not market_closed:
            df = df[df['trade_date'] < today]
            if df.empty:
                logger.warning(f"{symbol} 无历史数据（返回的只有今天的实时数据）")
                return pd.DataFrame()
        
        # Yahoo 数据没有 amount 和 change_pct，设为 NaN 保持类型一致
        df['amount'] = np.nan
        df['change_pct'] = np.nan
        
        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        return df
    
    def _fetch_from_akshare(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 akshare 获取指数数据（最后备选）"""
        # 转换代码格式
        code = symbol.replace('.SH', '').replace('.SZ', '')
        
        # 使用 akshare 的指数历史接口
        df = ak.index_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            return df
        
        # 标准化列名
        df['symbol'] = symbol
        df['trade_date'] = pd.to_datetime(df['日期']).dt.date
        df = df.rename(columns={
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct'
        })
        
        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        return df
    
    def _fetch_hk_index(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取港股指数数据 - 优先 akshare，备选 yfinance"""
        # 1. 优先使用 akshare 新浪接口（港股指数数据更稳定）
        hk_mapping = {
            'HSI.HK': 'HSI',
            'HSTECH.HK': 'HSTECH',
        }
        ak_symbol = hk_mapping.get(symbol)
        if ak_symbol:
            try:
                df = ak.stock_hk_index_daily_sina(symbol=ak_symbol)
                if df is not None and not df.empty:
                    df['symbol'] = symbol
                    df = df.rename(columns={
                        'date': 'trade_date',
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume',
                    })
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    start_dt = pd.to_datetime(start_date).date()
                    end_dt = pd.to_datetime(end_date).date()
                    df = df[(df['trade_date'] >= start_dt) & (df['trade_date'] <= end_dt)]
                    if not df.empty:
                        df['change_pct'] = df['close'].pct_change(fill_method=None) * 100
                        df['amount'] = None
                        logger.info(f"获取 {symbol} 港股指数数据: {len(df)} 条 (akshare)")
                        return df
            except Exception as e:
                logger.warning(f"akshare 获取港股指数 {symbol} 失败: {e}")

        # 2. 备选：yfinance
        yf_mapping = {
            'HSI.HK': '^HSI',
            'HSTECH.HK': '^HSTECH',
        }
        yf_symbol = yf_mapping.get(symbol)
        if yf_symbol:
            try:
                import yfinance as yf
                start = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                end = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')

                ticker = yf.Ticker(yf_symbol)
                df = ticker.history(
                    start=start,
                    end=end,
                    auto_adjust=False,
                    actions=False,
                    repair=False,
                )

                if not df.empty:
                    logger.debug(f"yfinance 原始返回 {symbol}: {len(df)} 条")
                    df = df.reset_index()
                    df['symbol'] = symbol
                    df['trade_date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.date
                    df = df.rename(columns={
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume',
                    })
                    start_dt = pd.to_datetime(start_date).date()
                    end_dt = pd.to_datetime(end_date).date()
                    df = df[(df['trade_date'] >= start_dt) & (df['trade_date'] <= end_dt)]

                    if not df.empty:
                        df['change_pct'] = df['close'].pct_change(fill_method=None) * 100
                        df['amount'] = None
                        logger.info(f"获取 {symbol} 港股指数数据: {len(df)} 条 (yfinance)")
                        return df
            except Exception as e:
                logger.warning(f"yfinance 获取 {symbol} 失败: {e}")

        logger.warning(f"{symbol} 未获取到数据")
        return None
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]

        logger.info(f"获取 {symbol} 港股指数数据: {len(df)} 条 (akshare)")
        return df
    
    def _format_code_sina(self, symbol: str) -> str:
        """转换为新浪代码格式"""
        if '.SH' in symbol:
            return "sh" + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return "sz" + symbol.replace('.SZ', '')
        return symbol
    
    def sync_all(self, symbols: List[str], incremental: bool = True,
                 max_workers: int = 1, **kwargs) -> Dict:
        """
        批量同步接口（保持兼容）
        
        Args:
            symbols: 指数代码列表
            incremental: 是否增量
            max_workers: 并发数
            
        Returns:
            同步结果
        """
        self.max_workers = max_workers
        return self.sync(symbols, incremental=incremental, **kwargs)
    
    def logout(self):
        """清理资源（指数同步无需登出）"""
        pass

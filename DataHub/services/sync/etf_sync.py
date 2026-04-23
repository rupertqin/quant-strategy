"""
ETF价格同步服务

保持原有功能：
- 支持增量/全量同步
- 支持日线数据获取
- 支持多数据源（baostock优先，备选yfinance、东财）
- 支持并发同步
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
import baostock as bs
import akshare as ak

from DataHub.config import RAW_ETF_PRICE_DIR
from .base import BaseSyncService, color_log, get_effective_end_date

logger = logging.getLogger(__name__)

# 全局 baostock 锁（多线程共享）
_baostock_lock = threading.Lock()


class ETFSync(BaseSyncService):
    """ETF价格同步 - 保持原有功能
    
    注意：ETF 优先使用 Yahoo Finance（国内访问 baostock 对 ETF 支持不稳定）
    """
    
    def __init__(self, max_workers: int = 1, data_source: str = "yahoo"):
        super().__init__(max_workers=max_workers, request_delay=(0.5, 2.0))
        self.data_source = data_source
        self._baostock_logged_in = False

    def _ensure_login(self):
        """登录 baostock（线程安全，延迟加载）"""
        if self._baostock_logged_in:
            return
        with _baostock_lock:
            if self._baostock_logged_in:
                return
            try:
                lg = bs.login()
                if lg.error_code != '0':
                    logger.error(color_log('error', f"❌ baostock 登录失败: {lg.error_msg}"))
                else:
                    logger.info("baostock 登录成功")
                    self._baostock_logged_in = True
            except Exception as e:
                logger.error(f"baostock 登录失败: {e}")
                raise
    
    def sync(self, symbols: List[str], incremental: bool = True,
             start_date: str = None, end_date: str = None, **kwargs) -> Dict:
        """
        同步ETF价格（保持原有接口）
        
        Args:
            symbols: ETF代码列表
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
        RAW_ETF_PRICE_DIR.mkdir(parents=True, exist_ok=True)
        return self._sync_parallel(symbols, self._sync_single)
    
    def _sync_single(self, symbol: str) -> Dict:
        """
        同步单只ETF（保持原有逻辑）
        
        支持增量更新：检查本地文件，从最新日期开始同步
        """
        file_path = RAW_ETF_PRICE_DIR / f"{symbol}.parquet"
        
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
            
            # 日期检查（在打印日志前执行，避免输出混乱）
            if start_date > end_date:
                logger.info(color_log('info', f"⚠️  {symbol} 数据已是最新，无需更新"))
                return {'status': 'skipped', 'symbol': symbol, 'message': 'Already up to date'}
            
            if incremental and file_path.exists() and existing_df is not None:
                logger.info(f"{symbol} 增量更新: {start_date} ~ {end_date}")
            else:
                logger.info(f"{symbol} 全量同步: {start_date} ~ {end_date}")
            
            # 获取数据 - 按优先级尝试多个数据源
            # ETF 默认 Yahoo 优先（国内 baostock 对 ETF 支持不稳定）
            new_df = None
            primary = self.data_source
            sources = []
            
            if primary == "yahoo":
                sources = [
                    ("yahoo", self._fetch_from_yfinance),
                    ("baostock", self._fetch_from_baostock),
                    ("em", self._fetch_from_em),
                ]
            else:
                sources = [
                    ("baostock", self._fetch_from_baostock),
                    ("yahoo", self._fetch_from_yfinance),
                    ("em", self._fetch_from_em),
                ]
            
            for source_name, fetch_func in sources:
                if new_df is not None and not new_df.empty:
                    break
                try:
                    new_df = fetch_func(symbol, start_date, end_date)
                    if new_df is not None and not new_df.empty:
                        logger.debug(f"{symbol} 从 {source_name} 获取成功")
                except Exception as e:
                    logger.warning(color_log('warning', f"{source_name} 获取 {symbol} 失败: {e}"))
            
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
                # 对齐 dtypes，避免 FutureWarning（空列 object 与 float64 不一致）
                for col in new_df.columns:
                    if col in existing_df.columns and existing_df[col].dtype != new_df[col].dtype:
                        new_df[col] = new_df[col].astype(existing_df[col].dtype, errors='ignore')
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['trade_date'], keep='last')
                combined = combined.sort_values('trade_date')
            else:
                combined = new_df
            
            # 保存
            combined.to_parquet(file_path, index=False, compression='zstd')
            
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
    
    def _fetch_from_baostock(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 baostock 获取ETF数据"""
        self._ensure_login()
        code = self._format_code(symbol)
        if code is None:
            logger.warning(f"{symbol} 无法转换为 baostock 格式")
            return None

        # baostock 要求 YYYY-MM-DD 格式
        def _fmt_date(d):
            if len(d) == 8:
                return f"{d[:4]}-{d[4:6]}-{d[6:]}"
            return d

        try:
            with _baostock_lock:
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                    start_date=_fmt_date(start_date),
                    end_date=_fmt_date(end_date),
                    frequency="d",
                    adjustflag="3"  # 不复权
                )

                if rs is None:
                    logger.warning(f"baostock 返回 None（可能未登录或网络异常）: {symbol}")
                    return None

                if rs.error_code != '0':
                    logger.warning(f"baostock 获取 {symbol} 失败: {rs.error_msg}")
                    return None

                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

            if not data_list:
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df['trade_date'] = pd.to_datetime(df['date']).dt.date
            df['symbol'] = symbol

            column_map = {
                'date': 'trade_date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'amount': 'amount',
                'pctChg': 'change_pct'
            }
            df = df.rename(columns=column_map)

            keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
            df = df[[c for c in keep_cols if c in df.columns]]

            logger.info(f"获取 {symbol} ETF数据: {len(df)} 条 (baostock)")
            return df

        except Exception as e:
            logger.warning(f"baostock 获取 {symbol} ETF数据失败: {e}")
            return None
    
    def _fetch_from_yfinance(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 Yahoo Finance 获取ETF数据（备选）"""
        try:
            import yfinance as yf
        except ImportError:
            logger.warning("yfinance 未安装，跳过 Yahoo Finance 数据源")
            return None
        
        # 转换代码格式 510300.SH -> 510300.SS
        yf_symbol = symbol.replace('.SH', '.SS').replace('.SZ', '.SZ')
        
        # 转换日期格式
        start = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(start=start, end=end, timeout=30)

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
        
        # Yahoo 数据没有 amount/change_pct，设为 NaN（保持 float64 类型一致）
        df['amount'] = np.nan
        df['change_pct'] = np.nan
        
        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        logger.info(f"获取 {symbol} ETF数据: {len(df)} 条 (Yahoo)")
        return df
    
    def _fetch_from_em(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从东财接口获取ETF数据（最后备选）"""
        # 转换代码格式 510300.SH -> 510300
        code = symbol.replace('.SH', '').replace('.SZ', '')
        
        df = ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"  # 前复权
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
    
    def _format_code(self, symbol: str) -> str:
        """转换代码格式"""
        if '.SH' in symbol:
            return "sh." + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return "sz." + symbol.replace('.SZ', '')
        return symbol
    
    def sync_all(self, symbols: List[str], incremental: bool = True,
                 max_workers: int = 1, **kwargs) -> Dict:
        """
        批量同步接口（保持兼容）
        
        Args:
            symbols: ETF代码列表
            incremental: 是否增量
            max_workers: 并发数
            
        Returns:
            同步结果
        """
        self.max_workers = max_workers
        return self.sync(symbols, incremental=incremental, **kwargs)
    
    def logout(self):
        """登出"""
        if self._baostock_logged_in:
            try:
                bs.logout()
                self._baostock_logged_in = False
            except:
                pass

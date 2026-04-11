"""
ETF数据爬虫 - 获取ETF价格和净值数据
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
import time
import akshare as ak

from .base_crawler import BaseCrawler


class ETFCrawler(BaseCrawler):
    """
    ETF数据爬虫
    
    获取ETF实时行情和历史数据
    """
    
    def __init__(self, repository=None):
        super().__init__(repository)
        self.source = 'akshare'
    
    def fetch(
        self,
        symbol: str = None,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        data_type: str = 'price'
    ) -> pd.DataFrame:
        """
        获取ETF数据
        
        Args:
            symbol: 单只ETF代码
            symbols: 多只ETF代码列表
            start_date: 开始日期
            end_date: 结束日期
            data_type: 'price'(价格) | 'nav'(净值)
            
        Returns:
            DataFrame
        """
        if data_type == 'price':
            return self._fetch_price(symbol, symbols, start_date, end_date)
        elif data_type == 'nav':
            return self._fetch_nav(symbol, symbols, start_date, end_date)
        else:
            raise ValueError(f"未知的数据类型: {data_type}")
    
    def _fetch_price(
        self,
        symbol: str = None,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取ETF价格数据
        
        ETF价格使用fund_etf_hist_sina接口
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        if symbols is None and symbol is not None:
            symbols = [symbol]
        elif symbols is None:
            raise ValueError("必须提供 symbol 或 symbols 参数")
        
        all_data = []
        
        for sym in symbols:
            try:
                # 转换代码: 510050.SH -> sh510050
                code = self._format_code(sym)
                
                # 获取历史数据
                df = ak.fund_etf_hist_sina(symbol=code)
                
                if df.empty:
                    continue
                
                # 添加symbol列
                df['symbol'] = sym
                
                # 日期筛选
                df['date'] = pd.to_datetime(df['date'])
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                
                if not df.empty:
                    all_data.append(df)
                
                time.sleep(0.3)
                
            except Exception as e:
                self.logger.error(f"获取ETF {sym} 失败: {e}")
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        return pd.concat(all_data, ignore_index=True)
    
    def _fetch_nav(
        self,
        symbol: str = None,
        symbols: List[str] = None,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取ETF净值数据
        
        注：akshare没有直接的ETF净值接口，这里用价格数据作为替代
        实际使用时可能需要其他数据源
        """
        self.logger.warning("ETF净值数据暂不支持，使用价格数据替代")
        return self._fetch_price(symbol, symbols, start_date, end_date)
    
    def fetch_all_etf_list(self) -> pd.DataFrame:
        """
        获取全市场ETF列表
        
        Returns:
            DataFrame with columns: symbol, code, name, exchange
        """
        try:
            # 使用akshare获取ETF列表
            df = ak.fund_etf_spot_em()
            
            if df.empty:
                return pd.DataFrame()
            
            # 标准化列名
            column_map = {
                '代码': 'code',
                '名称': 'name',
                '最新价': 'price',
                '涨跌幅': 'change_pct'
            }
            df = df.rename(columns=column_map)
            
            # 判断交易所并添加symbol
            def get_exchange(code):
                if code.startswith(('5', '51', '58')):
                    return 'SH'
                elif code.startswith(('1', '2')):
                    return 'SZ'
                return 'SH'
            
            df['exchange'] = df['code'].apply(get_exchange)
            df['symbol'] = df['code'] + '.' + df['exchange']
            df['type'] = 'ETF'
            
            return df[['symbol', 'code', 'name', 'exchange', 'type']]
            
        except Exception as e:
            self.logger.error(f"获取ETF列表失败: {e}")
            return pd.DataFrame()
    
    def _format_code(self, symbol: str) -> str:
        """
        转换代码格式
        510050.SH -> sh510050
        159915.SZ -> sz159915
        """
        if '.SH' in symbol:
            return 'sh' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return 'sz' + symbol.replace('.SZ', '')
        return symbol
    
    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗ETF数据
        """
        # 重命名列
        column_map = {
            'date': 'trade_date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'amount': 'amount'  # 可能不存在
        }
        
        # 只重命名存在的列
        existing_map = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=existing_map)
        
        # 转换日期格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        # 选择需要的列
        keep_cols = [
            'symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume'
        ]
        keep_cols = [c for c in keep_cols if c in df.columns]
        df = df[keep_cols]
        
        # 删除异常数据
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df = df[df[col] > 0]
        
        # 删除重复
        df = df.drop_duplicates(subset=['symbol', 'trade_date'], keep='first')
        
        return df
    
    def _save(self, df: pd.DataFrame) -> int:
        """
        保存到数据库
        """
        if self.repository is None:
            self.logger.warning("没有repository，数据未保存")
            return 0
        
        try:
            count = self.repository.save_etf_prices(df)
            return count
        except Exception as e:
            self.logger.error(f"保存ETF数据失败: {e}")
            return 0
    
    def sync_latest(self, symbols: List[str] = None) -> dict:
        """
        同步最新ETF数据
        """
        if symbols is None and self.repository:
            etfs = self.repository.get_all_etfs()
            symbols = etfs['symbol'].tolist() if not etfs.empty else []
        
        if not symbols:
            return {'status': 'failed', 'records': 0, 'message': 'No ETF symbols to sync'}
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)
        
        return self.sync(
            symbols=symbols,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            data_type='price'
        )

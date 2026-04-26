"""
股票数据仓库 - 提供股票相关的所有数据库操作
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

from .base_repository import BaseRepository
from DataHub.config import RAW_PRICE_DIR


# 价格数据存储路径（Parquet格式，符合规则10.1和11.1）
PRICE_DIR = RAW_PRICE_DIR


class StockRepository(BaseRepository):
    """
    股票数据仓库
    
    封装所有股票相关的数据库操作：
    - 基础信息查询
    - 价格数据查询和保存
    - 基本面数据查询和保存
    """
    
    # ==================== 基础信息 ====================
    
    def get_all_stocks(self) -> pd.DataFrame:
        """
        获取所有股票基础信息
        
        Returns:
            DataFrame with columns: symbol, code, name, exchange, industry
        """
        sql = """
            SELECT symbol, code, name, exchange, industry, ipo_date as list_date, 
                   1 as is_active
            FROM stock_basic
            ORDER BY symbol
        """
        return self.read_sql(sql)
    
    def get_stock_by_symbol(self, symbol: str) -> Optional[Dict]:
        """
        根据代码获取单只股票信息
        
        Args:
            symbol: 股票代码，如 '600519.SH'
            
        Returns:
            股票信息字典，不存在返回None
        """
        sql = """
            SELECT symbol, code, name, exchange, industry, ipo_date as list_date
            FROM stock_basic
            WHERE symbol = ?
        """
        result = self.execute(sql, (symbol,), fetch=True)
        return dict(result[0]) if result else None
    
    def search_stocks(self, keyword: str) -> pd.DataFrame:
        """
        根据关键词搜索股票
        
        Args:
            keyword: 股票代码或名称关键词
            
        Returns:
            匹配的股票列表
        """
        sql = """
            SELECT symbol, code, name, exchange, industry
            FROM stock_basic
            WHERE (symbol LIKE ? OR name LIKE ? OR code LIKE ?)
            ORDER BY symbol
            LIMIT 20
        """
        pattern = f'%{keyword}%'
        return self.read_sql(sql, (pattern, pattern, pattern))
    
    # ==================== 价格数据 ====================
    
    def get_daily_price(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        fields: List[str] = None
    ) -> pd.DataFrame:
        """
        获取股票日线数据
        
        从Parquet文件读取（价格数据唯一数据源）
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            fields: 指定字段列表，None表示全部
            
        Returns:
            DataFrame with index=trade_date
        """
        parquet_path = PRICES_DIR / f"{symbol}.parquet"
        
        if not parquet_path.exists():
            return pd.DataFrame()
        
        try:
            df = pd.read_parquet(parquet_path)
            
            if df.empty:
                return pd.DataFrame()
            
            # 转换日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 日期过滤
            if start_date:
                df = df[df['trade_date'] >= start_date]
            if end_date:
                df = df[df['trade_date'] <= end_date]
            
            # 字段选择
            if fields:
                available_fields = ['trade_date'] + [f for f in fields if f in df.columns]
                df = df[available_fields]
            
            # 排序并设置索引
            df = df.sort_values('trade_date')
            if 'trade_date' in df.columns:
                df = df.set_index('trade_date')
            
            return df
            
        except Exception as e:
            self.logger.error(f"读取 {symbol} 价格数据失败: {e}")
            return pd.DataFrame()
    
    def get_multiple_prices(
        self,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None,
        field: str = 'close'
    ) -> pd.DataFrame:
        """
        获取多只股票的价格数据（宽格式）
        
        从Parquet文件读取（价格数据唯一数据源）
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            field: 要获取的字段，默认'close'
            
        Returns:
            DataFrame: index=date, columns=symbols
        """
        if not symbols:
            return pd.DataFrame()
        
        all_data = []
        
        for symbol in symbols:
            parquet_path = PRICES_DIR / f"{symbol}.parquet"
            
            if not parquet_path.exists():
                continue
            
            try:
                df = pd.read_parquet(parquet_path)
                
                if df.empty or field not in df.columns:
                    continue
                
                # 日期过滤
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    
                    if start_date:
                        df = df[df['trade_date'] >= start_date]
                    if end_date:
                        df = df[df['trade_date'] <= end_date]
                
                df = df[['trade_date', field]].copy()
                df['symbol'] = symbol
                all_data.append(df)
                
            except Exception as e:
                self.logger.warning(f"读取 {symbol} 失败: {e}")
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        df = pd.concat(all_data, ignore_index=True)
        
        if df.empty:
            return pd.DataFrame()
        
        # 转换为宽格式
        pivot_df = df.pivot(index='trade_date', columns='symbol', values=field)
        return pivot_df
    
    def save_daily_prices(self, df: pd.DataFrame) -> int:
        """
        保存日线数据到Parquet文件
        
        价格数据唯一存储位置：storage/raw/stocks/price/{symbol}.parquet
        
        Args:
            df: DataFrame with columns: symbol, trade_date, open, high, low, close, volume...
            
        Returns:
            保存的记录数
        """
        if df.empty:
            return 0
        
        # 确保必要列存在
        required_cols = ['symbol', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")
        
        # 转换日期格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        total_saved = 0
        
        # 按symbol分组保存到各自的Parquet文件
        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol].copy()
            
            parquet_path = PRICES_DIR / f"{symbol}.parquet"
            
            try:
                if parquet_path.exists():
                    # 读取现有数据
                    existing_df = pd.read_parquet(parquet_path)
                    existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.date
                    
                    # 合并并去重（保留新数据）
                    combined_df = pd.concat([existing_df, symbol_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(
                        subset=['symbol', 'trade_date'], 
                        keep='last'
                    )
                    saved_count = len(combined_df) - len(existing_df)
                else:
                    # 新文件
                    combined_df = symbol_df
                    saved_count = len(symbol_df)
                    
                    # 确保目录存在
                    PRICES_DIR.mkdir(parents=True, exist_ok=True)
                
                # 保存到Parquet（按日期排序）
                combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
                combined_df.to_parquet(parquet_path, index=False, compression='snappy')
                
                total_saved += saved_count
                
            except Exception as e:
                self.logger.error(f"保存 {symbol} 价格数据失败: {e}")
                raise
        
        self.logger.info(f"保存日线数据: {total_saved} 条记录到Parquet")
        return total_saved
    
    def get_latest_price_date(self, symbol: str) -> Optional[str]:
        """
        获取某只股票的最新价格日期
        
        从Parquet文件读取（价格数据唯一数据源）
        
        Args:
            symbol: 股票代码
            
        Returns:
            最新日期字符串 'YYYY-MM-DD'，无数据返回None
        """
        parquet_path = PRICES_DIR / f"{symbol}.parquet"
        
        if not parquet_path.exists():
            return None
        
        try:
            df = pd.read_parquet(parquet_path)
            if df.empty or 'trade_date' not in df.columns:
                return None
            
            latest_date = pd.to_datetime(df['trade_date']).max()
            return latest_date.strftime('%Y-%m-%d')
        except Exception as e:
            self.logger.warning(f"读取 {symbol} Parquet文件失败: {e}")
            return None
    
    # ==================== 基本面数据 ====================
    
    def get_fundamental(
        self,
        symbol: str,
        report_type: str = None
    ) -> pd.DataFrame:
        """
        获取股票基本面数据
        
        Args:
            symbol: 股票代码
            report_type: 报告类型，如 '年报'、'季报'
            
        Returns:
            基本面数据DataFrame
        """
        sql = """
            SELECT report_date, report_type, eps, bps, roe,
                   revenue, net_profit, pe_ttm, pb, market_cap
            FROM stock_fundamental
            WHERE symbol = ?
        """
        params = [symbol]
        
        if report_type:
            sql += " AND report_type = ?"
            params.append(report_type)
        
        sql += " ORDER BY report_date DESC"
        
        return self.read_sql(sql, tuple(params))
    
    def save_fundamental(self, df: pd.DataFrame) -> int:
        """
        保存基本面数据
        
        Args:
            df: 基本面数据DataFrame
            
        Returns:
            保存的记录数
        """
        return self.save_dataframe(df, 'stock_fundamental', if_exists='append')
    
    # ==================== ETF相关 ====================
    
    def get_all_etfs(self) -> pd.DataFrame:
        """
        获取所有ETF基础信息
        
        Returns:
            ETF列表DataFrame
        """
        sql = """
            SELECT symbol, code, name, exchange, etf_type, '' as tracking_index
            FROM etf_basic
            ORDER BY symbol
        """
        return self.read_sql(sql)
    
    def get_etf_daily_price(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取ETF日线数据
        """
        sql = """
            SELECT trade_date, open, high, low, close, volume, nav, premium_ratio
            FROM etf_daily_price
            WHERE symbol = ?
        """
        params = [symbol]
        
        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY trade_date"
        
        df = self.read_sql(sql, tuple(params), parse_dates=['trade_date'])
        if not df.empty and 'trade_date' in df.columns:
            df.set_index('trade_date', inplace=True)
        return df
    
    def save_etf_prices(self, df: pd.DataFrame) -> int:
        """
        保存ETF价格数据
        """
        if df.empty:
            return 0
        
        # 确保日期格式正确
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        return self.save_dataframe(df, 'etf_daily_price', if_exists='append')
    
    # ==================== 统计方法 ====================
    
    def get_price_stats(self) -> Dict[str, Any]:
        """
        获取价格数据统计信息
        
        Returns:
            统计信息字典
        """
        stats = {}
        
        # 总记录数
        result = self.execute(
            "SELECT COUNT(*) as count FROM stock_daily_price",
            fetch=True
        )
        stats['total_price_records'] = result[0]['count'] if result else 0
        
        # 覆盖的股票数
        result = self.execute(
            "SELECT COUNT(DISTINCT symbol) as count FROM stock_daily_price",
            fetch=True
        )
        stats['covered_stocks'] = result[0]['count'] if result else 0
        
        # 日期范围
        result = self.execute(
            "SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date FROM stock_daily_price",
            fetch=True
        )
        if result:
            stats['date_range'] = {
                'min': result[0]['min_date'],
                'max': result[0]['max_date']
            }
        
        return stats

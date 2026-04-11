"""
股票数据仓库 - 提供股票相关的所有数据库操作
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from .base_repository import BaseRepository


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
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            fields: 指定字段列表，None表示全部
            
        Returns:
            DataFrame with index=trade_date
        """
        # 默认字段
        all_fields = [
            'trade_date', 'open', 'high', 'low', 'close',
            'volume', 'amount', 'change_pct', 'turnover_ratio'
        ]
        
        select_fields = fields if fields else all_fields
        field_str = ', '.join(select_fields)
        
        sql = f"""
            SELECT {field_str}
            FROM stock_daily_price
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
    
    def get_multiple_prices(
        self,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None,
        field: str = 'close'
    ) -> pd.DataFrame:
        """
        获取多只股票的价格数据（宽格式）
        
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
        
        # 构建IN子句
        placeholders = ', '.join(['?' for _ in symbols])
        
        sql = f"""
            SELECT trade_date, symbol, {field}
            FROM stock_daily_price
            WHERE symbol IN ({placeholders})
        """
        params = list(symbols)
        
        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)
        
        df = self.read_sql(sql, tuple(params), parse_dates=['trade_date'])
        
        if df.empty:
            return pd.DataFrame()
        
        # 转换为宽格式
        pivot_df = df.pivot(index='trade_date', columns='symbol', values=field)
        return pivot_df
    
    def save_daily_prices(self, df: pd.DataFrame) -> int:
        """
        保存日线数据到数据库
        
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
        
        # 使用INSERT OR REPLACE处理重复数据
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # 准备插入语句
            columns = df.columns.tolist()
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"""
                INSERT OR REPLACE INTO stock_daily_price 
                ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            # 批量插入
            data = [tuple(row) for row in df.values]
            cursor.executemany(sql, data)
            conn.commit()
            
            saved_count = cursor.rowcount
            self.logger.info(f"保存日线数据: {saved_count} 条记录")
            return saved_count
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"保存日线数据失败: {e}")
            raise
        finally:
            conn.close()
    
    def get_latest_price_date(self, symbol: str) -> Optional[str]:
        """
        获取某只股票的最新价格日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            最新日期字符串 'YYYY-MM-DD'，无数据返回None
        """
        sql = """
            SELECT MAX(trade_date) as latest_date
            FROM stock_daily_price
            WHERE symbol = ?
        """
        result = self.execute(sql, (symbol,), fetch=True)
        
        if result and result[0]['latest_date']:
            return result[0]['latest_date']
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

"""
Unified Data Client - 统一数据获取客户端
封装所有 akshare 和 baostock 调用
"""

import logging
from typing import Optional, List
import pandas as pd

logger = logging.getLogger(__name__)


class UnifiedDataClient:
    """
    统一数据获取客户端
    
    封装所有 akshare 和 baostock 的数据获取调用
    """
    
    def __init__(self, enable_baostock_fallback: bool = True):
        """
        初始化统一数据客户端
        
        Args:
            enable_baostock_fallback: 是否启用 baostock 作为备用源（当前默认优先使用baostock）
        """
        self.enable_baostock_fallback = enable_baostock_fallback
        
        # 检查数据源可用性
        self._akshare_available = self._check_akshare()
        self._baostock_available = self._check_baostock() if enable_baostock_fallback else False
        
        # baostock 登录状态
        self._baostock_logged_in = False
        
        logger.info(f"UnifiedDataClient initialized: akshare={self._akshare_available}, "
                   f"baostock={self._baostock_available}")
    
    def _check_akshare(self) -> bool:
        """检查 akshare 是否可用"""
        try:
            import akshare as ak
            return True
        except ImportError:
            logger.warning("akshare not available")
            return False
    
    def _check_baostock(self) -> bool:
        """检查 baostock 是否可用"""
        try:
            import baostock as bs
            return True
        except ImportError:
            logger.warning("baostock not available")
            return False
    
    def _baostock_login(self):
        """登录 baostock"""
        if not self._baostock_available or self._baostock_logged_in:
            return
        
        import baostock as bs
        try:
            lg = bs.login()
            if lg.error_code == "0":
                self._baostock_logged_in = True
                logger.info("baostock login success")
            else:
                logger.error(f"baostock login failed: {lg.error_msg}")
        except Exception as e:
            logger.error(f"baostock login error: {e}")
    
    def _baostock_logout(self):
        """登出 baostock"""
        if self._baostock_logged_in:
            import baostock as bs
            try:
                bs.logout()
                self._baostock_logged_in = False
                logger.info("baostock logout success")
            except Exception as e:
                logger.warning(f"baostock logout error: {e}")
    
    # ==================== 股票数据接口 ====================
    
    def get_stock_hist(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取A股历史行情数据（支持日线/周线/月线）
        
        Args:
            symbol: 股票代码，如 "000428.SZ"
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期
            period: 周期 - "daily"(日线), "weekly"(周线), "monthly"(月线)
            adjust: 复权类型 - "qfq"(前复权), "hfq"(后复权), ""(不复权)
        
        Returns:
            DataFrame 包含历史行情数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        start = start_date.replace("-", "")
        end = end_date.replace("-", "")
        
        # period 参数映射: daily/weekly/monthly
        period_map = {
            "daily": "daily",
            "weekly": "weekly", 
            "monthly": "monthly"
        }
        ak_period = period_map.get(period, "daily")
        
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period=ak_period,
            start_date=start,
            end_date=end,
            adjust=adjust
        )
        
        if df is not None and not df.empty:
            # yfinance 返回的列名是英文，需要转换
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
            # 重命名列以匹配统一格式
            column_map = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
            }
            df = df.rename(columns=column_map)
            df = df.sort_index()
        
        return df
    
    def get_etf_hist(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取ETF历史行情数据（支持日线/周线/月线）
        
        Args:
            symbol: ETF代码，如 "510300"
            start_date: 开始日期
            end_date: 结束日期
            period: 周期 - "daily"(日线), "weekly"(周线), "monthly"(月线)
            adjust: 复权类型
        
        Returns:
            DataFrame 包含历史行情数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        start = start_date.replace("-", "")
        end = end_date.replace("-", "")
        
        # period 参数映射: daily/weekly/monthly
        period_map = {
            "daily": "daily",
            "weekly": "weekly",
            "monthly": "monthly"
        }
        ak_period = period_map.get(period, "daily")
        
        # 使用 Yahoo Finance 获取 ETF 前复权数据
        # Yahoo 的 Close 列默认已经是前复权价格
        try:
            import yfinance as yf
            
            # 转换代码格式: 510300.SH -> 510300.SS
            if symbol.endswith(".SH"):
                yf_symbol = symbol.replace(".SH", ".SS")
            elif symbol.endswith(".SZ"):
                yf_symbol = symbol.replace(".SZ", ".SZ")
            else:
                yf_symbol = symbol
            
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(start=start_date, end=end_date)
            
        except Exception as e:
            logger.warning(f"Yahoo Finance failed for {symbol}: {e}, falling back to East Money")
            df = pd.DataFrame()
        
        # Yahoo Finance 失败时回退到东财接口
        if df is None or df.empty:
            try:
                clean_symbol = symbol.replace(".SH", "").replace(".SZ", "")
                df = ak.fund_etf_hist_em(
                    symbol=clean_symbol,
                    period=ak_period,
                    start_date=start,
                    end_date=end,
                    adjust="qfq"
                )
                # 重命名东财返回的中文列
                if df is not None and not df.empty:
                    column_map_em = {
                        '日期': 'Date',
                        '开盘': 'Open',
                        '最高': 'High',
                        '最低': 'Low',
                        '收盘': 'Close',
                        '成交量': 'Volume',
                    }
                    df = df.rename(columns=column_map_em)
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        df = df.set_index('Date')
            except Exception as e2:
                logger.error(f"East Money also failed for {symbol}: {e2}")
                return pd.DataFrame()
        
        if df is not None and not df.empty:
            # yfinance 返回的列名是英文，需要转换
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
            # 重命名列以匹配统一格式
            column_map = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
            }
            df = df.rename(columns=column_map)
            df = df.sort_index()
        
        return df
    
    def get_hk_stock_hist(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取港股历史行情数据（支持日线/周线/月线）
        
        Args:
            symbol: 港股代码，如 "00700.HK"
            start_date: 开始日期
            end_date: 结束日期
            period: 周期 - "daily"(日线), "weekly"(周线), "monthly"(月线)
            adjust: 复权类型
        
        Returns:
            DataFrame 包含历史行情数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        # 转换代码格式: 00700.HK -> 00700
        code = symbol.replace(".HK", "")
        
        # period 参数映射
        period_map = {
            "daily": "day",
            "weekly": "week",
            "monthly": "month"
        }
        ak_period = period_map.get(period, "day")
        
        # 使用 akshare 获取港股数据
        df = ak.stock_hk_hist(
            symbol=code,
            period=ak_period,
            start_date=start_date,
            end_date=end_date
        )
        
        if df is not None and not df.empty:
            # yfinance 返回的列名是英文，需要转换
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
            # 重命名列以匹配统一格式
            column_map = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
            }
            df = df.rename(columns=column_map)
            df = df.sort_index()
        
        return df
    
    def get_price_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取价格数据（自动识别股票/ETF/港股）
        优先使用 baostock，失败时回退到 akshare
        
        Args:
            symbol: 代码，如 "000428.SZ" 或 "510300" 或 "00700.HK"
            start_date: 开始日期
            end_date: 结束日期
            period: 周期 - "daily"(日线), "weekly"(周线), "monthly"(月线)
            adjust: 复权类型
        
        Returns:
            DataFrame 包含价格数据
        """
        is_stock = symbol.endswith(".SH") or symbol.endswith(".SZ")
        is_etf = (symbol.startswith("51") or symbol.startswith("15") or 
                  symbol.startswith("16") or symbol.startswith("50"))
        is_hk = symbol.endswith(".HK")
        
        # 优先使用 baostock（股票数据，支持日线）
        if is_stock and not is_etf and not is_hk and self._baostock_available and period == "daily":
            try:
                logger.info(f"Using baostock for {symbol}...")
                return self._get_price_via_baostock(symbol, start_date, end_date)
            except Exception as e:
                logger.warning(f"baostock failed for {symbol}: {e}")
        
        # 回退到 akshare（支持日线/周线/月线）
        if self._akshare_available:
            try:
                logger.info(f"Using akshare for {symbol} (period={period})...")
                if is_hk:
                    return self.get_hk_stock_hist(symbol, start_date, end_date, period, adjust)
                elif is_stock and not is_etf:
                    return self.get_stock_hist(symbol, start_date, end_date, period, adjust)
                else:
                    return self.get_etf_hist(symbol, start_date, end_date, period, adjust)
            except Exception as e:
                logger.warning(f"akshare failed for {symbol}: {type(e).__name__}: {e}")
        
        raise Exception(f"All data sources failed for {symbol}")
    
    def _get_price_via_baostock(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """通过 baostock 获取价格数据"""
        import baostock as bs
        
        self._baostock_login()
        
        if not self._baostock_logged_in:
            raise Exception("baostock not logged in")
        
        bs_symbol = symbol.replace(".", "-")
        
        rs = bs.query_history_k_data_plus(
            bs_symbol,
            "date,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # 与history_sync保持一致，flag=3用于统一历史数据
        )
        
        if rs.error_code != '0':
            raise Exception(f"baostock query error: {rs.error_msg}")
        
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list:
            df = pd.DataFrame(data_list, columns=["日期", "开盘", "最高", "最低", "收盘", "成交量"])
            df["日期"] = pd.to_datetime(df["日期"])
            df = df.set_index("日期")
            for col in ["开盘", "最高", "最低", "收盘", "成交量"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        
        return pd.DataFrame()
    
    # ==================== 涨停池接口 ====================
    
    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        获取涨停池数据
        
        Args:
            date: 日期 (YYYYMMDD 格式)
        
        Returns:
            DataFrame 包含涨停股票信息
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        df = ak.stock_zt_pool_em(date=date)
        return df if df is not None else pd.DataFrame()
    
    # ==================== 行业/板块接口 ====================
    
    def get_industry_list(self) -> pd.DataFrame:
        """获取行业列表"""
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        return ak.stock_board_industry_name_em()
    
    def get_industry_cons(self, symbol: str) -> pd.DataFrame:
        """获取行业成分股"""
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        # 尝试多个接口，兼容不同 akshare 版本
        try:
            # 方法1: 同花顺行业成分股 (较新版本)
            if hasattr(ak, 'stock_board_industry_cons_ths'):
                return ak.stock_board_industry_cons_ths(symbol=symbol)
        except Exception:
            pass
        
        try:
            # 方法2: 东方财富行业成分股
            if hasattr(ak, 'stock_board_industry_cons_em'):
                return ak.stock_board_industry_cons_em(symbol=symbol)
        except Exception:
            pass
        
        try:
            # 方法3: 使用板块成分股接口
            if hasattr(ak, 'stock_sector_fund_flow_rank'):
                # 获取行业资金流向，然后筛选
                df = ak.stock_sector_fund_flow_rank(indicator="今日")
                if not df.empty and '名称' in df.columns:
                    # 返回空DataFrame，后续逻辑处理
                    return pd.DataFrame()
        except Exception:
            pass
        
        # 如果都失败，返回空DataFrame
        return pd.DataFrame()
    
    # ==================== 债券/利率接口 ====================
    
    def get_bond_yield_curve(self) -> pd.DataFrame:
        """获取债券收益率曲线"""
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        return ak.bond_china_yield_curve()
    
    # ==================== 宏观数据接口 ====================
    
    def get_fx_rate(self, pair: str = "USD/CNY") -> pd.DataFrame:
        """
        获取外汇汇率
        
        Args:
            pair: 货币对，如 "USD/CNY"
        
        Returns:
            DataFrame 包含汇率数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        df = ak.fx_spot_quote()
        if pair:
            df = df[df['货币对'] == pair]
        return df
    
    def get_north_money_flow(self) -> pd.DataFrame:
        """
        获取北向资金流向
        
        Returns:
            DataFrame 包含北向资金数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        return ak.stock_hsgt_fund_flow_summary_em()
    
    def get_gold_price(self) -> pd.DataFrame:
        """
        获取上海黄金交易所现货价格
        
        Returns:
            DataFrame 包含黄金价格数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        # 使用期货黄金主力合约数据替代
        try:
            return ak.futures_zh_realtime(symbol="黄金")
        except:
            # 如果失败返回空DataFrame
            return pd.DataFrame()
    
    def get_industry_hist(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取行业指数历史行情
        
        Args:
            symbol: 行业名称
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期
        
        Returns:
            DataFrame 包含行业指数历史数据
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        return ak.stock_board_industry_hist_em(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
    
    # ==================== 工具接口 ====================
    
    def get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日历"""
        if not self._akshare_available:
            raise ImportError("akshare not available")
        
        import akshare as ak
        
        df = ak.tool_trading_date()
        mask = (df["calendarDate"] >= start_date) & (df["calendarDate"] <= end_date)
        return df[mask]["calendarDate"].tolist()
    
    def get_latest_trading_date(self) -> str:
        """获取最近交易日"""
        from datetime import datetime, timedelta
        
        today = datetime.now()
        for i in range(7):
            check_date = today - timedelta(days=i)
            date_str = check_date.strftime("%Y-%m-%d")
            calendar = self.get_trading_calendar(date_str, date_str)
            if calendar:
                return check_date.strftime("%Y%m%d")
        
        return today.strftime("%Y%m%d") if today.weekday() < 5 else (
            today - timedelta(days=(today.weekday() - 4))
        ).strftime("%Y%m%d")
    
    # ==================== 指数分时数据接口 ====================

    def get_index_intraday(self, symbol: str, date: str = None) -> pd.DataFrame:
        """
        获取指数当日分时数据（1分钟线）

        Args:
            symbol: 指数代码，如 '000001.SH'
            date: 日期，格式 'YYYYMMDD'，None表示当天

        Returns:
            DataFrame with columns: time, open, high, low, close, volume
        """
        if not self._akshare_available:
            raise ImportError("akshare not available")

        import akshare as ak

        # 转换代码格式
        if '.SH' in symbol:
            ak_code = symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            ak_code = symbol.replace('.SZ', '')
        else:
            ak_code = symbol

        # 获取当日分时数据
        df = ak.index_zh_a_hist_min_em(symbol=ak_code, period="1", start_date="", end_date="")

        if df is not None and not df.empty:
            # 重命名列
            df = df.rename(columns={
                '时间': 'time',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
            })

            # 确保数值列为 float
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 添加 symbol 列
            df['symbol'] = symbol

            return df

        return pd.DataFrame()

    def __del__(self):
        """析构时登出 baostock"""
        if hasattr(self, '_baostock_logged_in'):
            self._baostock_logout()


def create_data_client(**kwargs) -> UnifiedDataClient:
    """创建统一数据客户端的便捷函数"""
    return UnifiedDataClient(**kwargs)

"""
历史数据同步服务 - 下载全市场股票/ETF/指数历史日线数据到 Parquet

每个资产一个文件，包含全部历史数据
存储位置: 
- 股票: storage/raw/stocks/price/{symbol}.parquet
- ETF: storage/raw/etf/price/{symbol}.parquet
- 指数: storage/raw/index/price/{symbol}.parquet

用法:
    # ========== 收盘后快速同步当天数据（极速推荐）==========
    python DataHub/services/history_sync.py --today
    # 使用 stock_zh_a_spot 获取全市场当日数据并入库，同时更新复权因子


    # ========== 每日增量更新（支持历史日期补数据）==========
    # 同步全部股票（默认）
    python DataHub/services/history_sync.py --daily

    # 同步全部ETF
    python DataHub/services/history_sync.py --daily --symbol etf

    # 同步全部指数
    python DataHub/services/history_sync.py --daily --symbol index

    # 测试模式，只同步前10只
    python DataHub/services/history_sync.py --daily --limit 10

    # 使用更多并发（更快但可能被IP限制）
    python DataHub/services/history_sync.py --daily --workers 5

    # 包含北交所股票（默认跳过，因数据源不稳定）
    python DataHub/services/history_sync.py --daily --include-bj

    # 指定单日同步（快速补数据）
    python DataHub/services/history_sync.py --daily 20260413
    python DataHub/services/history_sync.py --daily 2026-04-13

    # 指定日期范围
    python DataHub/services/history_sync.py --daily 20260413~20260414
    python DataHub/services/history_sync.py --daily 2026-04-13~2026-04-14

    # 指定代码同步（单只或多只，支持省略后缀）
    python DataHub/services/history_sync.py --daily --symbol 600519
    python DataHub/services/history_sync.py --daily 20260413 --symbol 600519,300750,000858
    python DataHub/services/history_sync.py --daily --symbol 600519.SH,300750.SZ


    # ========== 首次全量同步（断点续传）==========
    python DataHub/services/history_sync.py --symbol 600519.SH,300750.SZ --skip-existing

    # 首次同步全部ETF
    python DataHub/services/history_sync.py --symbol etf --skip-existing

    # 首次同步全部指数
    python DataHub/services/history_sync.py --symbol index --skip-existing


    # ========== 全量更新（覆盖已有数据）==========
    python DataHub/services/history_sync.py --symbol 600519.SH --override

    # 覆盖同步全部指数
    python DataHub/services/history_sync.py --symbol index --override


    # ========== 复权因子同步 ==========
    python DataHub/services/history_sync.py --sync-factors


    # ========== 查看同步摘要 ==========
    python DataHub/services/history_sync.py --summary

参数说明:
    --today            同步当天数据（极速模式）：使用 stock_zh_a_spot 获取全市场
                         当日数据并入库，同时更新复权因子。适合收盘后快速同步。
    --daily [DATE]     每日增量更新。可选指定日期:
                         无参数: 自动同步到最新日期
                         单日: 20260413, 2026-04-13
                         范围: 20260413~20260414, 2026-04-13~2026-04-14
    --symbol SYMBOL    指定代码或类型简写:
                         - 具体代码: 600519, 600519.SH, 000001.SH (指数)
                         - 类型简写: stock(全部股票), etf(全部ETF), index(全部指数)
                         不指定时默认同步全部股票
    --override         覆盖已有数据（默认增量）
    --skip-existing    跳过已有文件的股票（首次同步时大幅提速，不读取文件内容）
    --summary          显示已同步数据摘要
    --sync-factors     只同步复权因子（不下载价格数据）
    --limit N          限制股票数量（测试用）
    --workers N        并发线程数（默认3，建议3-5，过多可能被IP屏蔽）
    --include-bj       包含北交所股票（默认跳过，因数据源不稳定）

日期格式:
    支持多种格式，自动识别:
    - YYYYMMDD: 20260413
    - YYYY-MM-DD: 2026-04-13
    - YYYY/MM/DD: 2026/04/13
    - 范围分隔符: ~ 或 — 或 －
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
import pandas as pd
import baostock as bs
import socket
from datetime import datetime
from typing import List, Optional, Dict

# 设置全局 socket 超时（防止网络请求无限等待）
socket.setdefaulttimeout(30)
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from DataHub.config import CRAWLER_REQUEST_DELAY, STORAGE_DIR, RAW_PRICE_DIR, RAW_ADJUST_FACTOR_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR

logger = logging.getLogger(__name__)

# 全局baostock锁（多线程共享，保护所有baostock操作）
_baostock_lock = Lock()


# ANSI 颜色代码（用于终端高亮显示）
class Colors:
    """终端颜色代码"""
    RED = '\033[91m'      # 亮红色 - 错误
    GREEN = '\033[92m'    # 亮绿色 - 成功
    YELLOW = '\033[93m'   # 亮黄色 - 警告
    BLUE = '\033[94m'     # 亮蓝色 - 信息
    MAGENTA = '\033[95m'  # 亮紫色
    CYAN = '\033[96m'     # 亮青色
    WHITE = '\033[97m'    # 亮白色
    BOLD = '\033[1m'      # 加粗
    UNDERLINE = '\033[4m' # 下划线
    END = '\033[0m'       # 重置


def color_log(level: str, message: str) -> str:
    """
    为日志消息添加颜色

    Args:
        level: 日志级别 ('error', 'warning', 'success', 'info')
        message: 原始消息

    Returns:
        带颜色的消息
    """
    color_map = {
        'error': Colors.RED + Colors.BOLD,
        'warning': Colors.YELLOW + Colors.BOLD,
        'success': Colors.GREEN,
        'info': Colors.CYAN,
    }
    color = color_map.get(level, '')
    return f"{color}{message}{Colors.END}"



class HistorySyncService:
    """
    历史数据同步服务

    每只股票保存为一个Parquet文件，包含全部历史数据
    """

    def __init__(self):
        """初始化服务（不自动登录baostock，按需登录）"""
        self.raw_price_dir = RAW_PRICE_DIR
        self.raw_price_dir.mkdir(parents=True, exist_ok=True)

        # 加载股票列表
        self.stock_list = self._load_stock_list()
        
        # 加载ETF列表
        self.etf_list = self._load_etf_list()
        
        # 加载指数列表
        self.index_list = self._load_index_list()

        # 登录状态标记
        self._baostock_logged_in = False
        
        # 待处理列表（无法自动获取的股票）
        self.pending_symbols = []

    def _load_stock_list(self) -> pd.DataFrame:
        """加载股票基础信息列表"""
        stock_csv = STORAGE_DIR / "stock_basic_info.csv"
        if stock_csv.exists():
            df = pd.read_csv(stock_csv)
            logger.info(f"加载股票列表: {len(df)} 只")
            return df
        else:
            logger.warning(f"股票列表文件不存在: {stock_csv}")
            return pd.DataFrame()
    
    def _load_etf_list(self) -> pd.DataFrame:
        """加载ETF基础信息列表"""
        etf_csv = STORAGE_DIR / "etf_basic_info.csv"
        if etf_csv.exists():
            df = pd.read_csv(etf_csv)
            logger.info(f"加载ETF列表: {len(df)} 只")
            return df
        else:
            logger.warning(f"ETF列表文件不存在: {etf_csv}")
            return pd.DataFrame()

    def _load_index_list(self) -> List[str]:
        """加载指数列表"""
        index_csv = STORAGE_DIR / "official_indices.csv"
        if index_csv.exists():
            df = pd.read_csv(index_csv)
            symbols = df['symbol'].tolist()
            logger.info(f"加载指数列表: {len(symbols)} 个")
            return symbols
        else:
            logger.warning(f"指数列表文件不存在: {index_csv}，使用默认列表")
            # 默认常用指数列表
            return [
                '000001.SH',  # 上证指数
                '000002.SH',  # 上证A指
                '000003.SH',  # 上证B指
                '000016.SH',  # 上证50
                '000300.SH',  # 沪深300
                '000688.SH',  # 科创50
                '000905.SH',  # 中证500
                '000852.SH',  # 中证1000
                '399001.SZ',  # 深证成指
                '399002.SZ',  # 深证A指
                '399003.SZ',  # 深证B指
                '399006.SZ',  # 创业板指
                '399300.SZ',  # 沪深300(深圳)
                '399673.SZ',  # 创业板50
            ]

    def _login_baostock(self):
        """登录baostock（线程安全，延迟加载）"""
        if self._baostock_logged_in:
            return

        # 使用全局锁保护登录操作
        with _baostock_lock:
            # 双重检查，避免多个线程重复登录
            if self._baostock_logged_in:
                return

            lg = bs.login()
            if lg.error_code != '0':
                logger.error(color_log('error', f"❌ baostock登录失败: {lg.error_msg}"))
            else:
                logger.info("baostock登录成功")
                self._baostock_logged_in = True

    def _format_code(self, symbol: str) -> Optional[str]:
        """
        转换代码格式为 baostock 格式: 600519.SH -> sh.600519
        
        Returns:
            baostock格式代码，如果不支持则返回 None
        """
        from lib.utils import StockCodeUtil
        return StockCodeUtil.to_baostock(symbol)
    
    def _is_bj_stock(self, symbol: str) -> bool:
        """判断是否为北交所股票"""
        return '.BJ' in symbol
    
    def _add_to_pending_list(self, symbol: str, reason: str = 'unknown'):
        """
        添加到待处理列表
        
        Args:
            symbol: 股票代码
            reason: 原因，如 'bj_not_supported'
        """
        self.pending_symbols.append({
            'symbol': symbol,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        })
    
    def get_pending_symbols(self) -> List[Dict]:
        """获取待处理列表"""
        return self.pending_symbols
    
    def clear_pending_symbols(self):
        """清空待处理列表"""
        self.pending_symbols = []

    def get_stock_file_path(self, symbol: str, asset_type: str = "stock") -> Path:
        """获取股票/ETF/指数数据文件路径"""
        if asset_type == "etf":
            return RAW_ETF_PRICE_DIR / f"{symbol}.parquet"
        elif asset_type == "index":
            return RAW_INDEX_PRICE_DIR / f"{symbol}.parquet"
        return self.raw_price_dir / f"{symbol}.parquet"

    def load_existing_data(self, symbol: str, asset_type: str = "stock") -> Optional[pd.DataFrame]:
        """加载股票/ETF已存在的历史数据"""
        file_path = self.get_stock_file_path(symbol, asset_type)
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path)
                logger.debug(f"加载 {symbol} 已有数据: {len(df)} 条")
                return df
            except Exception as e:
                logger.warning(color_log('warning', f"⚠️  读取 {symbol} 历史数据失败: {e}"))
        return None

    def fetch_stock_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        asset_type: str = "stock"
    ) -> Optional[pd.DataFrame]:
        """
        获取单只股票历史数据

        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            asset_type: 资产类型，'stock'(股票) 或 'etf'(ETF)

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        # ETF 使用 Yahoo Finance 获取前复权价格
        if asset_type == "etf":
            return self._fetch_etf_history_from_yfinance(symbol, start_date, end_date)

        # 指数使用 baostock/akshare 获取
        if asset_type == "index":
            return self._fetch_index_history(symbol, start_date, end_date)

        # 股票使用 baostock（不复权，复权因子单独存储）
        return self._fetch_stock_history_from_baostock(symbol, start_date, end_date)

    def _fetch_stock_history_from_baostock(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用 baostock 获取股票历史数据（不复权）
        """
        # 确保已登录（线程安全）
        self._login_baostock()

        try:
            code = self._format_code(symbol)

            # 检查是否支持该交易所
            if code is None and self._is_bj_stock(symbol):
                # 北交所：尝试akshare，但接口不稳定
                result = self._fetch_bj_stock_history(symbol, start_date, end_date)
                if result is None:
                    # 记录到待处理列表
                    self._add_to_pending_list(symbol, 'bj_not_supported')
                return result
            elif code is None:
                logger.warning(color_log('warning', f"⚠️  {symbol} 跳过: 不支持的交易所"))
                return None

            # 转换日期格式为 baostock 格式 YYYY-MM-DD
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

            # 使用全局锁保护baostock API调用（线程安全）
            with _baostock_lock:
                # 调用baostock接口 - flag=3 不复权
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,volume,amount,pctChg",
                    start_date=start_dt,
                    end_date=end_dt,
                    frequency="d",
                    adjustflag="3"  # 不复权
                )

                if rs.error_code != '0':
                    logger.warning(color_log('warning', f"⚠️  获取 {symbol} 数据失败: {rs.error_msg}"))
                    return None

                # 读取数据
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

            if not data_list:
                return None

            # 创建DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)

            # 添加symbol列
            df['symbol'] = symbol

            # 重命名列
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

            # 转换日期格式
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

            logger.info(f"获取 {symbol} 数据: {len(df)} 条 (不复权)")
            return df

        except Exception as e:
            logger.error(color_log('error', f"❌ 获取 {symbol} 历史数据失败: {e}"))
            return None

    def _fetch_etf_history_from_yfinance(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取 ETF 历史数据（前复权）
        
        使用 Yahoo Finance (yfinance) 获取前复权数据
        雅虎的 Close 列默认已经是前复权价格

        Args:
            symbol: ETF代码，如 '510300.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume
        """
        try:
            import yfinance as yf
            
            # 转换代码格式: 510300.SH -> 510300.SS (上交所加.SS，深交所加.SZ)
            if symbol.endswith('.SH'):
                yf_symbol = symbol.replace('.SH', '.SS')
            elif symbol.endswith('.SZ'):
                yf_symbol = symbol.replace('.SZ', '.SZ')
            else:
                yf_symbol = symbol
            
            # 转换日期格式: YYYYMMDD -> YYYY-MM-DD
            start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
            end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            
            # 使用 yfinance 获取数据（auto_adjust=True 获取前复权数据，timeout=30秒）
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(start=start_fmt, end=end_fmt, auto_adjust=True, timeout=30)
            
            if df is None or df.empty:
                logger.warning(f"{symbol} 未获取到数据")
                return None
            
            # 重置索引，将日期变为列
            df = df.reset_index()
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 重命名列以统一格式
            column_map = {
                'Date': 'trade_date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
            }
            df = df.rename(columns=column_map)
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 转换数值类型
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 选择需要的列（Yahoo 不提供 amount 和 change_pct）
            keep_cols = [
                'symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume'
            ]
            df = df[[c for c in keep_cols if c in df.columns]]
            
            logger.info(f"获取 {symbol} ETF数据: {len(df)} 条 (前复权/Yahoo)")
            return df
            
        except Exception as e:
            logger.warning(color_log('warning', f"⚠️  Yahoo Finance 获取 {symbol} 失败: {e}，尝试回退到东财接口..."))
            # 回退到东财接口
            return self._fetch_etf_history_from_em(symbol, start_date, end_date)

    def _fetch_etf_history_from_em(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取 ETF 历史数据（前复权）- 东财备选接口
        
        当 Yahoo Finance 失败时使用东财接口作为备选

        Args:
            symbol: ETF代码，如 '510300.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        try:
            import akshare as ak
            
            # 转换代码格式: 510300.SH -> 510300
            code = symbol.replace('.SH', '').replace('.SZ', '')
            
            # 使用东财接口获取 ETF 前复权数据
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if df is None or df.empty:
                logger.warning(f"{symbol} 未获取到数据")
                return None
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 重命名列以统一格式
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
            
            # 转换日期格式
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
            
            logger.info(f"获取 {symbol} ETF数据: {len(df)} 条 (前复权/东财备选)")
            return df
            
        except Exception as e:
            logger.error(color_log('error', f"❌ 东财接口获取 {symbol} ETF数据也失败: {e}"))
            return None

    def _fetch_index_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取指数历史数据 - 使用新浪

        Args:
            symbol: 指数代码，如 '000001.SH', '000300.SH', '399001.SZ'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        return self._fetch_index_from_sina(symbol, start_date, end_date)

    def _fetch_index_from_yfinance(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用 Yahoo Finance 获取指数数据（首选，速度快）

        Yahoo Finance 指数代码映射:
        - 000001.SH (上证指数) -> ^SSEC
        - 399001.SZ (深证成指) -> ^SZSC
        - 399006.SZ (创业板指) -> ^SZCI
        - 000300.SH (沪深300) -> 000300.SS
        - 000016.SH (上证50) -> 000016.SS
        - 000905.SH (中证500) -> 000905.SS
        - 000852.SH (中证1000) -> 000852.SS

        Args:
            symbol: 指数代码，如 '000001.SH', '399001.SZ'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume
        """
        import yfinance as yf

        # 指数代码映射表（只包含 Yahoo Finance 实际支持的指数）
        INDEX_YF_MAP = {
            # 主要市场指数
            '000001.SH': '^SSEC',    # 上证指数
            '399001.SZ': '^SZSC',    # 深证成指
            '399006.SZ': '^SZCI',    # 创业板指
            '399005.SZ': '^SZCC',    # 中小100
            # 规模指数
            '000300.SH': '000300.SS',  # 沪深300
            '000016.SH': '000016.SS',  # 上证50
            '000010.SH': '000010.SS',  # 上证180
            '000009.SH': '000009.SS',  # 上证380
            '000905.SH': '000905.SS',  # 中证500
            '000852.SH': '000852.SS',  # 中证1000
        }

        # 获取 Yahoo Finance 代码
        yf_symbol = INDEX_YF_MAP.get(symbol)
        if not yf_symbol:
            raise Exception(f"未映射的指数代码: {symbol}")

        # 转换日期格式
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        # 使用 yfinance 获取数据（timeout=30秒）
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(start=start_fmt, end=end_fmt, timeout=30)

        if df is None or df.empty:
            raise Exception(f"Yahoo Finance 返回空数据: {yf_symbol}")

        # 重置索引，将日期变为列
        df = df.reset_index()

        # 添加 symbol 列
        df['symbol'] = symbol

        # 列名映射
        column_map = {
            'Date': 'trade_date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
        }
        df = df.rename(columns=column_map)

        # 统一日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

        # 转换数值类型
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Yahoo 指数数据没有 amount 和 change_pct，置为 None
        df['amount'] = None
        df['change_pct'] = None

        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]

        logger.info(f"获取 {symbol} 指数数据: {len(df)} 条 (Yahoo Finance)")
        return df

    def _fetch_index_from_sina(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用新浪获取指数数据（通过 akshare）

        新浪接口代码格式: sh000001 (上证指数), sz399001 (深证成指)

        Args:
            symbol: 指数代码，如 '000001.SH', '399001.SZ'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        import akshare as ak

        # 转换代码格式: 000001.SH -> sh000001
        if symbol.endswith('.SH'):
            sina_symbol = f"sh{symbol.replace('.SH', '')}"
        elif symbol.endswith('.SZ'):
            sina_symbol = f"sz{symbol.replace('.SZ', '')}"
        else:
            sina_symbol = symbol

        # 使用 akshare 的新浪接口获取指数日线数据
        df = ak.stock_zh_index_daily(symbol=sina_symbol)

        if df is None or df.empty:
            logger.warning(f"{symbol} 未获取到数据")
            return None

        # 转换日期格式
        df['date'] = pd.to_datetime(df['date']).dt.date

        # 转换输入日期格式
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()

        # 筛选日期范围
        df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]

        if df.empty:
            logger.warning(f"{symbol} 日期范围内无数据")
            return None

        # 添加 symbol 列并重命名
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
        df['change_pct'] = df['close'].pct_change() * 100

        # 新浪接口没有 amount 列，设为 None
        df['amount'] = None

        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']
        df = df[[c for c in keep_cols if c in df.columns]]

        logger.info(f"获取 {symbol} 指数数据: {len(df)} 条 (新浪)")
        return df

    def _fetch_index_from_akshare(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用 akshare 获取指数数据（备选）

        不使用东财接口，优先使用腾讯财经接口
        """
        import akshare as ak

        # 转换代码格式为腾讯接口格式: 000001.SH -> sh000001, 399001.SZ -> sz399001
        if symbol.endswith('.SH'):
            tx_symbol = f"sh{symbol.replace('.SH', '')}"
        elif symbol.endswith('.SZ'):
            tx_symbol = f"sz{symbol.replace('.SZ', '')}"
        else:
            tx_symbol = symbol

        try:
            # 使用腾讯财经接口（支持上海和深圳指数）
            df = ak.stock_zh_index_daily_tx(symbol=tx_symbol)

            if df is None or df.empty:
                raise Exception("无数据")

            # 转换日期格式
            df['date'] = pd.to_datetime(df['date']).dt.date

            # 转换输入日期格式
            start_dt = pd.to_datetime(start_date).date()
            end_dt = pd.to_datetime(end_date).date()

            # 筛选日期范围
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]

            if df.empty:
                raise Exception("日期范围内无数据")

            # 添加symbol列并重命名
            df['symbol'] = symbol
            df = df.rename(columns={
                'date': 'trade_date',
                'close': 'close',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'amount': 'volume'  # 腾讯接口用 amount 表示成交量
            })

            # 计算涨跌幅
            df['change_pct'] = df['close'].pct_change() * 100

            # 添加 amount 列（成交额），腾讯接口没有，设为 None
            df['amount'] = None

            logger.info(f"获取 {symbol} 指数数据: {len(df)} 条 (akshare/腾讯)")
            return df

        except Exception as e:
            logger.error(f"akshare 获取指数 {symbol} 失败: {e}")
            raise

    def _fetch_bj_stock_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用 akshare 获取北交所股票历史数据
        
        注意：东财源接口不稳定，北交所数据获取可能受限
        
        Args:
            symbol: 股票代码，如 '920000.BJ'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            
        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        try:
            import akshare as ak
            import time
            
            # 转换代码格式: 920000.BJ -> 920000
            code = symbol.replace('.BJ', '')
            
            # 添加重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # 使用 akshare 获取历史数据（东财源）
                    df = ak.stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust=""  # 不复权，与baostock保持一致
                    )
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # 指数退避
                        continue
                    raise
            
            if df.empty:
                return None
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 重命名列以统一格式
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
            
            # 转换日期格式
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
            
            logger.info(f"获取 {symbol} 数据: {len(df)} 条 (akshare/北交所)")
            return df
            
        except Exception as e:
            error_msg = str(e)
            if 'Connection' in error_msg or 'RemoteDisconnected' in error_msg:
                logger.warning(color_log('warning', f"⚠️  {symbol} 跳过: 东财接口限制，北交所数据暂无法自动获取"))
            else:
                logger.warning(color_log('warning', f"⚠️  akshare获取 {symbol} 失败: {e}"))
            return None

    def save_pending_list(self, output_path: Optional[str] = None):
        """
        保存待处理列表到文件
        
        Args:
            output_path: 输出文件路径，默认 storage/outputs/pending_symbols.json
        """
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

    def fetch_adjust_factor(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取复权因子数据

        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: trade_date, adjust_factor
        """
        # 确保已登录（线程安全）
        self._login_baostock()

        try:
            code = self._format_code(symbol)
            
            # 检查是否支持该交易所
            if code is None:
                logger.debug(f"{symbol} 跳过复权因子: baostock 不支持北交所股票")
                return None

            # 转换日期格式
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

            # 使用全局锁保护baostock API调用（线程安全）
            with _baostock_lock:
                # 调用baostock复权因子接口
                rs = bs.query_adjust_factor(
                    code=code,
                    start_date=start_dt,
                    end_date=end_dt
                )

                if rs.error_code != '0':
                    logger.warning(color_log('warning', f"⚠️  获取 {symbol} 复权因子失败: {rs.error_msg}"))
                    return None

                # 读取数据
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"{symbol} 没有复权因子数据")
                return None

            # 创建DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)

            # 调试: 打印字段名
            logger.debug(f"{symbol} 复权因子字段: {rs.fields}")

            # 重命名列 - baostock 字段名: dividOperateDate, foreAdjustFactor
            column_map = {}
            for col in df.columns:
                col_lower = col.lower()
                if col_lower in ['dividoperatedate', 'date']:
                    column_map[col] = 'trade_date'
                elif col_lower == 'foreadjustfactor':  # 前复权因子
                    column_map[col] = 'adjust_factor'

            df = df.rename(columns=column_map)

            # 检查必要的列是否存在
            if 'trade_date' not in df.columns:
                logger.warning(f"{symbol} 复权因子数据缺少 trade_date 列，可用列: {df.columns.tolist()}")
                return None

            if 'adjust_factor' not in df.columns:
                logger.warning(f"{symbol} 复权因子数据缺少 adjust_factor 列，可用列: {df.columns.tolist()}")
                return None

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            # 转换复权因子为数值
            df['adjust_factor'] = pd.to_numeric(df['adjust_factor'], errors='coerce')

            # 只保留需要的列
            df = df[['trade_date', 'adjust_factor']]

            logger.info(f"获取 {symbol} 复权因子: {len(df)} 条")
            return df

        except Exception as e:
            logger.error(color_log('error', f"❌ 获取 {symbol} 复权因子失败: {e}"))
            return None

    def sync_adjust_factor(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        incremental: bool = True
    ) -> dict:
        """
        同步单只股票的复权因子

        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            incremental: 是否增量更新

        Returns:
            同步结果
        """
        RAW_ADJUST_FACTOR_DIR.mkdir(parents=True, exist_ok=True)
        file_path = RAW_ADJUST_FACTOR_DIR / f"{symbol}.parquet"

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        existing_df = None
        if incremental and file_path.exists():
            try:
                existing_df = pd.read_parquet(file_path)
                existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.date
                latest_date = existing_df['trade_date'].max()
                start_date = (pd.to_datetime(latest_date) + pd.Timedelta(days=1)).strftime("%Y%m%d")
            except Exception as e:
                logger.warning(color_log('warning', f"⚠️  读取已有复权因子失败: {e}"))
                start_date = "19900101"

        if start_date is None:
            start_date = "19900101"

        if start_date > end_date:
            return {'status': 'success', 'symbol': symbol, 'records': 0, 'message': 'Already up to date'}

        # 获取新数据
        new_df = self.fetch_adjust_factor(symbol, start_date, end_date)

        if new_df is None or new_df.empty:
            # 没有复权因子数据，表示该股票从未分红送股
            # 不创建文件，直接返回（节省空间）
            if file_path.exists():
                # 如果已有文件但新数据为空，保持现有文件
                return {'status': 'success', 'symbol': symbol, 'records': 0, 'message': 'No new adjust data'}
            else:
                # 从未分红，不创建文件
                logger.debug(f"{symbol} 无复权因子数据（从未分红），跳过创建")
                return {'status': 'success', 'symbol': symbol, 'records': 0, 'message': 'Never distributed dividend'}

        # 合并数据
        if existing_df is not None and not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['trade_date'], keep='last')
            combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
        else:
            combined_df = new_df

        # 只保存 factor != 1 的记录（实际发生复权的日期）
        # factor=1 表示没有分红送股，无需存储
        combined_df = combined_df[combined_df['adjust_factor'] != 1.0]

        if combined_df.empty:
            # 如果过滤后为空，删除已有文件（如果存在）
            if file_path.exists():
                file_path.unlink()
                logger.info(f"{symbol} 删除复权因子文件（无有效复权记录）")
            return {'status': 'success', 'symbol': symbol, 'records': 0, 'message': 'No valid adjust factors'}

        # 保存
        combined_df.to_parquet(file_path, index=False, compression='zstd')

        logger.info(color_log('success', f"✓ {symbol} 复权因子同步完成: {len(new_df)} 条新数据，共 {len(combined_df)} 条有效记录"))

        return {
            'status': 'success',
            'symbol': symbol,
            'records': len(new_df),
            'total_records': len(combined_df)
        }

    def sync_stock(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        incremental: bool = True,
        asset_type: str = "stock"
    ) -> dict:
        """
        同步单只股票的历史数据

        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'，None表示从最早开始
            end_date: 结束日期 'YYYYMMDD'，None表示到今天
            incremental: 是否增量更新，True表示只获取新数据
            asset_type: 资产类型，'stock'(股票) 或 'etf'(ETF)

        Returns:
            同步结果
        """
        file_path = self.get_stock_file_path(symbol, asset_type)

        # 确定日期范围
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
            
        # 限制结束日期为实际可用的最新数据日期（baostock 等数据源不接受未来日期）
        # 动态计算：当前年份+1年的12月31日（确保新年份数据可以同步）
        current_year = datetime.now().year
        max_available_date = f"{current_year + 1}1231"
        if end_date > max_available_date:
            logger.debug(f"限制结束日期从 {end_date} 到 {max_available_date}（数据源最大日期）")
            end_date = max_available_date

        existing_df = None
        if incremental and file_path.exists():
            existing_df = self.load_existing_data(symbol, asset_type)
            if existing_df is not None and not existing_df.empty:
                # 获取最新日期，从第二天开始同步
                latest_date = existing_df['trade_date'].max()
                start_date = (pd.to_datetime(latest_date) + pd.Timedelta(days=1)).strftime("%Y%m%d")
                logger.info(f"{symbol} 增量更新: {start_date} ~ {end_date}")

        if start_date is None:
            # 从上市日期开始（baostock支持从最早日期开始）
            start_date = "19900101"  # 设一个足够早的日期，baostock会返回实际最早数据
            logger.info(f"{symbol} 全量同步: 从上市日期开始 ~ {end_date}")

        # 如果开始日期大于结束日期，说明已经是最新
        if start_date > end_date:
            logger.info(f"{symbol} 数据已是最新，无需更新")
            return {
                'status': 'success',
                'symbol': symbol,
                'records': 0,
                'message': 'Already up to date'
            }

        # 检查 start_date 到 end_date 之间是否包含工作日（排除全是周末的情况）
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        has_weekday = False
        current_dt = start_dt
        while current_dt <= end_dt:
            if current_dt.weekday() < 5:  # 0-4 是周一到周五
                has_weekday = True
                break
            current_dt += pd.Timedelta(days=1)

        if not has_weekday:
            logger.info(f"{symbol} 增量日期范围内无交易日，跳过")
            return {
                'status': 'success',
                'symbol': symbol,
                'records': 0,
                'message': 'No trading days in range'
            }

        # 获取新数据
        new_df = self.fetch_stock_history(symbol, start_date, end_date, asset_type)

        if new_df is None or new_df.empty:
            logger.warning(color_log('warning', f"⚠️  {symbol} 没有获取到新数据"))
            return {
                'status': 'failed',
                'symbol': symbol,
                'records': 0,
                'message': 'No new data'
            }

        # 合并数据
        if existing_df is not None and not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # 去重
            combined_df = combined_df.drop_duplicates(subset=['trade_date'], keep='last')
            # 按日期排序
            combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
        else:
            combined_df = new_df

        # 保存
        combined_df.to_parquet(file_path, index=False, compression='zstd')

        # 同步复权因子（仅股票，ETF直接存储前复权价格，不需要复权因子）
        if asset_type == "stock":
            factor_result = self.sync_adjust_factor(symbol, start_date, end_date, incremental)

        logger.info(color_log('success', f"✓ {symbol} 同步完成: {len(new_df)} 条新数据，共 {len(combined_df)} 条"))

        return {
            'status': 'success',
            'symbol': symbol,
            'new_records': len(new_df),
            'total_records': len(combined_df),
            'date_range': f"{combined_df['trade_date'].min()} ~ {combined_df['trade_date'].max()}",
            'file_path': str(file_path)
        }

    def _fetch_daily_from_baostock(self, trade_date: str) -> pd.DataFrame:
        """
        使用 baostock 批量获取某日全部股票数据

        Args:
            trade_date: 交易日期 'YYYYMMDD'

        Returns:
            DataFrame with daily data for all stocks
        """
        self._login_baostock()

        # 获取沪深A股历史行情（全市场）
        with _baostock_lock:
            rs = bs.query_all_stock(day=trade_date)
            if rs.error_code != '0':
                raise RuntimeError(f"baostock 获取股票列表失败: {rs.error_msg}")

            # 获取所有股票代码
            stock_codes = []
            while (rs.error_code == '0') & rs.next():
                stock_codes.append(rs.get_row_data())

        if not stock_codes:
            raise RuntimeError("baostock 返回空股票列表")

        # 逐个获取每只股票的历史数据（baostock 不支持真正的批量历史查询）
        # 这里实际上还是逐个获取，但使用 baostock 的稳定接口
        all_data = []
        for code_row in stock_codes[:100]:  # 限制数量避免太慢
            try:
                code = code_row[0]  # baostock 格式: sh.600000
                with _baostock_lock:
                    rs = bs.query_history_k_data_plus(
                        code,
                        "date,open,high,low,close,volume,amount,pctChg",
                        start_date=trade_date,
                        end_date=trade_date,
                        frequency="d"
                    )
                    if rs.error_code == '0' and rs.next():
                        row = rs.get_row_data()
                        all_data.append({
                            'symbol': code.replace('sh.', '').replace('sz.', '') + '.SH' if code.startswith('sh') else '.SZ',
                            'trade_date': row[0],
                            'open': row[1],
                            'high': row[2],
                            'low': row[3],
                            'close': row[4],
                            'volume': row[5],
                            'amount': row[6],
                            'change_pct': row[7]
                        })
            except Exception as e:
                logger.debug(f"获取 {code_row} 失败: {e}")
                continue

        if not all_data:
            raise RuntimeError("baostock 未返回任何数据")

        return pd.DataFrame(all_data)

    def fetch_daily_bulk(self, trade_date: str) -> pd.DataFrame:
        """
        批量获取某日所有股票的日线数据（一次性获取，像实时数据那样）

        使用 akshare 的 stock_zh_a_daily_em 接口，一次请求获取全市场某日数据

        Args:
            trade_date: 交易日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        import akshare as ak

        logger.info(f"批量获取 {trade_date} 全市场日线数据...")

        try:
            # 转换日期格式 20260415 -> 2026-04-15
            date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

            # 使用 akshare 获取某日全部股票数据
            # 优先使用 baostock，然后 akshare，最后东财
            try:
                # 批量获取：优先使用 baostock（虽然逐个获取但稳定）
                # 注意：baostock 没有真正的批量历史数据接口，这里获取全市场股票列表后逐个获取
                baostock_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
                df = self._fetch_daily_from_baostock(baostock_date)
            except Exception as e1:
                logger.warning(f"baostock 批量接口失败: {e1}")
                # 降级到普通每日同步模式
                raise RuntimeError(f"baostock 批量获取失败，请使用 --daily 模式: {e1}")

            if df is None or df.empty:
                logger.warning(f"未获取到 {trade_date} 的数据")
                return pd.DataFrame()

            # 重命名列（兼容多种接口返回格式）
            column_map = {
                # 东财接口列名
                '股票代码': 'symbol',
                '代码': 'symbol',
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'change_pct',
                '振幅': 'amplitude',
                '涨跌额': 'change_amount',
                '换手率': 'turnover',
            }
            df = df.rename(columns=column_map)

            # 转换代码格式 600000 -> 600000.SH
            def format_symbol(code):
                code = str(code)
                # 如果已经带后缀，直接返回
                if '.SH' in code or '.SZ' in code or '.BJ' in code:
                    return code
                # 处理带前缀的代码
                if code.startswith('sh'):
                    return code[2:] + '.SH'
                elif code.startswith('sz'):
                    return code[2:] + '.SZ'
                elif code.startswith('bj'):
                    return code[2:] + '.BJ'
                # 纯数字代码根据规则判断
                elif code.startswith(('6', '68', '5')):
                    return code + '.SH'
                elif code.startswith(('0', '3', '1', '4', '8')):
                    return code + '.SZ'
                elif code.startswith(('8', '4', '9')) and len(code) >= 6:
                    return code + '.BJ'
                return code

            df['symbol'] = df['symbol'].apply(format_symbol)

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            logger.info(f"✓ 批量获取完成: {len(df)} 只股票")
            return df

        except Exception as e:
            logger.error(f"❌ 批量获取失败: {e}")
            return pd.DataFrame()

    def sync_daily_bulk(self, trade_date: str = None) -> dict:
        """
        批量同步某日的数据到所有股票文件（极速模式）

        适合收盘后快速同步当天数据，比逐个股票获取快 10-50 倍

        Args:
            trade_date: 交易日期 'YYYYMMDD'，None表示今天

        Returns:
            同步结果统计
        """
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        logger.info("="*60)
        logger.info(f"启动批量同步模式: {trade_date}")
        logger.info("="*60)

        # 1. 批量获取某日全市场数据
        bulk_df = self.fetch_daily_bulk(trade_date)
        if bulk_df.empty:
            return {'status': 'failed', 'message': 'No data fetched'}

        # 2. 按股票分组
        grouped = bulk_df.groupby('symbol')

        # 3. 获取所有股票列表（从已存在文件+新数据）
        all_symbols = set(grouped.groups.keys())

        # 添加已有文件中的股票（确保更新所有文件，即使某天没交易）
        if RAW_PRICE_DIR.exists():
            for f in RAW_PRICE_DIR.glob("*.parquet"):
                symbol = f.stem
                all_symbols.add(symbol)

        # 4. 逐个更新股票文件
        updated = 0
        skipped = 0
        failed = 0

        for symbol in sorted(all_symbols):
            try:
                file_path = self.get_stock_file_path(symbol)

                # 加载已有数据
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.date
                else:
                    existing_df = pd.DataFrame()

                # 获取该股票的新数据
                if symbol in grouped.groups:
                    new_df = grouped.get_group(symbol).copy()
                    # 确保列一致
                    for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']:
                        if col not in new_df.columns:
                            new_df[col] = None
                    new_df = new_df[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']]
                else:
                    # 该股票今天没有数据（停牌等）
                    skipped += 1
                    continue

                # 检查是否已存在该日期
                if not existing_df.empty and trade_date in existing_df['trade_date'].astype(str).values:
                    # 更新已有数据
                    mask = existing_df['trade_date'].astype(str) == trade_date
                    idx = existing_df[mask].index[0]
                    for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']:
                        if col in new_df.columns:
                            existing_df.loc[idx, col] = new_df.iloc[0][col]
                    combined_df = existing_df
                else:
                    # 追加新数据
                    if not existing_df.empty:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        combined_df = new_df

                # 排序并保存
                combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
                combined_df.to_parquet(file_path, index=False, compression='zstd')
                updated += 1

            except Exception as e:
                logger.error(f"❌ {symbol} 更新失败: {e}")
                failed += 1

        logger.info("="*60)
        logger.info(f"价格数据同步完成: 更新 {updated}, 跳过 {skipped}, 失败 {failed}")
        logger.info("="*60)

        # 批量同步复权因子（只同步可能有变化的股票）
        logger.info("\n开始同步复权因子...")
        factor_result = self._sync_factors_for_date(trade_date)

        return {
            'status': 'success',
            'trade_date': trade_date,
            'updated': updated,
            'skipped': skipped,
            'failed': failed,
            'factor_updated': factor_result.get('updated', 0),
            'factor_skipped': factor_result.get('skipped', 0)
        }

    def sync_today_data(self) -> dict:
        """
        同步当天数据（极速模式）

        使用 akshare.stock_zh_a_spot() 获取全市场当日实时数据，
        并更新到各个股票的 parquet 文件中。同时同步复权因子。

        适用于收盘后快速同步当天数据。

        Returns:
            同步结果统计
        """
        import akshare as ak
        from datetime import datetime

        today = datetime.now()
        today_str = today.strftime('%Y%m%d')
        today_date = today.date()

        logger.info("="*60)
        logger.info(f"启动当天数据同步: {today_str}")
        logger.info("="*60)

        # 1. 获取当天全市场数据
        logger.info("获取当天全市场数据...")
        try:
            spot_df = ak.stock_zh_a_spot()
            logger.info(f"✓ 获取完成: {len(spot_df)} 只股票")
        except Exception as e:
            logger.error(f"❌ 获取当天数据失败: {e}")
            return {'status': 'failed', 'message': str(e)}

        # 2. 数据清洗和转换
        # 列名映射
        column_map = {
            '代码': 'code',
            '名称': 'name',
            '最新价': 'close',
            '今开': 'open',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '昨收': 'prev_close',
            '换手率': 'turnover',
        }
        spot_df = spot_df.rename(columns=column_map)

        # 转换代码格式并添加 symbol 列
        def format_symbol(code):
            code = str(code)
            # 如果已经带后缀，直接返回
            if '.SH' in code or '.SZ' in code or '.BJ' in code:
                return code
            # 处理带前缀的代码 (bj920000 -> 920000.BJ)
            if code.startswith('sh'):
                return code[2:] + '.SH'
            elif code.startswith('sz'):
                return code[2:] + '.SZ'
            elif code.startswith('bj'):
                return code[2:] + '.BJ'
            # 纯数字代码根据规则判断
            elif code.startswith(('6', '68', '5')):
                return code + '.SH'
            elif code.startswith(('0', '3', '1', '4', '8')):
                return code + '.SZ'
            elif code.startswith(('8', '4', '9')) and len(code) >= 6:
                return code + '.BJ'
            return code

        spot_df['symbol'] = spot_df['code'].apply(format_symbol)
        spot_df['trade_date'] = today_date

        # 选择需要的列
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close',
                     'volume', 'amount', 'change_pct']
        spot_df = spot_df[[c for c in keep_cols if c in spot_df.columns]]

        # 3. 按股票分组并更新到各自的 parquet 文件
        grouped = spot_df.groupby('symbol')

        updated = 0
        skipped = 0
        failed = 0

        for symbol, group in grouped:
            try:
                file_path = self.get_stock_file_path(symbol)

                # 获取该股票的新数据（只有一条）
                new_df = group.copy()

                # 加载已有数据
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.date

                    # 检查是否已存在该日期
                    if today_date in existing_df['trade_date'].values:
                        # 更新已有数据
                        mask = existing_df['trade_date'] == today_date
                        idx = existing_df[mask].index[0]
                        for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct']:
                            if col in new_df.columns:
                                existing_df.loc[idx, col] = new_df.iloc[0][col]
                        combined_df = existing_df
                    else:
                        # 追加新数据
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    # 新建文件
                    combined_df = new_df

                # 排序并保存
                combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
                combined_df.to_parquet(file_path, index=False, compression='zstd')
                updated += 1

            except Exception as e:
                logger.error(f"❌ {symbol} 更新失败: {e}")
                failed += 1

        logger.info("="*60)
        logger.info(f"价格数据同步完成: 更新 {updated}, 失败 {failed}")
        logger.info("="*60)

        # 4. 同步复权因子（只同步今天有数据的股票）
        logger.info("\n开始同步复权因子...")
        factor_result = self._sync_factors_for_symbols(list(grouped.groups.keys()))

        return {
            'status': 'success',
            'trade_date': today_str,
            'updated': updated,
            'skipped': skipped,
            'failed': failed,
            'factor_updated': factor_result.get('updated', 0),
            'factor_skipped': factor_result.get('skipped', 0)
        }

    def _sync_factors_for_symbols(self, symbols: list) -> dict:
        """
        为指定股票列表同步复权因子

        Args:
            symbols: 股票代码列表

        Returns:
            同步结果统计
        """
        import random
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from threading import Lock

        RAW_ADJUST_FACTOR_DIR.mkdir(parents=True, exist_ok=True)

        updated = 0
        skipped = 0
        failed = 0
        completed = 0
        total = len(symbols)
        lock = Lock()

        def _sync_single(symbol: str) -> dict:
            """同步单只股票的复权因子"""
            time.sleep(random.uniform(0.3, 1.0))  # 随机延迟避免请求过快
            try:
                return self.sync_adjust_factor(symbol)
            except Exception as e:
                return {'status': 'failed', 'error': str(e)}

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_symbol = {
                executor.submit(_sync_single, symbol): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    with lock:
                        completed += 1
                        if result['status'] == 'success':
                            if result.get('records', 0) > 0:
                                updated += 1
                            else:
                                skipped += 1
                        else:
                            failed += 1

                        if completed % 100 == 0 or completed == total:
                            logger.info(f"复权因子进度: {completed}/{total} ({completed/total*100:.1f}%) - "
                                       f"成功:{updated} 跳过:{skipped} 失败:{failed}")
                except Exception as e:
                    with lock:
                        completed += 1
                        failed += 1

        return {
            'updated': updated,
            'skipped': skipped,
            'failed': failed
        }

    def _sync_factors_for_date(self, trade_date: str) -> dict:
        """
        为指定日期同步复权因子（只同步可能需要更新的股票）

        策略：
        1. 只同步今天有交易的股票（价格数据已更新）
        2. 已有复权因子的股票，检查最后更新日期
        3. 从未同步过的股票，尝试获取

        Args:
            trade_date: 交易日期 'YYYYMMDD'

        Returns:
            同步结果统计
        """
        import random

        RAW_ADJUST_FACTOR_DIR.mkdir(parents=True, exist_ok=True)

        # 获取所有有价格数据的股票
        all_symbols = [f.stem for f in RAW_PRICE_DIR.glob("*.parquet")]

        updated = 0
        skipped = 0
        failed = 0

        # 遍历所有股票，检查是否需要更新复权因子
        for symbol in all_symbols:
            try:
                file_path = RAW_ADJUST_FACTOR_DIR / f"{symbol}.parquet"

                # 检查是否已有复权因子文件
                if file_path.exists():
                    try:
                        existing_df = pd.read_parquet(file_path)
                        existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.date
                        latest_factor_date = existing_df['trade_date'].max()

                        # 如果复权因子已包含今天或更新日期，跳过
                        if str(latest_factor_date) >= trade_date:
                            skipped += 1
                            continue
                    except:
                        pass

                # 需要更新：获取该股票的复权因子
                result = self.sync_adjust_factor(symbol)

                if result['status'] == 'success':
                    if result['records'] > 0:
                        updated += 1
                    else:
                        skipped += 1
                else:
                    failed += 1

                # 随机延迟，避免请求过快
                time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                logger.error(f"❌ {symbol} 复权因子同步失败: {e}")
                failed += 1

        logger.info(f"复权因子同步完成: 更新 {updated}, 跳过 {skipped}, 失败 {failed}")
        return {'updated': updated, 'skipped': skipped, 'failed': failed}

    def _sync_single_stock(
        self,
        symbol: str,
        incremental: bool,
        start_date: str = None,
        end_date: str = None,
        asset_type: str = "stock"
    ) -> dict:
        """
        同步单只股票/ETF（线程安全包装）

        Args:
            symbol: 股票代码
            incremental: 是否增量更新
            start_date: 指定开始日期（可选）
            end_date: 指定结束日期（可选）
            asset_type: 资产类型，'stock'(股票) 或 'etf'(ETF)

        Returns:
            同步结果
        """
        # 如果指定了日期范围，先快速检查是否已包含该日期范围
        if start_date and end_date:
            file_path = self.get_stock_file_path(symbol, asset_type)
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path)
                    if not df.empty and 'trade_date' in df.columns:
                        latest_date = df['trade_date'].max()
                        if hasattr(latest_date, 'strftime'):
                            latest_date_str = latest_date.strftime('%Y%m%d')
                        else:
                            latest_date_str = str(latest_date).replace('-', '')

                        # 如果最新日期 >= 目标结束日期，直接跳过
                        if latest_date_str >= end_date:
                            return {
                                'status': 'success',
                                'symbol': symbol,
                                'records': 0,
                                'message': f'Skipped (already up to {latest_date_str})'
                            }
                except Exception:
                    pass  # 读取失败则继续同步

        # 随机延迟 0.5-2 秒，避免请求过于规律
        delay = random.uniform(0.5, 2.0)
        time.sleep(delay)

        try:
            result = self.sync_stock(
                symbol,
                start_date=start_date,
                end_date=end_date,
                incremental=incremental,
                asset_type=asset_type
            )
            return result
        except Exception as e:
            logger.error(color_log('error', f"❌ 同步 {symbol} 异常: {e}"))
            return {'status': 'failed', 'symbol': symbol, 'error': str(e)}

    def sync_all(
        self,
        symbols: List[str] = None,
        incremental: bool = True,
        skip_existing: bool = False,
        max_workers: int = 3,
        start_date: str = None,
        end_date: str = None,
        asset_type: str = "stock"
    ) -> dict:
        """
        同步所有股票/ETF数据（并行版本）

        Args:
            symbols: 股票代码列表，None表示全部
            incremental: 是否增量更新
            skip_existing: 是否完全跳过已存在的文件（首次全量同步时用）
            max_workers: 最大并发数（默认3，建议3-5）
            start_date: 指定开始日期（覆盖自动计算的日期）
            end_date: 指定结束日期（覆盖自动计算的日期）
            asset_type: 资产类型，'stock'(股票) 或 'etf'(ETF)

        Returns:
            同步结果统计
        """
        # 根据资产类型选择列表
        if symbols is None:
            if asset_type == "etf":
                if self.etf_list.empty:
                    return {'status': 'failed', 'message': '没有ETF列表'}
                symbols = self.etf_list['symbol'].tolist()
            elif asset_type == "index":
                symbols = self.index_list
            else:
                if self.stock_list.empty:
                    return {'status': 'failed', 'message': '没有股票列表'}
                symbols = self.stock_list['symbol'].tolist()

        # 如果跳过已有文件，快速扫描已存在的股票
        if skip_existing:
            existing_symbols = set()
            if asset_type == "etf":
                price_dir = RAW_ETF_PRICE_DIR
            elif asset_type == "index":
                price_dir = RAW_INDEX_PRICE_DIR
            else:
                price_dir = self.raw_price_dir
            for f in price_dir.glob("*.parquet"):
                existing_symbols.add(f.stem)

            original_count = len(symbols)
            symbols = [s for s in symbols if s not in existing_symbols]
            skipped_count = original_count - len(symbols)
            logger.info(f"跳过已有文件的 {skipped_count} 只，实际需同步 {len(symbols)} 只")

        total = len(symbols)
        if asset_type == "etf":
            asset_type_str = "ETF"
        elif asset_type == "index":
            asset_type_str = "指数"
        else:
            asset_type_str = "股票"
        logger.info(f"开始同步 {total} 只{asset_type_str}，并发数: {max_workers}")

        success_count = 0
        failed_symbols = []
        failed_symbols_info = []  # 记录失败原因
        total_new_records = 0
        completed = 0
        lock = Lock()

        def update_progress(result: dict, symbol: str):
            """更新进度（线程安全）"""
            nonlocal completed, success_count, total_new_records
            with lock:
                completed += 1
                if result['status'] == 'success':
                    success_count += 1
                    total_new_records += result.get('new_records', 0)
                else:
                    failed_symbols.append(symbol)

                # 每10只或完成时打印进度
                if completed % 10 == 0 or completed == total:
                    logger.info(f"进度: {completed}/{total} ({completed/total*100:.1f}%) - 成功: {success_count}")

        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务（传递日期参数用于快速跳过检查）
            future_to_symbol = {
                executor.submit(self._sync_single_stock, symbol, incremental, start_date, end_date, asset_type): symbol
                for symbol in symbols
            }

            # 处理完成的任务（带超时机制，单个任务最多 60 秒）
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=60)  # 60秒超时
                    update_progress(result, symbol)
                except TimeoutError:
                    logger.error(color_log('error', f"⏱️  {symbol} 同步超时（超过60秒），跳过"))
                    with lock:
                        completed += 1
                        failed_symbols.append(symbol)
                        failed_symbols_info.append((symbol, "timeout"))
                except Exception as e:
                    logger.error(color_log('error', f"❌ 处理 {symbol} 结果时异常: {e}"))
                    with lock:
                        completed += 1
                        failed_symbols.append(symbol)

        if failed_symbols:
            logger.info(color_log('warning', f"⚠️  同步完成: {success_count} 只成功, {len(failed_symbols)} 只失败"))
        else:
            logger.info(color_log('success', f"✓ 同步完成: {success_count} 只成功, {len(failed_symbols)} 只失败"))
        
        # 保存待处理列表（北交所等无法自动获取的股票）
        if self.pending_symbols:
            self.save_pending_list()

        return {
            'status': 'success',
            'total_symbols': total,
            'success': success_count,
            'failed': len(failed_symbols),
            'new_records': total_new_records,
            'failed_symbols': failed_symbols[:10] if failed_symbols else [],
            'pending_count': len(self.pending_symbols)
        }

    def list_existing_files(self, asset_type: str = "stock") -> List[Path]:
        """列出所有已存在的Parquet文件"""
        if asset_type == "etf":
            price_dir = RAW_ETF_PRICE_DIR
        elif asset_type == "index":
            price_dir = RAW_INDEX_PRICE_DIR
        else:
            price_dir = self.raw_price_dir
        files = sorted(price_dir.glob("*.parquet"))
        return files

    def get_sync_summary(self) -> dict:
        """获取同步摘要"""
        files = self.list_existing_files()

        summary = {
            'total_files': len(files),
            'files': [],
            'total_records': 0
        }

        for f in files:
            try:
                df = pd.read_parquet(f)
                summary['files'].append({
                    'symbol': f.stem,
                    'records': len(df),
                    'date_range': f"{df['trade_date'].min()} ~ {df['trade_date'].max()}"
                })
                summary['total_records'] += len(df)
            except Exception as e:
                logger.warning(color_log('warning', f"⚠️  读取文件失败 {f}: {e}"))

        return summary

    def get_latest_date_distribution(self) -> dict:
        """
        获取所有股票的最新日期分布统计

        Returns:
            {
                'total_stocks': 总股票数,
                'latest_overall': 全局最新日期,
                'distribution': {
                    '2025-04-14': 1500,
                    '2025-04-13': 200,
                    ...
                },
                'outdated_stocks': [股票列表]  # 最新日期不是全局最新日期的股票
            }
        """
        from collections import Counter

        files = self.list_existing_files()
        latest_dates = []
        stock_latest = {}  # symbol -> latest_date

        print(f"正在扫描 {len(files)} 只股票...")

        for f in files:
            try:
                df = pd.read_parquet(f)
                if df.empty or 'trade_date' not in df.columns:
                    continue

                latest_date = df['trade_date'].max()
                symbol = f.stem

                # 统一转换为date类型
                if hasattr(latest_date, 'date'):
                    latest_date = latest_date.date()
                elif isinstance(latest_date, str):
                    latest_date = pd.to_datetime(latest_date).date()

                latest_dates.append(latest_date)
                stock_latest[symbol] = latest_date

            except Exception as e:
                logger.warning(color_log('warning', f"⚠️  读取文件失败 {f}: {e}"))

        if not latest_dates:
            return {
                'total_stocks': 0,
                'latest_overall': None,
                'distribution': {},
                'outdated_stocks': []
            }

        # 统计分布
        latest_overall = max(latest_dates)
        date_counter = Counter(latest_dates)

        # 找出不是最新日期的股票
        outdated_stocks = [
            symbol for symbol, date in stock_latest.items()
            if date != latest_overall
        ]

        return {
            'total_stocks': len(latest_dates),
            'latest_overall': latest_overall,
            'distribution': dict(sorted(date_counter.items(), key=lambda x: x[0], reverse=True)),
            'outdated_stocks': outdated_stocks
        }


def parse_date_arg(date_str: str) -> tuple:
    """
    解析日期参数，支持多种格式

    支持格式:
        - 单日: 20260413, 2026-04-13, 2026/04/13
        - 范围: 20260413~20260414, 2026-04-13~2026-04-14, 2026/04/13~2026/04/14

    Returns:
        (start_date, end_date) 格式为 YYYYMMDD
    """
    import re

    if not date_str:
        return None, None

    # 统一分隔符为 ~
    date_str = date_str.replace('—', '~').replace('－', '~')

    # 提取所有日期数字
    def extract_date(s: str) -> str:
        """从字符串中提取 YYYYMMDD 格式"""
        # 移除所有非数字字符
        digits = re.sub(r'\D', '', s)
        if len(digits) == 8:
            return digits
        raise ValueError(f"无法解析日期: {s}")

    if '~' in date_str:
        # 范围格式
        parts = date_str.split('~')
        if len(parts) != 2:
            raise ValueError(f"日期范围格式错误: {date_str}")
        start = extract_date(parts[0].strip())
        end = extract_date(parts[1].strip())
        return start, end
    else:
        # 单日格式
        date = extract_date(date_str.strip())
        return date, date


def parse_symbol_arg(symbol_str: str) -> tuple:
    """
    解析 --symbol 参数
    
    支持:
    - 具体代码: "600519.SH,300750.SZ" -> (['600519.SH', '300750.SZ'], None)
    - 类型简写: "stock" -> ([], 'stock'), "etf" -> ([], 'etf'), "index" -> ([], 'index')
    
    Returns:
        (symbol_list, asset_type) - symbol_list 为空列表表示同步全部该类型
    """
    if not symbol_str:
        return [], 'stock'  # 默认全部股票
    
    # 类型简写映射
    type_aliases = {'stock', 'etf', 'index'}
    
    if symbol_str.lower() in type_aliases:
        return [], symbol_str.lower()
    
    # 具体代码列表
    symbols = [s.strip() for s in symbol_str.split(',')]
    return symbols, None


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='历史数据同步服务')
    parser.add_argument('--symbol', type=str, help='指定代码，支持: 1)具体代码如600519.SH,300750.SZ 2)类型简写:stock(全部股票),etf(全部ETF),index(全部指数)。不指定时默认同步全部股票')
    parser.add_argument('--daily', nargs='?', const=True, default=False,
                        help='每日增量更新。可指定日期: 20260413, 2026-04-13, 20260413~20260414')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD（已废弃，建议使用 --daily DATE）')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD（已废弃，建议使用 --daily DATE')
    parser.add_argument('--override', action='store_true', help='覆盖已有数据（默认增量）')
    parser.add_argument('--skip-existing', action='store_true', help='首次同步时跳过已有文件的股票（大幅提速）')
    parser.add_argument('--summary', action='store_true', help='显示同步摘要')
    parser.add_argument('--limit', type=int, help='限制股票数量（测试用）')
    parser.add_argument('--sync-factors', action='store_true', help='只同步复权因子（不下载价格数据）')
    parser.add_argument('--workers', type=int, default=3, help='并发线程数（默认3，建议3-5）')
    parser.add_argument('--no-logout', action='store_true', help='不执行baostock登出（用于并行执行时不影响其他进程）')
    parser.add_argument('--include-bj', action='store_true', help='包含北交所股票（默认跳过，因数据源不稳定）')
    parser.add_argument('--today', action='store_true', help='同步当天数据（极速模式）：使用 stock_zh_a_spot 获取全市场当日数据并入库，同时更新复权因子')

    args = parser.parse_args()

    # 处理 --daily 参数的日期
    if args.daily and isinstance(args.daily, str):
        try:
            start_date, end_date = parse_date_arg(args.daily)
            args.start_date = start_date
            args.end_date = end_date
        except ValueError as e:
            print(f"错误: {e}")
            print("日期格式示例:")
            print("  --daily 20260413")
            print("  --daily 2026-04-13")
            print("  --daily 20260413~20260414")
            print("  --daily 2026-04-13~2026-04-14")
            return

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    service = HistorySyncService()

    if args.sync_factors:
        # 只同步复权因子
        print("\n" + "="*60)
        print("只同步复权因子（不下载价格数据）")
        print("="*60)

        # 获取已有价格数据的股票列表
        symbols = [f.stem for f in service.raw_price_dir.glob('*.parquet')]
        print(f"发现 {len(symbols)} 只股票需要同步复权因子")
        print(f"并发数: {args.workers}，每只请求间隔: 0.5-2秒")

        def _sync_single_factor(symbol: str) -> dict:
            """同步单只股票的复权因子"""
            time.sleep(random.uniform(0.5, 2.0))
            try:
                return service.sync_adjust_factor(symbol)
            except Exception as e:
                return {'status': 'failed', 'error': str(e)}

        success = 0
        failed = 0
        skipped = 0
        completed = 0
        total = len(symbols)
        lock = Lock()

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_symbol = {
                executor.submit(_sync_single_factor, symbol): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    with lock:
                        completed += 1
                        if result['status'] == 'success':
                            if result.get('records', 0) > 0:
                                success += 1
                            else:
                                skipped += 1
                        else:
                            failed += 1

                        if completed % 100 == 0 or completed == total:
                            print(f"进度: {completed}/{total} ({completed/total*100:.1f}%) - 成功:{success} 跳过:{skipped} 失败:{failed}")
                except Exception as e:
                    with lock:
                        completed += 1
                        failed += 1

        print(f"\n复权因子同步完成: 成功 {success}, 跳过 {skipped}, 失败 {failed}")
        print("提示: '跳过'表示该股票从未分红送股，无需复权")

    elif args.summary:
        # 1. 基础同步摘要
        summary = service.get_sync_summary()
        print("\n" + "="*60)
        print("同步摘要")
        print("="*60)
        print(f"  总文件数: {summary['total_files']}")
        print(f"  总记录数: {summary['total_records']:,}")

        # 2. 最新日期分布统计
        print("\n" + "-"*60)
        print("最新日期分布")
        print("-"*60)

        dist = service.get_latest_date_distribution()

        if dist['total_stocks'] == 0:
            print("  没有找到任何股票数据文件")
        else:
            print(f"  全局最新日期: {dist['latest_overall']}")

            print(f"  {'日期':<15} {'股票数':>10} {'占比':>10}")
            print("  " + "-"*40)

            for date, count in dist['distribution'].items():
                pct = count / dist['total_stocks'] * 100
                marker = " <-- 最新" if date == dist['latest_overall'] else ""
                print(f"  {str(date):<15} {count:>10,} {pct:>9.1f}%{marker}")

        # 3. 文件列表
        print("\n" + "-"*60)
        print(f"文件列表 (前20个):")
        print("-"*60)
        for f in summary['files'][:20]:
            print(f"  {f['symbol']}: {f['records']:,} 条 ({f['date_range']})")
        if len(summary['files']) > 20:
            print(f"  ... 还有 {len(summary['files']) - 20} 个文件")

        print("="*60)

    elif args.today:
        # 同步当天数据（极速模式）
        from datetime import datetime
        today_str = datetime.now().strftime('%Y%m%d')
        print("\n" + "="*60)
        print(f"执行当天数据同步: {today_str}")
        print("="*60)
        print("⚡ 极速模式：使用 stock_zh_a_spot 获取全市场当日数据")
        print("📊 同时更新复权因子")
        print()

        result = service.sync_today_data()

        print("\n" + "="*60)
        print("当天数据同步结果")
        print("="*60)
        print(f"  交易日期: {result.get('trade_date', today_str)}")
        print(f"  价格数据:")
        print(f"    - 更新: {result.get('updated', 0)} 只")
        print(f"    - 跳过(无数据): {result.get('skipped', 0)} 只")
        print(f"    - 失败: {result.get('failed', 0)} 只")
        print(f"  复权因子:")
        print(f"    - 更新: {result.get('factor_updated', 0)} 只")
        print(f"    - 跳过(无需更新): {result.get('factor_skipped', 0)} 只")
        print("="*60)

    elif args.symbol:
        # 解析 --symbol 参数
        symbol_list, asset_type = parse_symbol_arg(args.symbol)
        
        if not symbol_list:
            # 类型简写: 同步全部该类型
            if asset_type == "etf":
                symbols = service.etf_list['symbol'].tolist()
                asset_type_str = "ETF"
            elif asset_type == "index":
                symbols = service.index_list
                asset_type_str = "指数"
            else:
                symbols = service.stock_list['symbol'].tolist()
                asset_type_str = "股票"
            
            print(f"\n将同步全部 {len(symbols)} 只{asset_type_str}")
            result = service.sync_all(
                symbols=symbols,
                incremental=not args.override,
                max_workers=args.workers,
                asset_type=asset_type
            )
        print("\n同步结果:")
        print(f"  成功: {result['success']}/{result['total_symbols']}")
        print(f"  失败: {result['failed']}")
        print(f"  新增记录: {result.get('new_records', 0):,}")
        if result.get('failed_symbols'):
            print(f"\n  失败代码:")
            for symbol in result['failed_symbols']:
                print(f"    - {symbol}")
    else:
        # 具体代码列表
        from lib.utils import StockCodeUtil
        from lib.utils.stock_code import detect_asset_type
        symbol_list = [StockCodeUtil.with_suffix(s) or s for s in symbol_list]
        
        # 自动识别资产类型
        detected_type = detect_asset_type(symbol_list[0], 'stock')
        
        if len(symbol_list) == 1:
            # 单只
            if args.override:
                file_path = service.get_stock_file_path(symbol_list[0], detected_type)
                if file_path.exists():
                    file_path.unlink()
                    print(f"已删除旧文件: {file_path}")
            result = service.sync_stock(
                symbol_list[0],
                start_date=args.start_date,
                end_date=args.end_date,
                incremental=not args.override,
                asset_type=detected_type
            )
            print("\n同步结果:")
            print(f"  状态: {result['status']}")
            print(f"  代码: {result['symbol']}")
            print(f"  新数据: {result.get('new_records', 0)} 条")
            print(f"  总数据: {result.get('total_records', 0)} 条")
            if result.get('date_range'):
                print(f"  日期范围: {result['date_range']}")
        else:
            # 多只
            print(f"\n同步 {len(symbol_list)} 只: {', '.join(symbol_list[:5])}{'...' if len(symbol_list) > 5 else ''}")
            result = service.sync_all(
                symbols=symbol_list,
                incremental=not args.override,
                max_workers=args.workers,
                start_date=args.start_date,
                end_date=args.end_date,
                asset_type=detected_type
            )
            print("\n同步结果:")
            print(f"  成功: {result['success']}/{result['total_symbols']}")
            print(f"  失败: {result['failed']}")
            print(f"  新增记录: {result.get('new_records', 0):,}")
            if result.get('failed_symbols'):
                print(f"\n  失败代码:")
                for symbol in result['failed_symbols']:
                    print(f"    - {symbol}")

    elif args.daily:
        # 每日增量更新 - 自动同步所有股票到最新日期
        print("\n" + "="*60)
        print("执行每日增量更新")
        print("="*60)

        # 解析 --symbol 参数
        symbol_list, asset_type = parse_symbol_arg(args.symbol)
        
        # 处理列表
        from lib.utils import StockCodeUtil
        if asset_type == "etf":
            asset_type_str = "ETF"
        elif asset_type == "index":
            asset_type_str = "指数"
        else:
            asset_type_str = "股票"

        if symbol_list:
            # 指定代码列表
            symbols = [StockCodeUtil.with_suffix(s) or s for s in symbol_list]
            print(f"指定{asset_type_str}: {len(symbols)} 只")
            print(f"  {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
        elif args.limit:
            # 测试模式：限制数量
            if asset_type == "etf":
                symbols = service.etf_list['symbol'].tolist()[:args.limit]
            elif asset_type == "index":
                symbols = service.index_list[:args.limit]
            else:
                symbols = service.stock_list['symbol'].tolist()[:args.limit]
            print(f"测试模式: 只同步前 {args.limit} 只{asset_type_str}")
        else:
            # 同步全部
            if asset_type == "etf":
                symbols = service.etf_list['symbol'].tolist()
            elif asset_type == "index":
                symbols = service.index_list
            else:
                symbols = service.stock_list['symbol'].tolist()
            print(f"将同步全部 {len(symbols)} 只{asset_type_str}")
        
        # 默认跳过北交所股票（除非指定 --include-bj）
        if asset_type != "index" and not args.include_bj:
            bj_count = sum(1 for s in symbols if '.BJ' in s)
            symbols = [s for s in symbols if '.BJ' not in s]
            if bj_count > 0:
                print(f"提示: 已跳过 {bj_count} 只北交所股票（使用 --include-bj 可包含）")

        # 如果指定了日期范围，使用指定日期（用于快速补数据）
        if args.start_date or args.end_date:
            start_date = args.start_date or datetime.now().strftime('%Y%m%d')
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            print(f"指定日期范围: {start_date} ~ {end_date}")
            print(f"提示: 已包含此日期范围的股票将被快速跳过")
        else:
            start_date = None
            end_date = None

        print(f"并发数: {args.workers}，每只请求间隔: 0.5-2秒")

        # 同步价格数据
        print("\n" + "-"*60)
        print("开始同步")
        print("-"*60)

        # 使用增量模式，从已有数据的最新日期开始
        result = service.sync_all(
            symbols=symbols,
            incremental=True,
            max_workers=args.workers,
            skip_existing=args.skip_existing,
            start_date=start_date,
            end_date=end_date,
            asset_type=asset_type
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总{asset_type_str}: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新增记录: {result['new_records']:,}")
        if result.get('failed_symbols'):
            print(f"\n  失败代码:")
            for symbol in result['failed_symbols']:
                print(f"    - {symbol}")

        print("\n" + "="*60)
        if asset_type == "etf":
            print("每日增量更新完成（ETF价格已同步）")
        elif asset_type == "index":
            print("每日增量更新完成（指数数据已同步）")
        else:
            print("每日增量更新完成（价格与复权因子已同步）")
        print("="*60)

    elif args.override:
        # 全量覆盖同步模式
        # 解析 --symbol 参数，默认股票
        symbol_list, asset_type = parse_symbol_arg(args.symbol)
        
        from lib.utils import StockCodeUtil
        if asset_type == "etf":
            asset_type_str = "ETF"
        elif asset_type == "index":
            asset_type_str = "指数"
        else:
            asset_type_str = "股票"

        if symbol_list:
            # 指定代码列表
            symbols = [StockCodeUtil.with_suffix(s) or s for s in symbol_list]
            print(f"指定{asset_type_str}: {len(symbols)} 只")
            print(f"  {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
        else:
            # 同步全部
            if asset_type == "etf":
                symbols = service.etf_list['symbol'].tolist()
            elif asset_type == "index":
                symbols = service.index_list
            else:
                symbols = service.stock_list['symbol'].tolist()
            print(f"将同步全部 {len(symbols)} 只{asset_type_str}")

        # 默认跳过北交所股票（指数不适用）
        if asset_type != "index" and not args.include_bj:
            bj_count = sum(1 for s in symbols if '.BJ' in s)
            symbols = [s for s in symbols if '.BJ' not in s]
            if bj_count > 0:
                print(f"提示: 已跳过 {bj_count} 只北交所股票（使用 --include-bj 可包含）")

        print("\n" + "="*60)
        print(f"执行{asset_type_str}覆盖同步")
        print("="*60)
        print(f"类型: {asset_type_str}")
        print(f"模式: 覆盖")
        print(f"并发: {args.workers}")
        print("="*60)

        result = service.sync_all(
            symbols=symbols,
            incremental=False,
            skip_existing=args.skip_existing,
            max_workers=args.workers,
            asset_type=asset_type
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总{asset_type_str}: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新记录: {result['new_records']:,}")
        if result.get('failed_symbols'):
            print(f"\n  失败代码:")
            for symbol in result['failed_symbols']:
                print(f"    - {symbol}")
        print("="*60)

    else:
        # 默认行为：同步全部股票（增量）
        symbols = service.stock_list['symbol'].tolist()
        print(f"\n将同步全部 {len(symbols)} 只股票")
        
        # 默认跳过北交所股票
        if not args.include_bj:
            bj_count = sum(1 for s in symbols if '.BJ' in s)
            symbols = [s for s in symbols if '.BJ' not in s]
            if bj_count > 0:
                print(f"提示: 已跳过 {bj_count} 只北交所股票")
        
        print("\n" + "="*60)
        print("执行股票增量同步")
        print("="*60)
        
        result = service.sync_all(
            symbols=symbols,
            incremental=True,
            skip_existing=args.skip_existing,
            max_workers=args.workers,
            asset_type='stock'
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总股票: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新记录: {result['new_records']:,}")
        if result.get('failed_symbols'):
            print(f"\n  失败代码:")
            for symbol in result['failed_symbols']:
                print(f"    - {symbol}")
        print("="*60)

    # 退出时登出baostock（只在已登录时，且未指定 --no-logout）
    if service._baostock_logged_in and not args.no_logout:
        bs.logout()
        logger.info("baostock已登出")


if __name__ == "__main__":
    main()

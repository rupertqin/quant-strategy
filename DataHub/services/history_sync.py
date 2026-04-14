"""
历史数据同步服务 - 下载全市场股票历史日线数据到 Parquet

每只股票一个文件，包含全部历史数据
存储位置: storage/raw/prices/{symbol}.parquet

用法:
    # ========== 每日增量更新（推荐日常使用）==========
    python DataHub/services/history_sync.py --daily

    # 测试模式，只同步前10只
    python DataHub/services/history_sync.py --daily --limit 10

    # 使用更多并发（更快但可能被IP限制）
    python DataHub/services/history_sync.py --daily --workers 5

    # 指定单日同步（快速补数据）
    python DataHub/services/history_sync.py --daily 20260413
    python DataHub/services/history_sync.py --daily 2026-04-13

    # 指定日期范围
    python DataHub/services/history_sync.py --daily 20260413~20260414
    python DataHub/services/history_sync.py --daily 2026-04-13~2026-04-14

    # 指定股票同步（单只或多只，支持省略后缀）
    python DataHub/services/history_sync.py --daily --symbol 600519
    python DataHub/services/history_sync.py --daily 20260413 --symbol 600519,300750,000858
    python DataHub/services/history_sync.py --daily --symbol 600519.SH,300750.SZ


    # ========== 首次全量同步（断点续传）==========
    python DataHub/services/history_sync.py --all --skip-existing


    # ========== 全量更新（覆盖已有数据）==========
    python DataHub/services/history_sync.py --symbol 600519.SH --full


    # ========== 复权因子同步 ==========
    python DataHub/services/history_sync.py --sync-factors


    # ========== 查看同步摘要 ==========
    python DataHub/services/history_sync.py --summary

参数说明:
    --all              同步所有股票
    --daily [DATE]     每日增量更新。可选指定日期:
                         无参数: 自动同步到最新日期
                         单日: 20260413, 2026-04-13
                         范围: 20260413~20260414, 2026-04-13~2026-04-14
    --symbol SYMBOL    指定股票，支持单只或多只逗号分隔，后缀可省略
                         如: 600519, 600519.SH, 600519,300750,000858
    --full             全量更新（覆盖已有数据，默认增量）
    --skip-existing    跳过已有文件的股票（首次同步时大幅提速，不读取文件内容）
    --summary          显示已同步数据摘要
    --sync-factors     只同步复权因子（不下载价格数据）
    --limit N          限制股票数量（测试用）
    --workers N        并发线程数（默认3，建议3-5，过多可能被IP屏蔽）

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
from datetime import datetime
from typing import List, Optional
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from DataHub.config import CRAWLER_REQUEST_DELAY, STORAGE_DIR

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
        self.raw_prices_dir = STORAGE_DIR / "raw" / "prices"
        self.raw_prices_dir.mkdir(parents=True, exist_ok=True)

        # 加载股票列表
        self.stock_list = self._load_stock_list()

        # 登录状态标记
        self._baostock_logged_in = False

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

    def _format_code(self, symbol: str) -> str:
        """转换代码格式: 600519.SH -> sh.600519 (baostock格式)"""
        if '.SH' in symbol:
            return 'sh.' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return 'sz.' + symbol.replace('.SZ', '')
        return symbol

    def get_stock_file_path(self, symbol: str) -> Path:
        """获取股票数据文件路径"""
        return self.raw_prices_dir / f"{symbol}.parquet"

    def load_existing_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """加载股票已存在的历史数据"""
        file_path = self.get_stock_file_path(symbol)
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
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取单只股票历史数据 (使用baostock - 前复权)

        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        # 确保已登录（线程安全）
        self._login_baostock()

        try:
            code = self._format_code(symbol)

            # 转换日期格式为 baostock 格式 YYYY-MM-DD
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

            # 使用全局锁保护baostock API调用（线程安全）
            with _baostock_lock:
                # 调用baostock接口 - flag=3 与历史数据保持一致
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,volume,amount,pctChg",
                    start_date=start_dt,
                    end_date=end_dt,
                    frequency="d",
                    adjustflag="3"  # 与历史数据保持一致
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
        adjust_dir = STORAGE_DIR / "raw" / "adjust_factors"
        adjust_dir.mkdir(parents=True, exist_ok=True)
        file_path = adjust_dir / f"{symbol}.parquet"

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
        incremental: bool = True
    ) -> dict:
        """
        同步单只股票的历史数据

        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'，None表示从最早开始
            end_date: 结束日期 'YYYYMMDD'，None表示到今天
            incremental: 是否增量更新，True表示只获取新数据

        Returns:
            同步结果
        """
        file_path = self.get_stock_file_path(symbol)

        # 确定日期范围
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        existing_df = None
        if incremental and file_path.exists():
            existing_df = self.load_existing_data(symbol)
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
            logger.info(color_log('success', f"✓ {symbol} 数据已是最新，无需更新"))
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
        new_df = self.fetch_stock_history(symbol, start_date, end_date)

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

        # 保存（不复权原始价格）
        combined_df.to_parquet(file_path, index=False, compression='zstd')

        # 同步复权因子
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

    def _sync_single_stock(
        self,
        symbol: str,
        incremental: bool,
        start_date: str = None,
        end_date: str = None
    ) -> dict:
        """
        同步单只股票（线程安全包装）

        Args:
            symbol: 股票代码
            incremental: 是否增量更新
            start_date: 指定开始日期（可选）
            end_date: 指定结束日期（可选）

        Returns:
            同步结果
        """
        # 如果指定了日期范围，先快速检查是否已包含该日期范围
        if start_date and end_date:
            file_path = self.get_stock_file_path(symbol)
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
                incremental=incremental
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
        end_date: str = None
    ) -> dict:
        """
        同步所有股票数据（并行版本）

        Args:
            symbols: 股票代码列表，None表示全部
            incremental: 是否增量更新
            skip_existing: 是否完全跳过已存在的文件（首次全量同步时用）
            max_workers: 最大并发数（默认3，建议3-5）
            start_date: 指定开始日期（覆盖自动计算的日期）
            end_date: 指定结束日期（覆盖自动计算的日期）

        Returns:
            同步结果统计
        """
        if symbols is None:
            if self.stock_list.empty:
                return {'status': 'failed', 'message': '没有股票列表'}
            symbols = self.stock_list['symbol'].tolist()

        # 如果跳过已有文件，快速扫描已存在的股票
        if skip_existing:
            existing_symbols = set()
            for f in self.raw_prices_dir.glob("*.parquet"):
                existing_symbols.add(f.stem)

            original_count = len(symbols)
            symbols = [s for s in symbols if s not in existing_symbols]
            skipped_count = original_count - len(symbols)
            logger.info(f"跳过已有文件的 {skipped_count} 只股票，实际需同步 {len(symbols)} 只")

        total = len(symbols)
        logger.info(f"开始同步 {total} 只股票，并发数: {max_workers}")

        success_count = 0
        failed_symbols = []
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
                executor.submit(self._sync_single_stock, symbol, incremental, start_date, end_date): symbol
                for symbol in symbols
            }

            # 处理完成的任务
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    update_progress(result, symbol)
                except Exception as e:
                    logger.error(color_log('error', f"❌ 处理 {symbol} 结果时异常: {e}"))
                    with lock:
                        completed += 1
                        failed_symbols.append(symbol)

        if failed_symbols:
            logger.info(color_log('warning', f"⚠️  同步完成: {success_count} 只成功, {len(failed_symbols)} 只失败"))
        else:
            logger.info(color_log('success', f"✓ 同步完成: {success_count} 只成功, {len(failed_symbols)} 只失败"))

        return {
            'status': 'success',
            'total_symbols': total,
            'success': success_count,
            'failed': len(failed_symbols),
            'new_records': total_new_records,
            'failed_symbols': failed_symbols[:10] if failed_symbols else []
        }

    def list_existing_files(self) -> List[Path]:
        """列出所有已存在的Parquet文件"""
        files = sorted(self.raw_prices_dir.glob("*.parquet"))
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


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='历史数据同步服务')
    parser.add_argument('--symbol', type=str, help='指定股票，支持单只或多只逗号分隔，如 600519.SH 或 600519.SH,300750.SZ')
    parser.add_argument('--all', action='store_true', help='同步所有股票')
    parser.add_argument('--daily', nargs='?', const=True, default=False,
                        help='每日增量更新。可指定日期: 20260413, 2026-04-13, 20260413~20260414')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD（已废弃，建议使用 --daily DATE）')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD（已废弃，建议使用 --daily DATE')
    parser.add_argument('--full', action='store_true', help='全量更新（非增量）')
    parser.add_argument('--skip-existing', action='store_true', help='首次同步时跳过已有文件的股票（大幅提速）')
    parser.add_argument('--summary', action='store_true', help='显示同步摘要')
    parser.add_argument('--limit', type=int, help='限制股票数量（测试用）')
    parser.add_argument('--sync-factors', action='store_true', help='只同步复权因子（不下载价格数据）')
    parser.add_argument('--workers', type=int, default=3, help='并发线程数（默认3，建议3-5）')
    parser.add_argument('--no-logout', action='store_true', help='不执行baostock登出（用于并行执行时不影响其他进程）')

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
        symbols = [f.stem for f in service.raw_prices_dir.glob('*.parquet')]
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
            print(f"  落后股票数: {len(dist['outdated_stocks'])} ({len(dist['outdated_stocks'])/dist['total_stocks']*100:.1f}%)")

            print("\n  日期分布:")
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

    elif args.symbol:
        # 同步指定股票（支持逗号分隔）
        symbol_list = [s.strip() for s in args.symbol.split(',')]

        # 补全代码后缀
        from lib.utils import StockCodeUtil
        symbol_list = [StockCodeUtil.with_suffix(s) or s for s in symbol_list]

        if len(symbol_list) == 1:
            # 单只股票
            result = service.sync_stock(
                symbol_list[0],
                start_date=args.start_date,
                end_date=args.end_date,
                incremental=not args.full
            )
            print("\n同步结果:")
            print(f"  状态: {result['status']}")
            print(f"  股票: {result['symbol']}")
            print(f"  新数据: {result.get('new_records', 0)} 条")
            print(f"  总数据: {result.get('total_records', 0)} 条")
            if result.get('date_range'):
                print(f"  日期范围: {result['date_range']}")
        else:
            # 多只股票，使用批量同步
            print(f"\n同步 {len(symbol_list)} 只股票: {', '.join(symbol_list[:5])}{'...' if len(symbol_list) > 5 else ''}")
            result = service.sync_all(
                symbols=symbol_list,
                incremental=not args.full,
                max_workers=args.workers,
                start_date=args.start_date,
                end_date=args.end_date
            )
            print("\n批量同步结果:")
            print(f"  成功: {result['success']}/{result['total_symbols']}")
            print(f"  失败: {result['failed']}")
            print(f"  新增记录: {result.get('new_records', 0):,}")

    elif args.daily:
        # 每日增量更新 - 自动同步所有股票到最新日期
        print("\n" + "="*60)
        print("执行每日增量更新")
        print("="*60)

        # 处理指定股票列表（支持逗号分隔，自动补全后缀）
        from lib.utils import StockCodeUtil
        symbols = None
        if args.symbol:
            symbols = [s.strip() for s in args.symbol.split(',')]
            symbols = [StockCodeUtil.with_suffix(s) or s for s in symbols]  # 补全后缀
            print(f"指定股票: {len(symbols)} 只")
            print(f"  {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
        elif args.limit:
            symbols = service.stock_list['symbol'].tolist()[:args.limit]
            print(f"测试模式: 只同步前 {args.limit} 只股票")
        else:
            print(f"将同步全部 {len(service.stock_list)} 只股票")

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

        # 同步价格数据（内部已包含复权因子同步）
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
            end_date=end_date
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总股票: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新增记录: {result['new_records']:,}")
        if result['failed_symbols']:
            print(f"  失败股票: {', '.join(result['failed_symbols'])}")

        print("\n" + "="*60)
        print("每日增量更新完成（价格与复权因子已同步）")
        print("="*60)

    elif args.all:
        # 同步所有股票（可指定全量或增量）
        symbols = None
        if args.limit:
            symbols = service.stock_list['symbol'].tolist()[:args.limit]

        result = service.sync_all(
            symbols=symbols,
            incremental=not args.full,
            skip_existing=args.skip_existing,
            max_workers=args.workers
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总股票: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新记录: {result['new_records']:,}")

    else:
        parser.print_help()

    # 退出时登出baostock（只在已登录时，且未指定 --no-logout）
    if service._baostock_logged_in and not args.no_logout:
        bs.logout()
        logger.info("baostock已登出")


if __name__ == "__main__":
    main()

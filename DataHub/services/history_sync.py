"""
历史数据同步服务 - 下载全市场股票历史日线数据到 Parquet

每只股票一个文件，包含全部历史数据

用法:
    # 首次全量同步所有股票（断点续传）
    python DataHub/services/history_sync.py --all --skip-existing
    
    # 每日增量更新（只更新到最新交易日）
    python DataHub/services/history_sync.py --daily
    
    # 同步单只股票
    python DataHub/services/history_sync.py --symbol 600519.SH
    
    # 查看同步摘要
    python DataHub/services/history_sync.py --summary

参数说明:
    --all              同步所有股票
    --skip-existing    跳过已有文件的股票（首次同步时大幅提速，不读取文件内容）
    --daily            每日增量更新模式（自动跳过非交易日）
    --symbol           指定单只股票同步
    --full             全量更新（覆盖已有数据）
    --summary          显示已同步数据摘要
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

from DataHub.config import CRAWLER_REQUEST_DELAY, STORAGE_DIR

logger = logging.getLogger(__name__)


class HistorySyncService:
    """
    历史数据同步服务
    
    每只股票保存为一个Parquet文件，包含全部历史数据
    """
    
    def __init__(self):
        self.raw_prices_dir = STORAGE_DIR / "raw" / "prices"
        self.raw_prices_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载股票列表
        self.stock_list = self._load_stock_list()
        
        # 登录baostock
        self._login_baostock()
        
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
        """登录baostock"""
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"baostock登录失败: {lg.error_msg}")
        else:
            logger.info("baostock登录成功")
    
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
                logger.warning(f"读取 {symbol} 历史数据失败: {e}")
        return None
    
    def fetch_stock_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取单只股票历史数据 (使用baostock)
        
        Args:
            symbol: 股票代码，如 '600519.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            
        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        try:
            code = self._format_code(symbol)
            
            # 转换日期格式为 baostock 格式 YYYY-MM-DD
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
            
            # 调用baostock接口
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,volume,amount,pctChg",
                start_date=start_dt,
                end_date=end_dt,
                frequency="d",
                adjustflag="3"  # 前复权
            )
            
            if rs.error_code != '0':
                logger.warning(f"获取 {symbol} 数据失败: {rs.error_msg}")
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
            
            return df
            
        except Exception as e:
            logger.error(f"获取 {symbol} 历史数据失败: {e}")
            return None
    
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
        new_df = self.fetch_stock_history(symbol, start_date, end_date)
        
        if new_df is None or new_df.empty:
            logger.warning(f"{symbol} 没有获取到新数据")
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
        
        logger.info(f"{symbol} 同步完成: {len(new_df)} 条新数据，共 {len(combined_df)} 条")
        
        return {
            'status': 'success',
            'symbol': symbol,
            'new_records': len(new_df),
            'total_records': len(combined_df),
            'date_range': f"{combined_df['trade_date'].min()} ~ {combined_df['trade_date'].max()}",
            'file_path': str(file_path)
        }
    
    def sync_all(
        self,
        symbols: List[str] = None,
        incremental: bool = True,
        skip_existing: bool = False
    ) -> dict:
        """
        同步所有股票数据
        
        Args:
            symbols: 股票代码列表，None表示全部
            incremental: 是否增量更新
            skip_existing: 是否完全跳过已存在的文件（首次全量同步时用）
            
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
        
        logger.info(f"开始同步 {len(symbols)} 只股票")
        
        success_count = 0
        failed_symbols = []
        total_new_records = 0
        
        for i, symbol in enumerate(symbols, 1):
            if i % 100 == 0 or i == 1:
                logger.info(f"进度: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%)")
            
            result = self.sync_stock(symbol, incremental=incremental)
            
            if result['status'] == 'success':
                success_count += 1
                total_new_records += result.get('new_records', 0)
            else:
                failed_symbols.append(symbol)
            
            # 请求间隔
            time.sleep(CRAWLER_REQUEST_DELAY)
        
        logger.info(f"同步完成: {success_count} 只成功, {len(failed_symbols)} 只失败")
        
        return {
            'status': 'success',
            'total_symbols': len(symbols),
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
                logger.warning(f"读取文件失败 {f}: {e}")
        
        return summary


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='历史数据同步服务')
    parser.add_argument('--symbol', type=str, help='同步指定股票，如 600519.SH')
    parser.add_argument('--all', action='store_true', help='同步所有股票')
    parser.add_argument('--daily', action='store_true', help='每日增量更新（同步到最新日期）')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD')
    parser.add_argument('--full', action='store_true', help='全量更新（非增量）')
    parser.add_argument('--skip-existing', action='store_true', help='首次同步时跳过已有文件的股票（大幅提速）')
    parser.add_argument('--summary', action='store_true', help='显示同步摘要')
    parser.add_argument('--limit', type=int, help='限制股票数量（测试用）')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    service = HistorySyncService()
    
    if args.summary:
        summary = service.get_sync_summary()
        print("\n同步摘要:")
        print(f"  总文件数: {summary['total_files']}")
        print(f"  总记录数: {summary['total_records']:,}")
        print("\n文件列表 (前20个):")
        for f in summary['files'][:20]:
            print(f"  {f['symbol']}: {f['records']:,} 条 ({f['date_range']})")
        if len(summary['files']) > 20:
            print(f"  ... 还有 {len(summary['files']) - 20} 个文件")
    
    elif args.symbol:
        # 同步单只股票
        result = service.sync_stock(
            args.symbol,
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
    
    elif args.daily:
        # 每日增量更新 - 自动同步所有股票到最新日期
        print("\n" + "="*60)
        print("执行每日增量更新")
        print("="*60)
        
        symbols = None
        if args.limit:
            symbols = service.stock_list['symbol'].tolist()[:args.limit]
            print(f"测试模式: 只同步前 {args.limit} 只股票")
        else:
            print(f"将同步全部 {len(service.stock_list)} 只股票")
        
        # 默认使用增量模式，从已有数据的最新日期开始
        result = service.sync_all(
            symbols=symbols,
            incremental=True,  # 强制增量模式
            skip_existing=args.skip_existing
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总股票: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新增记录: {result['new_records']:,}")
        if result['failed_symbols']:
            print(f"  失败股票: {', '.join(result['failed_symbols'])}")
    
    elif args.all:
        # 同步所有股票（可指定全量或增量）
        symbols = None
        if args.limit:
            symbols = service.stock_list['symbol'].tolist()[:args.limit]
        
        result = service.sync_all(
            symbols=symbols,
            incremental=not args.full,
            skip_existing=args.skip_existing
        )
        print("\n同步结果:")
        print(f"  状态: {result['status']}")
        print(f"  总股票: {result['total_symbols']}")
        print(f"  成功: {result['success']}")
        print(f"  失败: {result['failed']}")
        print(f"  新记录: {result['new_records']:,}")
    
    else:
        parser.print_help()
    
    # 退出时登出baostock
    bs.logout()
    logger.info("baostock已登出")


if __name__ == "__main__":
    main()

"""
ETF历史数据同步服务 - 下载ETF历史日线数据到Parquet

存储位置: storage/raw/etf/price/{symbol}.parquet

用法:
    # 同步所有ETF
    python DataHub/services/etf_sync.py --all
    
    # 同步指定ETF
    python DataHub/services/etf_sync.py --symbol 510300
    
    # 每日增量更新
    python DataHub/services/etf_sync.py --daily
    
    # 首次全量同步（断点续传）
    python DataHub/services/etf_sync.py --all --skip-existing

参数说明:
    --all              同步所有ETF
    --symbol SYMBOL    指定ETF代码，如 510300
    --daily            每日增量更新
    --full             全量更新（覆盖已有数据）
    --skip-existing    跳过已有文件的ETF
    --limit N          限制ETF数量（测试用）
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
import pandas as pd
import akshare as ak
import baostock as bs
from datetime import datetime
from typing import List, Optional
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from DataHub.config import RAW_ETF_PRICE_DIR, CRAWLER_REQUEST_DELAY

logger = logging.getLogger(__name__)

# 全局baostock锁
_baostock_lock = Lock()


class ETFDataSync:
    """ETF数据同步服务"""
    
    def __init__(self):
        """初始化服务"""
        self.raw_price_dir = RAW_ETF_PRICE_DIR
        self.raw_price_dir.mkdir(parents=True, exist_ok=True)
        
        # ETF列表（主要宽基指数ETF）
        self.etf_list = self._load_etf_list()
        
        # baostock登录状态
        self._baostock_logged_in = False
        
    def _login_baostock(self):
        """登录baostock（延迟加载）"""
        if self._baostock_logged_in:
            return
        
        with _baostock_lock:
            if self._baostock_logged_in:
                return
            
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"baostock登录失败: {lg.error_msg}")
            else:
                logger.info("baostock登录成功")
                self._baostock_logged_in = True
    
    def _clean_code(self, code) -> str:
        """清理代码格式，确保只返回纯数字代码"""
        code_str = str(code)
        # 移除 sh/sz 前缀
        code_str = code_str.replace('sh', '').replace('sz', '')
        # 移除等号及以后的内容
        if '=' in code_str:
            code_str = code_str.split('=')[-1]
        return code_str.strip()
    
    def _load_etf_list(self) -> pd.DataFrame:
        """
        加载ETF列表 - 优先从本地CSV读取，失败时从接口获取
        
        优先级: 本地CSV > 新浪接口 > 东财接口 > 默认列表
        """
        # 优先级1: 本地CSV（最可靠）
        csv_path = Path("storage/etf_basic_info.csv")
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                if 'symbol' in df.columns and len(df) > 0:
                    df['代码'] = df['symbol'].apply(lambda x: x.replace('.SH', '').replace('.SZ', ''))
                    logger.info(f"从本地CSV加载ETF列表: {len(df)} 只")
                    return df
            except Exception as e:
                logger.warning(f"本地CSV读取失败: {e}")
        
        # 优先级2: 新浪接口
        try:
            df = ak.fund_etf_category_sina(symbol="ETF基金")
            df['代码'] = df['代码'].apply(self._clean_code)
            df['symbol'] = df['代码'].apply(lambda x: f"{x}.SH" if str(x).startswith('5') else f"{x}.SZ")
            logger.info(f"使用新浪接口获取ETF列表: {len(df)} 只")
            return df
        except Exception as e:
            logger.warning(f"新浪接口失败: {e}")
        
        # 优先级3: 东财接口（最后备选）
        try:
            df = ak.fund_etf_spot_em()
            df['代码'] = df['代码'].apply(self._clean_code)
            df['symbol'] = df['代码'].apply(lambda x: f"{x}.SH" if str(x).startswith('5') else f"{x}.SZ")
            logger.info(f"使用东财接口获取ETF列表: {len(df)} 只")
            return df
        except Exception as e:
            logger.error(f"所有ETF列表接口失败: {e}")
        
        # 返回默认ETF列表
        logger.warning("使用默认ETF列表")
        default_etfs = [
            ('510300', '华泰柏瑞沪深300ETF'),
            ('510050', '华夏上证50ETF'),
            ('510500', '南方中证500ETF'),
            ('588080', '易方达上证科创板50ETF'),
            ('159915', '易方达创业板ETF'),
            ('159949', '华安创业板50ETF'),
            ('512880', '国泰中证全指证券公司ETF'),
            ('515050', '华夏中证5G通信主题ETF'),
            ('512690', '鹏华中证酒ETF'),
            ('512000', '华宝中证医疗ETF'),
        ]
        return pd.DataFrame(default_etfs, columns=['代码', '名称'])
    
    def get_etf_file_path(self, symbol: str) -> Path:
        """获取ETF数据文件路径"""
        return self.raw_price_dir / f"{symbol}.parquet"
    
    def _format_code_for_baostock(self, symbol: str) -> Optional[str]:
        """转换代码格式为baostock格式: 510300.SH -> sh.510300"""
        if '.SH' in symbol:
            return 'sh.' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            return 'sz.' + symbol.replace('.SZ', '')
        return None
    
    def _fetch_from_baostock(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        从baostock获取ETF历史数据（优先级1）
        """
        self._login_baostock()
        
        code = self._format_code_for_baostock(symbol)
        if code is None:
            logger.debug(f"{symbol} 不支持baostock格式")
            return None
        
        try:
            # 转换日期格式
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
            
            with _baostock_lock:
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,volume,amount,pctChg",
                    start_date=start_dt,
                    end_date=end_dt,
                    frequency="d",
                    adjustflag="3"  # 前复权
                )
            
            if rs.error_code != '0':
                logger.warning(f"baostock获取 {symbol} 失败: {rs.error_msg}")
                return None
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"baostock返回 {symbol} 空数据")
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 列名映射
            df = df.rename(columns={
                'date': 'trade_date',
                'pctChg': 'change_pct'
            })
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 过滤无效数据
            df = df[df['volume'] > 0]
            
            logger.info(f"baostock获取 {symbol} 成功: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.warning(f"baostock获取 {symbol} 异常: {e}")
            return None
    
    def _fetch_from_akshare_sina(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        从akshare新浪接口获取ETF历史数据（优先级1 - 数据最完整）
        
        注意: 新浪接口可以获取2012年以来的ETF历史数据，
              而baostock只能获取2026年以来的数据
        """
        # 转换格式: 510300.SH -> sh510300
        if '.SH' in symbol:
            sina_symbol = 'sh' + symbol.replace('.SH', '')
        elif '.SZ' in symbol:
            sina_symbol = 'sz' + symbol.replace('.SZ', '')
        else:
            sina_symbol = symbol
        
        try:
            # 使用新浪接口（非东财）
            df = ak.fund_etf_hist_sina(symbol=sina_symbol)
            
            if df.empty:
                return None
            
            # 日期过滤
            df['date'] = pd.to_datetime(df['date'])
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            
            if df.empty:
                return None
            
            # 列名映射
            df = df.rename(columns={
                'date': 'trade_date',
            })
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 确保change_pct存在
            if 'change_pct' not in df.columns and 'close' in df.columns:
                df['change_pct'] = df['close'].pct_change() * 100
            
            logger.debug(f"akshare新浪接口获取 {symbol} 成功: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.warning(f"akshare新浪接口获取 {symbol} 失败: {e}")
            return None
    
    def _fetch_from_em(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = ""
    ) -> Optional[pd.DataFrame]:
        """
        从东财接口获取ETF历史数据（最后备选）
        """
        code = symbol.replace('.SH', '').replace('.SZ', '')
        
        try:
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if df.empty:
                return None
            
            # 添加symbol列
            df['symbol'] = symbol
            
            # 列名映射
            column_map = {
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'change_pct',
            }
            df = df.rename(columns=column_map)
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            logger.info(f"东财接口获取 {symbol} 成功: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.warning(f"东财接口获取 {symbol} 失败: {e}")
            return None
    
    def fetch_etf_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = ""
    ) -> Optional[pd.DataFrame]:
        """
        获取单只ETF历史数据 - 按优先级尝试多个数据源
        
        优先级: 新浪 > baostock > 东财
        
        注意: baostock对ETF只有2026年以来的数据（约3个月），
              而新浪接口可以获取2012年以来的完整历史数据。
        
        Args:
            symbol: ETF代码，如 '510300.SH'
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            adjust: '' 不复权, 'qfq' 前复权, 'hfq' 后复权
        
        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount, change_pct
        """
        errors = []
        
        # 优先级1: akshare新浪接口（数据最完整，2012年以来）
        try:
            df = self._fetch_from_akshare_sina(symbol, start_date, end_date)
            if df is not None and not df.empty:
                # 验证数据完整性：如果只有几个月数据，可能是baostock级别的短数据
                if len(df) > 100:  # 至少要有100条记录（约5个月）
                    logger.info(f"{symbol} 使用新浪接口数据: {len(df)} 条 ({df['trade_date'].min().date()} ~ {df['trade_date'].max().date()})")
                    return df
                else:
                    logger.debug(f"{symbol} 新浪接口数据太短({len(df)}条)，尝试其他数据源")
        except Exception as e:
            errors.append(f"akshare_sina: {e}")
            logger.debug(f"akshare新浪接口失败: {e}")
        
        # 优先级2: baostock（最稳定，但ETF只有2026年数据）
        try:
            df = self._fetch_from_baostock(symbol, start_date, end_date)
            if df is not None and not df.empty:
                logger.info(f"{symbol} 使用baostock数据: {len(df)} 条 ({df['trade_date'].min().date()} ~ {df['trade_date'].max().date()})")
                return df
        except Exception as e:
            errors.append(f"baostock: {e}")
            logger.debug(f"baostock失败: {e}")
        
        # 优先级3: 东财接口（最后备选）
        try:
            df = self._fetch_from_em(symbol, start_date, end_date, adjust)
            if df is not None and not df.empty:
                logger.info(f"{symbol} 使用东财接口数据: {len(df)} 条")
                return df
        except Exception as e:
            errors.append(f"em: {e}")
        
        # 全部失败
        logger.error(f"获取 {symbol} 历史数据失败，所有数据源都不可用: {errors}")
        return None
    
    def save_etf_data(self, symbol: str, df: pd.DataFrame) -> bool:
        """保存ETF数据到Parquet文件"""
        try:
            file_path = self.get_etf_file_path(symbol)
            df.to_parquet(file_path, index=False, compression='zstd')
            logger.info(f"保存 {symbol} 数据: {len(df)} 条 -> {file_path}")
            return True
        except Exception as e:
            logger.error(f"保存 {symbol} 数据失败: {e}")
            return False
    
    def sync_etf(
        self,
        symbol: str,
        start_date: str = "20100101",
        end_date: Optional[str] = None,
        full_update: bool = False
    ) -> dict:
        """
        同步单只ETF数据
        
        Args:
            symbol: ETF代码
            start_date: 开始日期
            end_date: 结束日期，默认为今天
            full_update: 是否全量更新
        
        Returns:
            同步结果
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        file_path = self.get_etf_file_path(symbol)
        
        # 如果不是全量更新且文件存在，则增量更新
        if not full_update and file_path.exists():
            try:
                existing_df = pd.read_parquet(file_path)
                last_date = existing_df['trade_date'].max()
                start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=5)).strftime('%Y%m%d')
                logger.info(f"{symbol} 增量更新: {start_date} ~ {end_date}")
            except Exception as e:
                logger.warning(f"读取 {symbol} 历史数据失败，转为全量更新: {e}")
                full_update = True
        
        # 获取数据
        new_df = self.fetch_etf_history(symbol, start_date, end_date)
        if new_df is None or new_df.empty:
            return {'status': 'failed', 'symbol': symbol, 'message': '无数据'}
        
        # 合并数据（增量更新）
        if not full_update and file_path.exists():
            try:
                existing_df = pd.read_parquet(file_path)
                # 删除重叠日期的数据
                existing_df = existing_df[existing_df['trade_date'] < new_df['trade_date'].min()]
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception:
                combined_df = new_df
        else:
            combined_df = new_df
        
        # 去重并排序
        combined_df = combined_df.drop_duplicates(subset=['trade_date'], keep='last')
        combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
        
        # 保存
        if self.save_etf_data(symbol, combined_df):
            return {
                'status': 'success',
                'symbol': symbol,
                'records': len(combined_df),
                'start_date': combined_df['trade_date'].min().strftime('%Y-%m-%d'),
                'end_date': combined_df['trade_date'].max().strftime('%Y-%m-%d')
            }
        else:
            return {'status': 'failed', 'symbol': symbol, 'message': '保存失败'}
    
    def sync_all_etfs(
        self,
        start_date: str = "20100101",
        end_date: Optional[str] = None,
        skip_existing: bool = False,
        full_update: bool = False,
        limit: Optional[int] = None,
        workers: int = 1
    ) -> dict:
        """
        同步所有ETF数据
        
        注意: 由于akshare底层使用mini_racer，多线程会导致崩溃，
              默认使用单线程(workers=1)
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 获取ETF列表
        etf_codes = self.etf_list['代码'].tolist()
        
        # 添加后缀
        symbols = []
        for code in etf_codes:
            symbol = f"{code}.SH" if str(code).startswith('5') else f"{code}.SZ"
            symbols.append(symbol)
        
        # 去重
        symbols = list(set(symbols))
        
        # 限制数量
        if limit:
            symbols = symbols[:limit]
            logger.info(f"测试模式: 限制处理 {limit} 只ETF")
        
        # 跳过已有文件
        if skip_existing:
            existing_symbols = [s for s in symbols if self.get_etf_file_path(s).exists()]
            symbols = [s for s in symbols if not self.get_etf_file_path(s).exists()]
            logger.info(f"跳过已有数据的ETF: {len(existing_symbols)} 只")
        
        logger.info(f"需要同步的ETF: {len(symbols)} 只")
        
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        # 串行处理（避免mini_racer多线程崩溃）
        if workers == 1:
            logger.info("使用单线程模式（避免mini_racer崩溃）")
            for i, symbol in enumerate(symbols):
                if i > 0:
                    time.sleep(random.uniform(0.5, 1.0))  # 请求间隔
                result = self.sync_etf(symbol, start_date, end_date, full_update)
                if result['status'] == 'success':
                    results['success'] += 1
                else:
                    results['failed'] += 1
                
                # 每10只报告进度
                if (i + 1) % 10 == 0:
                    logger.info(f"进度: {i + 1}/{len(symbols)} - 成功:{results['success']} 失败:{results['failed']}")
        else:
            logger.warning(f"警告: 使用多线程(workers={workers})可能导致mini_racer崩溃")
            
            def sync_single(symbol: str) -> dict:
                time.sleep(random.uniform(0.5, 1.5))
                return self.sync_etf(symbol, start_date, end_date, full_update)
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_symbol = {executor.submit(sync_single, symbol): symbol for symbol in symbols}
                
                for future in as_completed(future_to_symbol):
                    result = future.result()
                    if result['status'] == 'success':
                        results['success'] += 1
                    else:
                        results['failed'] += 1
        
        logger.info(f"同步完成: 成功 {results['success']}, 失败 {results['failed']}")
        return results
    
    def sync_daily(self) -> dict:
        """每日增量同步"""
        today = datetime.now().strftime('%Y%m%d')
        logger.info(f"启动ETF每日同步: {today}")
        return self.sync_all_etfs(start_date=today, end_date=today)


def save_etf_list_from_api():
    """
    从API获取全市场ETF列表并保存到CSV
    
    这是第一步：建立ETF列表库
    """
    import pandas as pd
    from DataHub.config import STORAGE_DIR
    
    print("📥 从API获取ETF全市场列表...")
    
    errors = []
    df = None
    
    # 优先级1: 新浪接口
    try:
        df = ak.fund_etf_category_sina(symbol="ETF基金")
        df['代码'] = df['代码'].apply(lambda x: str(x).replace('sh', '').replace('sz', '').split('=')[-1])
        df['symbol'] = df['代码'].apply(lambda x: f"{x}.SH" if str(x).startswith('5') else f"{x}.SZ")
        df['exchange'] = df['代码'].apply(lambda x: 'SH' if str(x).startswith('5') else 'SZ')
        # 获取名称列
        if '名称' in df.columns:
            df['name'] = df['名称']
        elif 'name' in df.columns:
            pass
        else:
            df['name'] = ''
        print(f"✅ 使用新浪接口获取到 {len(df)} 只ETF")
    except Exception as e:
        errors.append(f"新浪接口: {e}")
        print(f"⚠️ 新浪接口失败: {e}")
    
    # 优先级2: 东财接口
    if df is None or df.empty:
        try:
            df = ak.fund_etf_spot_em()
            df['代码'] = df['代码'].astype(str)
            df['symbol'] = df['代码'].apply(lambda x: f"{x}.SH" if str(x).startswith('5') else f"{x}.SZ")
            df['exchange'] = df['代码'].apply(lambda x: 'SH' if str(x).startswith('5') else 'SZ')
            df['name'] = df['名称']
            print(f"✅ 使用东财接口获取到 {len(df)} 只ETF")
        except Exception as e:
            errors.append(f"东财接口: {e}")
            print(f"❌ 东财接口也失败: {e}")
    
    if df is None or df.empty:
        print(f"❌ 所有接口都失败: {errors}")
        return
    
    # 标准化列名
    result_df = pd.DataFrame({
        'symbol': df['symbol'],
        'code': df['代码'],
        'name': df.get('name', ''),
        'exchange': df['exchange']
    })
    
    # 去重并排序
    result_df = result_df.drop_duplicates(subset=['symbol'])
    result_df = result_df.sort_values('symbol').reset_index(drop=True)
    
    # 保存到CSV
    csv_path = STORAGE_DIR / "etf_basic_info.csv"
    result_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ 已保存 {len(result_df)} 只ETF到: {csv_path}")
    print(f"\n前10只:")
    print(result_df.head(10).to_string(index=False))
    print(f"\n💡 提示: 现在可以运行 `python DataHub/services/etf_sync.py --all` 同步数据")


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETF数据同步服务')
    parser.add_argument('--symbol', type=str, help='指定ETF，如 510300')
    parser.add_argument('--all', action='store_true', help='同步所有ETF')
    parser.add_argument('--daily', action='store_true', help='每日增量更新')
    parser.add_argument('--full', action='store_true', help='全量更新（覆盖已有数据）')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已有文件的ETF')
    parser.add_argument('--limit', type=int, help='限制ETF数量（测试用）')
    parser.add_argument('--workers', type=int, default=1, help='并发线程数（默认1，避免mini_racer崩溃）')
    parser.add_argument('--save-list', action='store_true', help='保存已同步的ETF列表到CSV')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 保存列表模式
    if args.save_list:
        save_etf_list_from_api()
        return
    
    service = ETFDataSync()
    
    if args.symbol:
        # 转换代码格式
        symbol = args.symbol
        if not symbol.endswith(('.SH', '.SZ')):
            symbol = f"{symbol}.SH" if symbol.startswith('5') else f"{symbol}.SZ"
        
        result = service.sync_etf(symbol, full_update=args.full)
        print(f"\n同步结果: {result}")
        
    elif args.daily:
        result = service.sync_daily()
        print(f"\n同步完成: {result}")
        
    elif args.all:
        result = service.sync_all_etfs(
            skip_existing=args.skip_existing,
            full_update=args.full,
            limit=args.limit,
            workers=args.workers
        )
        print(f"\n同步完成: {result}")
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

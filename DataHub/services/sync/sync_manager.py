"""
同步管理器 - 协调各类同步任务

提供统一的同步调度接口
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

from DataHub.config import RAW_PRICE_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR, STORAGE_DIR
from .stock_sync import StockPriceSync
from .etf_sync import ETFSync
from .index_sync import IndexSync
from .factor_sync import AdjustFactorSync
from .base import get_effective_end_date

logger = logging.getLogger(__name__)


class SyncManager:
    """同步任务管理器"""
    
    def __init__(self, max_workers: int = 1):
        """
        初始化管理器
        
        Args:
            max_workers: 默认并发数
        """
        self.max_workers = max_workers
        self.stock_sync = StockPriceSync(max_workers=max_workers)
        self.etf_sync = ETFSync(max_workers=max_workers)
        self.index_sync = IndexSync(max_workers=max_workers)
        self.factor_sync = AdjustFactorSync(max_workers=max_workers)
        
        # 加载列表
        self._stock_list = None
        self._etf_list = None
        self._index_list = None
    
    def sync_today_data(self, sync_factors: bool = False) -> Dict:
        """
        同步当天数据（极速模式）
        
        使用 akshare.stock_zh_a_spot() 获取全市场当日实时数据，
        并更新到各个股票的 parquet 文件中。
        
        与旧模块 history_sync --today 功能一致。
        
        Args:
            sync_factors: 是否同步复权因子（默认False）
            
        Returns:
            同步结果
        """
        import akshare as ak
        
        today = datetime.now()
        today_str = today.strftime('%Y%m%d')
        today_date = today.date()
        
        logger.info(f"开始同步当天数据: {today_str}")
        
        # 1. 获取当天全市场数据
        logger.info("获取当天全市场数据...")
        try:
            spot_df = ak.stock_zh_a_spot()
            logger.info(f"✓ 获取完成: {len(spot_df)} 只股票")
        except Exception as e:
            logger.error(f"❌ 获取当天数据失败: {e}")
            return {'status': 'failed', 'message': str(e)}
        
        # 2. 数据清洗和转换
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
            if '.SH' in code or '.SZ' in code or '.BJ' in code:
                return code
            if code.startswith('sh'):
                return code[2:] + '.SH'
            elif code.startswith('sz'):
                return code[2:] + '.SZ'
            elif code.startswith('bj'):
                return code[2:] + '.BJ'
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
                # 判断资产类型
                asset_type = "etf" if self._is_etf(symbol) else "stock"
                
                if asset_type == "etf":
                    file_path = RAW_ETF_PRICE_DIR / f"{symbol}.parquet"
                else:
                    file_path = RAW_PRICE_DIR / f"{symbol}.parquet"
                
                # 获取该股票的新数据（只有一条）
                new_df = group.copy()
                
                # 检查价格数据是否有效
                if pd.isna(new_df.iloc[0]['close']):
                    logger.warning(f"⚠️ {symbol} 价格数据无效(NaN)，跳过")
                    skipped += 1
                    continue
                
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
        
        logger.info(f"价格数据同步完成: 更新 {updated}, 失败 {failed}")
        
        # 4. 同步复权因子（可选，默认不同步）
        factor_result = {'updated': 0, 'skipped': 0, 'failed': 0}
        if sync_factors:
            logger.info("同步复权因子...")
            # 获取今天有数据的股票（排除ETF）
            stock_symbols = [s for s in spot_df['symbol'].unique() if not self._is_etf(s)]
            if stock_symbols:
                result = self.factor_sync.sync(stock_symbols)
                factor_result['updated'] = result.get('success', 0)
                factor_result['failed'] = result.get('failed', 0)
        
        return {
            'status': 'success',
            'trade_date': today_str,
            'updated': updated,
            'skipped': skipped,
            'failed': failed,
            'factor_updated': factor_result.get('updated', 0),
            'factor_skipped': factor_result.get('skipped', 0)
        }
    
    def sync_daily(self, asset_type: str = 'stock', start_date: str = None,
                   end_date: str = None, incremental: bool = True,
                   limit: int = None, include_bj: bool = False,
                   skip_existing: bool = False, override: bool = False) -> Dict:
        """
        每日增量更新（与原 --daily 功能一致）

        Args:
            asset_type: 资产类型 - 'stock', 'etf', 'index'
            start_date: 开始日期 'YYYYMMDD'，None表示从最新日期开始
            end_date: 结束日期 'YYYYMMDD'，None表示今天
            incremental: 是否增量更新
            limit: 限制数量（测试用）
            include_bj: 是否包含北交所（仅对股票有效）
            skip_existing: 是否跳过已有文件的股票
            override: 是否覆盖已有数据

        Returns:
            同步结果
        """
        # 注意：start_date 保持 None，由各个 sync_service 根据本地文件自动计算
        if end_date is None:
            end_date = get_effective_end_date()
            now = datetime.now()
            if now.hour < 15:
                logger.info(f"当前未收盘，同步结束日期自动调整为上一个交易日: {end_date}")

        logger.info(f"开始每日增量更新: {asset_type}, 日期: {start_date or '自动'} ~ {end_date}")

        # 获取代码列表
        if asset_type == 'etf':
            symbols = self._get_etf_list()
            sync_service = self.etf_sync
            asset_name = "ETF"
            price_dir = RAW_ETF_PRICE_DIR
        elif asset_type == 'index':
            symbols = self._get_index_list()
            sync_service = self.index_sync
            asset_name = "指数"
            price_dir = RAW_INDEX_PRICE_DIR
        else:  # stock
            symbols = self._get_stock_list(include_bj=include_bj)
            sync_service = self.stock_sync
            asset_name = "股票"
            price_dir = RAW_PRICE_DIR

        # 跳过已有文件
        if skip_existing:
            existing_symbols = {f.stem for f in price_dir.glob("*.parquet")}
            original_count = len(symbols)
            symbols = [s for s in symbols if s not in existing_symbols]
            skipped_count = original_count - len(symbols)
            logger.info(f"跳过已有文件的 {skipped_count} 只，实际需同步 {len(symbols)} 只")

        if limit:
            symbols = symbols[:limit]
            logger.info(f"测试模式: 只同步前 {limit} 只{asset_name}")

        logger.info(f"将同步 {len(symbols)} 只{asset_name}")

        # 执行同步
        result = sync_service.sync(
            symbols,
            incremental=incremental and not override,
            start_date=start_date,
            end_date=end_date,
            override=override
        )

        # 保存待处理列表（如果有）
        if hasattr(sync_service, 'pending_symbols') and sync_service.pending_symbols:
            sync_service.save_pending_list()

        return {
            'status': result['status'],
            'asset_type': asset_type,
            'total': result['total'],
            'success': result['success'],
            'failed': result['failed'],
            'date_range': f"{start_date or 'auto'}~{end_date}"
        }
    
    def sync_etf_daily(self, **kwargs) -> Dict:
        """同步ETF每日数据"""
        return self.sync_daily(asset_type='etf', **kwargs)
    
    def sync_index_daily(self, **kwargs) -> Dict:
        """同步指数每日数据"""
        return self.sync_daily(asset_type='index', **kwargs)
    
    def sync_stock_daily(self, **kwargs) -> Dict:
        """同步股票每日数据"""
        return self.sync_daily(asset_type='stock', **kwargs)
    
    def sync_factors_only(self) -> Dict:
        """
        只同步复权因子（全量覆盖所有有价格数据的股票）
        
        Returns:
            同步结果
        """
        logger.info("开始同步复权因子（全量覆盖模式）...")
        
        # 获取所有有价格数据的股票
        symbols = [f.stem for f in RAW_PRICE_DIR.glob("*.parquet")]
        
        logger.info(f"发现 {len(symbols)} 只股票需要同步复权因子")
        
        result = self.factor_sync.sync(symbols)
        
        return {
            'status': result['status'],
            'total': result['total'],
            'success': result['success'],
            'failed': result['failed'],
            'mode': 'full_overwrite'
        }
    
    def sync_stock_list(self, symbols: List[str], incremental: bool = True, 
                        sync_factor: bool = True) -> Dict:
        """
        同步指定股票列表
        
        Args:
            symbols: 股票代码列表
            incremental: 是否增量更新
            sync_factor: 是否同时同步复权因子
            
        Returns:
            同步结果
        """
        logger.info(f"开始同步 {len(symbols)} 只股票...")
        
        # 1. 同步价格
        price_result = self.stock_sync.sync(symbols, incremental=incremental)
        
        # 2. 同步复权因子（可选）
        factor_result = None
        if sync_factor:
            logger.info("同步复权因子...")
            factor_result = self.factor_sync.sync(symbols)
        
        return {
            'status': 'success',
            'price': price_result,
            'factor': factor_result
        }
    
    def _get_symbols_to_update(self) -> List[str]:
        """获取需要更新的股票列表（从配置文件）"""
        try:
            from DataHub.config import STOCK_LIST
            return STOCK_LIST
        except:
            # 如果没有配置，从已有文件推断
            return [f.stem for f in RAW_PRICE_DIR.glob("*.parquet")]
    
    def _get_updated_symbols(self, date: str) -> List[str]:
        """获取指定日期有价格数据的股票"""
        import pandas as pd
        
        symbols = []
        for f in RAW_PRICE_DIR.glob("*.parquet"):
            try:
                df = pd.read_parquet(f)
                if not df.empty and 'trade_date' in df.columns:
                    latest_date = pd.to_datetime(df['trade_date'].iloc[-1]).strftime('%Y%m%d')
                    if latest_date == date:
                        symbols.append(f.stem)
            except:
                pass
        
        return symbols
    
    def _is_etf(self, symbol: str) -> bool:
        """判断是否为ETF"""
        try:
            from DataHub.core.data_reader import is_etf
            return is_etf(symbol)
        except Exception:
            # 回退到前缀规则
            if symbol.startswith(('510', '511', '512', '513', '515', '516', '517', '518', '560', '561', '563', '564', '565', '568', '569', '580', '581', '582', '583', '588')) and symbol.endswith('.SH'):
                return True
            if symbol.startswith(('15', '16', '18')) and symbol.endswith('.SZ'):
                return True
            return False
    
    def _get_stock_list(self, include_bj: bool = False) -> List[str]:
        """获取股票列表"""
        if self._stock_list is None:
            stock_csv = STORAGE_DIR / "stock_basic_info.csv"
            if stock_csv.exists():
                df = pd.read_csv(stock_csv)
                self._stock_list = df['symbol'].tolist()
            else:
                # 从已有文件推断
                self._stock_list = [f.stem for f in RAW_PRICE_DIR.glob("*.parquet")]
        
        if not include_bj:
            return [s for s in self._stock_list if not s.endswith('.BJ')]
        return self._stock_list
    
    def _get_etf_list(self) -> List[str]:
        """获取ETF列表"""
        if self._etf_list is None:
            etf_csv = STORAGE_DIR / "etf_basic_info.csv"
            if etf_csv.exists():
                df = pd.read_csv(etf_csv)
                self._etf_list = df['symbol'].tolist()
            else:
                # 从已有文件推断
                self._etf_list = [f.stem for f in RAW_ETF_PRICE_DIR.glob("*.parquet")]
        return self._etf_list
    
    def _get_index_list(self) -> List[str]:
        """获取指数列表"""
        if self._index_list is None:
            index_csv = STORAGE_DIR / "official_indices.csv"
            if index_csv.exists():
                df = pd.read_csv(index_csv)
                self._index_list = df['symbol'].tolist()
            else:
                # 默认常用指数 + 从已有文件推断
                default_indices = [
                    '000001.SH',  # 上证指数
                    '000002.SH',  # 上证A指
                    '000003.SH',  # 上证B指
                    '000016.SH',  # 上证50
                    '000300.SH',  # 沪深300
                    '000688.SH',  # 科创50
                    '000905.SH',  # 中证500
                    '000852.SH',  # 中证1000
                    '399002.SZ',  # 深证A指
                    '399003.SZ',  # 深证B指
                    '399006.SZ',  # 创业板指
                    '399300.SZ',  # 沪深300(深圳)
                    '399673.SZ',  # 创业板50
                ]
                existing = [f.stem for f in RAW_INDEX_PRICE_DIR.glob("*.parquet")]
                self._index_list = list(set(default_indices + existing))
        return self._index_list
    
    def get_sync_summary(self) -> Dict:
        """获取同步摘要"""
        from pathlib import Path
        files = sorted(RAW_PRICE_DIR.glob("*.parquet"))
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

    def get_latest_date_distribution(self) -> Dict:
        """获取所有股票的最新日期分布统计"""
        from collections import Counter
        from pathlib import Path

        files = sorted(RAW_PRICE_DIR.glob("*.parquet"))
        latest_dates = []
        stock_latest = {}

        print(f"正在扫描 {len(files)} 只股票...")

        for f in files:
            try:
                df = pd.read_parquet(f)
                if df.empty or 'trade_date' not in df.columns:
                    continue
                latest_date = df['trade_date'].max()
                symbol = f.stem
                if hasattr(latest_date, 'date'):
                    latest_date = latest_date.date()
                elif isinstance(latest_date, str):
                    latest_date = pd.to_datetime(latest_date).date()
                latest_dates.append(latest_date)
                stock_latest[symbol] = latest_date
            except Exception as e:
                logger.warning(f"读取文件失败 {f}: {e}")

        if not latest_dates:
            return {
                'total_stocks': 0,
                'latest_overall': None,
                'distribution': {},
                'outdated_stocks': []
            }

        latest_overall = max(latest_dates)
        date_counter = Counter(latest_dates)
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

    def sync_index_intraday(self) -> Dict:
        """同步指数分时数据（1分钟线）"""
        from DataHub.core.data_client import UnifiedDataClient
        from DataHub.core.data_reader import save_index_intraday
        from DataHub.config import RAW_INDEX_INTRADAY_DIR

        core_indices = {
            '000001.SH': '上证指数',
            '000300.SH': '沪深300',
            '000016.SH': '上证50',
            '000688.SH': '科创50',
            '399006.SZ': '创业板指',
            '399296.SZ': '创成长',
        }

        client = UnifiedDataClient()
        today_str = datetime.now().strftime('%Y%m%d')
        success_count = 0
        failed_count = 0

        for symbol, name in core_indices.items():
            try:
                print(f"  获取 {name}({symbol}) 分时数据...", end=" ")
                df = client.get_index_intraday(symbol)
                if df is not None and not df.empty:
                    save_index_intraday(df, symbol, today_str)
                    print(f"✓ {len(df)} 条")
                    success_count += 1
                else:
                    print("✗ 无数据")
                    failed_count += 1
            except Exception as e:
                print(f"✗ 失败: {e}")
                failed_count += 1

        return {
            'status': 'success',
            'success': success_count,
            'failed': failed_count
        }

    def close(self):
        """清理资源"""
        self.stock_sync.logout()
        self.etf_sync.logout()
        self.index_sync.logout()
        self.factor_sync.logout()

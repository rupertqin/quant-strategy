"""
实时数据服务 - 盘中行情获取与管理

负责获取、存储和提供实时行情数据
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
import json

logger = logging.getLogger(__name__)

# 实时数据默认保存目录 (原始数据放入 storage/raw)
REALTIME_OUTPUT_DIR = Path(project_root) / "storage" / "raw" / "realtime"


class RealtimeDataService:
    """
    实时数据服务
    
    负责盘中行情的获取、存储和查询
    """
    
    def __init__(self, output_dir: Path = None):
        """
        初始化实时数据服务
        
        Args:
            output_dir: 实时数据输出目录
        """
        self.output_dir = output_dir or REALTIME_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def fetch_realtime_data(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取盘中实时行情数据（使用akshare sina源）
        
        Args:
            symbols: 股票代码列表，如 ['600519.SH', '300750.SZ']，None表示全市场
            
        Returns:
            DataFrame with real-time data
        """
        import akshare as ak
        
        self.logger.info("获取实时行情数据...")
        
        # 获取全市场实时行情
        df = ak.stock_zh_a_spot()
        
        # 转换代码格式: sh600000 -> 600000.SH
        df['symbol'] = df['代码'].apply(lambda x:
            x[2:] + '.SH' if x.startswith('sh') else
            x[2:] + '.SZ' if x.startswith('sz') else
            x[2:] + '.BJ' if x.startswith('bj') else x
        )
        
        # 筛选指定股票
        if symbols:
            df = df[df['symbol'].isin(symbols)]
        
        # 重命名列
        column_map = {
            '最新价': 'close',
            '今开': 'open',
            '最高价': 'high',
            '最低价': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_pct',
            '名称': 'name'
        }
        
        available_cols = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=available_cols)
        df['trade_date'] = datetime.now().date()
        
        # 确保必要列存在
        required_cols = ['symbol', 'name', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        self.logger.info(f"获取到 {len(df)} 只股票实时数据")
        return df
    
    def save_realtime_data(self, df: pd.DataFrame) -> str:
        """
        保存实时数据到JSON文件
        
        Args:
            df: 实时数据DataFrame
            
        Returns:
            保存的文件路径
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成文件名: realtime_YYYYMMDD_HHMMSS.json
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = self.output_dir / f"realtime_{timestamp}.json"
        
        # 转换为字典列表
        records = df.to_dict('records')
        
        # 处理日期类型
        for record in records:
            if 'trade_date' in record and hasattr(record['trade_date'], 'isoformat'):
                record['trade_date'] = record['trade_date'].isoformat()
        
        data = {
            'fetch_time': timestamp,
            'record_count': len(records),
            'data': records
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"实时数据已保存: {filepath}")
        return str(filepath)
    
    def fetch_and_save(self, symbols: List[str] = None) -> str:
        """
        获取并保存实时数据（一键操作）
        
        Args:
            symbols: 指定股票列表，None表示全市场
            
        Returns:
            保存的文件路径
        """
        df = self.fetch_realtime_data(symbols)
        return self.save_realtime_data(df)
    
    def load_realtime_data(self, filepath: str = None) -> pd.DataFrame:
        """
        从JSON文件加载实时数据
        
        Args:
            filepath: 文件路径，None则自动查找最新的实时数据文件
            
        Returns:
            DataFrame with real-time data
        """
        if filepath is None:
            filepath = self.find_latest_file()
            if filepath is None:
                raise FileNotFoundError("未找到实时数据文件")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data['data'])
        
        # 转换日期字符串回date对象
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        return df
    
    def find_latest_file(self) -> Optional[str]:
        """
        查找最新的实时数据文件
        
        Returns:
            最新文件路径，如果没有则返回None
        """
        if not self.output_dir.exists():
            return None
        
        files = list(self.output_dir.glob("realtime_*.json"))
        if not files:
            return None
        
        latest = max(files, key=lambda p: p.stat().st_mtime)
        return str(latest)
    
    def find_todays_latest_file(self) -> Optional[str]:
        """
        查找当天最新的实时数据文件
        
        优先级：盘后数据(15:00后) > 盘中数据
        判断依据：文件内容中的 fetch_time
        
        Returns:
            当天最新文件路径，如果没有则返回None
        """
        if not self.output_dir.exists():
            return None
        
        today_str = datetime.now().strftime('%Y%m%d')
        files = list(self.output_dir.glob(f"realtime_{today_str}_*.json"))
        
        if not files:
            return None
        
        # 从文件内容读取 fetch_time，分离盘后和盘中数据
        post_market_files = []
        intraday_files = []
        
        for f in files:
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    fetch_time = data.get('fetch_time', '')
                    if fetch_time and len(fetch_time) >= 15:
                        # fetch_time 格式: YYYYMMDD_HHMMSS
                        hour = int(fetch_time[9:11])
                        if hour >= 15:
                            post_market_files.append((f, fetch_time))
                        else:
                            intraday_files.append((f, fetch_time))
                    else:
                        intraday_files.append((f, fetch_time))
            except Exception:
                # 读取失败视为盘中数据
                intraday_files.append((f, ''))
        
        # 优先级：盘后 > 盘中，各自取 fetch_time 最新的
        if post_market_files:
            # 盘后数据取 fetch_time 最新的（过滤掉空的）
            valid_files = [(f, t) for f, t in post_market_files if t]
            if valid_files:
                latest = max(valid_files, key=lambda x: x[1])
                return str(latest[0])
            else:
                # 如果没有有效的 fetch_time，返回第一个
                return str(post_market_files[0][0])
        elif intraday_files:
            # 盘中数据取 fetch_time 最新的（过滤掉空的）
            valid_files = [(f, t) for f, t in intraday_files if t]
            if valid_files:
                latest = max(valid_files, key=lambda x: x[1])
                return str(latest[0])
            else:
                # 如果没有有效的 fetch_time，返回第一个
                return str(intraday_files[0][0])
        
        return None
    
    def get_todays_realtime_data(self, auto_fetch: bool = True) -> Optional[pd.DataFrame]:
        """
        获取当天的实时数据
        
        如果当天已有数据，直接返回；如果没有且auto_fetch=True，则自动获取
        
        Args:
            auto_fetch: 如果没有当天数据，是否自动获取
            
        Returns:
            实时数据DataFrame，如果没有则返回None
        """
        # 先尝试加载当天已有数据
        todays_file = self.find_todays_latest_file()
        
        if todays_file:
            self.logger.info(f"使用当天已有数据: {Path(todays_file).name}")
            return self.load_realtime_data(todays_file)
        
        # 没有当天数据，自动获取
        if auto_fetch:
            self.logger.info("当天无数据，自动获取实时行情...")
            try:
                filepath = self.fetch_and_save()
                return self.load_realtime_data(filepath)
            except Exception as e:
                self.logger.error(f"自动获取实时数据失败: {e}")
                return None
        
        return None
    
    def has_post_market_data(self) -> bool:
        """
        检查是否已有盘后数据（收盘数据）
        
        判断逻辑：检查实时数据文件是否在15:00之后获取
        
        Returns:
            True表示有盘后数据，False表示只有盘中数据
        """
        filepath = self.find_todays_latest_file()
        if not filepath:
            return False
        
        fname = Path(filepath).name
        try:
            time_part = fname.replace('realtime_', '').replace('.json', '')
            if '_' in time_part:
                _, hhmmss = time_part.split('_')
                hour = int(hhmmss[:2])
                return hour >= 15
        except Exception:
            pass
        
        return False
    
    def merge_realtime_to_history(self, hist_df: pd.DataFrame, realtime: pd.Series) -> pd.DataFrame:
        """
        将实时数据合并到历史K线（内存中）
        
        Args:
            hist_df: 历史日线数据
            realtime: 实时行情Series
            
        Returns:
            合并后的DataFrame
        """
        # 确保 trade_date 列是 datetime 类型
        hist_df['trade_date'] = pd.to_datetime(hist_df['trade_date'])
        
        today = datetime.now()
        today_date = today.date()
        
        # 获取最后一天日期
        last_date = hist_df['trade_date'].iloc[-1]
        if isinstance(last_date, pd.Timestamp):
            last_date = last_date.date()
        
        # 如果历史数据已有今天数据，更新它；否则追加新行
        if not hist_df.empty and last_date == today_date:
            idx = hist_df.index[-1]
            hist_df.loc[idx, 'close'] = float(realtime['close'])
            hist_df.loc[idx, 'high'] = max(float(hist_df.loc[idx, 'high']), float(realtime.get('high', realtime['close'])))
            hist_df.loc[idx, 'low'] = min(float(hist_df.loc[idx, 'low']), float(realtime.get('low', realtime['close'])))
            hist_df.loc[idx, 'volume'] = float(realtime['volume'])
            hist_df.loc[idx, 'amount'] = float(realtime.get('amount', 0))
            hist_df.loc[idx, 'change_pct'] = float(realtime['change_pct'])
        else:
            new_row = pd.DataFrame([{
                'trade_date': today,
                'open': float(realtime.get('open', realtime['close'])),
                'high': float(realtime.get('high', realtime['close'])),
                'low': float(realtime.get('low', realtime['close'])),
                'close': float(realtime['close']),
                'volume': float(realtime['volume']),
                'amount': float(realtime.get('amount', 0)),
                'change_pct': float(realtime['change_pct']),
                'symbol': realtime.get('symbol', '')
            }])
            hist_df = pd.concat([hist_df, new_row], ignore_index=True)
        
        return hist_df


# 便捷函数 - 供其他模块快速使用
def get_realtime_service() -> RealtimeDataService:
    """获取实时数据服务实例"""
    return RealtimeDataService()


def get_todays_realtime_data(auto_fetch: bool = True) -> Optional[pd.DataFrame]:
    """
    便捷函数：获取当天实时数据
    
    Args:
        auto_fetch: 如果没有当天数据，是否自动获取
        
    Returns:
        实时数据DataFrame，如果没有则返回None
    """
    service = RealtimeDataService()
    return service.get_todays_realtime_data(auto_fetch)


if __name__ == "__main__":
    """直接运行此脚本时，获取并保存实时数据"""
    import logging
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("📡 DataHub 实时数据获取")
    print("=" * 60)
    
    service = RealtimeDataService()
    
    try:
        # 获取并保存实时数据
        filepath = service.fetch_and_save()
        print(f"\n✅ 实时数据已保存: {filepath}")
        
        # 显示数据统计
        df = service.load_realtime_data(filepath)
        print(f"\n📊 数据统计:")
        print(f"   股票数量: {len(df)}")
        print(f"   数据列: {list(df.columns)}")
        
        # 显示前5条
        print(f"\n📈 前5只股票:")
        print(df[['symbol', 'name', 'close', 'change_pct']].head().to_string(index=False))
        
    except Exception as e:
        print(f"\n❌ 获取失败: {e}")
        import traceback
        traceback.print_exc()

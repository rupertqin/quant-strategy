#!/usr/bin/env python3
"""
基于历史价格的信号回测

不依赖预先生成的信号文件，而是直接从历史价格数据计算信号

用法:
    python ShortTerm/backtest/historical_signal_backtest.py --start 20250101 --end 20260422
    python ShortTerm/backtest/historical_signal_backtest.py --start 20250101 --end 20260422 --top_n 100
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import argparse
from tqdm import tqdm

from DataHub.core.data_reader import load_stock_prices

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SignalCalculator:
    """从价格数据计算交易信号"""
    
    @staticmethod
    def calculate_ma(prices: pd.Series, window: int) -> pd.Series:
        """计算移动平均线"""
        return prices.rolling(window=window).mean()
    
    @staticmethod
    def calculate_macd(prices: pd.Series, fast=12, slow=26, signal=9) -> tuple:
        """计算MACD"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal).mean()
        macd = (dif - dea) * 2
        return dif, dea, macd
    
    @staticmethod
    def calculate_kdj(high: pd.Series, low: pd.Series, close: pd.Series, n=9) -> tuple:
        """计算KDJ"""
        lowest_low = low.rolling(window=n).min()
        highest_high = high.rolling(window=n).max()
        rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
        k = rsv.ewm(com=2).mean()
        d = k.ewm(com=2).mean()
        j = 3 * k - 2 * d
        return k, d, j
    
    @staticmethod
    def detect_signals(df: pd.DataFrame) -> List[Dict]:
        """
        从价格数据检测交易信号
        
        Returns:
            List of signal dicts
        """
        if len(df) < 60:
            return []
        
        signals = []
        
        # 计算指标
        df = df.copy()
        df['ma5'] = SignalCalculator.calculate_ma(df['close'], 5)
        df['ma10'] = SignalCalculator.calculate_ma(df['close'], 10)
        df['ma20'] = SignalCalculator.calculate_ma(df['close'], 20)
        df['ma60'] = SignalCalculator.calculate_ma(df['close'], 60)
        
        df['macd_dif'], df['macd_dea'], df['macd'] = SignalCalculator.calculate_macd(df['close'])
        df['kdj_k'], df['kdj_d'], df['kdj_j'] = SignalCalculator.kdj(df['high'], df['low'], df['close'])
        
        # 计算成交量均线
        if 'volume' in df.columns:
            df['volume_ma20'] = df['volume'].rolling(20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma20']
        
        # 获取最新数据
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        # 信号1: 均线多头排列
        if latest['ma5'] > latest['ma10'] > latest['ma20']:
            signals.append({
                'signal_type': 'right',
                'signal_name': '均线多头排列',
                'strength': 'strong' if latest['close'] > latest['ma5'] else 'medium',
                'score': 75 if latest['close'] > latest['ma5'] else 65,
                'description': '均线呈多头排列，趋势良好'
            })
        
        # 信号2: MACD金叉
        if latest['macd_dif'] > latest['macd_dea'] and prev['macd_dif'] <= prev['macd_dea']:
            signals.append({
                'signal_type': 'right',
                'signal_name': 'MACD金叉',
                'strength': 'medium',
                'score': 70,
                'description': 'MACD金叉，动能转强'
            })
        
        # 信号3: MACD底背离
        price_low = df['close'].tail(20).min()
        macd_low = df['macd'].tail(20).min()
        if latest['close'] <= price_low * 1.02 and latest['macd'] > macd_low * 1.1:
            signals.append({
                'signal_type': 'left',
                'signal_name': 'MACD底背离',
                'strength': 'medium',
                'score': 68,
                'description': '价格创新低但MACD未创新低，可能反弹'
            })
        
        # 信号4: KDJ底背离
        k_low = df['kdj_k'].tail(20).min()
        if latest['close'] <= price_low * 1.02 and latest['kdj_k'] > k_low * 1.1:
            signals.append({
                'signal_type': 'left',
                'signal_name': 'KDJ底背离',
                'strength': 'medium',
                'score': 65,
                'description': '价格创新低但KDJ未创新低，超卖反弹'
            })
        
        # 信号5: 放量上涨
        if 'volume_ratio' in df.columns and latest['volume_ratio'] > 1.5:
            price_change = (latest['close'] - prev['close']) / prev['close'] * 100
            if price_change > 2:
                signals.append({
                    'signal_type': 'right',
                    'signal_name': '放量上涨',
                    'strength': 'strong',
                    'score': 80,
                    'description': f'放量上涨，量比{latest["volume_ratio"]:.1f}，涨幅{price_change:.1f}%'
                })
        
        # 信号6: 突破MA20
        if latest['close'] > latest['ma20'] and prev['close'] <= prev['ma20']:
            signals.append({
                'signal_type': 'right',
                'signal_name': '突破MA20',
                'strength': 'medium',
                'score': 72,
                'description': '价格突破20日均线'
            })
        
        # 添加技术指标到每个信号
        for signal in signals:
            signal['technicals'] = {
                'ma5': round(latest['ma5'], 2),
                'ma10': round(latest['ma10'], 2),
                'ma20': round(latest['ma20'], 2),
                'ma60': round(latest['ma60'], 2),
                'macd_dif': round(latest['macd_dif'], 3),
                'macd_dea': round(latest['macd_dea'], 3),
                'kdj_k': round(latest['kdj_k'], 2),
                'kdj_d': round(latest['kdj_d'], 2),
                'kdj_j': round(latest['kdj_j'], 2),
            }
            if 'volume_ratio' in df.columns:
                signal['technicals']['volume_ratio'] = round(latest['volume_ratio'], 2)
        
        return signals


class HistoricalSignalBacktest:
    """基于历史价格的信号回测"""
    
    def __init__(self, top_n: int = 100):
        self.top_n = top_n
        self.all_stocks = self._get_all_stock_codes()
    
    def _get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码列表"""
        from DataHub.config import get_storage_path
        price_dir = get_storage_path("raw", "stocks", "price")
        codes = []
        for f in price_dir.glob("*.parquet"):
            # 从文件名提取代码 000001.SZ.parquet -> 000001.SZ
            code = f.stem
            codes.append(code)
        return sorted(codes)
        
    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        dates = []
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        return dates
    
    def scan_date(self, date_str: str, limit: int = None) -> List[Dict]:
        """
        扫描指定日期的所有股票信号
        
        Args:
            date_str: 日期 (YYYYMMDD)
            limit: 限制扫描股票数量（测试用）
        """
        signals = []
        stocks_to_scan = self.all_stocks[:limit] if limit else self.all_stocks
        
        logger.info(f"扫描 {date_str} 的信号，共 {len(stocks_to_scan)} 只股票")
        
        for symbol in tqdm(stocks_to_scan, desc=f"扫描 {date_str}"):
            try:
                # 加载股票历史价格
                df = load_stock_prices(symbol, adjust='qfq')
                if df is None or len(df) < 60:
                    continue
                
                # 转换日期
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 找到指定日期的数据
                target_date = pd.to_datetime(date_str)
                mask = df['trade_date'] <= target_date
                
                if not mask.any():
                    continue
                
                # 取到目标日期为止的数据
                hist_df = df[mask].copy()
                
                if len(hist_df) < 60:
                    continue
                
                # 检查最新日期是否是目标日期（确保当天有交易）
                latest_date = hist_df['trade_date'].iloc[-1]
                if latest_date.strftime('%Y%m%d') != date_str:
                    continue
                
                # 计算信号
                stock_signals = SignalCalculator.detect_signals(hist_df)
                
                if stock_signals:
                    latest = hist_df.iloc[-1]
                    
                    for signal in stock_signals:
                        signals.append({
                            'symbol': symbol,
                            'name': symbol,  # 可以从basic_info获取名称
                            'date': date_str,
                            'close_price': round(latest['close'], 2),
                            'change_pct': round((latest['close'] - hist_df.iloc[-2]['close']) / hist_df.iloc[-2]['close'] * 100, 2) if len(hist_df) > 1 else 0,
                            **signal
                        })
                        
            except Exception as e:
                logger.debug(f"扫描 {symbol} 失败: {e}")
                continue
        
        logger.info(f"{date_str}: 发现 {len(signals)} 个信号")
        return signals
    
    def run_backtest(self, start_date: str, end_date: str, 
                     holding_periods: List[int] = None,
                     limit: int = None) -> Dict:
        """
        运行回测
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            holding_periods: 持有期列表
            limit: 每期限扫描股票数量（None=全部）
        """
        if holding_periods is None:
            holding_periods = [1, 3, 5, 15, 30]
        
        dates = self.get_trading_dates(start_date, end_date)
        logger.info(f"回测期间: {dates[0]} 至 {dates[-1]}, 共 {len(dates)} 个交易日")
        
        # 存储每日信号
        daily_signals = {}
        
        for date_str in dates:
            signals = self.scan_date(date_str, limit=limit)
            daily_signals[date_str] = signals
        
        # 按持有期回测
        results = {}
        
        for period in holding_periods:
            logger.info(f"\n回测持有 {period} 天策略...")
            
            trades = []
            
            for date_str in dates:
                signals = daily_signals.get(date_str, [])
                if not signals:
                    continue
                
                # 选出TopN
                top_signals = sorted(signals, key=lambda x: x.get('score', 0), reverse=True)[:self.top_n]
                
                for signal in top_signals:
                    symbol = signal['symbol']
                    entry_price = signal['close_price']
                    
                    # 计算未来收益
                    try:
                        df = load_stock_prices(symbol, adjust='qfq')
                        if df is None:
                            continue
                        
                        df['trade_date'] = pd.to_datetime(df['trade_date'])
                        entry_date = pd.to_datetime(date_str)
                        
                        # 找到入场日位置
                        entry_mask = df['trade_date'] >= entry_date
                        if not entry_mask.any():
                            continue
                        
                        entry_idx = df[entry_mask].index[0]
                        exit_idx = entry_idx + period
                        
                        if exit_idx >= len(df):
                            continue
                        
                        exit_price = df.iloc[exit_idx]['close']
                        exit_date = df.iloc[exit_idx]['trade_date'].strftime('%Y%m%d')
                        
                        return_pct = (exit_price - entry_price) / entry_price * 100
                        
                        trades.append({
                            'symbol': symbol,
                            'entry_date': date_str,
                            'exit_date': exit_date,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'score': signal.get('score', 0),
                            'return_pct': return_pct,
                            'win': return_pct > 0,
                            'signal_name': signal.get('signal_name', '')
                        })
                        
                    except Exception as e:
                        logger.debug(f"计算收益失败 {symbol}: {e}")
                        continue
            
            if trades:
                df_trades = pd.DataFrame(trades)
                
                # 计算统计指标
                daily_returns = df_trades.groupby('entry_date')['return_pct'].mean()
                
                results[period] = {
                    'total_trades': len(df_trades),
                    'win_rate': df_trades['win'].mean() * 100,
                    'avg_return': df_trades['return_pct'].mean(),
                    'median_return': df_trades['return_pct'].median(),
                    'std_return': df_trades['return_pct'].std(),
                    'max_return': df_trades['return_pct'].max(),
                    'min_return': df_trades['return_pct'].min(),
                    'cumulative_return': (np.prod(1 + daily_returns / 100) - 1) * 100,
                    'daily_returns': daily_returns,
                    'trades': df_trades
                }
        
        return results
    
    def generate_report(self, results: Dict) -> str:
        """生成回测报告"""
        lines = []
        lines.append("="*80)
        lines.append("基于历史价格的信号回测报告")
        lines.append("="*80)
        lines.append(f"选股数量: Top{self.top_n}")
        lines.append("")
        
        # 各持有期表现
        lines.append("-"*80)
        lines.append("各持有期表现对比")
        lines.append("-"*80)
        
        headers = ['指标'] + [f'{p}天' for p in results.keys()]
        lines.append(f"{headers[0]:<20} " + " ".join([f"{h:>12}" for h in headers[1:]]))
        lines.append("-"*80)
        
        metrics = [
            ('总交易次数', 'total_trades', '{:.0f}'),
            ('胜率(%)', 'win_rate', '{:.2f}'),
            ('平均收益(%)', 'avg_return', '{:.2f}'),
            ('中位数收益(%)', 'median_return', '{:.2f}'),
            ('收益标准差', 'std_return', '{:.2f}'),
            ('最大收益(%)', 'max_return', '{:.2f}'),
            ('最小收益(%)', 'min_return', '{:.2f}'),
            ('累计收益率(%)', 'cumulative_return', '{:.2f}'),
        ]
        
        for name, key, fmt in metrics:
            values = [fmt.format(results[p][key]) for p in results.keys()]
            lines.append(f"{name:<20} " + " ".join([f"{v:>12}" for v in values]))
        
        lines.append("-"*80)
        
        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='基于历史价格的信号回测',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 测试模式（只扫描100只股票）
  python ShortTerm/backtest/historical_signal_backtest.py --start 20260413 --end 20260422 --limit 100
  
  # 完整回测（扫描全部股票，较慢）
  python ShortTerm/backtest/historical_signal_backtest.py --start 20260101 --end 20260422
  
  # 指定持有期
  python ShortTerm/backtest/historical_signal_backtest.py --start 20260101 --end 20260422 --holdings 1,3,5,15,30
        """
    )
    
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end', type=str, required=True, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--top_n', type=int, default=100, help='选股数量 (默认100)')
    parser.add_argument('--holdings', type=str, default='1,3,5', help='持有期天数 (默认: 1,3,5)')
    parser.add_argument('--limit', type=int, help='每期限扫描股票数量（测试用）')
    parser.add_argument('--export', action='store_true', help='导出交易记录')
    
    args = parser.parse_args()
    
    holding_periods = [int(x.strip()) for x in args.holdings.split(',')]
    
    # 创建回测器
    backtest = HistoricalSignalBacktest(top_n=args.top_n)
    
    # 运行回测
    results = backtest.run_backtest(
        start_date=args.start,
        end_date=args.end,
        holding_periods=holding_periods,
        limit=args.limit
    )
    
    if results:
        print(backtest.generate_report(results))
        
        # 导出交易记录
        if args.export:
            from DataHub.config import get_storage_path
            output_dir = get_storage_path("outputs", "backtest")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for period, data in results.items():
                if 'trades' in data:
                    filepath = output_dir / f"historical_top{args.top_n}_hold{period}d.csv"
                    data['trades'].to_csv(filepath, index=False, encoding='utf-8-sig')
                    logger.info(f"交易记录已导出: {filepath}")
    else:
        print("回测失败")


if __name__ == "__main__":
    main()

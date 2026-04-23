"""
TopN信号分股票多持有期回测

策略逻辑:
    1. 每天选出信号分最高的前N只股票（等权重买入）
    2. 分别测试持有1天、3天、5天、15天、30天、90天的收益率
    3. 计算累计收益和胜率

用法:
    python ShortTerm/backtest/top10_holdings_backtest.py
    python ShortTerm/backtest/top10_holdings_backtest.py --start 20250101 --end 20260422
    python ShortTerm/backtest/top10_holdings_backtest.py --top_n 100 --holdings 1,3,5,15,30,90
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import logging
import argparse

from DataHub.core.data_reader import load_stock_prices

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """交易记录"""
    symbol: str
    name: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    score: int
    holding_days: int
    return_pct: float
    win: bool


@dataclass
class DailyPortfolio:
    """每日持仓"""
    date: str
    top_stocks: List[Dict]  # 选中的股票列表
    holdings: Dict[int, List[TradeRecord]] = field(default_factory=dict)  # 不同持有期的持仓


class TopNHoldingsBacktest:
    """TopN信号分股票多持有期回测"""
    
    def __init__(self, signals_dir: Path = None, top_n: int = 100):
        self.signals_dir = signals_dir or project_root / "storage" / "outputs" / "signals"
        self.top_n = top_n
        self.daily_results: List[DailyPortfolio] = []
        self.all_trades: List[TradeRecord] = []
        
    def get_available_dates(self, start_date: str = None, end_date: str = None) -> List[str]:
        """获取可用的信号日期列表"""
        dates = []
        
        for file in self.signals_dir.glob("stock_signals_*.json"):
            # 排除 intraday 和 latest 文件
            if "intraday" in file.name or "latest" in file.name or "all" in file.name:
                continue
            
            # 提取日期
            try:
                date_str = file.stem.replace("stock_signals_", "")
                if len(date_str) == 8 and date_str.isdigit():
                    if start_date and date_str < start_date:
                        continue
                    if end_date and date_str > end_date:
                        continue
                    dates.append(date_str)
            except:
                continue
        
        return sorted(dates)
    
    def load_signals(self, date_str: str) -> List[Dict]:
        """加载指定日期的信号数据"""
        filepath = self.signals_dir / f"stock_signals_{date_str}.json"
        
        if not filepath.exists():
            logger.warning(f"信号文件不存在: {filepath}")
            return []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('signals', [])
        except Exception as e:
            logger.error(f"加载信号文件失败 {filepath}: {e}")
            return []
    
    def select_top_stocks(self, signals: List[Dict]) -> List[Dict]:
        """选出信号分最高的前N只股票"""
        if not signals:
            return []
        
        # 按股票代码分组，取每只股票的信号分（同一股票可能有多个信号）
        stock_scores = defaultdict(list)
        for signal in signals:
            symbol = signal.get('symbol', '')
            if symbol:
                stock_scores[symbol].append(signal)
        
        # 对每只股票，取最高信号分
        top_stocks = []
        for symbol, sig_list in stock_scores.items():
            # 取该股票信号分最高的信号
            best_signal = max(sig_list, key=lambda x: x.get('score', 0))
            top_stocks.append({
                'symbol': symbol,
                'name': best_signal.get('name', ''),
                'score': best_signal.get('score', 0),
                'close_price': best_signal.get('close_price', 0),
                'signal_type': best_signal.get('signal_type', ''),
                'signal_name': best_signal.get('signal_name', ''),
            })
        
        # 按信号分排序，取前N
        top_stocks.sort(key=lambda x: x['score'], reverse=True)
        return top_stocks[:self.top_n]
    
    def calculate_returns(self, symbol: str, entry_date: str, holding_days: int) -> Optional[Dict]:
        """计算指定持有期的收益"""
        try:
            df = load_stock_prices(symbol)
            if df is None or df.empty:
                return None
            
            if 'trade_date' not in df.columns or 'close' not in df.columns:
                return None
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
            df = df.dropna(subset=['trade_date'])
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            entry_dt = pd.to_datetime(entry_date)
            
            # 找到入场日
            mask = df['trade_date'] >= entry_dt
            if not mask.any():
                return None
            
            entry_idx = df[mask].index[0]
            if entry_idx >= len(df):
                return None
            
            entry_price = df.iloc[entry_idx]['close']
            
            # 找到出场日
            exit_idx = entry_idx + holding_days
            if exit_idx >= len(df):
                return None
            
            exit_price = df.iloc[exit_idx]['close']
            exit_date = df.iloc[exit_idx]['trade_date'].strftime('%Y-%m-%d')
            
            return_pct = (exit_price - entry_price) / entry_price * 100
            
            return {
                'entry_price': entry_price,
                'exit_price': exit_price,
                'exit_date': exit_date,
                'return_pct': return_pct,
                'win': return_pct > 0
            }
        except Exception as e:
            logger.debug(f"计算收益失败 {symbol}: {e}")
            return None
    
    def run_backtest(self, start_date: str = None, end_date: str = None, 
                     holding_periods: List[int] = None) -> Dict:
        """运行回测"""
        if holding_periods is None:
            holding_periods = [1, 3, 5]
        
        # 获取可用日期
        dates = self.get_available_dates(start_date, end_date)
        if not dates:
            logger.error("没有找到可用的信号数据")
            return {}
        
        logger.info(f"回测期间: {dates[0]} 至 {dates[-1]}, 共 {len(dates)} 个交易日")
        logger.info(f"选股数量: Top{self.top_n}")
        logger.info(f"持有期: {holding_periods} 天")
        
        # 按持有期存储交易记录
        trades_by_period = {period: [] for period in holding_periods}
        
        # 遍历每个交易日
        for date_str in dates:
            # 加载信号
            signals = self.load_signals(date_str)
            if not signals:
                continue
            
            # 选出TopN股票
            top_stocks = self.select_top_stocks(signals)
            if not top_stocks:
                continue
            
            logger.info(f"{date_str}: 选中 {len(top_stocks)} 只股票")
            
            # 对每个持有期计算收益
            for period in holding_periods:
                for stock in top_stocks:
                    result = self.calculate_returns(
                        stock['symbol'], 
                        datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d'),
                        period
                    )
                    
                    if result:
                        trade = TradeRecord(
                            symbol=stock['symbol'],
                            name=stock['name'],
                            entry_date=date_str,
                            exit_date=result['exit_date'].replace('-', ''),
                            entry_price=result['entry_price'],
                            exit_price=result['exit_price'],
                            score=stock['score'],
                            holding_days=period,
                            return_pct=result['return_pct'],
                            win=result['win']
                        )
                        trades_by_period[period].append(trade)
        
        # 计算统计指标
        results = {}
        for period in holding_periods:
            trades = trades_by_period[period]
            if not trades:
                continue
            
            returns = [t.return_pct for t in trades]
            wins = [t for t in trades if t.win]
            
            # 计算每日等权收益（按入场日分组）
            daily_returns = defaultdict(list)
            for trade in trades:
                daily_returns[trade.entry_date].append(trade.return_pct)
            
            # 计算累计收益（假设每天投入等额资金）
            daily_avg_returns = []
            for date in sorted(daily_returns.keys()):
                avg_return = np.mean(daily_returns[date])
                daily_avg_returns.append(avg_return)
            
            # 累计收益率（复利计算）
            cumulative_return = 1.0
            for r in daily_avg_returns:
                cumulative_return *= (1 + r / 100)
            cumulative_return_pct = (cumulative_return - 1) * 100
            
            # 简单加总收益（用于对比）
            simple_total_return = sum(daily_avg_returns)
            
            results[period] = {
                'total_trades': len(trades),
                'win_count': len(wins),
                'win_rate': len(wins) / len(trades) * 100 if trades else 0,
                'avg_return': np.mean(returns),
                'median_return': np.median(returns),
                'std_return': np.std(returns),
                'max_return': max(returns),
                'min_return': min(returns),
                'cumulative_return': cumulative_return_pct,
                'simple_total_return': simple_total_return,
                'sharpe_ratio': np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0,
                'trading_days': len(daily_returns),
                'avg_daily_return': np.mean(daily_avg_returns) if daily_avg_returns else 0,
            }
        
        self.results = results
        self.trades_by_period = trades_by_period
        return results
    
    def generate_report(self) -> str:
        """生成回测报告"""
        if not self.results:
            return "无回测结果"
        
        lines = []
        lines.append("=" * 80)
        lines.append("Top10信号分股票多持有期回测报告")
        lines.append("=" * 80)
        lines.append(f"选股策略: 每日信号分最高的前{self.top_n}只股票")
        lines.append(f"持仓方式: 等权重买入")
        lines.append("")
        
        # 各持有期表现
        lines.append("-" * 80)
        lines.append("各持有期表现对比")
        lines.append("-" * 80)
        lines.append(f"{'指标':<20} {'1天':>15} {'3天':>15} {'5天':>15}")
        lines.append("-" * 80)
        
        metrics = [
            ('交易次数', 'total_trades', '{:.0f}'),
            ('胜率(%)', 'win_rate', '{:.2f}'),
            ('平均收益(%)', 'avg_return', '{:.2f}'),
            ('中位数收益(%)', 'median_return', '{:.2f}'),
            ('收益标准差', 'std_return', '{:.2f}'),
            ('最大收益(%)', 'max_return', '{:.2f}'),
            ('最小收益(%)', 'min_return', '{:.2f}'),
            ('累计收益率(%)', 'cumulative_return', '{:.2f}'),
            ('年化收益率(%)', 'avg_daily_return', '{:.2f}'),
            ('夏普比率', 'sharpe_ratio', '{:.2f}'),
        ]
        
        for name, key, fmt in metrics:
            values = []
            for period in [1, 3, 5]:
                if period in self.results:
                    val = self.results[period].get(key, 0)
                    values.append(fmt.format(val))
                else:
                    values.append('N/A')
            lines.append(f"{name:<20} {values[0]:>15} {values[1]:>15} {values[2]:>15}")
        
        lines.append("-" * 80)
        lines.append("")
        
        # 详细交易记录摘要
        for period in [1, 3, 5]:
            if period not in self.trades_by_period:
                continue
            
            trades = self.trades_by_period[period]
            if not trades:
                continue
            
            lines.append(f"\n{'='*80}")
            lines.append(f"持有 {period} 天 - 交易记录摘要 (前20笔)")
            lines.append(f"{'='*80}")
            lines.append(f"{'日期':<12} {'代码':<12} {'名称':<10} {'分数':>6} {'收益(%)':>10} {'结果':>6}")
            lines.append("-" * 80)
            
            # 按日期排序，显示前20笔
            sorted_trades = sorted(trades, key=lambda x: x.entry_date)[:20]
            for t in sorted_trades:
                result = "✓" if t.win else "✗"
                lines.append(f"{t.entry_date:<12} {t.symbol:<12} {t.name:<10} {t.score:>6} {t.return_pct:>10.2f} {result:>6}")
        
        lines.append("\n" + "=" * 80)
        lines.append("回测完成")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def export_trades_to_csv(self, output_dir: Path = None):
        """导出交易记录到CSV"""
        if output_dir is None:
            output_dir = project_root / "storage" / "outputs" / "backtest"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for period, trades in self.trades_by_period.items():
            if not trades:
                continue
            
            df = pd.DataFrame([
                {
                    'symbol': t.symbol,
                    'name': t.name,
                    'entry_date': t.entry_date,
                    'exit_date': t.exit_date,
                    'entry_price': t.entry_price,
                    'exit_price': t.exit_price,
                    'score': t.score,
                    'holding_days': t.holding_days,
                    'return_pct': t.return_pct,
                    'win': t.win
                }
                for t in trades
            ])
            
            filepath = output_dir / f"top{self.top_n}_hold{period}d_trades.csv"
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"交易记录已导出: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description='TopN信号分股票多持有期回测',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python ShortTerm/backtest/top10_holdings_backtest.py
  python ShortTerm/backtest/top10_holdings_backtest.py --start 20250101 --end 20260422
  python ShortTerm/backtest/top10_holdings_backtest.py --top_n 100 --holdings 1,3,5,15,30,90
        """
    )
    
    parser.add_argument('--start', type=str, default='20250101', help='开始日期 (YYYYMMDD，默认20250101)')
    parser.add_argument('--end', type=str, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--top_n', type=int, default=100, help='选股数量 (默认100)')
    parser.add_argument('--holdings', type=str, default='1,3,5,15,30,90', 
                        help='持有期天数，逗号分隔 (默认: 1,3,5,15,30,90)')
    parser.add_argument('--export', action='store_true', help='导出交易记录到CSV')
    
    args = parser.parse_args()
    
    # 解析持有期
    holding_periods = [int(x.strip()) for x in args.holdings.split(',')]
    
    # 创建回测器
    backtest = TopNHoldingsBacktest(top_n=args.top_n)
    
    # 运行回测
    results = backtest.run_backtest(
        start_date=args.start,
        end_date=args.end,
        holding_periods=holding_periods
    )
    
    if results:
        # 打印报告
        print(backtest.generate_report())
        
        # 导出交易记录
        if args.export:
            backtest.export_trades_to_csv()
    else:
        print("回测失败，请检查数据")


if __name__ == "__main__":
    main()

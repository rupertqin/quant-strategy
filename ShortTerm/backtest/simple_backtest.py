#!/usr/bin/env python3
"""
简化版回测 - 直接从价格数据计算信号

用法:
    python simple_backtest.py --start 2025-01-01 --end 2026-04-22 --top 100 --hold 3
    python simple_backtest.py --start 2025-01-01 --hold 1,3,5,15,30  # 多周期对比
"""

import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_price_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """加载单只股票的历史价格"""
    file_path = PROJECT_ROOT / "storage" / "raw" / "stocks" / "price" / f"{symbol}.parquet"
    if not file_path.exists():
        return pd.DataFrame()
    
    df = pd.read_parquet(file_path)
    df['date'] = pd.to_datetime(df['trade_date'])
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    return df.sort_values('date')


def calculate_signals(df: pd.DataFrame) -> Dict:
    """
    基于价格数据计算信号分
    简化版：用技术指标打分
    """
    if len(df) < 60:
        return {'total_score': 0}
    
    # 先计算指标
    df = df.copy()
    df['ma20'] = df['close'].rolling(20).mean()
    df['returns'] = df['close'].pct_change()
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['high_20d'] = df['high'].rolling(20).max()
    
    # 取最新行（确保指标已计算）
    latest = df.iloc[-1]
    
    # 如果指标是NaN，返回0分
    if pd.isna(latest['ma20']):
        return {'total_score': 0}
    
    # 1. 趋势得分 (价格在MA20上方)
    trend_score = 20 if latest['close'] > latest['ma20'] else 0
    
    # 2. 动量得分 (20日涨幅)
    price_20d_ago = df['close'].iloc[-20] if len(df) >= 20 else df['close'].iloc[0]
    change_20d = (latest['close'] - price_20d_ago) / price_20d_ago * 100
    momentum_score = min(max(change_20d, -20), 20) + 20  # 映射到0-40
    
    # 3. 成交量得分 (放量)
    vol_avg = latest['vol_ma20'] if pd.notna(latest['vol_ma20']) and latest['vol_ma20'] > 0 else latest['volume']
    vol_ratio = latest['volume'] / vol_avg if vol_avg > 0 else 1
    volume_score = min(vol_ratio * 10, 20)
    
    # 4. 波动率得分 (波动适中)
    volatility = df['returns'].rolling(20).std().iloc[-1] * 100 if pd.notna(df['returns'].rolling(20).std().iloc[-1]) else 2
    volatility_score = 20 - min(abs(volatility - 2) * 5, 20)  # 2%波动率最优
    
    # 5. 形态得分 (突破近期高点)
    high_20d = latest['high_20d'] if pd.notna(latest['high_20d']) else latest['high']
    breakout_score = 20 if latest['close'] >= high_20d * 0.98 else 0
    
    total_score = trend_score + momentum_score + volume_score + volatility_score + breakout_score
    
    return {
        'total_score': total_score,
        'trend': trend_score,
        'momentum': momentum_score,
        'volume': volume_score,
        'volatility': volatility_score,
        'breakout': breakout_score,
        'close': latest['close'],
        'volume_val': latest['volume'],
        'change_20d': change_20d
    }


def get_all_stocks() -> List[str]:
    """获取所有股票代码"""
    price_dir = PROJECT_ROOT / "storage" / "raw" / "stocks" / "price"
    stocks = [f.stem for f in price_dir.glob("*.parquet")]
    return sorted(stocks)


def run_backtest(start_date: str, end_date: str, top_n: int, 
                 holding_days: int, max_stocks: int = None) -> Dict:
    """
    运行回测
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        top_n: 每天选前N只
        holding_days: 持有天数
        max_stocks: 最多处理多少只股票（用于测试）
    """
    print(f"\n{'='*60}")
    print(f"简化版回测: Top{top_n} 持有{holding_days}天")
    print(f"{'='*60}")
    print(f"日期范围: {start_date} ~ {end_date}")
    
    # 获取交易日期列表
    sample_file = PROJECT_ROOT / "storage" / "raw" / "stocks" / "price" / "000001.SZ.parquet"
    if not sample_file.exists():
        print("错误: 找不到价格数据")
        return {}
    
    sample_df = pd.read_parquet(sample_file)
    sample_df['date'] = pd.to_datetime(sample_df['trade_date'])
    trading_days = sample_df[(sample_df['date'] >= start_date) & 
                             (sample_df['date'] <= end_date)]['date'].dt.date.unique()
    trading_days = sorted(trading_days)
    print(f"交易日数量: {len(trading_days)}")
    
    # 获取股票列表
    all_stocks = get_all_stocks()
    if max_stocks:
        all_stocks = all_stocks[:max_stocks]
    print(f"处理股票数: {len(all_stocks)}")
    
    # 回测主循环
    trades = []
    daily_returns = []
    
    for i, trade_day in enumerate(trading_days):
        trade_date = trade_day.strftime('%Y-%m-%d')
        
        # 计算卖出日
        sell_idx = i + holding_days
        if sell_idx >= len(trading_days):
            continue
        sell_date = trading_days[sell_idx].strftime('%Y-%m-%d')
        
        # 1. 计算所有股票的信号
        signals = []
        for symbol in all_stocks:
            # 加载到当前交易日的数据
            df = load_price_data(symbol, '2020-01-01', trade_date)
            if len(df) < 30:
                continue
            
            signal = calculate_signals(df)
            if signal['total_score'] > 0:
                signals.append({
                    'symbol': symbol,
                    **signal
                })
        
        if not signals:
            continue
        
        # 2. 选出Top N
        signals_df = pd.DataFrame(signals)
        signals_df = signals_df.nlargest(top_n, 'total_score')
        
        # 3. 计算收益
        day_returns = []
        for _, row in signals_df.iterrows():
            symbol = row['symbol']
            
            # 加载持有期的价格数据
            hold_df = load_price_data(symbol, trade_date, sell_date)
            if len(hold_df) < 2:
                continue
            
            buy_price = hold_df.iloc[0]['close']
            sell_price = hold_df.iloc[-1]['close']
            return_pct = (sell_price - buy_price) / buy_price * 100
            
            trades.append({
                'entry_date': trade_date,
                'exit_date': sell_date,
                'symbol': symbol,
                'score': row['total_score'],
                'buy_price': buy_price,
                'sell_price': sell_price,
                'return_pct': return_pct,
                'win': return_pct > 0
            })
            day_returns.append(return_pct)
        
        if day_returns:
            avg_return = np.mean(day_returns)
            daily_returns.append({
                'date': trade_date,
                'return_pct': avg_return,
                'num_stocks': len(day_returns)
            })
        
        if (i + 1) % 10 == 0:
            print(f"  已处理: {i+1}/{len(trading_days)} 交易日")
    
    # 计算统计指标
    if not trades:
        print("没有完成任何交易")
        return {}
    
    trades_df = pd.DataFrame(trades)
    daily_df = pd.DataFrame(daily_returns)
    
    # 累计收益（复利）
    daily_df['cum_return'] = (1 + daily_df['return_pct'] / 100).cumprod() - 1
    total_return = daily_df['cum_return'].iloc[-1] * 100
    
    # 年化收益
    num_days = len(daily_df)
    annual_return = ((1 + total_return / 100) ** (252 / num_days) - 1) * 100 if num_days > 0 else 0
    
    results = {
        'trades': trades_df,
        'daily': daily_df,
        'total_trades': len(trades_df),
        'win_rate': trades_df['win'].mean() * 100,
        'avg_return': trades_df['return_pct'].mean(),
        'total_return': total_return,
        'annual_return': annual_return,
        'sharpe': daily_df['return_pct'].mean() / daily_df['return_pct'].std() * np.sqrt(252) if daily_df['return_pct'].std() > 0 else 0,
        'max_drawdown': calculate_max_drawdown(daily_df['cum_return']),
        'holding_days': holding_days,
        'top_n': top_n
    }
    
    return results


def calculate_max_drawdown(cum_returns: pd.Series) -> float:
    """计算最大回撤"""
    peak = cum_returns.expanding().max()
    drawdown = (cum_returns - peak) / (1 + peak)
    return drawdown.min() * 100


def print_report(results: Dict):
    """打印回测报告"""
    if not results:
        return
    
    print(f"\n{'='*60}")
    print(f"回测结果: Top{results['top_n']} 持有{results['holding_days']}天")
    print(f"{'='*60}")
    print(f"总交易次数: {results['total_trades']}")
    print(f"胜率: {results['win_rate']:.2f}%")
    print(f"平均收益: {results['avg_return']:.2f}%")
    print(f"累计收益: {results['total_return']:.2f}%")
    print(f"年化收益: {results['annual_return']:.2f}%")
    print(f"夏普比率: {results['sharpe']:.2f}")
    print(f"最大回撤: {results['max_drawdown']:.2f}%")


def main():
    parser = argparse.ArgumentParser(description='简化版回测')
    parser.add_argument('--start', default='2025-01-01', help='开始日期')
    parser.add_argument('--end', default='2026-04-22', help='结束日期')
    parser.add_argument('--top', type=int, default=100, help='选股数量')
    parser.add_argument('--hold', default='3', help='持有天数，逗号分隔多周期')
    parser.add_argument('--limit', type=int, help='限制处理股票数（测试用）')
    parser.add_argument('--export', action='store_true', help='导出结果')
    
    args = parser.parse_args()
    
    holding_periods = [int(x.strip()) for x in args.hold.split(',')]
    
    all_results = []
    for hold_days in holding_periods:
        result = run_backtest(
            start_date=args.start,
            end_date=args.end,
            top_n=args.top,
            holding_days=hold_days,
            max_stocks=args.limit
        )
        if result:
            print_report(result)
            all_results.append(result)
    
    # 多周期对比
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("多周期对比")
        print(f"{'='*60}")
        print(f"{'持有期':<8}{'胜率':>10}{'平均收益':>12}{'累计收益':>12}{'年化收益':>12}{'夏普':>8}")
        print("-" * 60)
        for r in all_results:
            print(f"{r['holding_days']}天{r['win_rate']:>10.1f}%{r['avg_return']:>11.2f}%{r['total_return']:>11.2f}%{r['annual_return']:>11.2f}%{r['sharpe']:>8.2f}")
    
    # 导出
    if args.export and all_results:
        output_dir = PROJECT_ROOT / "storage" / "outputs" / "backtest"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for r in all_results:
            trades_file = output_dir / f"simple_top{r['top_n']}_hold{r['holding_days']}d.csv"
            r['trades'].to_csv(trades_file, index=False)
            print(f"\n导出: {trades_file}")


if __name__ == "__main__":
    main()

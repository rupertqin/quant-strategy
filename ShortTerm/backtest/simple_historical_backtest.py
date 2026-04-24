#!/usr/bin/env python3
"""
简化版历史回测 - 直接从Parquet读取价格数据
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import argparse
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_price_data(symbol: str) -> pd.DataFrame:
    """直接从Parquet加载价格数据"""
    from DataHub.config import get_storage_path
    filepath = get_storage_path("raw", "stocks", "price") / f"{symbol}.parquet"
    if not filepath.exists():
        return None
    
    df = pd.read_parquet(filepath)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df.sort_values('trade_date').reset_index(drop=True)


def calculate_signals(df: pd.DataFrame) -> list:
    """计算交易信号"""
    if len(df) < 30:
        return []
    
    signals = []
    
    # 计算技术指标
    df = df.copy()
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    
    # MACD
    ema12 = df['close'].ewm(span=12).mean()
    ema26 = df['close'].ewm(span=26).mean()
    df['macd_dif'] = ema12 - ema26
    df['macd_dea'] = df['macd_dif'].ewm(span=9).mean()
    
    # KDJ
    low_list = df['low'].rolling(9).min()
    high_list = df['high'].rolling(9).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    
    score = 0
    
    # 信号1: 均线多头排列
    if latest['ma5'] > latest['ma10'] > latest['ma20']:
        score += 20
        signals.append('均线多头排列')
    
    # 信号2: MACD金叉
    if latest['macd_dif'] > latest['macd_dea'] and prev['macd_dif'] <= prev['macd_dea']:
        score += 25
        signals.append('MACD金叉')
    
    # 信号3: 突破MA20
    if latest['close'] > latest['ma20'] and prev['close'] <= prev['ma20']:
        score += 20
        signals.append('突破MA20')
    
    # 信号4: KDJ低位金叉
    if latest['kdj_k'] > latest['kdj_d'] and prev['kdj_k'] <= prev['kdj_d'] and latest['kdj_k'] < 50:
        score += 20
        signals.append('KDJ低位金叉')
    
    # 信号5: 放量上涨
    if 'volume' in df.columns:
        vol_ma20 = df['volume'].rolling(20).mean().iloc[-1]
        if latest['volume'] > vol_ma20 * 1.5:
            price_change = (latest['close'] - prev['close']) / prev['close'] * 100
            if price_change > 2:
                score += 25
                signals.append('放量上涨')
    
    return signals, score


def backtest_period(start_date: str, end_date: str, top_n: int = 100, 
                    holding_days: int = 3, max_stocks: int = None):
    """回测指定持有期"""
    
    # 获取所有股票代码
    from DataHub.config import get_storage_path
    price_dir = get_storage_path("raw", "stocks", "price")
    all_stocks = [f.stem for f in price_dir.glob("*.parquet")]
    
    if max_stocks:
        all_stocks = all_stocks[:max_stocks]
    
    logger.info(f"共 {len(all_stocks)} 只股票，回测持有 {holding_days} 天策略")
    
    # 生成交易日列表
    dates = pd.date_range(start=start_date, end=end_date, freq='B')  # B = business days
    date_strs = [d.strftime('%Y-%m-%d') for d in dates]
    
    logger.info(f"回测期间: {date_strs[0]} 至 {date_strs[-1]}, {len(date_strs)} 个交易日")
    
    all_trades = []
    
    for date_str in date_strs:
        date = pd.to_datetime(date_str)
        
        # 扫描当日信号
        daily_signals = []
        
        for symbol in tqdm(all_stocks, desc=f"扫描 {date_str}", leave=False):
            try:
                df = load_price_data(symbol)
                if df is None or len(df) < 30:
                    continue
                
                # 只取到当前日期的数据
                mask = df['trade_date'] <= date
                if not mask.any():
                    continue
                
                hist_df = df[mask].copy()
                if len(hist_df) < 30:
                    continue
                
                # 确保是最新数据
                latest_date = hist_df['trade_date'].iloc[-1]
                if latest_date.strftime('%Y-%m-%d') != date_str:
                    continue
                
                signals, score = calculate_signals(hist_df)
                
                if score >= 60:  # 只记录高分信号
                    latest = hist_df.iloc[-1]
                    daily_signals.append({
                        'symbol': symbol,
                        'date': date_str,
                        'close': latest['close'],
                        'score': score,
                        'signals': ','.join(signals)
                    })
                    
            except Exception as e:
                continue
        
        if not daily_signals:
            logger.warning(f"{date_str}: 未找到信号")
            continue
        
        # 选出TopN
        top_signals = sorted(daily_signals, key=lambda x: x['score'], reverse=True)[:top_n]
        logger.info(f"{date_str}: 选中 {len(top_signals)} 只股票 (最高分数: {top_signals[0]['score']})")
        
        # 计算收益
        for signal in top_signals:
            try:
                df = load_price_data(signal['symbol'])
                if df is None:
                    continue
                
                entry_mask = df['trade_date'] == date
                if not entry_mask.any():
                    continue
                
                entry_idx = df[entry_mask].index[0]
                exit_idx = entry_idx + holding_days
                
                if exit_idx >= len(df):
                    continue
                
                entry_price = df.loc[entry_idx, 'close']
                exit_price = df.loc[exit_idx, 'close']
                exit_date = df.loc[exit_idx, 'trade_date']
                
                return_pct = (exit_price - entry_price) / entry_price * 100
                
                all_trades.append({
                    'symbol': signal['symbol'],
                    'entry_date': date_str,
                    'exit_date': exit_date.strftime('%Y-%m-%d'),
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'score': signal['score'],
                    'return_pct': return_pct,
                    'win': return_pct > 0,
                    'signals': signal['signals']
                })
                
            except Exception as e:
                continue
    
    return pd.DataFrame(all_trades)


def main():
    parser = argparse.ArgumentParser(description='简化版历史回测')
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--top_n', type=int, default=100, help='选股数量 (默认100)')
    parser.add_argument('--holding', type=int, default=3, help='持有天数 (默认3)')
    parser.add_argument('--max_stocks', type=int, help='限制扫描股票数量（测试用）')
    
    args = parser.parse_args()
    
    # 运行回测
    df = backtest_period(
        start_date=args.start,
        end_date=args.end,
        top_n=args.top_n,
        holding_days=args.holding,
        max_stocks=args.max_stocks
    )
    
    if df.empty:
        print("回测失败，未找到交易记录")
        return
    
    # 计算统计
    print("\n" + "="*80)
    print(f"回测结果: Top{args.top_n} 持有 {args.holding} 天")
    print("="*80)
    print(f"总交易次数: {len(df)}")
    print(f"盈利次数: {df['win'].sum()} ({df['win'].mean()*100:.2f}%)")
    print(f"平均收益率: {df['return_pct'].mean():.2f}%")
    print(f"中位数收益率: {df['return_pct'].median():.2f}%")
    print(f"收益标准差: {df['return_pct'].std():.2f}%")
    print(f"最大盈利: {df['return_pct'].max():.2f}%")
    print(f"最大亏损: {df['return_pct'].min():.2f}%")
    
    # 按日期统计
    daily = df.groupby('entry_date')['return_pct'].mean()
    print(f"\n日均收益: {daily.mean():.2f}%")
    print(f"累计收益: {(np.prod(1 + daily/100) - 1)*100:.2f}%")
    
    # 保存结果
    from DataHub.config import get_storage_path
    output_dir = get_storage_path("outputs", "backtest")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"simple_top{args.top_n}_hold{args.holding}d.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n交易记录已保存: {output_file}")


if __name__ == "__main__":
    main()

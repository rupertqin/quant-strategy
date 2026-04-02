"""
市场状态判断模块 V2
综合判断：宏观因子 + 技术面指标
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from DataHub.core.data_client import UnifiedDataClient

logger = logging.getLogger(__name__)


class MarketRegime:
    """市场状态判断 - 宏观+技术综合版"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            config_path = os.path.join(parent_dir, "config.yaml")
            if not os.path.exists(config_path):
                config_path = os.path.join(current_dir, "config.yaml")
        
        self.config = self._load_config(config_path) if os.path.exists(config_path) else {}
        self.cache_dir = self.config.get('cache', {}).get('dir', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.data_client = UnifiedDataClient()
        
        # 板块分类
        self.offensive_sectors = ['半导体', '新能源', '科技', '计算机', '通信', '传媒', '券商']
        self.defensive_sectors = ['黄金', '银行', '公用事业', '医药', '食品饮料', '电力']

    def _load_config(self, path: str) -> dict:
        import yaml
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {path}, using default config")
            return {
                'cache': {'dir': 'cache'},
                'event_params': {'min_zt_count': 3},
                'output': {'signals_file': 'signals.json', 'history_file': 'history.csv'}
            }

    # ========== 宏观因子 ==========
    
    def get_usd_cny_rate(self) -> dict:
        """获取美元人民币汇率"""
        try:
            df = self.data_client.get_fx_rate("USD/CNY")
            if df.empty:
                return {'current': 7.2, 'change_5d': 0, 'date': None}

            latest = df.iloc[0]
            return {
                'current': float(latest['卖报价']),
                'change_5d': 0,
                'date': None
            }
        except Exception as e:
            logger.warning(f"获取汇率失败: {e}")
            return {'current': 7.2, 'change_5d': 0, 'date': None}

    def get_north_money_flow(self) -> dict:
        """获取北向资金流向"""
        try:
            df = self.data_client.get_north_money_flow()
            north_df = df[df['资金方向'] == '北向']
            if north_df.empty:
                return {'recent_3d_avg': 0, 'today': 0}

            today = north_df['成交净买额'].sum() if '成交净买额' in north_df.columns else 0

            return {
                'today': today,
                'recent_3d_avg': today,
                'recent_5d_avg': today
            }
        except Exception as e:
            logger.warning(f"获取北向资金失败: {e}")
            return {'recent_3d_avg': 0, 'today': 0}

    def get_gold_price(self) -> dict:
        """获取黄金价格"""
        try:
            df = self.data_client.get_gold_price()
            if df.empty:
                return {'current': 550, 'change_5d': 0}

            latest = df.iloc[0]
            close_col = 'close' if 'close' in df.columns else '最新价' if '最新价' in df.columns else '收盘价'
            if close_col not in df.columns:
                return {'current': 550, 'change_5d': 0}
                
            current = latest[close_col]
            if len(df) >= 5 and close_col in df.columns:
                change_5d = (current - df.iloc[4][close_col]) / df.iloc[4][close_col]
            else:
                change_5d = 0

            return {'current': current, 'change_5d': change_5d}
        except Exception as e:
            logger.warning(f"获取黄金价格失败: {e}")
            return {'current': 550, 'change_5d': 0}

    # ========== 技术面因子 ==========
    
    def get_market_breadth(self) -> dict:
        """
        获取市场涨跌家数
        Returns: {'up': 上涨家数, 'down': 下跌家数, 'ratio': 涨跌比}
        """
        try:
            import akshare as ak
            # 获取全市场实时行情
            df = ak.stock_zh_a_spot_em()
            
            # 计算涨跌
            up_count = len(df[df['涨跌幅'] > 0])
            down_count = len(df[df['涨跌幅'] < 0])
            flat_count = len(df[df['涨跌幅'] == 0])
            total = len(df)
            
            return {
                'up': up_count,
                'down': down_count,
                'flat': flat_count,
                'total': total,
                'up_ratio': up_count / total if total > 0 else 0.5,
                'breadth_score': (up_count - down_count) / total if total > 0 else 0  # -1到1之间
            }
        except Exception as e:
            logger.warning(f"获取市场涨跌家数失败: {e}")
            return {'up': 2500, 'down': 2500, 'flat': 0, 'total': 5000, 'up_ratio': 0.5, 'breadth_score': 0}

    def _get_index_history(self, code: str, days: int = 120) -> pd.DataFrame:
        """
        获取指数历史数据（用于技术分析）
        
        Args:
            code: 指数代码
            days: 获取天数
        
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        try:
            import akshare as ak
            
            # 方法1: 使用腾讯财经接口（新浪源）
            try:
                # 转换代码格式 000300 -> sh000300
                if code.startswith('0'):
                    tx_code = f'sh{code}'
                else:
                    tx_code = f'sz{code}'
                
                df = ak.stock_zh_index_daily_tx(symbol=tx_code)
                if not df.empty and len(df) >= 20:
                    df = df.rename(columns={
                        'amount': 'volume'
                    })
                    df = df.tail(days).reset_index(drop=True)
                    return df
            except Exception as e:
                logger.debug(f"stock_zh_index_daily_tx 失败: {e}")
            
            return pd.DataFrame()
        except Exception as e:
            logger.debug(f"获取指数历史数据失败 {code}: {e}")
            return pd.DataFrame()

    def _dow_theory_analysis(self, df: pd.DataFrame) -> dict:
        """
        道氏理论分析
        
        核心原则：
        1. 三种趋势：主要趋势(数月-数年)、次要趋势(3周-3月)、短期趋势(<3周)
        2. 指数相互验证：不同指数应相互确认
        3. 成交量验证：趋势需要成交量配合
        4. 收盘价最重要
        """
        if df.empty or len(df) < 60:
            return {'primary_trend': 'UNKNOWN', 'secondary_trend': 'UNKNOWN', 'note': '数据不足'}
        
        closes = df['close'].values
        volumes = df['volume'].values if 'volume' in df.columns else None
        
        # 1. 主要趋势判断 (使用60日均线)
        ma60 = df['close'].rolling(60).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        current = closes[-1]
        
        if current > ma60 * 1.05:
            primary_trend = 'BULL'  # 牛市（主要上升趋势）
            primary_desc = '主要上升趋势'
        elif current < ma60 * 0.95:
            primary_trend = 'BEAR'  # 熊市（主要下降趋势）
            primary_desc = '主要下降趋势'
        else:
            primary_trend = 'SIDEWAYS'  # 横盘整理
            primary_desc = '主要趋势横盘'
        
        # 2. 次要趋势判断 (使用20日均线与60日均线的关系)
        if ma20 > ma60 * 1.02:
            secondary_trend = 'UP'
            secondary_desc = '次要趋势上升'
        elif ma20 < ma60 * 0.98:
            secondary_trend = 'DOWN'
            secondary_desc = '次要趋势下降'
        else:
            secondary_trend = 'SIDEWAYS'
            secondary_desc = '次要趋势震荡'
        
        # 3. 计算趋势强度
        high_60 = df['high'].tail(60).max()
        low_60 = df['low'].tail(60).min()
        range_60 = high_60 - low_60
        
        if range_60 > 0:
            position_in_range = (current - low_60) / range_60
        else:
            position_in_range = 0.5
        
        # 4. 成交量分析（如果有数据）
        volume_signal = 'neutral'
        if volumes is not None and len(volumes) >= 20:
            recent_vol = volumes[-5:].mean()
            avg_vol = volumes[-20:].mean()
            
            if primary_trend == 'BULL' and recent_vol > avg_vol * 1.2:
                volume_signal = 'confirming'  # 上涨放量，确认趋势
            elif primary_trend == 'BULL' and recent_vol < avg_vol * 0.8:
                volume_signal = 'warning'  # 上涨缩量，警示
            elif primary_trend == 'BEAR' and recent_vol > avg_vol * 1.2:
                volume_signal = 'confirming'  # 下跌放量，确认趋势
            else:
                volume_signal = 'neutral'
        
        return {
            'primary_trend': primary_trend,
            'primary_desc': primary_desc,
            'secondary_trend': secondary_trend,
            'secondary_desc': secondary_desc,
            'position_in_range': round(float(position_in_range), 2),
            'ma60': round(float(ma60), 2),
            'ma20': round(float(ma20), 2),
            'volume_signal': volume_signal,
            'trend_strength': self._calculate_trend_strength(df)
        }

    def _calculate_trend_strength(self, df: pd.DataFrame) -> dict:
        """计算趋势强度指标"""
        if len(df) < 20:
            return {'adx': 0, 'strength': 'weak'}
        
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        
        # 简化ADX计算
        tr_list = []
        plus_dm_list = []
        minus_dm_list = []
        
        for i in range(1, min(15, len(closes))):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            tr_list.append(tr)
            
            plus_dm = highs[i] - highs[i-1] if highs[i] - highs[i-1] > lows[i-1] - lows[i] else 0
            minus_dm = lows[i-1] - lows[i] if lows[i-1] - lows[i] > highs[i] - highs[i-1] else 0
            
            plus_dm_list.append(max(plus_dm, 0))
            minus_dm_list.append(max(minus_dm, 0))
        
        if sum(tr_list) > 0:
            plus_di = 100 * sum(plus_dm_list) / sum(tr_list)
            minus_di = 100 * sum(minus_dm_list) / sum(tr_list)
            dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
        else:
            dx = 0
        
        if dx > 40:
            strength = 'strong'
        elif dx > 25:
            strength = 'moderate'
        else:
            strength = 'weak'
        
        return {'adx': round(dx, 1), 'strength': strength}

    def _elliott_wave_analysis(self, df: pd.DataFrame) -> dict:
        """
        波浪理论分析
        
        核心概念：
        1. 5浪推动（1-2-3-4-5）
        2. 3浪调整（A-B-C）
        3. 斐波那契比例关系
        4. 浪的识别基于高低点
        """
        if df.empty or len(df) < 30:
            return {'wave_count': 'UNKNOWN', 'current_phase': 'unknown', 'note': '数据不足'}
        
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        
        # 识别局部极值点（简化版）
        peaks = []
        troughs = []
        
        window = 3
        for i in range(window, len(closes) - window):
            # 峰值
            if highs[i] == max(highs[i-window:i+window+1]):
                peaks.append((i, highs[i]))
            # 谷值
            if lows[i] == min(lows[i-window:i+window+1]):
                troughs.append((i, lows[i]))
        
        # 分析最近的趋势结构
        if len(peaks) < 2 or len(troughs) < 2:
            return {'wave_count': 'INSUFFICIENT_DATA', 'current_phase': 'unknown'}
        
        # 获取最近的极值
        recent_peaks = peaks[-3:]
        recent_troughs = troughs[-3:]
        
        # 判断当前处于什么阶段
        current_price = float(closes[-1])
        last_peak_price = float(recent_peaks[-1][1]) if recent_peaks else current_price
        last_trough_price = float(recent_troughs[-1][1]) if recent_troughs else current_price
        
        # 计算斐波那契回调位
        if last_peak_price != last_trough_price:
            fib_range = last_peak_price - last_trough_price
            fib_382 = last_peak_price - fib_range * 0.382
            fib_500 = last_peak_price - fib_range * 0.500
            fib_618 = last_peak_price - fib_range * 0.618
        else:
            fib_382 = fib_500 = fib_618 = current_price
        
        # 判断当前位置
        if current_price > last_peak_price * 0.98:
            phase = '可能的第5浪或顶部'
        elif current_price > fib_382:
            phase = '可能的第3浪或第4浪调整'
        elif current_price > fib_618:
            phase = '可能的调整浪B或第2浪'
        else:
            phase = '可能的底部或调整浪C'
        
        # 计算浪的结构特征
        if len(recent_peaks) >= 2 and len(recent_troughs) >= 2:
            # 检查是否符合5浪结构特征
            wave_1 = recent_troughs[0][1] if recent_troughs else 0
            wave_2 = recent_peaks[0][1] if recent_peaks else 0
            
            # 计算波动幅度
            recent_volatility = np.std(closes[-20:]) / np.mean(closes[-20:]) * 100 if len(closes) >= 20 else 0
            
            wave_structure = {
                'recent_peaks': [(str(df.iloc[p[0]]['date']), round(float(p[1]), 2)) for p in recent_peaks],
                'recent_troughs': [(str(df.iloc[t[0]]['date']), round(float(t[1]), 2)) for t in recent_troughs],
                'volatility_pct': round(float(recent_volatility), 2),
                'fib_382': round(float(fib_382), 2),
                'fib_500': round(float(fib_500), 2),
                'fib_618': round(float(fib_618), 2),
            }
        else:
            wave_structure = {}
        
        return {
            'wave_count': int(len(peaks) + len(troughs)),
            'current_phase': phase,
            'last_peak': round(float(last_peak_price), 2),
            'last_trough': round(float(last_trough_price), 2),
            'current_vs_peak': round(float((current_price - last_peak_price) / last_peak_price * 100), 2),
            'structure': wave_structure
        }

    def get_index_performance(self) -> dict:
        """
        获取主要指数表现（结合道氏理论和波浪理论）
        Returns: {
            '沪深300': {
                'change': 涨跌幅, 
                'trend': 趋势,
                'dow_theory': {...},      # 道氏理论分析
                'elliott_wave': {...}     # 波浪理论分析
            },
            ...
        }
        """
        indices = {
            '沪深300': ('000300', 'sh000300'),
            '中证1000': ('000852', 'sh000852'),
            '创业板': ('399006', 'sz399006'),
            '上证指数': ('000001', 'sh000001')
        }
        
        result = {}
        
        # 方法1: 尝试使用新浪实时行情接口（更可靠）
        try:
            import akshare as ak
            spot_df = ak.stock_zh_index_spot_sina()
            
            for name, (em_code, sina_code) in indices.items():
                try:
                    # 新浪接口使用 sh/sz 前缀
                    idx_row = spot_df[spot_df['代码'] == sina_code]
                    if not idx_row.empty:
                        row = idx_row.iloc[0]
                        change_pct = float(row.get('涨跌幅', 0))
                        close = float(row.get('最新价', 0))
                        # 根据涨跌幅判断趋势
                        trend = 'UP' if change_pct > 0 else 'DOWN' if change_pct < 0 else 'NEUTRAL'
                        
                        result[name] = {
                            'change': round(change_pct, 2),
                            'trend': trend,
                            'close': close
                        }
                        logger.debug(f"获取指数 {name}: {change_pct:+.2f}%")
                except Exception as e:
                    logger.debug(f"新浪接口获取 {name} 失败: {e}")
        except Exception as e:
            logger.debug(f"新浪实时行情接口失败: {e}")
        
        # 方法2: 使用历史数据接口（备用）
        if len(result) < len(indices):
            try:
                import akshare as ak
                from datetime import datetime, timedelta
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                start_str = start_date.strftime('%Y%m%d')
                end_str = end_date.strftime('%Y%m%d')
                
                for name, (em_code, _) in indices.items():
                    if name in result:
                        continue
                    try:
                        df = ak.index_zh_a_hist(symbol=em_code, period="daily", start_date=start_str, end_date=end_str)
                        
                        if not df.empty and len(df) >= 2:
                            latest = df.iloc[-1]
                            prev = df.iloc[-2]
                            change_pct = (latest['收盘'] - prev['收盘']) / prev['收盘'] * 100 if prev['收盘'] > 0 else 0
                            
                            if len(df) >= 5:
                                ma5 = df['收盘'].tail(5).mean()
                                trend = 'UP' if latest['收盘'] > ma5 else 'DOWN'
                            else:
                                trend = 'NEUTRAL'
                                
                            result[name] = {
                                'change': round(change_pct, 2),
                                'trend': trend,
                                'close': latest['收盘']
                            }
                    except Exception as e:
                        logger.debug(f"历史数据获取 {name} 失败: {e}")
                        
            except Exception as e:
                logger.debug(f"历史数据接口失败: {e}")
        
        # 添加道氏理论和波浪理论分析
        for name, (em_code, _) in indices.items():
            if name in result:
                # 获取历史数据进行技术分析
                hist_df = self._get_index_history(em_code, days=90)
                
                if not hist_df.empty:
                    # 道氏理论分析
                    result[name]['dow_theory'] = self._dow_theory_analysis(hist_df)
                    
                    # 波浪理论分析
                    result[name]['elliott_wave'] = self._elliott_wave_analysis(hist_df)
                else:
                    result[name]['dow_theory'] = {'note': '无法获取历史数据'}
                    result[name]['elliott_wave'] = {'note': '无法获取历史数据'}
        
        # 填充缺失的指数
        for name in indices.keys():
            if name not in result:
                result[name] = {
                    'change': 0, 
                    'trend': 'NEUTRAL', 
                    'close': 0,
                    'dow_theory': {'note': '数据缺失'},
                    'elliott_wave': {'note': '数据缺失'}
                }
        
        # 添加跨指数验证（道氏理论原则）
        result['inter_index_validation'] = self._validate_across_indices(result)
        
        return result

    def _validate_across_indices(self, indices_data: dict) -> dict:
        """
        跨指数验证（道氏理论原则）
        主要趋势应该被不同指数相互确认
        """
        # 排除非指数键
        index_names = [k for k in indices_data.keys() if k not in ['inter_index_validation']]
        
        if len(index_names) < 2:
            return {'validation': 'INSUFFICIENT_DATA', 'note': '指数数据不足'}
        
        # 统计各主要趋势方向
        primary_trends = []
        for name in index_names:
            if 'dow_theory' in indices_data[name]:
                trend = indices_data[name]['dow_theory'].get('primary_trend', 'UNKNOWN')
                primary_trends.append((name, trend))
        
        # 判断是否一致
        trend_counts = {}
        for _, trend in primary_trends:
            trend_counts[trend] = trend_counts.get(trend, 0) + 1
        
        dominant_trend = max(trend_counts.items(), key=lambda x: x[1]) if trend_counts else ('UNKNOWN', 0)
        consistency = dominant_trend[1] / len(primary_trends) if primary_trends else 0
        
        if consistency >= 0.75:
            validation = 'CONFIRMED'
            note = f'主要趋势一致（{dominant_trend[0]}），相互确认'
        elif consistency >= 0.5:
            validation = 'PARTIAL'
            note = '主要趋势部分确认，存在分歧'
        else:
            validation = 'DIVERGENCE'
            note = '主要趋势分歧明显，信号不一致'
        
        return {
            'validation': validation,
            'consistency': round(consistency, 2),
            'dominant_trend': dominant_trend[0],
            'trend_details': primary_trends,
            'note': note
        }

    def get_sector_strength(self) -> dict:
        """
        获取板块强度对比（进攻 vs 防守）
        """
        try:
            import akshare as ak
            # 尝试多个接口获取行业板块数据
            try:
                df = ak.stock_board_industry_name_em()
                # 获取涨幅列
                if '涨跌幅' in df.columns:
                    change_col = '涨跌幅'
                elif '最新价' in df.columns and '昨收' in df.columns:
                    df['涨跌幅'] = (df['最新价'] - df['昨收']) / df['昨收'] * 100
                    change_col = '涨跌幅'
                else:
                    return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': '数据格式不支持'}
                
                name_col = '板块名称' if '板块名称' in df.columns else df.columns[0]
            except:
                # 备用接口
                df = ak.stock_sector_detail(symbol="半导体")
                return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': '使用备用数据'}
            
            offensive_sum = 0
            offensive_count = 0
            defensive_sum = 0
            defensive_count = 0
            
            for _, row in df.iterrows():
                sector_name = str(row.get(name_col, ''))
                try:
                    change = float(row.get(change_col, 0))
                except:
                    continue
                
                # 判断属于进攻还是防守
                is_offensive = any(s in sector_name for s in self.offensive_sectors)
                is_defensive = any(s in sector_name for s in self.defensive_sectors)
                
                if is_offensive:
                    offensive_sum += change
                    offensive_count += 1
                elif is_defensive:
                    defensive_sum += change
                    defensive_count += 1
            
            offensive_avg = offensive_sum / offensive_count if offensive_count > 0 else 0
            defensive_avg = defensive_sum / defensive_count if defensive_count > 0 else 0
            
            return {
                'offensive_avg': round(offensive_avg, 2),
                'defensive_avg': round(defensive_avg, 2),
                'offensive_count': offensive_count,
                'defensive_count': defensive_count,
                'bias': round(offensive_avg - defensive_avg, 2),
                'leader': '进攻' if offensive_avg > defensive_avg else '防守' if defensive_avg > offensive_avg else '中性'
            }
        except Exception as e:
            logger.warning(f"获取板块强度失败: {e}")
            return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': str(e)}

    def get_limit_up_stats(self) -> dict:
        """
        获取涨停统计
        """
        try:
            import akshare as ak
            from datetime import datetime
            
            today = datetime.now().strftime('%Y%m%d')
            df_zt = ak.stock_zt_pool_em(date=today)
            
            zt_count = len(df_zt)
            
            # 按板块统计
            if '所属行业' in df_zt.columns:
                sector_counts = df_zt['所属行业'].value_counts()
                hot_sectors = len(sector_counts[sector_counts >= 3])  # 有3家以上涨停的板块
                max_sector_zt = sector_counts.iloc[0] if len(sector_counts) > 0 else 0
            else:
                hot_sectors = 0
                max_sector_zt = 0
            
            # 评估市场情绪
            if zt_count >= 80:
                sentiment = '极热'
            elif zt_count >= 50:
                sentiment = '活跃'
            elif zt_count >= 30:
                sentiment = '正常'
            elif zt_count >= 15:
                sentiment = '低迷'
            else:
                sentiment = '冷清'
                
            return {
                'zt_count': zt_count,
                'hot_sectors': hot_sectors,
                'max_sector_zt': max_sector_zt,
                'sentiment': sentiment,
                'assessment': f'{zt_count}家涨停/{hot_sectors}个热点板块' if hot_sectors > 0 else f'{zt_count}家涨停(分散)'
            }
        except Exception as e:
            logger.warning(f"获取涨停统计失败: {e}")
            return {'zt_count': 0, 'hot_sectors': 0, 'max_sector_zt': 0, 'sentiment': '未知', 'assessment': '数据获取失败'}

    # ========== 综合判断 ==========
    
    def get_market_status(self) -> dict:
        """
        综合判断市场状态（宏观+技术）
        """
        # 1. 宏观因子
        currency = self.get_usd_cny_rate()
        north_money = self.get_north_money_flow()
        gold = self.get_gold_price()
        
        # 2. 技术面因子
        breadth = self.get_market_breadth()
        indices = self.get_index_performance()
        sectors = self.get_sector_strength()
        zt_stats = self.get_limit_up_stats()
        
        # 3. 评分系统
        score = 0
        reasons = []
        tech_reasons = []
        
        # 宏观风险评分（原逻辑）
        if currency['change_5d'] > 0.02:
            score += 4
            reasons.append("汇率快速贬值")
        elif currency['change_5d'] > 0.01:
            score += 2
            
        if north_money['recent_3d_avg'] < -50:
            score += 4
            reasons.append("北向资金大幅流出")
        elif north_money['recent_3d_avg'] < -20:
            score += 2
            
        if gold['change_5d'] > 0.02:
            score += 2
            reasons.append("避险情绪升温")
        
        # 技术面评分
        # 涨跌家数比
        if breadth['up_ratio'] > 0.7:
            tech_reasons.append(f"普涨({breadth['up']}/{breadth['total']})")
        elif breadth['up_ratio'] < 0.3:
            score += 2
            tech_reasons.append(f"普跌({breadth['down']}/{breadth['total']})")
        
        # 指数趋势
        up_indices = sum(1 for v in indices.values() if v.get('change', 0) > 0)
        if up_indices >= 3:
            tech_reasons.append(f"指数共振({up_indices}/4上涨)")
        elif up_indices <= 1:
            score += 2
            tech_reasons.append("指数走弱")
        
        # 板块风格
        if sectors['bias'] > 1:
            tech_reasons.append("进攻板块领涨")
        elif sectors['bias'] < -1:
            score += 1
            tech_reasons.append("防守板块避险")
        
        # 涨停热度
        if zt_stats['zt_count'] >= 50:
            tech_reasons.append(f"涨停活跃({zt_stats['zt_count']}家)")
        elif zt_stats['zt_count'] < 20:
            score += 1
            tech_reasons.append("涨停稀少")
        
        # 4. 最终判断
        if score >= 6:
            regime = 'DEFENSIVE'
            regime_desc = '防御'
        elif score >= 3:
            regime = 'NEUTRAL'
            regime_desc = '中性'
        else:
            regime = 'AGGRESSIVE'
            regime_desc = '进攻'

        return {
            'regime': regime,
            'regime_desc': regime_desc,
            'score': score,
            'macro_score': score - len(tech_reasons) if tech_reasons else score,
            'tech_score': len(tech_reasons),
            'reasons': reasons,
            'tech_reasons': tech_reasons,
            'macro': {
                'currency': currency,
                'north_money': north_money,
                'gold': gold
            },
            'technical': {
                'breadth': breadth,
                'indices': indices,
                'sectors': sectors,
                'zt_stats': zt_stats
            },
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def get_position_multiplier(self) -> float:
        """根据市场状态返回仓位乘数"""
        status = self.get_market_status()

        if status['regime'] == 'AGGRESSIVE':
            return 1.0
        elif status['regime'] == 'NEUTRAL':
            return 0.7
        else:
            return 0.4

    def get_sector_preference(self) -> list:
        """根据市场状态返回推荐的板块方向"""
        status = self.get_market_status()

        if status['regime'] == 'DEFENSIVE':
            return ['黄金', '军工', '医药', '公用事业']
        elif status['regime'] == 'AGGRESSIVE':
            return ['科技', '新能源', '消费', '券商']
        else:
            return ['中特估', '高股息', '半导体']


if __name__ == "__main__":
    regime = MarketRegime()

    print("市场状态分析 V2 - 宏观+技术面")
    print("=" * 60)

    status = regime.get_market_status()
    print(f"\n【综合判断】")
    print(f"市场状态: {status['regime_desc']} ({status['regime']})")
    print(f"风险评分: {status['score']}/10 (宏观{status['macro_score']} + 技术{status['tech_score']})")
    
    print(f"\n【宏观因子】")
    print(f"  汇率: {status['macro']['currency']['current']:.4f}")
    print(f"  北向: {status['macro']['north_money']['today']:.1f}亿")
    print(f"  黄金: {status['macro']['gold']['current']:.2f}")
    
    print(f"\n【技术面】")
    tech = status['technical']
    print(f"  涨跌比: {tech['breadth']['up']}/{tech['breadth']['total']} ({tech['breadth']['up_ratio']*100:.1f}%上涨)")
    print(f"  涨停: {tech['zt_stats']['assessment']}")
    print(f"  板块风格: {tech['sectors']['leader']}主导 (偏差{tech['sectors']['bias']:.2f}%)")
    print(f"  指数: ", end='')
    for name, data in tech['indices'].items():
        print(f"{name}{data['change']:+.2f}% ", end='')
    print()
    
    print(f"\n【信号】")
    all_reasons = status['reasons'] + status['tech_reasons']
    print(f"  风险因素: {', '.join(status['reasons']) if status['reasons'] else '无'}")
    print(f"  积极因素: {', '.join(status['tech_reasons']) if status['tech_reasons'] else '无'}")
    print(f"\n仓位建议: {regime.get_position_multiplier():.0%}")
    print(f"推荐方向: {regime.get_sector_preference()}")

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
            return {'up': 2500, 'down': 2500, 'total': 5000, 'up_ratio': 0.5, 'breadth_score': 0}

    def get_index_performance(self) -> dict:
        """
        获取主要指数表现
        Returns: {
            '沪深300': {'change': 涨跌幅, 'trend': 趋势},
            '中证1000': {...},
            '创业板': {...}
        }
        """
        indices = {
            '沪深300': '000300',
            '中证1000': '000852', 
            '创业板': '399006',
            '上证指数': '000001'
        }
        
        result = {}
        try:
            import akshare as ak
            for name, code in indices.items():
                try:
                    if code.startswith('0'):
                        # 上海指数
                        df = ak.index_zh_a_hist(symbol=code, period="daily", start_date="20250328", end_date="20260402")
                    else:
                        # 深圳指数
                        df = ak.index_zh_a_hist(symbol=code, period="daily", start_date="20250328", end_date="20260402")
                    
                    if not df.empty:
                        latest = df.iloc[-1]
                        prev = df.iloc[-2] if len(df) > 1 else latest
                        change_pct = (latest['收盘'] - prev['收盘']) / prev['收盘'] * 100 if prev['收盘'] > 0 else 0
                        
                        # 判断趋势
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
                    logger.warning(f"获取指数 {name} 失败: {e}")
                    result[name] = {'change': 0, 'trend': 'NEUTRAL', 'close': 0}
                    
        except Exception as e:
            logger.warning(f"获取指数数据失败: {e}")
            # 默认值
            for name in indices.keys():
                result[name] = {'change': 0, 'trend': 'NEUTRAL', 'close': 0}
                
        return result

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

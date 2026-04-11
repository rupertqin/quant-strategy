"""
指数计算器 - 基于个股数据计算指数表现

核心思想：不直接获取指数接口数据，而是基于成分股计算指数表现
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable
from pathlib import Path
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class IndexDefinition:
    """指数定义"""
    
    # 指数成分股筛选规则
    # 注意：A股代码是6位数字，pattern 需要匹配带 .SZ/.SH 后缀的代码
    RULES = {
        '创业板': {
            'code_pattern': r'^(300|301)\d{4}(\.SZ)?$',
            'exchange': 'SZ',
            'description': '创业板股票（300/301开头）'
        },
        '科创板': {
            'code_pattern': r'^(688|689)\d{4}(\.SH)?$',
            'exchange': 'SH',
            'description': '科创板股票（688/689开头）'
        },
        '上证指数': {
            'code_pattern': r'^(600|601|603|605)\d{4}(\.SH)?$',
            'exchange': 'SH',
            'description': '上海主板股票'
        },
        '深证成指': {
            'code_pattern': r'^(000|001|002|003)\d{4}(\.SZ)?$',
            'exchange': 'SZ',
            'description': '深圳主板股票'
        },
        '沪深300': {
            'code_pattern': r'^((600|601|603|605)\d{4}(\.SH)?|(000|001|002|003)\d{4}(\.SZ)?)$',
            'exchange': None,  # 跨市场
            'top_n': 300,  # 取市值最大的300只
            'description': 'A股大市值股票（简化版）'
        },
        '中证1000': {
            'code_pattern': r'^((600|601|603|605|000|001|002|003|300|301)\d{4}(\.(SH|SZ))?)$',
            'exchange': None,
            'exclude_top_n': 300,  # 排除沪深300后取1000只
            'description': 'A股小市值股票（简化版）'
        },
        '北交所': {
            'code_pattern': r'^(43|8|82|83|87|88)\d{3,5}(\.BJ)?$',
            'exchange': 'BJ',
            'description': '北交所股票'
        }
    }
    
    # 代码前缀规则（用于快速筛选，不含后缀）
    PREFIX_RULES = {
        '创业板': ['300', '301'],
        '科创板': ['688', '689'],
        '上证指数': ['600', '601', '603', '605'],
        '深证成指': ['000', '001', '002', '003'],
        '沪深300': ['600', '601', '603', '605', '000', '001', '002', '003'],
        '中证1000': ['600', '601', '603', '605', '000', '001', '002', '003', '300', '301'],
        '北交所': ['43', '8', '82', '83', '87', '88'],
    }
    
    @classmethod
    def get_rule(cls, index_name: str) -> Optional[Dict]:
        """获取指数规则"""
        return cls.RULES.get(index_name)
    
    @classmethod
    def list_indices(cls) -> List[str]:
        """列出所有支持的指数"""
        return list(cls.RULES.keys())


class IndexCalculator:
    """
    指数计算器
    
    基于个股价格数据计算指数表现
    """
    
    def __init__(self, data_path: Optional[Path] = None):
        """
        初始化
        
        Args:
            data_path: 个股数据存储路径
        """
        self.data_path = data_path or Path(__file__).parent.parent.parent / "storage" / "raw" / "prices"
        self.stock_basic_path = Path(__file__).parent.parent.parent / "storage" / "stock_basic_info.csv"
        
    def filter_constituents(
        self,
        all_stocks_df: pd.DataFrame,
        index_name: str,
        date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        筛选指数成分股
        
        Args:
            all_stocks_df: 全市场股票数据 DataFrame
            index_name: 指数名称
            date: 日期（用于历史回溯，暂不使用）
            
        Returns:
            成分股 DataFrame
        """
        import re
        
        rule = IndexDefinition.get_rule(index_name)
        if not rule:
            logger.warning(f"未知的指数: {index_name}")
            return pd.DataFrame()
        
        # 获取代码列名
        code_col = self._get_code_column(all_stocks_df)
        if not code_col:
            logger.error("无法找到股票代码列")
            return pd.DataFrame()
        
        # 获取代码列表（保持原始格式）
        codes = all_stocks_df[code_col].astype(str)
        
        # 使用前缀匹配（更可靠）
        prefixes = IndexDefinition.PREFIX_RULES.get(index_name, [])
        if not prefixes:
            # 回退到正则匹配
            pattern = rule['code_pattern']
            mask = codes.str.match(pattern, na=False)
        else:
            # 提取6位数字代码进行前缀匹配
            numeric_codes = codes.str.extract(r'(\d{6})', expand=False)
            mask = numeric_codes.str.startswith(tuple(prefixes), na=False)
        
        # 如果指定了交易所，进一步筛选
        if rule.get('exchange'):
            exchange_col = self._get_exchange_column(all_stocks_df)
            if exchange_col and exchange_col in all_stocks_df.columns:
                mask &= (all_stocks_df[exchange_col] == rule['exchange'])
        
        constituents = all_stocks_df[mask].copy()
        
        # 按市值排序（如果有市值数据）
        if 'market_cap' in constituents.columns:
            constituents = constituents.sort_values('market_cap', ascending=False)
        
        # 取前N只
        if 'top_n' in rule:
            constituents = constituents.head(rule['top_n'])
        
        logger.info(f"{index_name} 成分股数量: {len(constituents)}")
        return constituents
    
    def calculate_index_value(
        self,
        price_df: pd.DataFrame,
        index_name: str,
        method: str = 'equal_weight'
    ) -> pd.Series:
        """
        计算指数点位
        
        Args:
            price_df: 个股价格数据，index=date, columns=stocks
            index_name: 指数名称
            method: 计算方法 - 'equal_weight'(等权) / 'market_cap'(市值加权)
            
        Returns:
            指数点位 Series，index=date
        """
        if price_df.empty:
            return pd.Series()
        
        # 获取成分股列表（当前使用列名）
        constituents = list(price_df.columns)
        
        # 筛选有效的成分股价格数据
        valid_data = price_df[constituents].dropna(how='all', axis=1)
        
        if valid_data.empty:
            logger.warning(f"{index_name}: 没有有效的价格数据")
            return pd.Series()
        
        if method == 'equal_weight':
            # 等权法：取平均涨跌幅，然后累积
            returns = valid_data.pct_change()
            avg_return = returns.mean(axis=1, skipna=True)
            # 处理首行 NaN（设为0）
            avg_return = avg_return.fillna(0)
            index_value = (1 + avg_return).cumprod()
            # 设置基准为1000点
            index_value = index_value * 1000
            # 处理可能的NaN值
            index_value = index_value.ffill().fillna(1000)
            
        elif method == 'market_cap':
            # 市值加权（需要市值数据，暂用等权）
            logger.warning("市值加权暂不支持，使用等权")
            return self.calculate_index_value(price_df, index_name, 'equal_weight')
        else:
            raise ValueError(f"未知的计算方法: {method}")
        
        return index_value
    
    def calculate_index_change(
        self,
        price_df: pd.DataFrame,
        index_name: str,
        periods: int = 1
    ) -> pd.Series:
        """
        计算指数涨跌幅
        
        Args:
            price_df: 个股价格数据
            index_name: 指数名称
            periods: 计算周期（1=日涨跌幅）
            
        Returns:
            指数涨跌幅 Series
        """
        index_value = self.calculate_index_value(price_df, index_name)
        if index_value.empty:
            return pd.Series()
        
        return index_value.pct_change(periods=periods) * 100
    
    def calculate_all_indices(
        self,
        price_df: pd.DataFrame,
        method: str = 'equal_weight'
    ) -> Dict[str, pd.Series]:
        """
        计算所有指数
        
        Args:
            price_df: 个股价格数据 (columns = 股票代码如 '300001.SZ')
            method: 计算方法
            
        Returns:
            {指数名: 指数点位Series}
        """
        results = {}
        
        # 从 price_df 的列创建股票列表
        all_symbols = list(price_df.columns)
        stocks_df = pd.DataFrame({'symbol': all_symbols})
        
        for index_name in IndexDefinition.list_indices():
            try:
                # 筛选成分股
                constituents_df = self.filter_constituents(stocks_df, index_name)
                if constituents_df.empty:
                    continue
                
                # 获取成分股代码
                constituent_codes = constituents_df['symbol'].tolist()
                
                # 在价格数据中选择这些列
                available_codes = [c for c in constituent_codes if c in price_df.columns]
                if len(available_codes) < 1:
                    logger.warning(f"{index_name}: 没有可用的价格数据")
                    continue
                    
                constituent_prices = price_df[available_codes]
                index_value = self.calculate_index_value(
                    constituent_prices, index_name, method
                )
                if not index_value.empty:
                    results[index_name] = index_value
                    
            except Exception as e:
                logger.error(f"计算 {index_name} 失败: {e}")
        
        return results
    
    def get_index_performance_summary(
        self,
        price_df: pd.DataFrame,
        lookback_days: int = 5
    ) -> Dict[str, Dict]:
        """
        获取指数表现摘要
        
        Args:
            price_df: 个股价格数据
            lookback_days: 回看天数
            
        Returns:
            {
                '创业板': {
                    'current_value': 当前点位,
                    'change_pct': 涨跌幅%,
                    'ma5': 5日均线,
                    'trend': 'UP'/'DOWN'/'NEUTRAL'
                }
            }
        """
        results = {}
        indices = self.calculate_all_indices(price_df)
        
        for name, series in indices.items():
            if series.empty or len(series) < 2:
                continue
            
            current = series.iloc[-1]
            prev = series.iloc[-2]
            change_pct = (current / prev - 1) * 100 if prev > 0 else 0
            
            # 计算均线
            ma5 = series.rolling(5).mean().iloc[-1] if len(series) >= 5 else current
            ma20 = series.rolling(20).mean().iloc[-1] if len(series) >= 20 else current
            
            # 判断趋势
            if current > ma5 > ma20:
                trend = 'UP'
            elif current < ma5 < ma20:
                trend = 'DOWN'
            else:
                trend = 'NEUTRAL'
            
            results[name] = {
                'current_value': round(current, 2),
                'change_pct': round(change_pct, 2),
                'ma5': round(ma5, 2),
                'ma20': round(ma20, 2),
                'trend': trend,
                'constituent_count': len(self.filter_constituents(price_df.T, name))
            }
        
        return results
    
    # ========== Helper Methods ==========
    
    def _get_code_column(self, df: pd.DataFrame) -> Optional[str]:
        """获取股票代码列名"""
        candidates = ['symbol', 'code', '股票代码', '代码']
        for col in candidates:
            if col in df.columns:
                return col
        # 如果没有找到，假设第一列是代码
        return df.columns[0] if len(df.columns) > 0 else None
    
    def _get_exchange_column(self, df: pd.DataFrame) -> Optional[str]:
        """获取交易所列名"""
        candidates = ['exchange', '交易所']
        for col in candidates:
            if col in df.columns:
                return col
        # 从 symbol 推断
        code_col = self._get_code_column(df)
        if code_col:
            # 创建交易所列
            df['_exchange'] = df[code_col].astype(str).str.extract(r'\.(\w+)$')[0]
            return '_exchange'
        return None
    
    @lru_cache(maxsize=32)
    def _load_stock_basic_info(self) -> pd.DataFrame:
        """加载股票基础信息"""
        if self.stock_basic_path.exists():
            return pd.read_csv(self.stock_basic_path)
        return pd.DataFrame()


# ========== 便捷函数 ==========

def calculate_index_from_stocks(
    price_df: pd.DataFrame,
    index_name: str
) -> pd.Series:
    """便捷函数：从个股价格计算指数"""
    calc = IndexCalculator()
    return calc.calculate_index_value(price_df, index_name)


def get_calculated_indices(price_df: pd.DataFrame) -> Dict[str, pd.Series]:
    """便捷函数：计算所有指数"""
    calc = IndexCalculator()
    return calc.calculate_all_indices(price_df)


def get_index_summary(price_df: pd.DataFrame) -> Dict[str, Dict]:
    """便捷函数：获取指数表现摘要"""
    calc = IndexCalculator()
    return calc.get_index_performance_summary(price_df)

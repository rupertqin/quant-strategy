"""
股票代码处理工具类
统一处理各种格式的股票代码
"""

import re
from functools import lru_cache
from typing import Optional


class StockCodeUtil:
    """股票代码处理工具类"""
    
    # 正则表达式：匹配6位数字代码（支持前缀如sh/sz）
    CODE_PATTERN = re.compile(r'(?:^|[^\d])(\d{6})(?:[^\d]|$)')
    
    # 交易所后缀映射
    EXCHANGE_SUFFIX = {
        'SH': '.SH',  # 上海
        'SZ': '.SZ',  # 深圳
        'BJ': '.BJ',  # 北京
    }
    
    # 代码前缀判断交易所
    PREFIX_EXCHANGE = {
        '600': 'SH', '601': 'SH', '603': 'SH', '605': 'SH',  # 沪市主板
        '688': 'SH', '689': 'SH',  # 科创板
        '000': 'SZ', '001': 'SZ', '002': 'SZ', '003': 'SZ',  # 深市主板/中小板
        '300': 'SZ', '301': 'SZ',  # 创业板
        '430': 'BJ', '8': 'BJ', '82': 'BJ', '83': 'BJ', '87': 'BJ', '88': 'BJ',  # 北交所/新三板
    }
    
    @classmethod
    def extract(cls, code_str: str) -> Optional[str]:
        """
        从字符串中提取6位数字代码
        
        Args:
            code_str: 任意格式的代码字符串，如 '600519.SH', 'sh600519', '贵州茅台(600519)'
            
        Returns:
            6位数字代码，如 '600519'，未找到返回 None
        """
        if not code_str:
            return None
        
        match = cls.CODE_PATTERN.search(str(code_str))
        return match.group(1) if match else None
    
    @classmethod
    def normalize(cls, code_str: str) -> Optional[str]:
        """
        标准化代码格式（提取纯数字）
        
        Args:
            code_str: 任意格式的代码
            
        Returns:
            6位纯数字代码
        """
        return cls.extract(code_str)
    
    @classmethod
    def with_suffix(cls, code_str: str) -> Optional[str]:
        """
        添加交易所后缀，如 '600519' -> '600519.SH'
        
        Args:
            code_str: 任意格式的代码
            
        Returns:
            带后缀的代码，如 '600519.SH'
        """
        code = cls.extract(code_str)
        if not code:
            return None
        
        exchange = cls.get_exchange(code)
        suffix = cls.EXCHANGE_SUFFIX.get(exchange, '')
        return f"{code}{suffix}" if suffix else code
    
    @classmethod
    def get_exchange(cls, code_str: str) -> Optional[str]:
        """
        判断代码所属交易所
        
        Args:
            code_str: 任意格式的代码
            
        Returns:
            'SH', 'SZ', 'BJ' 或 None
        """
        code = cls.extract(code_str)
        if not code:
            return None
        
        for prefix, exchange in sorted(cls.PREFIX_EXCHANGE.items(), key=lambda x: len(x[0]), reverse=True):
            if code.startswith(prefix):
                return exchange
        return None
    
    @classmethod
    # 注意：缓存会在模块重载时自动清除
    @lru_cache(maxsize=1)
    def get_name_mapper(cls) -> dict:
        """
        获取全市场代码到名称的映射字典（缓存）
        从本地 storage/stock_basic_info.csv 读取
        
        Returns:
            {code: name} 字典，code为6位纯数字
        """
        import os
        
        # 查找项目根目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # lib/utils -> lib -> project_root
        project_root = os.path.dirname(os.path.dirname(current_dir))
        csv_path = os.path.join(project_root, 'storage', 'stock_basic_info.csv')
        
        try:
            if os.path.exists(csv_path):
                import pandas as pd
                df = pd.read_csv(csv_path)
                if not df.empty and 'symbol' in df.columns and 'name' in df.columns:
                    # 从 symbol(如 600000.SH) 提取6位数字代码
                    codes = df['symbol'].astype(str).str.extract(r'(\d{6})', expand=False)
                    names = df['name'].astype(str).str.strip()
                    mapper = dict(zip(codes, names))
                    print(f"[StockCodeUtil] 从CSV加载 {len(mapper)} 条股票名称映射")
                    return mapper
            else:
                print(f"[StockCodeUtil] CSV文件不存在: {csv_path}")
        except Exception as e:
            print(f"[StockCodeUtil] 读取CSV失败: {e}")
        return {}
    
    @classmethod
    def get_name(cls, code_str: str) -> str:
        """
        获取股票名称
        
        Args:
            code_str: 任意格式的代码，如 '600519.SH', '贵州茅台 600519'
            
        Returns:
            股票名称，如 '贵州茅台'，未找到返回空字符串
        """
        code = cls.extract(code_str)
        if not code:
            return ''
        
        mapper = cls.get_name_mapper()
        return mapper.get(code, '')
    
    @classmethod
    def format_display(cls, code_str: str, include_name: bool = True) -> str:
        """
        格式化显示代码和名称
        
        Args:
            code_str: 任意格式的代码
            include_name: 是否包含名称
            
        Returns:
            格式化字符串，如 '600519(贵州茅台)' 或 '600519.SH'
        """
        code = cls.extract(code_str)
        if not code:
            return str(code_str) if code_str else ''
        
        code_with_suffix = cls.with_suffix(code) or code
        
        if include_name:
            name = cls.get_name(code)
            if name:
                return f"{code_with_suffix}({name})"
        
        return code_with_suffix
    
    @classmethod
    def is_same(cls, code1: str, code2: str) -> bool:
        """
        判断两个代码是否相同（比较6位数字）
        
        Args:
            code1, code2: 任意格式的代码
            
        Returns:
            是否是同一只股票
        """
        c1 = cls.extract(code1)
        c2 = cls.extract(code2)
        return c1 is not None and c2 is not None and c1 == c2


# 便捷函数（全局可用）
def get_stock_name(code: str) -> str:
    """获取股票名称的便捷函数"""
    return StockCodeUtil.get_name(code)


def format_stock(code: str, include_name: bool = True) -> str:
    """格式化股票显示的便捷函数"""
    return StockCodeUtil.format_display(code, include_name)


def normalize_code(code: str) -> Optional[str]:
    """标准化代码的便捷函数"""
    return StockCodeUtil.normalize(code)

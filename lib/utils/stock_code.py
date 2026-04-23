"""
股票代码处理工具类
统一处理各种格式的股票代码
"""

import re
import csv
from pathlib import Path
from functools import lru_cache
from typing import Optional, Tuple, Set


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
        '399': 'SZ',  # 深圳指数
        '430': 'BJ', '8': 'BJ', '82': 'BJ', '83': 'BJ', '87': 'BJ', '88': 'BJ',  # 北交所/新三板
        '92': 'BJ',  # 北交所新股 (920000-920099)
        # 沪市ETF (标准 + 扩展)
        '510': 'SH', '511': 'SH', '512': 'SH', '513': 'SH', '515': 'SH', '516': 'SH', '517': 'SH', '518': 'SH', '519': 'SH',
        '520': 'SH', '530': 'SH',  # 扩展沪市ETF
        '560': 'SH', '561': 'SH', '562': 'SH', '563': 'SH',  # 扩展沪市ETF
        '588': 'SH', '589': 'SH',  # 科创板ETF
        '159': 'SZ',  # 深市ETF
    }
    
    _index_codes_sh: Optional[Set[str]] = None
    _core_indices: Optional[dict] = None
    
    @classmethod
    def _get_index_codes_sh(cls) -> Set[str]:
        """从 official_indices.csv 读取上海指数代码"""
        if cls._index_codes_sh is not None:
            return cls._index_codes_sh
        
        cls._index_codes_sh = set()
        # 使用环境变量配置的 storage 路径
        try:
            from DataHub.config import get_storage_path
            csv_path = get_storage_path('official_indices.csv')
        except ImportError:
            csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'
        
        if csv_path.exists():
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        code = row.get('code', '').strip()
                        market = row.get('market', '').strip()
                        if code and market == '上海':
                            cls._index_codes_sh.add(code)
            except Exception:
                pass
        
        return cls._index_codes_sh
    
    @classmethod
    def get_core_indices(cls) -> dict:
        """
        从 official_indices.csv 读取主要指数列表
        
        Returns:
            {symbol: name} 字典，如 {'000001.SH': '上证指数', ...}
        """
        if cls._core_indices is not None:
            return cls._core_indices
        
        cls._core_indices = {}
        # 使用环境变量配置的 storage 路径
        try:
            from DataHub.config import get_storage_path
            csv_path = get_storage_path('official_indices.csv')
        except ImportError:
            csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'
        
        if csv_path.exists():
            try:
                with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig 自动处理 BOM
                    reader = csv.DictReader(f)
                    for row in reader:
                        # 处理可能的 BOM 字符
                        symbol = row.get('symbol', row.get('\ufeffsymbol', '')).strip()
                        name = row.get('name', '').strip()
                        if symbol and name:
                            cls._core_indices[symbol] = name
            except Exception as e:
                print(f"读取 official_indices.csv 失败: {e}")
        
        return cls._core_indices
    
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
        
        # 特殊处理：上海指数代码（以000开头，但不是深市股票）
        if code in cls._get_index_codes_sh():
            return 'SH'
        
        for prefix, exchange in sorted(cls.PREFIX_EXCHANGE.items(), key=lambda x: len(x[0]), reverse=True):
            if code.startswith(prefix):
                return exchange
        return None
    
    @classmethod
    def parse_prefixed_code(cls, code_str: str) -> Tuple[Optional[str], Optional[str]]:
        """
        解析带交易所前缀的代码
        
        支持格式: sh600000, sz000001, bj920000, SH600000, SZ000001
        
        Args:
            code_str: 带前缀的代码，如 'sh600000'
            
        Returns:
            Tuple[code, exchange]: (纯数字代码, 交易所)，解析失败返回 (None, None)
            
        Examples:
            >>> StockCodeUtil.parse_prefixed_code('sh600000')
            ('600000', 'SH')
            >>> StockCodeUtil.parse_prefixed_code('sz300750')
            ('300750', 'SZ')
            >>> StockCodeUtil.parse_prefixed_code('bj920000')
            ('920000', 'BJ')
        """
        if not code_str:
            return None, None
        
        code_str = str(code_str).strip().lower()
        
        # 处理带前缀的格式
        if code_str.startswith('sh'):
            code = code_str[2:]
            exchange = 'SH'
        elif code_str.startswith('sz'):
            code = code_str[2:]
            exchange = 'SZ'
        elif code_str.startswith('bj'):
            code = code_str[2:]
            exchange = 'BJ'
        else:
            # 没有前缀，尝试提取数字并判断交易所
            code = cls.extract(code_str)
            exchange = cls.get_exchange(code) if code else None
        
        # 验证提取的代码
        if code and len(code) == 6 and code.isdigit():
            return code, exchange
        
        return None, None

    @classmethod
    def to_baostock(cls, code_str: str) -> Optional[str]:
        """
        转换为 baostock 格式: sh.600000 / sz.000001

        Args:
            code_str: 任意格式的代码，如 '600000.SH', 'sh600000', '600000'

        Returns:
            baostock 格式代码，如 'sh.600000'，不支持则返回 None

        Examples:
            >>> StockCodeUtil.to_baostock('600000.SH')
            'sh.600000'
            >>> StockCodeUtil.to_baostock('000001.SZ')
            'sz.000001'
            >>> StockCodeUtil.to_baostock('920000.BJ')  # 北交所不支持
            None
        """
        code, exchange = cls.parse_prefixed_code(code_str)
        if not code or not exchange:
            return None

        # baostock 不支持北交所
        if exchange == 'BJ':
            return None

        return f"{exchange.lower()}.{code}"

    @classmethod
    def to_akshare(cls, code_str: str) -> Optional[str]:
        """
        转换为 akshare 格式: 600000 / 000001 (纯数字，无后缀)

        Args:
            code_str: 任意格式的代码

        Returns:
            6位纯数字代码

        Examples:
            >>> StockCodeUtil.to_akshare('600000.SH')
            '600000'
            >>> StockCodeUtil.to_akshare('sh600000')
            '600000'
        """
        return cls.extract(code_str)

    @classmethod
    def to_tushare(cls, code_str: str) -> Optional[str]:
        """
        转换为 tushare 格式: 600000.SH / 000001.SZ

        Args:
            code_str: 任意格式的代码

        Returns:
            带后缀的代码，如 '600000.SH'
        """
        return cls.with_suffix(code_str)

    @classmethod
    def to_eastmoney(cls, code_str: str) -> Optional[str]:
        """
        转换为东财格式: 600000 / 000001 (纯数字)

        与 akshare 相同，都是纯数字格式

        Args:
            code_str: 任意格式的代码

        Returns:
            6位纯数字代码
        """
        return cls.extract(code_str)

    @classmethod
    def convert(cls, code_str: str, target: str) -> Optional[str]:
        """
        通用转换方法，根据目标接口转换代码格式

        Args:
            code_str: 任意格式的代码
            target: 目标接口，支持 'baostock', 'akshare', 'tushare', 'eastmoney'

        Returns:
            目标格式的代码

        Examples:
            >>> StockCodeUtil.convert('600000.SH', 'baostock')
            'sh.600000'
            >>> StockCodeUtil.convert('sh600000', 'akshare')
            '600000'
        """
        converters = {
            'baostock': cls.to_baostock,
            'akshare': cls.to_akshare,
            'tushare': cls.to_tushare,
            'eastmoney': cls.to_eastmoney,
        }

        converter = converters.get(target.lower())
        if not converter:
            raise ValueError(f"不支持的目标接口: {target}，支持: {list(converters.keys())}")

        return converter(code_str)

    @classmethod
    # 注意：缓存会在模块重载时自动清除
    @lru_cache(maxsize=1)
    def get_name_mapper(cls) -> dict:
        """
        获取全市场代码到名称的映射字典（缓存）
        从本地 storage/stock_basic_info.csv、etf_basic_info.csv 和 official_indices.csv 读取
        
        Returns:
            {code: name} 字典，code为6位纯数字
        """
        import os
        
        # 使用环境变量配置的 storage 路径
        try:
            from DataHub.config import get_storage_path
            stock_csv = get_storage_path('stock_basic_info.csv')
            etf_csv = get_storage_path('etf_basic_info.csv')
            index_csv = get_storage_path('official_indices.csv')
        except ImportError:
            # 回退到默认路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            stock_csv = os.path.join(project_root, 'storage', 'stock_basic_info.csv')
            etf_csv = os.path.join(project_root, 'storage', 'etf_basic_info.csv')
            index_csv = os.path.join(project_root, 'storage', 'official_indices.csv')
        
        mapper = {}
        
        # 1. 读取股票数据
        try:
            if os.path.exists(stock_csv):
                import pandas as pd
                df = pd.read_csv(stock_csv)
                if not df.empty and 'symbol' in df.columns and 'name' in df.columns:
                    codes = df['symbol'].astype(str).str.extract(r'(\d{6})', expand=False)
                    names = df['name'].astype(str).str.strip()
                    mapper.update(dict(zip(codes, names)))
                    print(f"[StockCodeUtil] 从stock_basic_info.csv加载 {len(codes)} 条股票名称映射")
            else:
                print(f"[StockCodeUtil] 股票CSV文件不存在: {stock_csv}")
        except Exception as e:
            print(f"[StockCodeUtil] 读取股票CSV失败: {e}")
        
        # 2. 读取ETF数据
        try:
            if os.path.exists(etf_csv):
                import pandas as pd
                df = pd.read_csv(etf_csv)
                if not df.empty and 'symbol' in df.columns and 'name' in df.columns:
                    codes = df['symbol'].astype(str).str.extract(r'(\d{6})', expand=False)
                    names = df['name'].astype(str).str.strip()
                    etf_count = len(codes)
                    mapper.update(dict(zip(codes, names)))
                    print(f"[StockCodeUtil] 从etf_basic_info.csv加载 {etf_count} 条ETF名称映射")
            else:
                print(f"[StockCodeUtil] ETF CSV文件不存在: {etf_csv}")
        except Exception as e:
            print(f"[StockCodeUtil] 读取ETF CSV失败: {e}")
        
        # 3. 读取指数数据
        try:
            if os.path.exists(index_csv):
                import pandas as pd
                df = pd.read_csv(index_csv)
                if not df.empty and 'symbol' in df.columns and 'name' in df.columns:
                    codes = df['symbol'].astype(str).str.extract(r'(\d{6})', expand=False)
                    names = df['name'].astype(str).str.strip()
                    index_count = len(codes)
                    mapper.update(dict(zip(codes, names)))
                    print(f"[StockCodeUtil] 从official_indices.csv加载 {index_count} 条指数名称映射")
            else:
                print(f"[StockCodeUtil] 指数CSV文件不存在: {index_csv}")
        except Exception as e:
            print(f"[StockCodeUtil] 读取指数CSV失败: {e}")
        
        if mapper:
            print(f"[StockCodeUtil] 总共加载 {len(mapper)} 条名称映射")
        
        return mapper
    
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


# 缓存指数代码集合（从 official_indices.csv 读取）
_index_codes_cache = None


def _get_index_codes():
    """从 official_indices.csv 读取所有指数代码"""
    global _index_codes_cache

    if _index_codes_cache is not None:
        return _index_codes_cache

    _index_codes_cache = set()
    # 使用环境变量配置的 storage 路径
    try:
        from DataHub.config import get_storage_path
        csv_path = get_storage_path('official_indices.csv')
    except ImportError:
        csv_path = Path(__file__).parent.parent.parent / 'storage' / 'official_indices.csv'

    if csv_path.exists():
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get('code', '').strip()
                    if code:
                        _index_codes_cache.add(code)
        except Exception:
            pass

    return _index_codes_cache


def is_index(symbol: str) -> bool:
    """
    判断代码是否为指数（结合后缀判断）
    
    注意代码重名问题：
    - 000001.SH = 上证指数（指数）
    - 000001.SZ = 平安银行（股票）
    - 必须结合后缀判断！
    
    硬编码规则：
    - 399xxx.SZ = 深证指数（如399001.SZ深证成指、399006.SZ创业板指）
    
    查表确认：
    - 其他代码查 official_indices.csv
    """
    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
    if not code.isdigit():
        return False
    
    # 硬编码规则：399xxx.SZ 一定是深证指数
    if symbol.endswith('.SZ') and code.startswith('399'):
        return True
    
    # 其他情况查表确认
    return code in _get_index_codes()


def detect_asset_type(symbol: str, default: str = "stock") -> str:
    """
    根据代码自动检测资产类型（ETF/股票/指数）

    ETF 代码前缀：
    - 上海：510, 511, 512, 513, 515, 516, 517, 518, 519, 560, 561, 562, 563, 564, 588(科创)
    - 深圳：15xxxx, 16xxxx

    指数代码：基于 official_indices.csv 中的配置

    Args:
        symbol: 股票/ETF/指数代码，支持带后缀格式如 '510300.SH'
        default: 默认资产类型，如果无法识别则返回此值

    Returns:
        'stock', 'etf', 'index'

    Examples:
        >>> detect_asset_type('600519.SH')
        'stock'
        >>> detect_asset_type('510300.SH')
        'etf'
        >>> detect_asset_type('159915.SZ')
        'etf'
        >>> detect_asset_type('000001.SH')
        'index'
        >>> detect_asset_type('HSI.HK')
        'index'
    """
    # 港股指数直接识别为指数
    if symbol.endswith('.HK'):
        return 'index'

    # 去除后缀
    code = symbol.replace('.SH', '').replace('.SZ', '').replace('.BJ', '')

    if not code.isdigit():
        return default

    # 优先检查指数（基于 official_indices.csv）
    if is_index(symbol):
        return 'index'
    
    # ETF 代码规则（完整前缀列表，按交易所官方规则）
    # 沪市ETF：500/501/51x/52x/53x/56x/58x/59x 开头
    # 深市ETF：159/169 开头
    etf_prefixes = (
        # 沪市ETF
        '500', '501',  # 早期ETF
        '510', '511', '512', '513', '514', '515', '516', '517', '518', '519',  # 主流ETF
        '520', '521', '522', '523', '524', '525', '526', '527', '528', '529',  # 港股通ETF
        '530', '531', '532', '533', '534', '535', '536', '537', '538', '539',  # 其他ETF
        '560', '561', '562', '563', '564', '565', '566', '567', '568', '569',  # 新增ETF
        '580', '581', '582', '583', '584', '585', '586', '587', '588', '589',  # 科创/其他
        '590', '591', '592', '593', '594', '595', '596', '597', '598', '599',  # 预留扩展
        # 深市ETF
        '159', '169',  # 主流ETF
    )
    
    if code.startswith(etf_prefixes):
        return 'etf'
    
    return default

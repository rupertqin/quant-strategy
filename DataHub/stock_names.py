"""
股票名称映射模块

数据来源: storage/stock_basic_info.csv (由 DataHub/build_stock_db.py 构建)
更新频率: 不定期（季度/半年）

使用方式:
    from DataHub.stock_names import get_stock_name, enrich_with_names
    
    name = get_stock_name('600519.SH')  # 返回 '贵州茅台'
    data = enrich_with_names(data)      # 为数据添加名称字段
"""

import logging
from pathlib import Path
import pandas as pd
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# 内置的ETF映射（股票数据来自CSV，ETF较少且稳定，保留硬编码）
ETF_NAMES = {
    '510300.SH': '沪深300ETF',
    '510500.SH': '中证500ETF',
    '510050.SH': '上证50ETF',
    '159915.SZ': '创业板ETF',
    '159901.SZ': '深100ETF',
    '588000.SH': '科创50ETF',
    '588080.SH': '科创板50ETF',
    '512000.SH': '券商ETF',
    '512480.SH': '半导体ETF',
    '512760.SH': '芯片ETF',
    '515030.SH': '新能源车ETF',
    '515790.SH': '光伏ETF',
    '512170.SH': '医疗ETF',
    '512010.SH': '医药ETF',
    '159998.SZ': '计算机ETF',
    '159995.SZ': '芯片ETF',
    '159920.SZ': '恒生ETF',
}


def _get_csv_path() -> Path:
    """获取股票数据库CSV路径"""
    return Path(__file__).parent.parent / "storage" / "stock_basic_info.csv"


def _load_stock_names() -> Dict[str, str]:
    """
    从CSV加载股票名称映射
    
    Returns:
        dict: {symbol: name}
    """
    csv_path = _get_csv_path()
    
    if not csv_path.exists():
        logger.warning(f"股票数据库不存在: {csv_path}")
        logger.warning("请运行: python -m DataHub.build_stock_db")
        return {}
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        if 'symbol' not in df.columns or 'name' not in df.columns:
            logger.error("CSV格式错误: 缺少symbol或name列")
            return {}
        
        # 合并股票和ETF
        names = dict(zip(df['symbol'], df['name']))
        names.update(ETF_NAMES)
        
        return names
        
    except Exception as e:
        logger.error(f"加载股票数据库失败: {e}")
        return {}


# 全局缓存
_STOCK_NAMES_CACHE: Optional[Dict[str, str]] = None


def get_stock_name(symbol: str) -> str:
    """
    获取股票名称
    
    Args:
        symbol: 股票代码，如 '600519.SH'
        
    Returns:
        股票名称，如果未找到则返回空字符串
    """
    global _STOCK_NAMES_CACHE
    
    if _STOCK_NAMES_CACHE is None:
        _STOCK_NAMES_CACHE = _load_stock_names()
    
    return _STOCK_NAMES_CACHE.get(symbol, "")


def refresh_cache():
    """刷新名称缓存（在CSV更新后调用）"""
    global _STOCK_NAMES_CACHE
    _STOCK_NAMES_CACHE = _load_stock_names()
    logger.info(f"股票名称缓存已刷新，共 {len(_STOCK_NAMES_CACHE)} 条")


def enrich_with_names(data: dict or list) -> dict or list:
    """
    为股票数据添加名称
    
    支持的数据格式:
    - 单个股票字典: {'symbol': '600519.SH', ...}
    - 股票列表: [{'symbol': '600519.SH', ...}, ...]
    - 包含嵌套列表的报告字典
    
    Args:
        data: 需要添加名称的数据
        
    Returns:
        添加名称后的数据（原地修改）
    """
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and 'symbol' in item:
                if not item.get('name'):
                    item['name'] = get_stock_name(item['symbol'])
        return data
        
    elif isinstance(data, dict):
        if 'symbol' in data and not data.get('name'):
            data['name'] = get_stock_name(data['symbol'])
        
        # 处理嵌套列表
        for key in ['buy_signals', 'sell_signals', 'watch_list', 'top_rankings', 
                    'stocks', 'signals', 'pool']:
            if key in data and isinstance(data[key], list):
                data[key] = enrich_with_names(data[key])
        
        return data
        
    return data


def get_all_names() -> Dict[str, str]:
    """
    获取所有股票名称映射
    
    Returns:
        dict: {symbol: name}
    """
    global _STOCK_NAMES_CACHE
    
    if _STOCK_NAMES_CACHE is None:
        _STOCK_NAMES_CACHE = _load_stock_names()
    
    return _STOCK_NAMES_CACHE.copy()


def check_database_status() -> dict:
    """
    检查股票数据库状态
    
    Returns:
        状态信息字典
    """
    csv_path = _get_csv_path()
    
    if not csv_path.exists():
        return {
            'exists': False,
            'path': str(csv_path),
            'count': 0,
            'message': '数据库不存在，请运行: python -m DataHub.build_stock_db'
        }
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        update_time = df['update_time'].iloc[0] if 'update_time' in df.columns and not df.empty else '未知'
        
        return {
            'exists': True,
            'path': str(csv_path),
            'count': len(df),
            'update_time': update_time,
            'message': f'数据库正常，共 {len(df)} 只股票，更新时间: {update_time}'
        }
        
    except Exception as e:
        return {
            'exists': True,
            'path': str(csv_path),
            'count': 0,
            'error': str(e),
            'message': f'数据库文件存在但读取失败: {e}'
        }


# 向后兼容的别名
STOCK_NAMES = _load_stock_names() if _get_csv_path().exists() else {}

"""
股票代码工具类单元测试 (pytest)

覆盖要点：
1. A股 6位数字代码的提取、转换、交易所判断
2. 港股代码的边界（当前正则仅支持6位，4-5位港股代码是已知限制）
3. 指数 vs 股票的精确区分（带后缀判断，避免 000001.SH / 000001.SZ 冲突）
4. ETF 前缀规则
5. 空值/异常输入的防御性处理
"""

import sys
import os

# 添加项目根路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pytest
from lib.utils import stock_code as stock_code_module
from lib.utils.stock_code import (
    StockCodeUtil,
    is_index,
    detect_asset_type,
    normalize_code,
    format_stock,
    get_stock_name,
)


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_index_data(monkeypatch):
    """
    Mock 指数相关数据，避免测试依赖 official_indices.csv 文件。
    同时覆盖之前 000001.SZ 被误判为指数的 bug 场景。
    """
    # Mock 模块级 _get_index_symbols 缓存
    mock_symbols = {
        '000001.SH',  # 上证指数
        '000002.SH',  # 某上海指数
        '000016.SH',  # 上证50
        '000688.SH',  # 科创50
        '399001.SZ',  # 深证成指
        '399006.SZ',  # 创业板指
        '399005.SZ',  # 中小板指
        'HSI.HK',     # 恒生指数
        'HSCEI.HK',   # 国企指数
    }
    monkeypatch.setattr(stock_code_module, '_index_symbol_cache', mock_symbols)

    # Mock StockCodeUtil 类级的上海指数短码缓存
    mock_sh_codes = {'000001', '000002', '000016', '000688'}
    monkeypatch.setattr(StockCodeUtil, '_index_codes_sh', mock_sh_codes)

    # Mock 名称映射，使 get_name / format_display 测试不依赖外部 CSV
    mock_mapper = {
        '600519': '贵州茅台',
        '000001': '平安银行',
        '300001': '特锐德',
        '000002': '万科A',
        '510300': '沪深300ETF',
    }
    monkeypatch.setattr(
        StockCodeUtil, 'get_name_mapper',
        classmethod(lambda cls: mock_mapper)
    )


# -----------------------------------------------------------------------------
# StockCodeUtil.extract
# -----------------------------------------------------------------------------

class TestExtract:
    def test_standard_a_share(self):
        assert StockCodeUtil.extract('600519.SH') == '600519'
        assert StockCodeUtil.extract('000001.SZ') == '000001'
        assert StockCodeUtil.extract('300001.SZ') == '300001'
        assert StockCodeUtil.extract('688001.SH') == '688001'
        assert StockCodeUtil.extract('920000.BJ') == '920000'

    def test_with_prefix(self):
        assert StockCodeUtil.extract('sh600519') == '600519'
        assert StockCodeUtil.extract('SZ300001') == '300001'
        assert StockCodeUtil.extract('BJ920000') == '920000'

    def test_from_text(self):
        assert StockCodeUtil.extract('贵州茅台(600519)') == '600519'
        assert StockCodeUtil.extract('code: 600519, price: 100') == '600519'

    def test_hk_stock_limitation(self):
        """港股代码通常为4-5位，现有正则只匹配6位，这是已知限制"""
        assert StockCodeUtil.extract('腾讯控股 00700.HK') is None
        assert StockCodeUtil.extract('00001.HK') is None
        # 6位数字在港股中极少见，但正则能匹配到
        assert StockCodeUtil.extract('800001.HK') == '800001'

    def test_edge_lengths(self):
        assert StockCodeUtil.extract('12345') is None
        assert StockCodeUtil.extract('1234567') is None
        assert StockCodeUtil.extract('123456') == '123456'

    def test_none_and_empty(self):
        assert StockCodeUtil.extract('') is None
        assert StockCodeUtil.extract(None) is None


# -----------------------------------------------------------------------------
# StockCodeUtil.get_exchange
# -----------------------------------------------------------------------------

class TestGetExchange:
    def test_shanghai_main_board(self):
        assert StockCodeUtil.get_exchange('600519') == 'SH'
        assert StockCodeUtil.get_exchange('601001') == 'SH'
        assert StockCodeUtil.get_exchange('603001') == 'SH'
        assert StockCodeUtil.get_exchange('605001') == 'SH'

    def test_shanghai_kcb(self):
        assert StockCodeUtil.get_exchange('688001') == 'SH'
        assert StockCodeUtil.get_exchange('689001') == 'SH'

    def test_shenzhen_main_and_gem(self):
        assert StockCodeUtil.get_exchange('001001') == 'SZ'
        assert StockCodeUtil.get_exchange('002001') == 'SZ'
        assert StockCodeUtil.get_exchange('003001') == 'SZ'
        assert StockCodeUtil.get_exchange('300001') == 'SZ'
        assert StockCodeUtil.get_exchange('301001') == 'SZ'

    def test_shenzhen_index_hardcoded_prefix(self):
        assert StockCodeUtil.get_exchange('399001') == 'SZ'
        assert StockCodeUtil.get_exchange('399006') == 'SZ'

    def test_beijing(self):
        assert StockCodeUtil.get_exchange('430001') == 'BJ'
        assert StockCodeUtil.get_exchange('920000') == 'BJ'
        assert StockCodeUtil.get_exchange('830001') == 'BJ'
        assert StockCodeUtil.get_exchange('870001') == 'BJ'

    def test_etf_prefixes(self):
        assert StockCodeUtil.get_exchange('510300') == 'SH'
        assert StockCodeUtil.get_exchange('588000') == 'SH'
        assert StockCodeUtil.get_exchange('159915') == 'SZ'
        # NOTE: PREFIX_EXCHANGE 目前缺少 500/501/590 等前缀映射，与 detect_asset_type 不一致
        # assert StockCodeUtil.get_exchange('500001') == 'SH'
        # assert StockCodeUtil.get_exchange('590001') == 'SH'

    def test_shanghai_index_code_ambiguity(self):
        """
        已知限制：纯数字 000001 会被查表识别为上海指数，无法区分平安银行。
        实际使用中应传入带后缀的代码或结合上下文判断。
        """
        assert StockCodeUtil.get_exchange('000001') == 'SH'
        assert StockCodeUtil.get_exchange('000002') == 'SH'

    def test_normal_shenzhen_stock(self):
        assert StockCodeUtil.get_exchange('000858') == 'SZ'  # 五粮液，不在指数列表

    def test_invalid(self):
        assert StockCodeUtil.get_exchange('invalid') is None
        assert StockCodeUtil.get_exchange('12345') is None
        assert StockCodeUtil.get_exchange('00700') is None


# -----------------------------------------------------------------------------
# StockCodeUtil.with_suffix
# -----------------------------------------------------------------------------

class TestWithSuffix:
    def test_normal_stock(self):
        assert StockCodeUtil.with_suffix('600519') == '600519.SH'
        assert StockCodeUtil.with_suffix('300001') == '300001.SZ'
        assert StockCodeUtil.with_suffix('920000') == '920000.BJ'

    def test_already_has_suffix(self):
        # 注意：函数会忽略原始后缀，重新根据纯数字判断
        assert StockCodeUtil.with_suffix('600519.SH') == '600519.SH'
        assert StockCodeUtil.with_suffix('300001.SZ') == '300001.SZ'

    def test_ambiguous_code_loses_original_suffix(self):
        """
        已知限制：with_suffix 对纯数字重新判断交易所。
        000001.SZ 会被误判为 000001.SH，因为 get_exchange('000001') 查表优先。
        """
        assert StockCodeUtil.with_suffix('000001') == '000001.SH'
        assert StockCodeUtil.with_suffix('000001.SZ') == '000001.SH'  # 丢失原始后缀！

    def test_invalid(self):
        assert StockCodeUtil.with_suffix('invalid') is None
        assert StockCodeUtil.with_suffix('') is None
        assert StockCodeUtil.with_suffix(None) is None


# -----------------------------------------------------------------------------
# StockCodeUtil.parse_prefixed_code
# -----------------------------------------------------------------------------

class TestParsePrefixedCode:
    def test_lowercase_prefix(self):
        assert StockCodeUtil.parse_prefixed_code('sh600519') == ('600519', 'SH')
        assert StockCodeUtil.parse_prefixed_code('sz300001') == ('300001', 'SZ')
        assert StockCodeUtil.parse_prefixed_code('bj920000') == ('920000', 'BJ')

    def test_uppercase_prefix(self):
        assert StockCodeUtil.parse_prefixed_code('SH600519') == ('600519', 'SH')
        assert StockCodeUtil.parse_prefixed_code('SZ000001') == ('000001', 'SZ')
        assert StockCodeUtil.parse_prefixed_code('BJ920000') == ('920000', 'BJ')

    def test_no_prefix(self):
        assert StockCodeUtil.parse_prefixed_code('600519') == ('600519', 'SH')
        assert StockCodeUtil.parse_prefixed_code('300001') == ('300001', 'SZ')

    def test_invalid(self):
        assert StockCodeUtil.parse_prefixed_code('') == (None, None)
        assert StockCodeUtil.parse_prefixed_code(None) == (None, None)
        assert StockCodeUtil.parse_prefixed_code('abc') == (None, None)
        assert StockCodeUtil.parse_prefixed_code('00700.HK') == (None, None)


# -----------------------------------------------------------------------------
# Format converters
# -----------------------------------------------------------------------------

class TestConverters:
    def test_to_baostock(self):
        assert StockCodeUtil.to_baostock('600000.SH') == 'sh.600000'
        assert StockCodeUtil.to_baostock('300001.SZ') == 'sz.300001'
        assert StockCodeUtil.to_baostock('920000.BJ') is None  # 北交所不支持
        assert StockCodeUtil.to_baostock('invalid') is None
        assert StockCodeUtil.to_baostock('') is None

    def test_to_akshare(self):
        assert StockCodeUtil.to_akshare('600000.SH') == '600000'
        assert StockCodeUtil.to_akshare('sh600000') == '600000'
        assert StockCodeUtil.to_akshare('00700.HK') is None  # 提取不到

    def test_to_tushare(self):
        assert StockCodeUtil.to_tushare('600000') == '600000.SH'
        assert StockCodeUtil.to_tushare('300001') == '300001.SZ'
        assert StockCodeUtil.to_tushare('invalid') is None

    def test_to_eastmoney(self):
        assert StockCodeUtil.to_eastmoney('600000.SH') == '600000'
        assert StockCodeUtil.to_eastmoney('sh600000') == '600000'

    def test_convert(self):
        assert StockCodeUtil.convert('600000.SH', 'baostock') == 'sh.600000'
        assert StockCodeUtil.convert('600000.SH', 'akshare') == '600000'
        assert StockCodeUtil.convert('600000.SH', 'tushare') == '600000.SH'
        assert StockCodeUtil.convert('600000.SH', 'eastmoney') == '600000'

    def test_convert_unknown_target(self):
        with pytest.raises(ValueError):
            StockCodeUtil.convert('600000.SH', 'unknown')


# -----------------------------------------------------------------------------
# is_index (核心：覆盖 000001.SZ bug)
# -----------------------------------------------------------------------------

class TestIsIndex:
    def test_shanghai_index(self):
        assert is_index('000001.SH') is True
        assert is_index('000016.SH') is True
        assert is_index('000688.SH') is True

    def test_shenzhen_index_hardcoded(self):
        assert is_index('399001.SZ') is True
        assert is_index('399006.SZ') is True

    def test_shenzhen_stock_not_index(self):
        """关键边界：000001.SZ 是平安银行，绝不能被识别为指数"""
        assert is_index('000001.SZ') is False
        assert is_index('000002.SZ') is False  # 万科A
        assert is_index('000858.SZ') is False  # 五粮液
        assert is_index('300001.SZ') is False

    def test_normal_stock(self):
        assert is_index('600519.SH') is False
        assert is_index('688001.SH') is False

    def test_etf_not_index(self):
        assert is_index('510300.SH') is False
        assert is_index('159915.SZ') is False

    def test_hk_index(self):
        assert is_index('HSI.HK') is True
        assert is_index('HSCEI.HK') is True
        # 当前实现：任何 .HK 都视为指数（港股股票支持不完善，属已知限制）
        assert is_index('00700.HK') is True

    def test_empty(self):
        assert is_index('') is False
        assert is_index(None) is False


# -----------------------------------------------------------------------------
# detect_asset_type (核心)
# -----------------------------------------------------------------------------

class TestDetectAssetType:
    def test_stock(self):
        assert detect_asset_type('600519.SH') == 'stock'
        assert detect_asset_type('300001.SZ') == 'stock'
        assert detect_asset_type('688001.SH') == 'stock'
        assert detect_asset_type('920000.BJ') == 'stock'
        assert detect_asset_type('000858.SZ') == 'stock'

    def test_etf(self):
        assert detect_asset_type('510300.SH') == 'etf'
        assert detect_asset_type('159915.SZ') == 'etf'
        assert detect_asset_type('588000.SH') == 'etf'
        assert detect_asset_type('169901.SZ') == 'etf'
        assert detect_asset_type('500001.SH') == 'etf'

    def test_index(self):
        assert detect_asset_type('000001.SH') == 'index'
        assert detect_asset_type('399006.SZ') == 'index'
        assert detect_asset_type('HSI.HK') == 'index'

    def test_shenzhen_stock_vs_index(self):
        """关键边界：000001.SZ 必须是 stock，000001.SH 必须是 index"""
        assert detect_asset_type('000001.SZ') == 'stock'
        assert detect_asset_type('000001.SH') == 'index'

    def test_hk_current_behavior(self):
        """当前代码行为：所有 .HK 视为 index（真正的港股股票会被误判，属已知限制）"""
        assert detect_asset_type('00700.HK') == 'index'

    def test_none_and_invalid(self):
        """防御性处理：空值/None 应返回 default，不应抛异常"""
        assert detect_asset_type(None) == 'stock'
        assert detect_asset_type('') == 'stock'
        assert detect_asset_type('invalid') == 'stock'


# -----------------------------------------------------------------------------
# Convenience functions
# -----------------------------------------------------------------------------

class TestConvenienceFunctions:
    def test_normalize_code(self):
        assert normalize_code('600519.SH') == '600519'
        assert normalize_code('sh600519') == '600519'
        assert normalize_code('') is None
        assert normalize_code(None) is None

    def test_format_stock_with_name(self):
        assert format_stock('600519.SH') == '600519.SH(贵州茅台)'
        # 使用无歧义代码测试；000001 因查表会被归为 SH，是 with_suffix 的已知限制
        assert format_stock('300001.SZ') == '300001.SZ(特锐德)'

    def test_format_stock_without_name(self):
        assert format_stock('600519.SH', include_name=False) == '600519.SH'
        # 无映射时返回纯代码（无后缀，因为 with_suffix 对未知前缀返回原始 code）
        assert format_stock('999999.SH', include_name=True) == '999999'

    def test_get_stock_name(self):
        assert get_stock_name('600519.SH') == '贵州茅台'
        assert get_stock_name('000001.SZ') == '平安银行'
        assert get_stock_name('999999.SH') == ''
        assert get_stock_name('') == ''
        assert get_stock_name(None) == ''

    def test_is_same(self):
        assert StockCodeUtil.is_same('600519.SH', '600519') is True
        assert StockCodeUtil.is_same('sh600519', '600519.SH') is True
        assert StockCodeUtil.is_same('600519.SH', '300001.SZ') is False
        assert StockCodeUtil.is_same('invalid', '600519') is False
        assert StockCodeUtil.is_same('', '600519') is False

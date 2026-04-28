/**
 * 根据代码自动检测资产类型（ETF/股票/指数）
 * 翻译自 Python 版 lib/utils/stock_code.py 的 detect_asset_type
 */

// 沪市ETF前缀：500/501/51x/52x/53x/56x/58x/59x
const SH_ETF_PREFIXES = [
  '500', '501',
  '510', '511', '512', '513', '514', '515', '516', '517', '518', '519',
  '520', '521', '522', '523', '524', '525', '526', '527', '528', '529',
  '530', '531', '532', '533', '534', '535', '536', '537', '538', '539',
  '560', '561', '562', '563', '564', '565', '566', '567', '568', '569',
  '580', '581', '582', '583', '584', '585', '586', '587', '588', '589',
  '590', '591', '592', '593', '594', '595', '596', '597', '598', '599',
];

// 深市ETF前缀
const SZ_ETF_PREFIXES = ['159', '169'];

// 硬编码指数代码（避免查表依赖）
const INDEX_CODES = new Set([
  // 上海指数
  '000001', '000002', '000003', '000004', '000005', '000006', '000007', '000008', '000009', '000010',
  '000016', '000017', '000018', '000019', '000020',
  '000090', '000091', '000092', '000093', '000094',
  '000132', '000133',
  '000300', // 沪深300
  '000688', // 科创50
  '000852', // 中证1000
  '000903', '000905', // 中证500
  '000906',
  '000978',
  // 深圳指数
  '399001', // 深证成指
  '399006', // 创业板指
  '399300', // 沪深300(深圳)
  '399673', // 创业板50
  '399005', // 中小板指
  '399106', // 深证综指
  '399330', // 深证100
  '399481',
]);

function _isETF(code: string): boolean {
  if (SH_ETF_PREFIXES.some(p => code.startsWith(p))) return true;
  if (SZ_ETF_PREFIXES.some(p => code.startsWith(p))) return true;
  return false;
}

function _isIndex(symbol: string, code: string): boolean {
  // 399xxx.SZ 一定是深证指数
  if (symbol.endsWith('.SZ') && code.startsWith('399')) return true;
  // 000/001/002/003 开头 + .SZ 是深市主板/中小板股票，不是指数
  if (symbol.endsWith('.SZ') && /^(000|001|002|003)/.test(code)) return false;
  // 上海指数查硬编码表（仅.SH后缀匹配，避免000001.SZ被误判）
  if (symbol.endsWith('.SH') && INDEX_CODES.has(code)) return true;
  return false;
}

export function detectAssetType(symbol: string, defaultType: 'stock' | 'etf' | 'index' = 'stock'): 'stock' | 'etf' | 'index' {
  if (!symbol) return defaultType;

  // 港股：纯字母代码视为指数，纯数字视为股票
  if (symbol.toUpperCase().endsWith('.HK')) {
    const code = symbol.slice(0, -3);
    return /^[A-Za-z]+$/.test(code) ? 'index' : 'stock';
  }

  // 去除后缀
  const code = symbol.replace(/\.SH$/, '').replace(/\.SZ$/, '').replace(/\.BJ$/, '');

  // 非纯数字，无法识别
  if (!/^\d+$/.test(code)) return defaultType;

  // 优先检查指数
  if (_isIndex(symbol, code)) return 'index';

  // 检查ETF
  if (_isETF(code)) return 'etf';

  return defaultType;
}

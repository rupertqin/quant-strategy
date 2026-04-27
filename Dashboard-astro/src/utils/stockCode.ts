/**
 * 股票代码工具
 */

const INDEX_CODE_MAP: Record<string, string> = {
  '上证指数': '000001.SH',
  '深证成指': '399001.SZ',
  '创业板指': '399006.SZ',
  '沪深300': '000300.SH',
  '上证50': '000016.SH',
  '中证500': '000905.SH',
  '中证1000': '000852.SH',
};

export function getIndexCode(name: string): string {
  return INDEX_CODE_MAP[name] || '';
}

export function getIndexNameByCode(code: string): string {
  const entry = Object.entries(INDEX_CODE_MAP).find(([, c]) => c === code);
  return entry ? entry[0] : code;
}

export function extractCode(symbol: string): string {
  return symbol.split('.')[0];
}

export function extractExchange(symbol: string): string {
  const parts = symbol.split('.');
  return parts[1] || '';
}

// 名称缓存
let nameCache: Record<string, string> = {};

export async function getStockName(symbol: string): Promise<string> {
  if (!symbol) return '';
  if (nameCache[symbol]) return nameCache[symbol];

  try {
    const res = await fetch('/data/stock_names.json');
    if (res.ok) {
      const names = await res.json();
      nameCache = names;
      return names[symbol] || '';
    }
  } catch {
    // ignore
  }
  return '';
}

export function getStockNameSync(symbol: string, names: Record<string, string>): string {
  return names[symbol] || '';
}

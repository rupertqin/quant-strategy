/**
 * 数据加载工具 - 构建时从文件系统读取数据
 * 所有数据在 Astro 构建时读取，生成纯静态 HTML
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { getStorageDir } from './env';

const STORAGE_DIR = resolve(getStorageDir(), 'outputs');

function loadJson<T>(...pathParts: string[]): T | null {
  try {
    const fullPath = resolve(STORAGE_DIR, ...pathParts);
    const text = readFileSync(fullPath, 'utf-8')
      .replace(/:\s*NaN/g, ': null')
      .replace(/:\s*-NaN/g, ': null')
      .replace(/:\s*Infinity/g, ': null')
      .replace(/:\s*-Infinity/g, ': null');
    return JSON.parse(text);
  } catch {
    return null;
  }
}

export interface SignalData {
  status: string;
  scan_time: string;
  total_stocks: number;
  total_signals: number;
  stocks: StockWithSignals[];
}

export interface StockWithSignals {
  symbol: string;
  name: string;
  risk_score: number;
  risk_explanations: string[];
  signal_score: number;
  stage: string;
  dimension_breakdown?: {
    trend: number;
    momentum: number;
    volume: number;
  };
  has_buy_signal: boolean;
  signal_count: number;
  signals: StockSignal[];
}

export interface StockSignal {
  symbol: string;
  name: string;
  signal_type: 'left' | 'right';
  signal_name: string;
  strength: string;
  period: string;
  trigger_date: string;
  close_price: number;
  change_pct: number;
  volume_ratio: number;
  description: string;
  score: number;
  technicals: Record<string, any>;
}

export interface HealthScore {
  symbol: string;
  name: string;
  health_score: number;
  risk_level: string;
  risk_score: number;
  risk_explanations: string[];
  signal_score: number;
  has_buy_signal: boolean;
  signal_count: number;
  signals: StockSignal[];
  close_price: number;
  change_pct: number;
  dimension_score?: number;
  stage?: string;
  dimension_breakdown?: {
    trend: number;
    momentum: number;
    volume: number;
  };
}

export interface RiskAlert {
  symbol: string;
  name: string;
  health_score: number;
  risk_level: string;
  warnings: string[];
  recommendation: string;
  details: Record<string, any>;
}

export interface TechnicalData {
  date: string;
  total_zt_count: number;
  market_type: string;
  signals: any[];
  hot_sectors: HotSector[];
  technical_indicators: {
    market_breadth: Record<string, any>;
    index_performance: Record<string, any>;
  };
  macro_indicators: {
    currency: MacroIndicator;
    dxy: MacroIndicator;
    oil: MacroIndicator;
    gold: MacroIndicator;
  };
  index_history?: Record<string, any[]>;
  index_intraday?: Record<string, any[]>;
  price_fetch_time?: string;
  intraday_mode?: boolean;
  _generated_at?: string;
}

export interface HotSector {
  sector: string;
  zt_count: number;
  stocks: string[];
  lead_stock_code: string;
  lead_stock_name: string;
  sector_change_pct: number;
}

export interface MacroIndicator {
  current: number;
  change_pct: number;
  unit: string;
  source: string;
}

export interface PoolSummary {
  buy_count: number;
  sell_count: number;
  watch_count: number;
}

export interface PoolWatchData {
  date: string;
  summary: PoolSummary;
  stocks: PoolStock[];
}

export interface PoolStock {
  symbol: string;
  name: string;
  close: number;
  change_pct: number;
  ma5: number;
  ma10: number;
  ma20: number;
  ma60: number;
  signal: string;
  score: number;
}

// ===== 数据加载函数（构建时执行） =====

export function loadSignalsData(symbols?: string[]): SignalData | null {
  const data = loadJson<any>('shortterm', 'signals', 'signal_latest.json');
  if (!data) return null;

  if (symbols?.length && Array.isArray(data.signals)) {
    const set = new Set(symbols);
    const filtered = data.signals.filter((s: any) => set.has(s?.symbol));
    return {
      ...data,
      signals: filtered,
      total_signals: filtered.length,
      total_stocks: new Set(filtered.map((s: any) => s?.symbol)).size,
    };
  }

  return data;
}

export function loadTechnicalData(): TechnicalData | null {
  const data = loadJson<TechnicalData>('shortterm', 'technical_overview', 'latest.json');
  if (data) {
    data._generated_at = (data as any).price_fetch_time || data.date;
  }
  return data;
}

export function loadPoolWatchData(): PoolWatchData | null {
  return loadJson<PoolWatchData>('shortterm', 'pool_watch', 'latest.json');
}

export function loadLongTermWeights(): any[] | null {
  return loadJson<any[]>('longterm', 'weights', 'latest.json');
}

export function loadStockNames(): Record<string, string> {
  return loadJson<Record<string, string>>('shortterm', 'stock_names.json') || {};
}

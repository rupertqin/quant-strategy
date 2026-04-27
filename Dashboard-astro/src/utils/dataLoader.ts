/**
 * 数据加载工具 - 从预生成的 JSON 文件读取数据
 * 纯静态站点，所有数据在构建时或运行前同步到 public/data/
 */

const DATA_BASE = '/data';

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

// ===== 数据加载函数 =====

export async function loadSignalsData(): Promise<SignalData | null> {
  try {
    const res = await fetch(`${DATA_BASE}/signals/signal_latest.json`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function loadTechnicalData(): Promise<TechnicalData | null> {
  try {
    const res = await fetch(`${DATA_BASE}/technical/latest.json`);
    if (!res.ok) return null;
    const data = await res.json();
    data._generated_at = data.price_fetch_time || data.date;
    return data;
  } catch {
    return null;
  }
}

export async function loadPoolWatchData(): Promise<PoolWatchData | null> {
  try {
    const res = await fetch(`${DATA_BASE}/pool-watch/latest.json`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function loadLongTermWeights(): Promise<any[] | null> {
  try {
    const res = await fetch(`${DATA_BASE}/longterm/weights.json`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function loadStockHistory(symbol: string): Promise<any[] | null> {
  try {
    const res = await fetch(`${DATA_BASE}/prices/${symbol}.json`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function loadIndexHistory(symbol: string): Promise<any[] | null> {
  try {
    const res = await fetch(`${DATA_BASE}/index/${symbol}.json`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function loadStockNames(): Promise<Record<string, string>> {
  try {
    const res = await fetch(`${DATA_BASE}/stock_names.json`);
    if (!res.ok) return {};
    return await res.json();
  } catch {
    return {};
  }
}

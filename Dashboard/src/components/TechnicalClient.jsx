import { useState, useMemo } from 'react';
import IndexChart from './IndexChart.jsx';
import LoadingSpinner from './LoadingSpinner.jsx';
import { formatPercent, getChangeColor, formatPrice } from '../utils/formatters';

const INDEX_CODE_MAP = {
  '上证指数': '000001.SH',
  '深证成指': '399001.SZ',
  '创业板指': '399006.SZ',
  '沪深300': '000300.SH',
  '上证50': '000016.SH',
  '中证500': '000905.SH',
  '中证1000': '000852.SH',
};

const MACRO_ITEMS = [
  { key: 'currency', label: '💱 离岸人民币', unit: '' },
  { key: 'dxy', label: '📊 美元指数', unit: '' },
  { key: 'oil', label: '🛢️ 原油', unit: '美元/桶' },
  { key: 'gold', label: '🥇 黄金', unit: '美元/盎司' },
];

const PERIODS = [
  { label: '日线', key: 'daily' },
  { label: '周线', key: 'weekly' },
  { label: '月线', key: 'monthly' },
];

const REGIME_MAP = {
  'AGGRESSIVE': { emoji: '🟢', name: '积极进攻' },
  'DEFENSIVE': { emoji: '🔴', name: '防御避险' },
  'NEUTRAL': { emoji: '🟡', name: '震荡中性' },
  'UNKNOWN': { emoji: '⚪', name: '未知' },
};

const TREND_EMOJI = { BULL: '🟢', BEAR: '🔴', SIDEWAYS: '🟡', UNKNOWN: '⚪' };

function toDateString(row) {
  return String(row?.date || row?.trade_date || row?.time || '');
}

function takeLastRows(rows, limit) {
  if (!Array.isArray(rows)) return [];
  if (!limit || limit <= 0) return rows;
  return rows.slice(-limit);
}

function getWeekKey(dateStr) {
  const date = new Date(dateStr);
  const day = date.getDay();
  const diff = day === 0 ? -2 : day === 6 ? -1 : 5 - day;
  const friday = new Date(date);
  friday.setDate(date.getDate() + diff);
  return friday.toISOString().split('T')[0];
}

function getMonthKey(dateStr) {
  const date = new Date(dateStr);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function aggregateToPeriod(rows, period) {
  const map = new Map();
  const sorted = [...rows].sort((a, b) => toDateString(a).localeCompare(toDateString(b)));

  sorted.forEach((d) => {
    const dateStr = toDateString(d);
    if (!dateStr) return;

    const key = period === 'weekly' ? getWeekKey(dateStr) : getMonthKey(dateStr);
    const candleDate = period === 'monthly' ? dateStr : key;

    if (!map.has(key)) {
      map.set(key, {
        time: candleDate,
        date: candleDate,
        trade_date: candleDate,
        open: Number(d.open),
        high: Number(d.high),
        low: Number(d.low),
        close: Number(d.close),
        volume: Number(d.volume || 0),
      });
    } else {
      const item = map.get(key);
      item.high = Math.max(item.high, Number(d.high));
      item.low = Math.min(item.low, Number(d.low));
      item.close = Number(d.close);
      item.volume += Number(d.volume || 0);
      if (period === 'monthly') {
        item.time = dateStr;
        item.date = dateStr;
        item.trade_date = dateStr;
      }
    }
  });

  return Array.from(map.values()).sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));
}

function buildPeriodHistory(indexHistoryDaily, dailyLimit, weeklyLimit, monthlyLimit) {
  const result = { daily: {}, weekly: {}, monthly: {} };
  const source = indexHistoryDaily || {};

  for (const [name, rows] of Object.entries(source)) {
    const dailyRows = takeLastRows(rows, dailyLimit);
    const weeklySourceRows = weeklyLimit ? takeLastRows(rows, weeklyLimit * 5 + 15) : rows;
    const monthlySourceRows = monthlyLimit ? takeLastRows(rows, monthlyLimit * 21 + 35) : rows;

    const weeklyRows = aggregateToPeriod(weeklySourceRows, 'weekly');
    const monthlyRows = aggregateToPeriod(monthlySourceRows, 'monthly');

    result.daily[name] = dailyRows;
    result.weekly[name] = takeLastRows(weeklyRows, weeklyLimit);
    result.monthly[name] = takeLastRows(monthlyRows, monthlyLimit);
  }

  return result;
}

export default function TechnicalClient({
  initialData,
  dailyLimit = null,
  weeklyLimit = null,
  monthlyLimit = null,
}) {
  const [data] = useState(initialData);
  const [activePeriod, setActivePeriod] = useState('daily');

  if (!data) {
    return <p className="text-gray-500 py-8">暂无技术面数据</p>;
  }

  const ti = data.technical_indicators || {};
  const indexHistory = data.index_history || {};
  const indexIntraday = data.index_intraday || {};
  const periodHistory = buildPeriodHistory(indexHistory, dailyLimit, weeklyLimit, monthlyLimit);
  const indexPerformance = ti.index_performance || {};
  const hotSectors = data.hot_sectors || [];
  const signals = data.signals || [];
  const macro = data.macro_indicators || {};
  const marketBreadth = ti.market_breadth || {};
  const ztSentiment = ti.zt_sentiment || {};
  const dtSentiment = ti.dt_sentiment || {};
  const interValidation = ti.inter_index_validation || {};
  const compositeScore = ti.composite_score ?? data.composite_score ?? 50;
  const regime = data.regime || 'NEUTRAL';
  const ztCount = ztSentiment.zt_count ?? data.zt_count ?? 0;
  const dtCount = dtSentiment.dt_count ?? data.dt_count ?? 0;

  const getSentiment = () => {
    if (ztCount > dtCount * 5) return '🔥极热';
    if (ztCount > dtCount * 2) return '🟢活跃';
    if (ztCount > dtCount) return '🟡正常';
    return '🔴谨慎';
  };

  const getDtSentiment = () => {
    if (dtCount <= 0 || ztCount <= 0) return '🟡 平衡';
    const ratio = ztCount / dtCount;
    if (ratio >= 5) return '🔥 极热';
    if (ratio >= 2) return '🟢 活跃';
    if (ratio >= 1) return '🟡 平衡';
    return '🔴 恐慌';
  };

  const getDtColor = () => {
    if (dtCount <= 0 || ztCount <= 0) return 'text-gray-500';
    const ratio = ztCount / dtCount;
    if (ratio >= 2) return 'text-green-600';
    if (ratio >= 1) return 'text-yellow-600';
    return 'text-red-600';
  };

  const filteredIndexPerf = Object.fromEntries(
    Object.entries(indexPerformance).filter(([k]) => k !== '深证成指' && k !== 'inter_index_validation')
  );

  let mainIndex = null;
  let mainIndexName = '';
  for (const name of ['沪深300', '上证指数']) {
    const idx = filteredIndexPerf[name];
    if (idx?.analysis?.daily?.dow_theory) {
      mainIndex = idx;
      mainIndexName = name;
      break;
    }
  }

  const activeAnalysisKey = activePeriod;

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-[28px] font-bold text-gray-800 tracking-tight">🔥 今日技术面</h1>
        <p className="text-sm text-gray-500 mt-1">
          涨停板扫描 | 板块热度分析 | 市场状态监控
          {data.date && ` | 数据时间: ${String(data.date).replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3')}`}
          {data.data_status && ` [${data.data_status}]`}
        </p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">市场状态</p>
          <p className="text-lg font-bold text-gray-800">
            {REGIME_MAP[regime]?.emoji || '⚪'} {REGIME_MAP[regime]?.name || regime}
          </p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">综合评分</p>
          <p className="text-lg font-bold text-gray-800">{Math.round(compositeScore / 10)}/10</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">涨停:跌停</p>
          <p className="text-lg font-bold text-gray-800">{ztCount}:{dtCount}</p>
          <p className={`text-xs ${getDtColor()}`}>{getSentiment()}</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">热点板块</p>
          <p className="text-lg font-bold text-gray-800">{hotSectors.length}</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">涨停总数</p>
          <p className="text-lg font-bold text-gray-800">{data.total_zt_count ?? 0}</p>
          <p className="text-xs text-gray-500">{data.market_type || ''}</p>
        </div>
      </div>

      <h2 className="text-lg font-bold mb-3 text-gray-800">🌍 宏观指标</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        {MACRO_ITEMS.map(item => {
          const m = macro[item.key];
          const current = m?.current ?? 0;
          const change = m?.change_pct ?? 0;
          return (
            <div className="metric-card text-center" key={item.key}>
              <p className="text-xs text-gray-500">{item.label}</p>
              <p className="text-lg font-bold text-gray-800">
                {current > 0 ? `${current.toFixed(item.key === 'currency' ? 4 : 2)} ${item.unit}` : '--'}
              </p>
              {current > 0 && (
                <p className={`text-xs ${getChangeColor(change)}`}>
                  {change > 0 ? '+' : ''}{change.toFixed(2)}%
                </p>
              )}
            </div>
          );
        })}
      </div>

      <h2 className="text-lg font-bold mb-3 text-gray-800">📊 技术面分析</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">技术面评分</p>
          <p className="text-lg font-bold text-gray-800">{compositeScore}/100</p>
          <p className={`text-xs ${compositeScore >= 70 ? 'text-green-600' : compositeScore >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
            {compositeScore >= 70 ? '🟢 积极' : compositeScore >= 50 ? '🟡 中性' : '🔴 谨慎'}
          </p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">涨跌家数</p>
          <p className="text-lg font-bold text-gray-800">{marketBreadth.up_count ?? 0}:{marketBreadth.down_count ?? 0}</p>
          <p className="text-xs text-gray-500">
            ▲ 上涨{(marketBreadth.up_ratio ?? 0.5).toFixed(1)}%
          </p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">市场情绪</p>
          <p className="text-lg font-bold text-gray-800">{marketBreadth.interpretation || '未知'}</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-xs text-gray-500">涨跌停比</p>
          <p className="text-lg font-bold text-gray-800">{ztCount}:{dtCount}</p>
          <p className={`text-xs ${getDtColor()}`}>{getDtSentiment()}</p>
        </div>
      </div>

      <h2 className="text-lg font-bold mb-3 text-gray-800">📈 指数走势 & 技术分析</h2>

      <div className="flex gap-2 mb-4">
        {PERIODS.map(p => (
          <button
            key={p.key}
            onClick={() => setActivePeriod(p.key)}
            className={`tab-btn ${activePeriod === p.key ? 'active' : ''}`}
          >
            📊 {p.label}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        {Object.entries(periodHistory[activePeriod] || {}).map(([name, histData]) => {
          if (name === '深证成指') return null;
          const code = INDEX_CODE_MAP[name] || '';
          const idx = filteredIndexPerf[name];
          const analysis = idx?.analysis?.[activeAnalysisKey];
          const elliott = analysis?.elliott_wave;
          const peaks = elliott?.structure?.recent_peaks || [];
          const troughs = elliott?.structure?.recent_troughs || [];

          return (
            <div className="metric-card" key={name}>
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  {code ? (
                    <a href={`/stock/${code}/`} className="font-bold text-gray-800 hover:text-primary-600 transition-colors">
                      {name}
                    </a>
                  ) : (
                    <span className="font-bold text-gray-800">{name}</span>
                  )}
                  {idx?.change_pct !== undefined && (
                    <span className={`text-sm font-medium ${getChangeColor(idx.change_pct)}`}>
                      {idx.change_pct > 0 ? '+' : ''}{idx.change_pct.toFixed(2)}%
                    </span>
                  )}
                </div>
              </div>
              <IndexChart histData={histData} intradayData={indexIntraday[name] || []} name={name} />
              <div className="mt-2 text-xs text-gray-500 flex gap-3">
                {peaks.length > 0 && (
                  <span>📈 最近峰值: {peaks[peaks.length - 1][1]?.toFixed(2)} ({peaks[peaks.length - 1][0]})</span>
                )}
                {troughs.length > 0 && (
                  <span>📉 最近谷值: {troughs[troughs.length - 1][1]?.toFixed(2)} ({troughs[troughs.length - 1][0]})</span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      <h3 className="text-md font-bold mb-2 text-gray-800">📊 道氏理论概览 ({PERIODS.find(p => p.key === activePeriod)?.label})</h3>
      <div className="metric-card overflow-x-auto mb-6">
        <table className="data-table">
          <thead>
            <tr>
              <th>指数</th>
              <th>主要趋势</th>
              <th>次要趋势</th>
              <th>趋势强度</th>
              <th>区间位置</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(filteredIndexPerf).map(([name, idxData]) => {
              const analysis = idxData?.analysis?.[activeAnalysisKey];
              const dow = analysis?.dow_theory;
              if (!dow || !dow.primary_trend) return null;
              const strength = dow.trend_strength || {};
              return (
                <tr key={name}>
                  <td className="font-medium text-gray-800">{name}</td>
                  <td>
                    {TREND_EMOJI[dow.primary_trend] || '⚪'} {dow.primary_desc || dow.primary_trend}
                  </td>
                  <td>{dow.secondary_desc || '未知'}</td>
                  <td>ADX: {strength.adx ?? 0} ({strength.strength || 'weak'})</td>
                  <td>{((dow.position_in_range ?? 0) * 100).toFixed(0)}%</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <h3 className="text-md font-bold mb-2 text-gray-800">🌊 波浪理论概览 ({PERIODS.find(p => p.key === activePeriod)?.label})</h3>
      <div className="metric-card overflow-x-auto mb-6">
        <table className="data-table">
          <thead>
            <tr>
              <th>指数</th>
              <th>当前阶段</th>
              <th>最近峰值</th>
              <th>最近谷值</th>
              <th>距峰值</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(filteredIndexPerf).map(([name, idxData]) => {
              const analysis = idxData?.analysis?.[activeAnalysisKey];
              const wave = analysis?.elliott_wave;
              if (!wave || !wave.current_phase) return null;
              return (
                <tr key={name}>
                  <td className="font-medium text-gray-800">{name}</td>
                  <td>{wave.current_phase}</td>
                  <td>{wave.last_peak || '-'}</td>
                  <td>{wave.last_trough || '-'}</td>
                  <td>{(wave.current_vs_peak ?? 0).toFixed(1)}%</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {mainIndex && (
        <div className="metric-card mb-6">
          <h3 className="text-md font-bold mb-3 text-gray-800">{mainIndexName} {PERIODS.find(p => p.key === activePeriod)?.label} 详细分析</h3>

          {(() => {
            const analysis = mainIndex.analysis?.[activeAnalysisKey];
            const dow = analysis?.dow_theory;
            const elliott = analysis?.elliott_wave;
            if (!dow || !dow.primary_trend) return <p className="text-gray-500">分析数据不足</p>;

            const strength = dow.trend_strength || {};
            const adx = strength.adx ?? 0;
            const volSignal = dow.volume_signal || 'neutral';
            const volMap = { confirming: { emoji: '✅', text: '确认趋势' }, warning: { emoji: '⚠️', text: '背离警示' }, neutral: { emoji: '➖', text: '中性' } };

            return (
              <div className="space-y-4">
                <div>
                  <p className="text-sm font-semibold text-gray-700 mb-1">道氏理论</p>
                  <div className="grid grid-cols-2 gap-2 text-sm text-gray-600">
                    <p>主要趋势: {TREND_EMOJI[dow.primary_trend] || '⚪'} {dow.primary_desc || dow.primary_trend}</p>
                    <p>次要趋势: {dow.secondary_desc || '未知'}</p>
                  </div>
                  <div className="mt-2">
                    <div className="flex items-center gap-2 text-sm">
                      <span className="text-gray-500">趋势强度 ADX:</span>
                      <div className="flex-1 bg-gray-200 rounded-full h-2">
                        <div className="bg-gradient-to-r from-blue-500 to-purple-600 h-2 rounded-full" style={{ width: `${Math.min(adx, 100)}%` }} />
                      </div>
                      <span className="font-medium text-gray-700">{adx} ({strength.strength || 'weak'})</span>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    成交量信号: {volMap[volSignal]?.emoji || '➖'} {volMap[volSignal]?.text || '中性'}
                  </p>
                </div>

                {elliott && elliott.current_phase && (
                  <div>
                    <p className="text-sm font-semibold text-gray-700 mb-1">波浪理论</p>
                    <p className="text-sm text-gray-600">当前阶段: {elliott.current_phase}</p>
                    {elliott.structure && (
                      <p className="text-xs text-gray-500 mt-1">
                        波动率: {(elliott.structure.volatility_pct ?? 0).toFixed(2)}%
                      </p>
                    )}
                    {elliott.structure?.fib_382 != null && (
                      <div className="grid grid-cols-3 gap-2 mt-2">
                        <div className="text-center bg-gray-50 rounded-lg p-2">
                          <p className="text-xs text-gray-500">38.2%</p>
                          <p className="font-bold text-gray-800">{elliott.structure.fib_382.toFixed(0)}</p>
                        </div>
                        <div className="text-center bg-gray-50 rounded-lg p-2">
                          <p className="text-xs text-gray-500">50.0%</p>
                          <p className="font-bold text-gray-800">{elliott.structure.fib_500.toFixed(0)}</p>
                        </div>
                        <div className="text-center bg-gray-50 rounded-lg p-2">
                          <p className="text-xs text-gray-500">61.8%</p>
                          <p className="font-bold text-gray-800">{elliott.structure.fib_618.toFixed(0)}</p>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}

      {interValidation && activePeriod === 'daily' && (
        <div className="metric-card mb-6">
          <h3 className="text-md font-bold mb-2 text-gray-800">指数验证</h3>
          <p className="text-sm mb-2 text-gray-700">
            {({ CONFIRMED: '✅', PARTIAL: '⚠️', DIVERGENCE: '❌' })[interValidation.validation] || '➖'} {interValidation.note || ''}
          </p>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-gray-500">一致性:</span>
            <div className="flex-1 bg-gray-200 rounded-full h-2 max-w-xs">
              <div className="bg-gradient-to-r from-green-400 to-green-600 h-2 rounded-full" style={{ width: `${(interValidation.consistency ?? 0) * 100}%` }} />
            </div>
            <span className="font-medium text-gray-700">{((interValidation.consistency ?? 0) * 100).toFixed(0)}%</span>
          </div>
        </div>
      )}

      <h2 className="text-lg font-bold mb-3 text-gray-800">🔥 热点板块</h2>
      {hotSectors.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
          {hotSectors.map(sector => (
            <div className="hot-sector-card" key={sector.sector}>
              <div className="flex justify-between items-start mb-2">
                <h3 className="font-bold text-lg">{sector.sector}</h3>
                <span className="bg-white/20 px-2 py-1 rounded-md text-sm">
                  涨停 {sector.zt_count} 家
                </span>
              </div>
              <div className="text-sm opacity-90 mb-2">
                龙头股: {sector.lead_stock_code} {sector.lead_stock_name ? `(${sector.lead_stock_name})` : ''}
              </div>
              <div className="text-sm opacity-80">
                个股: {sector.stocks?.slice(0, 5).join(', ')}
                {sector.stocks?.length > 5 && ` 等${sector.stocks.length}只`}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="text-gray-500 mb-8">暂无热点板块数据</p>
      )}

      <h2 className="text-lg font-bold mb-3 text-gray-800">📋 涨停信号列表</h2>
      {signals.length > 0 ? (
        <div className="metric-card overflow-x-auto">
          <table className="data-table">
            <thead>
              <tr>
                <th>代码</th>
                <th>名称</th>
                <th>信号</th>
                <th className="text-right">价格</th>
                <th className="text-right">涨跌幅</th>
                <th>描述</th>
              </tr>
            </thead>
            <tbody>
              {signals.map((sig, i) => (
                <tr key={i}>
                  <td className="font-medium text-gray-800">{sig.symbol}</td>
                  <td>{sig.name}</td>
                  <td>
                    <span className={`px-2 py-0.5 rounded-md text-xs ${sig.signal_type === 'left' ? 'bg-amber-50 text-amber-700 border border-amber-200' : 'bg-emerald-50 text-emerald-700 border border-emerald-200'}`}>
                      {sig.signal_type === 'left' ? '📉 左侧' : '📈 右侧'} {sig.signal_name}
                    </span>
                  </td>
                  <td className="text-right font-mono text-gray-700">{formatPrice(sig.close_price)}</td>
                  <td className={`text-right font-mono ${getChangeColor(sig.change_pct)}`}>
                    {formatPercent(sig.change_pct)}
                  </td>
                  <td className="text-xs text-gray-600">{sig.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="text-gray-500">暂无涨停信号</p>
      )}
    </div>
  );
}

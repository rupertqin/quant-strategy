import { useState, useEffect, useMemo } from 'react';
import SignalTable from './SignalTable.jsx';
import { useStockPool } from '../hooks/useStockPool.js';

export default function SignalWatchClient() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  const { poolSet, add, remove } = useStockPool();

  useEffect(() => {
    fetch('/data/signals/signal_latest.json')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.text();
      })
      .then(text => {
        // Python JSON may contain NaN/Infinity which JS JSON.parse doesn't support
        const cleaned = text
          .replace(/: NaN/g, ': null')
          .replace(/: -NaN/g, ': null')
          .replace(/: Infinity/g, ': null')
          .replace(/: -Infinity/g, ': null');
        return JSON.parse(cleaned);
      })
      .then(json => {
        console.log('[SignalWatch] loaded data, stocks count:', json?.stocks?.length);
        console.log('[SignalWatch] total_signals:', json?.total_signals);
        setData(json);
        setLoading(false);
      })
      .catch(err => {
        console.error('加载信号数据失败:', err);
        setLoading(false);
      });
  }, []);

  const stocks = data?.stocks || [];
  const signals = useMemo(() => stocks.flatMap(s => s.signals || []), [stocks]);

  const healthScores = useMemo(() => {
    return stocks.map(s => ({
      symbol: s.symbol,
      name: s.name,
      health_score: s.signal_score ?? 0,
      risk_level: s.risk_score > 50 ? 'high' : s.risk_score > 25 ? 'medium' : 'low',
      risk_score: s.risk_score ?? 0,
      risk_explanations: s.risk_explanations || [],
      signal_score: s.signal_score ?? 0,
      has_buy_signal: s.has_buy_signal ?? false,
      signal_count: s.signal_count ?? 0,
      signals: s.signals || [],
      close_price: s.signals?.[0]?.close_price,
      change_pct: s.signals?.[0]?.change_pct,
      stage: s.stage,
      dimension_breakdown: s.dimension_breakdown,
    }));
  }, [stocks]);

  const riskAlerts = useMemo(() => healthScores.filter(h => h.risk_score > 50), [healthScores]);
  const totalSignals = data?.total_signals || signals.length;
  const totalStocks = data?.total_stocks || stocks.length;
  const leftCount = signals.filter(s => s.signal_type === 'left').length;
  const rightCount = signals.filter(s => s.signal_type === 'right').length;
  const dailyCount = signals.filter(s => s.period === 'daily').length;
  const weeklyCount = signals.filter(s => s.period === 'weekly').length;

  if (loading) {
    return <p className="text-gray-500 py-8">加载中...</p>;
  }

  if (!data) {
    return <p className="text-gray-500 py-8">暂无信号数据 (data is null)</p>;
  }

  if (stocks.length === 0) {
    return <p className="text-gray-500 py-8">数据已加载但 stocks 为空</p>;
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-[28px] font-bold text-gray-800 tracking-tight">个股信号监控</h1>
        <p className="text-sm text-gray-500 mt-1">
          扫描 {totalStocks} 只股票，发现 {totalSignals} 个信号
          {data?.scan_time && ` | 扫描时间: ${data.scan_time}`}
        </p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
        <div className="metric-card text-center">
          <p className="text-2xl font-bold text-gray-800">{totalSignals}</p>
          <p className="text-xs text-gray-500 mt-1">总信号数</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-2xl font-bold text-amber-600">{leftCount}</p>
          <p className="text-xs text-gray-500 mt-1">左侧信号</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-2xl font-bold text-emerald-600">{rightCount}</p>
          <p className="text-xs text-gray-500 mt-1">右侧信号</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-2xl font-bold text-blue-600">{dailyCount}</p>
          <p className="text-xs text-gray-500 mt-1">日线信号</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-2xl font-bold text-purple-600">{weeklyCount}</p>
          <p className="text-xs text-gray-500 mt-1">周线信号</p>
        </div>
      </div>

      {riskAlerts.length > 0 && (
        <div className="mb-6">
          <h2 className="text-lg font-bold mb-3 text-gray-800">⚠️ 风险预警 ({riskAlerts.length}只)</h2>
          <div className="bg-red-50 border border-red-200 rounded-xl p-4">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {riskAlerts.slice(0, 12).map(alert => (
                <div key={alert.symbol} className="bg-white rounded-lg p-3 border-l-4 border-red-500 shadow-sm">
                  <div className="flex justify-between">
                    <span className="font-bold text-sm text-gray-800">{alert.symbol} {alert.name}</span>
                    <span className="text-xs text-red-600 font-medium">健康度 {alert.health_score}</span>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    {alert.risk_explanations?.join('；') || '风险提示'}
                  </p>
                </div>
              ))}
            </div>
            {riskAlerts.length > 12 && (
              <p className="text-sm text-gray-500 mt-3">还有 {riskAlerts.length - 12} 只风险股票...</p>
            )}
          </div>
        </div>
      )}

      <div className="metric-card">
        <h2 className="text-lg font-bold mb-4 text-gray-800">📋 信号列表</h2>
        <SignalTable
          stocks={stocks}
          healthScores={healthScores}
          showPoolActions={true}
          onAddToPool={add}
          onRemoveFromPool={remove}
          poolSet={poolSet}
        />
      </div>
    </div>
  );
}

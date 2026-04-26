import { useState, useMemo } from 'react';
import { ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Plus, Minus } from 'lucide-react';

export default function SignalTable({
  stocks,
  healthScores,
  showPoolActions = false,
  onAddToPool,
  onRemoveFromPool,
  poolSet = new Set(),
}) {
  const [filterType, setFilterType] = useState('all');
  const [filterStage, setFilterStage] = useState('all');
  const [sortBy, setSortBy] = useState('score-desc');
  const [currentPage, setCurrentPage] = useState(0);
  const [searchQuery, setSearchQuery] = useState('');
  const [expanded, setExpanded] = useState(new Set());
  const pageSize = 20;

  const healthMap = useMemo(() => {
    const map = {};
    healthScores.forEach(h => { map[h.symbol] = h; });
    return map;
  }, [healthScores]);

  const filtered = useMemo(() => {
    let result = [...stocks];

    if (searchQuery.trim()) {
      const q = searchQuery.trim().toLowerCase();
      result = result.filter(s =>
        s.symbol.toLowerCase().includes(q) ||
        (s.name && s.name.toLowerCase().includes(q))
      );
    }

    if (filterType !== 'all') {
      result = result.filter(s => {
        const signals = s.signals || [];
        if (filterType === 'left') return signals.some(sig => sig.signal_type === 'left');
        if (filterType === 'right') return signals.some(sig => sig.signal_type === 'right');
        return true;
      });
    }

    if (filterStage !== 'all') {
      result = result.filter(s => s.stage === filterStage);
    }

    result.sort((a, b) => {
      const ha = healthMap[a.symbol];
      const hb = healthMap[b.symbol];
      switch (sortBy) {
        case 'score-desc': return (b.signal_score ?? 0) - (a.signal_score ?? 0);
        case 'score-asc': return (a.signal_score ?? 0) - (b.signal_score ?? 0);
        case 'risk-desc': return (hb?.risk_score ?? 0) - (ha?.risk_score ?? 0);
        case 'risk-asc': return (ha?.risk_score ?? 0) - (hb?.risk_score ?? 0);
        case 'signal-count': return (b.signal_count ?? 0) - (a.signal_count ?? 0);
        default: return (b.signal_score ?? 0) - (a.signal_score ?? 0);
      }
    });

    return result;
  }, [stocks, filterType, filterStage, sortBy, searchQuery, healthMap]);

  const totalPages = Math.ceil(filtered.length / pageSize);
  const pageData = filtered.slice(currentPage * pageSize, (currentPage + 1) * pageSize);

  if (currentPage >= totalPages && totalPages > 0) {
    setCurrentPage(0);
  }

  const toggleExpand = (symbol) => {
    setExpanded(prev => {
      const next = new Set(prev);
      if (next.has(symbol)) next.delete(symbol);
      else next.add(symbol);
      return next;
    });
  };

  const formatPct = v => v === undefined || v === null ? 'N/A' : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
  const formatPrice = v => v === undefined || v === null ? 'N/A' : `¥${v.toFixed(2)}`;

  const getChangeColor = v => {
    if (v > 0) return 'text-red-600';
    if (v < 0) return 'text-green-600';
    return 'text-gray-500';
  };

  const getStageBadge = stage => {
    const map = {
      left: { cls: 'bg-amber-100 text-amber-700', label: '📉 左侧' },
      right: { cls: 'bg-emerald-100 text-emerald-700', label: '📈 右侧' },
    };
    const s = map[stage] || { cls: 'bg-gray-100 text-gray-600', label: stage || '-' };
    return <span className={`px-2 py-0.5 rounded text-xs ${s.cls}`}>{s.label}</span>;
  };

  const getStrengthBadge = strength => {
    const map = {
      strong: 'bg-red-100 text-red-700',
      medium: 'bg-orange-100 text-orange-700',
      weak: 'bg-gray-100 text-gray-600',
    };
    const label = { strong: '强', medium: '中', weak: '弱' };
    return (
      <span className={`px-2 py-0.5 rounded text-xs ${map[strength] || map.weak}`}>
        {label[strength] || strength}
      </span>
    );
  };

  const getRiskColor = score => {
    if (score > 50) return 'text-red-600';
    if (score > 25) return 'text-orange-600';
    return 'text-green-600';
  };

  const handlePoolAction = (e, symbol, inPool) => {
    e.stopPropagation();
    if (inPool) {
      onRemoveFromPool?.(symbol);
    } else {
      onAddToPool?.(symbol);
    }
  };

  const colSpan = showPoolActions ? 10 : 9;

  return (
    <div>
      {/* 工具栏 */}
      <div className="flex flex-wrap gap-3 mb-4">
        <input
          type="text"
          placeholder="搜索代码/名称"
          value={searchQuery}
          onChange={e => { setSearchQuery(e.target.value); setCurrentPage(0); }}
          className="px-3 py-2 border border-gray-200 rounded-lg text-sm w-40 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <select
          value={filterType}
          onChange={e => { setFilterType(e.target.value); setCurrentPage(0); }}
          className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">全部类型</option>
          <option value="left">含左侧信号</option>
          <option value="right">含右侧信号</option>
        </select>
        <select
          value={filterStage}
          onChange={e => { setFilterStage(e.target.value); setCurrentPage(0); }}
          className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">全部阶段</option>
          <option value="left">左侧阶段</option>
          <option value="right">右侧阶段</option>
        </select>
        <select
          value={sortBy}
          onChange={e => setSortBy(e.target.value)}
          className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="score-desc">信号分 (高→低)</option>
          <option value="score-asc">信号分 (低→高)</option>
          <option value="risk-desc">风险分 (高→低)</option>
          <option value="risk-asc">风险分 (低→高)</option>
          <option value="signal-count">信号数 (多→少)</option>
        </select>
      </div>

      {/* 表格 */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-gray-600">
              <th className="px-3 py-2 text-left font-semibold w-8"></th>
              <th className="px-3 py-2 text-left font-semibold">股票</th>
              <th className="px-3 py-2 text-left font-semibold">阶段</th>
              <th className="px-3 py-2 text-right font-semibold">价格</th>
              <th className="px-3 py-2 text-right font-semibold">涨跌</th>
              <th className="px-3 py-2 text-center font-semibold">信号数</th>
              <th className="px-3 py-2 text-center font-semibold">风险分</th>
              <th className="px-3 py-2 text-center font-semibold">信号分</th>
              <th className="px-3 py-2 text-left font-semibold">维度拆解</th>
              {showPoolActions && (
                <th className="px-3 py-2 text-center font-semibold w-20">操作</th>
              )}
            </tr>
          </thead>
          <tbody>
            {pageData.map(stock => {
              const health = healthMap[stock.symbol];
              const isExpanded = expanded.has(stock.symbol);
              const signals = stock.signals || [];
              const firstSignal = signals[0];
              const inPool = poolSet.has(stock.symbol);

              return (
                <>
                  <tr
                    key={stock.symbol}
                    className="border-b border-gray-100 hover:bg-gray-50 cursor-pointer"
                    onClick={() => toggleExpand(stock.symbol)}
                  >
                    <td className="px-3 py-3">
                      {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                    </td>
                    <td className="px-3 py-3">
                      <div>
                        <a
                          href={`/stock-chart/?symbol=${stock.symbol}`}
                          className="font-bold text-blue-600 hover:underline"
                          onClick={e => e.stopPropagation()}
                        >
                          {stock.symbol}
                        </a>
                        {stock.name && <span className="text-xs text-gray-500 ml-1">{stock.name}</span>}
                      </div>
                    </td>
                    <td className="px-3 py-3">{getStageBadge(stock.stage)}</td>
                    <td className="px-3 py-3 text-right font-mono">{formatPrice(firstSignal?.close_price ?? stock.close_price)}</td>
                    <td className={`px-3 py-3 text-right font-mono ${getChangeColor(firstSignal?.change_pct ?? stock.change_pct)}`}>
                      {formatPct(firstSignal?.change_pct ?? stock.change_pct)}
                    </td>
                    <td className="px-3 py-3 text-center">
                      <span className="font-bold">{stock.signal_count || signals.length}</span>
                    </td>
                    <td className="px-3 py-3 text-center">
                      <span className={`font-bold ${getRiskColor(health?.risk_score ?? 0)}`}>
                        {health?.risk_score ?? '-'}
                      </span>
                    </td>
                    <td className="px-3 py-3 text-center">
                      <span className="font-bold text-blue-600">{stock.signal_score ?? '-'}</span>
                    </td>
                    <td className="px-3 py-3 text-xs">
                      {stock.dimension_breakdown ? (
                        <div className="flex gap-2">
                          <span className="text-blue-600">趋势 {stock.dimension_breakdown.trend}</span>
                          <span className="text-purple-600">动量 {stock.dimension_breakdown.momentum}</span>
                          <span className="text-orange-600">量能 {stock.dimension_breakdown.volume}</span>
                        </div>
                      ) : (
                        <span className="text-gray-400">-</span>
                      )}
                    </td>
                    {showPoolActions && (
                      <td className="px-3 py-3 text-center">
                        <button
                          onClick={(e) => handlePoolAction(e, stock.symbol, inPool)}
                          className={`inline-flex items-center justify-center w-7 h-7 rounded-full text-xs font-bold transition-colors ${
                            inPool
                              ? 'bg-red-50 text-red-600 hover:bg-red-100 border border-red-200'
                              : 'bg-blue-50 text-blue-600 hover:bg-blue-100 border border-blue-200'
                          }`}
                          title={inPool ? '从股票池移除' : '添加到股票池'}
                        >
                          {inPool ? <Minus size={14} /> : <Plus size={14} />}
                        </button>
                      </td>
                    )}
                  </tr>

                  {/* 展开详情 */}
                  {isExpanded && (
                    <tr key={`${stock.symbol}-detail`}>
                      <td colSpan={colSpan} className="px-3 py-4 bg-gray-50">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                          {/* 左侧：风险详情 */}
                          <div>
                            <p className="text-xs font-semibold text-gray-500 mb-2">⚠️ 风险详情</p>
                            {health?.risk_explanations?.length > 0 ? (
                              <div className="space-y-2">
                                {health.risk_explanations.map((exp, i) => (
                                  <div key={i} className="text-xs px-3 py-2 rounded bg-red-50 text-red-700 border border-red-100">
                                    {exp}
                                  </div>
                                ))}
                              </div>
                            ) : (
                              <p className="text-xs text-gray-400">暂无风险详情</p>
                            )}
                            {health?.risk_recommendation && (
                              <p className="text-xs text-gray-500 mt-2">
                                建议: {health.risk_recommendation}
                              </p>
                            )}
                          </div>

                          {/* 右侧：信号列表 */}
                          <div>
                            <p className="text-xs font-semibold text-gray-500 mb-2">📋 信号详情 ({signals.length}个)</p>
                            {signals.length === 0 ? (
                              <p className="text-xs text-gray-400">暂无信号</p>
                            ) : (
                            <div className="space-y-2">
                              {signals.map((sig, i) => (
                                <div key={i} className="bg-white rounded p-3 border border-gray-200">
                                  <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                      {sig.signal_type === 'left' ? (
                                        <span className="px-2 py-0.5 rounded text-xs bg-amber-100 text-amber-700">📉 左侧</span>
                                      ) : (
                                        <span className="px-2 py-0.5 rounded text-xs bg-emerald-100 text-emerald-700">📈 右侧</span>
                                      )}
                                      <span className="font-semibold text-sm">{sig.signal_name}</span>
                                      {getStrengthBadge(sig.strength)}
                                      <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-600">
                                        {sig.period === 'daily' ? '日线' : sig.period === 'weekly' ? '周线' : '月线'}
                                      </span>
                                    </div>
                                    <span className="text-xs text-gray-400">{sig.trigger_date}</span>
                                  </div>
                                  <p className="text-xs text-gray-600 mt-1">{sig.description}</p>
                                  <div className="flex gap-4 mt-1 text-xs text-gray-500">
                                    <span>信号分: <b>{sig.score}</b></span>
                                    <span>量比: <b>{sig.volume_ratio?.toFixed(2) ?? '-'}</b></span>
                                    <span>价格: {formatPrice(sig.close_price)}</span>
                                    <span className={getChangeColor(sig.change_pct)}>{formatPct(sig.change_pct)}</span>
                                  </div>
                                </div>
                              ))}
                            </div>
                            )}
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              );
            })}
          </tbody>
        </table>
      </div>

      {filtered.length === 0 && (
        <p className="text-center text-gray-500 py-8">没有匹配的股票</p>
      )}

      {/* 分页 */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-100">
          <p className="text-sm text-gray-500">
            共 {filtered.length} 只，第 {currentPage + 1}/{totalPages} 页
          </p>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
              disabled={currentPage === 0}
              className="p-2 rounded-lg border border-gray-200 disabled:opacity-30 hover:bg-gray-50"
            >
              <ChevronLeft size={16} />
            </button>
            <span className="text-sm px-2">
              {currentPage + 1} / {totalPages}
            </span>
            <button
              onClick={() => setCurrentPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={currentPage >= totalPages - 1}
              className="p-2 rounded-lg border border-gray-200 disabled:opacity-30 hover:bg-gray-50"
            >
              <ChevronRight size={16} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

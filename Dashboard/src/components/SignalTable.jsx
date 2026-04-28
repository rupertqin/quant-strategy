import { useState, useMemo, Fragment } from 'react';
import { ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Plus, Minus } from 'lucide-react';
import StockSignals from './StockSignals.jsx';

export default function SignalTable({
  stocks,
  healthScores,
  showPoolActions = false,
  onAddToPool,
  onRemoveFromPool,
  poolSet = new Set(),
  dataMode,
  updateTime,
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
    if (v > 0) return 'text-fin-up';
    if (v < 0) return 'text-fin-down';
    return 'text-gray-500';
  };

  const getStageBadge = stage => {
    const map = {
      left: { cls: 'bg-amber-50 text-amber-600 border border-amber-200/60', label: '📉 左侧' },
      right: { cls: 'bg-emerald-50 text-emerald-600 border border-emerald-200/60', label: '📈 右侧' },
    };
    const s = map[stage] || { cls: 'bg-slate-50 text-slate-500 border border-slate-200/60', label: stage || '-' };
    return <span className={`px-2 py-1 rounded-md text-[12px] font-medium tracking-wide ${s.cls}`}>{s.label}</span>;
  };

  const getStrengthBadge = strength => {
    const map = {
      strong: 'bg-red-50 text-red-700 border border-red-200',
      medium: 'bg-orange-50 text-orange-700 border border-orange-200',
      weak: 'bg-gray-50 text-gray-600 border border-gray-200',
    };
    const label = { strong: '强', medium: '中', weak: '弱' };
    return (
      <span className={`px-2 py-0.5 rounded-md text-xs ${map[strength] || map.weak}`}>
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
      <div className="flex flex-wrap gap-3 mb-5">
        <input
          type="text"
          placeholder="搜索代码/名称"
          value={searchQuery}
          onChange={e => { setSearchQuery(e.target.value); setCurrentPage(0); }}
          className="px-4 py-2 border border-slate-200 rounded-lg text-[14px] w-48 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all text-slate-700 placeholder:text-slate-400 shadow-sm"
        />
        <select
          value={filterType}
          onChange={e => { setFilterType(e.target.value); setCurrentPage(0); }}
          className="px-4 py-2 border border-slate-200 rounded-lg text-[14px] focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 bg-white transition-all text-slate-700 shadow-sm cursor-pointer"
        >
          <option value="all">全部类型</option>
          <option value="left">含左侧信号</option>
          <option value="right">含右侧信号</option>
        </select>
        <select
          value={filterStage}
          onChange={e => { setFilterStage(e.target.value); setCurrentPage(0); }}
          className="px-4 py-2 border border-slate-200 rounded-lg text-[14px] focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 bg-white transition-all text-slate-700 shadow-sm cursor-pointer"
        >
          <option value="all">全部阶段</option>
          <option value="left">左侧阶段</option>
          <option value="right">右侧阶段</option>
        </select>
        <select
          value={sortBy}
          onChange={e => setSortBy(e.target.value)}
          className="px-4 py-2 border border-slate-200 rounded-lg text-[14px] focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 bg-white transition-all text-slate-700 shadow-sm cursor-pointer"
        >
          <option value="score-desc">信号分 (高→低)</option>
          <option value="score-asc">信号分 (低→高)</option>
          <option value="risk-desc">风险分 (高→低)</option>
          <option value="risk-asc">风险分 (低→高)</option>
          <option value="signal-count">信号数 (多→少)</option>
        </select>
      </div>

      {/* 表格 */}
      <div className="overflow-x-auto bg-white rounded-xl border border-slate-200 shadow-sm">
        <table className="data-table">
          <thead>
            <tr>
              <th className="w-8"></th>
              <th>股票</th>
              <th>阶段</th>
              <th className="text-right">价格</th>
              <th className="text-right">涨跌</th>
              <th className="text-center">信号数</th>
              <th className="text-center">风险分</th>
              <th className="text-center">信号分</th>
              <th>维度拆解</th>
              {showPoolActions && (
                <th className="text-center w-20">操作</th>
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {pageData.map(stock => {
              const health = healthMap[stock.symbol];
              const isExpanded = expanded.has(stock.symbol);
              const signals = stock.signals || [];
              const firstSignal = signals[0];
              const inPool = poolSet.has(stock.symbol);

              return (
                <Fragment key={stock.symbol}>
                  <tr
                    className="cursor-pointer group"
                    onClick={() => toggleExpand(stock.symbol)}
                  >
                    <td className="w-8">
                      {isExpanded ? <ChevronUp size={16} className="text-slate-400 group-hover:text-blue-500 transition-colors" /> : <ChevronDown size={16} className="text-slate-400 group-hover:text-blue-500 transition-colors" />}
                    </td>
                    <td>
                      <div className="flex flex-col">
                        <a
                          href={`/stock/${stock.symbol}/`}
                          className="font-bold text-slate-800 hover:text-blue-600 transition-colors"
                          onClick={e => e.stopPropagation()}
                        >
                          {stock.symbol}
                        </a>
                        {stock.name && <span className="text-[12px] text-slate-500">{stock.name}</span>}
                      </div>
                    </td>
                    <td>{getStageBadge(stock.stage)}</td>
                    <td className="text-right font-mono text-slate-700 font-medium">{formatPrice(firstSignal?.close_price ?? stock.close_price)}</td>
                    <td className={`text-right font-mono font-medium ${getChangeColor(firstSignal?.change_pct ?? stock.change_pct)}`}>
                      {formatPct(firstSignal?.change_pct ?? stock.change_pct)}
                    </td>
                    <td className="text-center">
                      <span className="inline-flex items-center justify-center min-w-[1.5rem] h-6 px-1.5 rounded-md bg-slate-100 text-slate-700 font-bold text-xs">{stock.signal_count || signals.length}</span>
                    </td>
                    <td className="text-center">
                      <span className={`font-bold ${getRiskColor(health?.risk_score ?? 0)}`}>
                        {health?.risk_score ?? '-'}
                      </span>
                    </td>
                    <td className="text-center">
                      <span className="font-bold text-blue-600">{stock.signal_score ?? '-'}</span>
                    </td>
                    <td className="text-xs">
                      {stock.dimension_breakdown ? (
                        <div className="flex gap-2">
                          <span className="text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded">趋势 {stock.dimension_breakdown.trend}</span>
                          <span className="text-indigo-600 bg-indigo-50 px-1.5 py-0.5 rounded">动量 {stock.dimension_breakdown.momentum}</span>
                          <span className="text-orange-600 bg-orange-50 px-1.5 py-0.5 rounded">量能 {stock.dimension_breakdown.volume}</span>
                        </div>
                      ) : (
                        <span className="text-slate-400">-</span>
                      )}
                    </td>
                    {showPoolActions && (
                      <td className="text-center">
                        <button
                          onClick={(e) => handlePoolAction(e, stock.symbol, inPool)}
                          className={`inline-flex items-center justify-center w-7 h-7 rounded-md text-xs font-bold transition-all ${
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
                    <tr key={`${stock.symbol}-detail`} className="bg-slate-50/50">
                      <td colSpan={colSpan} className="p-0">
                        <div className="px-4 py-6 sm:px-8 border-l-2 border-blue-500 bg-gradient-to-r from-blue-50/30 to-transparent">
                          <StockSignals
                            symbol={stock.symbol}
                            signals={signals}
                            signalScore={stock.signal_score}
                            riskScore={health?.risk_score}
                            riskExplanations={health?.risk_explanations}
                            scoreLabel={health?.risk_level}
                            signalCount={signals.length}
                            dataMode={dataMode}
                            updateTime={updateTime}
                            hideHeader={true}
                            forceExpanded={true}
                          />
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
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
              className="p-2 rounded-md border border-gray-200 disabled:opacity-30 hover:bg-gray-50 transition-colors"
            >
              <ChevronLeft size={16} />
            </button>
            <span className="text-sm px-2 text-gray-600">
              {currentPage + 1} / {totalPages}
            </span>
            <button
              onClick={() => setCurrentPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={currentPage >= totalPages - 1}
              className="p-2 rounded-md border border-gray-200 disabled:opacity-30 hover:bg-gray-50 transition-colors"
            >
              <ChevronRight size={16} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

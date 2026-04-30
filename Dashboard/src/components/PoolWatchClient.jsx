import { useState, useMemo, useRef, useEffect } from 'react';
import { RotateCcw, Trash2, Plus, Search, ChevronDown } from 'lucide-react';
import SignalTable from './SignalTable.jsx';
import LoadingSpinner from './LoadingSpinner.jsx';
import { useStockPool } from '../hooks/useStockPool.js';
import SIGNAL_DATA from '../generated/signals.json';

function normalizeSignalPayload(json) {
  if (Array.isArray(json?.stocks)) return json;

  const rawSignals = Array.isArray(json?.signals) ? json.signals : [];
  const grouped = new Map();

  rawSignals.forEach((sig) => {
    const symbol = sig?.symbol;
    if (!symbol) return;

    if (!grouped.has(symbol)) {
      grouped.set(symbol, {
        symbol,
        name: sig?.name || '',
        signals: [],
        signal_count: 0,
        signal_score: 0,
        risk_score: 0,
        risk_explanations: [],
        has_buy_signal: false,
        stage: null,
        close_price: sig?.close_price ?? null,
        change_pct: sig?.change_pct ?? null,
      });
    }

    const item = grouped.get(symbol);
    item.signals.push(sig);
    item.signal_count += 1;
    item.signal_score = Math.max(item.signal_score, Number(sig?.score || 0));
    item.has_buy_signal = item.has_buy_signal || sig?.signal_type === 'right';
    if (item.stage !== 'right') item.stage = sig?.signal_type || item.stage;
    if (item.close_price == null && sig?.close_price != null) item.close_price = sig.close_price;
    if (item.change_pct == null && sig?.change_pct != null) item.change_pct = sig.change_pct;
  });

  return {
    ...json,
    stocks: Array.from(grouped.values()),
    total_signals: Number(json?.total_signals || rawSignals.length || 0),
    total_stocks: Number(json?.total_stocks || grouped.size || 0),
  };
}

export default function PoolWatchClient() {
  const [data] = useState(() => normalizeSignalPayload(SIGNAL_DATA));
  const [searchQuery, setSearchQuery] = useState('');
  const [showDropdown, setShowDropdown] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const [addMsg, setAddMsg] = useState('');
  const dropdownRef = useRef(null);

  const { pool, poolSet, add, remove, reset, clear, initialized } = useStockPool();

  const allSignalStocks = data?.stocks || [];

  const signalStockMap = useMemo(() => {
    const map = {};
    allSignalStocks.forEach(s => { map[s.symbol] = s; });
    return map;
  }, [allSignalStocks]);

  const poolStocks = useMemo(() => {
    if (!initialized) return [];
    return pool.map(symbol => {
      const s = signalStockMap[symbol];
      if (s) return s;
      return {
        symbol,
        name: '',
        signals: [],
        signal_count: 0,
        signal_score: 0,
        risk_score: 0,
        close_price: null,
        change_pct: null,
        stage: null,
        dimension_breakdown: null,
      };
    });
  }, [pool, signalStockMap, initialized]);

  const validStocks = useMemo(() => poolStocks.filter(s => s.close_price != null), [poolStocks]);
  const missingStocks = useMemo(() => poolStocks.filter(s => s.close_price == null), [poolStocks]);

  const healthScores = useMemo(() => {
    return poolStocks.map(s => ({
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
      close_price: s.close_price,
      change_pct: s.change_pct,
      stage: s.stage,
      dimension_breakdown: s.dimension_breakdown,
    }));
  }, [poolStocks]);

  const candidates = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    const matched = allSignalStocks.filter(s => {
      if (!q) return true;
      return (
        s.symbol.toLowerCase().includes(q) ||
        (s.name && s.name.toLowerCase().includes(q))
      );
    });
    // 未添加的排在前面，已添加的排在后面
    return matched
      .sort((a, b) => {
        const aAdded = poolSet.has(a.symbol) ? 1 : 0;
        const bAdded = poolSet.has(b.symbol) ? 1 : 0;
        return aAdded - bAdded;
      })
      .slice(0, 100);
  }, [allSignalStocks, poolSet, searchQuery]);

  const handleSelect = (symbol) => {
    if (poolSet.has(symbol)) return;
    const ok = add(symbol);
    if (ok) {
      setAddMsg(`已添加 ${symbol}`);
      setSearchQuery('');
      setShowDropdown(false);
      setHighlightedIndex(0);
      setTimeout(() => setAddMsg(''), 2000);
    }
  };

  const handleKeyDown = (e) => {
    if (!showDropdown) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHighlightedIndex(i => Math.min(i + 1, candidates.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHighlightedIndex(i => Math.max(i - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (candidates[highlightedIndex]) {
        handleSelect(candidates[highlightedIndex].symbol);
      }
    } else if (e.key === 'Escape') {
      setShowDropdown(false);
    }
  };

  useEffect(() => {
    const onClick = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', onClick);
    return () => document.removeEventListener('mousedown', onClick);
  }, []);

  const handleReset = () => {
    if (confirm('确定要重置为默认股票池吗？当前自定义股票将丢失。')) {
      reset();
    }
  };

  const handleClear = () => {
    if (confirm('确定要清空股票池吗？')) {
      clear();
    }
  };

  const poolBuyCount = validStocks.filter(s => s.has_buy_signal).length;
  const poolRiskCount = validStocks.filter(s => (s.risk_score ?? 0) >= 60 && !s.has_buy_signal).length;

  if (!initialized) {
    return <LoadingSpinner text="正在加载股票池数据..." />;
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-[28px] font-bold text-gray-800 tracking-tight">股票池监控</h1>
        <p className="text-sm text-gray-500 mt-1">
          数据时间: {data?.price_fetch_time?.split(' ')?.[0] || data?.date || '未知'}
          {' '}| 股票池共 {pool.length} 只（有数据 {validStocks.length} 只）
        </p>
      </div>

      <div className="metric-card mb-6">
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative" ref={dropdownRef}>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                <input
                  type="text"
                  readOnly
                  placeholder="搜索股票代码/名称"
                  value={searchQuery}
                  onChange={e => {
                    setSearchQuery(e.target.value);
                    setShowDropdown(true);
                    setHighlightedIndex(0);
                  }}
                  onFocus={e => {
                    setTimeout(() => {
                      e.target.removeAttribute('readOnly');
                    });
                    setShowDropdown(true);
                  }}
                  onBlur={e => {
                    e.target.setAttribute('readOnly', true);
                  }}
                  onKeyDown={handleKeyDown}
                  className="pl-8 pr-3 py-2 border border-gray-200 rounded-md text-sm w-56 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                />
              </div>
              <button
                onClick={() => setShowDropdown(v => !v)}
                className="inline-flex items-center gap-1 px-3 py-2 bg-primary-600 text-white rounded-md text-sm font-medium hover:bg-primary-700 transition-colors"
              >
                <Plus size={16} />
                添加
                <ChevronDown size={14} />
              </button>
            </div>
            {showDropdown && (
              <div className="absolute z-20 mt-1 w-72 max-h-64 overflow-y-auto bg-white border border-gray-200 rounded-md shadow-lg">
                {candidates.length === 0 ? (
                  <div className="px-3 py-2 text-sm text-gray-400">无匹配结果</div>
                ) : (
                  candidates.map((s, idx) => {
                    const isAdded = poolSet.has(s.symbol);
                    return (
                      <div
                        key={s.symbol}
                        onClick={() => handleSelect(s.symbol)}
                        className={`px-3 py-2 text-sm flex items-center justify-between ${
                          isAdded
                            ? 'cursor-default text-gray-300'
                            : idx === highlightedIndex
                              ? 'bg-primary-50 text-primary-700 cursor-pointer'
                              : 'hover:bg-gray-50 text-gray-700 cursor-pointer'
                        }`}
                      >
                        <span className={`font-medium ${isAdded ? 'text-gray-300' : ''}`}>{s.symbol}</span>
                        <div className="flex items-center gap-2">
                          <span className={`text-xs truncate max-w-[8rem] ${isAdded ? 'text-gray-300' : 'text-gray-400'}`}>
                            {s.name}
                          </span>
                          {isAdded && (
                            <span className="text-[10px] px-1 py-0.5 rounded bg-gray-100 text-gray-400">已添加</span>
                          )}
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            )}
          </div>
          {addMsg && (
            <span className="text-xs text-gray-500">{addMsg}</span>
          )}
          <div className="flex-1" />
          <button
            onClick={handleReset}
            className="inline-flex items-center gap-1 px-3 py-2 bg-amber-50 text-amber-700 border border-amber-200 rounded-md text-sm font-medium hover:bg-amber-100 transition-colors"
          >
            <RotateCcw size={14} />
            重置默认
          </button>
          <button
            onClick={handleClear}
            className="inline-flex items-center gap-1 px-3 py-2 bg-red-50 text-red-700 border border-red-200 rounded-md text-sm font-medium hover:bg-red-100 transition-colors"
          >
            <Trash2 size={14} />
            清空
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div className="metric-card text-center">
          <p className="text-3xl font-bold text-primary-600">{pool.length}</p>
          <p className="text-sm text-gray-500 mt-1">股票池数量</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-3xl font-bold text-green-600">{poolBuyCount}</p>
          <p className="text-sm text-gray-500 mt-1">买入信号</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-3xl font-bold text-yellow-600">{validStocks.length - poolBuyCount}</p>
          <p className="text-sm text-gray-500 mt-1">观察/无信号</p>
        </div>
        <div className="metric-card text-center">
          <p className="text-3xl font-bold text-red-600">{poolRiskCount}</p>
          <p className="text-sm text-gray-500 mt-1">风险预警</p>
        </div>
      </div>

      {missingStocks.length > 0 && (
        <div className="mb-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
          <p className="text-xs text-yellow-700">
            以下股票暂无信号数据：{missingStocks.map(s => s.symbol).join('、')}
          </p>
        </div>
      )}

      <div className="metric-card">
        <h2 className="text-lg font-bold mb-4 text-gray-800">股票明细</h2>
        {validStocks.length === 0 && pool.length > 0 ? (
          <p className="text-center text-gray-500 py-8">股票池中的股票暂无今日信号数据</p>
        ) : pool.length === 0 ? (
          <p className="text-center text-gray-500 py-8">股票池为空，请添加股票或点击「重置默认」</p>
        ) : (
          <SignalTable
            stocks={validStocks}
            healthScores={healthScores}
            showPoolActions={true}
            onAddToPool={add}
            onRemoveFromPool={remove}
            poolSet={poolSet}
            dataMode={data?.intraday_mode ? '实盘' : '收盘'}
            updateTime={data?.price_fetch_time || data?.scan_time}
          />
        )}
      </div>
    </div>
  );
}

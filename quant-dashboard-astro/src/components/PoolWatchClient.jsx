import { useState, useEffect, useMemo } from 'react';
import { RotateCcw, Trash2, Plus } from 'lucide-react';
import SignalTable from './SignalTable.jsx';
import { useStockPool } from '../hooks/useStockPool.js';

export default function PoolWatchClient() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [inputSymbol, setInputSymbol] = useState('');
  const [addMsg, setAddMsg] = useState('');

  const { pool, poolSet, add, remove, reset, clear, initialized } = useStockPool();

  // 加载信号数据（股票池需要从全量信号中过滤）
  useEffect(() => {
    fetch('/data/signals/signal_latest.json')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.text();
      })
      .then(text => {
        const cleaned = text
          .replace(/: NaN/g, ': null')
          .replace(/: -NaN/g, ': null')
          .replace(/: Infinity/g, ': null')
          .replace(/: -Infinity/g, ': null');
        return JSON.parse(cleaned);
      })
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch(err => {
        console.error('加载信号数据失败:', err);
        setLoading(false);
      });
  }, []);

  const allSignalStocks = data?.stocks || [];

  // 建立全量信号映射表
  const signalStockMap = useMemo(() => {
    const map = {};
    allSignalStocks.forEach(s => { map[s.symbol] = s; });
    return map;
  }, [allSignalStocks]);

  // 构建股票池对应的股票数据
  const poolStocks = useMemo(() => {
    if (!initialized) return [];
    return pool.map(symbol => {
      const s = signalStockMap[symbol];
      if (s) return s;
      // 信号数据中找不到，返回占位对象
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

  // 过滤无数据的占位股票（但保留用户在池中手动添加的，只是展示时提示）
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

  const handleAdd = () => {
    const s = inputSymbol.trim().toUpperCase();
    if (!s) return;
    if (!s.includes('.')) {
      setAddMsg('格式错误，请输入完整代码如 600519.SH');
      return;
    }
    const ok = add(s);
    if (ok) {
      setAddMsg(`已添加 ${s}`);
      setInputSymbol('');
      setTimeout(() => setAddMsg(''), 2000);
    } else {
      setAddMsg(`${s} 已在股票池中`);
      setTimeout(() => setAddMsg(''), 2000);
    }
  };

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

  // 统计池内有效股票的信号数
  const poolBuyCount = validStocks.filter(s => s.has_buy_signal).length;
  const poolRiskCount = validStocks.filter(s => (s.risk_score ?? 0) >= 60 && !s.has_buy_signal).length;

  if (loading || !initialized) {
    return <p className="text-gray-500 py-8">加载中...</p>;
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-[28px] font-bold text-gray-800 tracking-tight">股票池监控</h1>
        <p className="text-sm text-gray-500 mt-1">
          数据时间: {data?.date || data?.scan_time?.split(' ')?.[0] || '未知'}
          {' '}| 股票池共 {pool.length} 只（有数据 {validStocks.length} 只）
        </p>
      </div>

      {/* 管理工具栏 */}
      <div className="metric-card mb-6">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="输入代码如 600519.SH"
              value={inputSymbol}
              onChange={e => setInputSymbol(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleAdd()}
              className="px-3 py-2 border border-gray-200 rounded-md text-sm w-48 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            />
            <button
              onClick={handleAdd}
              className="inline-flex items-center gap-1 px-3 py-2 bg-primary-600 text-white rounded-md text-sm font-medium hover:bg-primary-700 transition-colors"
            >
              <Plus size={16} />
              添加
            </button>
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

      {/* 统计卡片 */}
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

      {/* 无数据提示 */}
      {missingStocks.length > 0 && (
        <div className="mb-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
          <p className="text-xs text-yellow-700">
            以下股票暂无信号数据：{missingStocks.map(s => s.symbol).join('、')}
          </p>
        </div>
      )}

      {/* 股票明细 */}
      <div className="metric-card">
        <h2 className="text-lg font-bold mb-4 text-gray-800">股票明细</h2>
        {validStocks.length === 0 && pool.length > 0 ? (
          <p className="text-center text-gray-500 py-8">股票池中的股票暂无今日信号数据</p>
        ) : pool.length === 0 ? (
          <p className="text-center text-gray-500 py-8">股票池为空，请添加股票或点击「重置默认」</p>
        ) : (
          <SignalTable
            stocks={poolStocks}
            healthScores={healthScores}
            showPoolActions={true}
            onAddToPool={add}
            onRemoveFromPool={remove}
            poolSet={poolSet}
          />
        )}
      </div>
    </div>
  );
}

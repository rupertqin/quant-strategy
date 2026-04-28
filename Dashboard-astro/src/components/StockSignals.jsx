import { useMemo, useState } from 'react';

function getScoreStyle(score) {
  if (score >= 70) return { bg: '#27ae60', text: 'white' };
  if (score >= 50) return { bg: '#f39c12', text: 'white' };
  return { bg: '#e74c3c', text: 'white' };
}

function getRiskStyle(riskScore) {
  if (riskScore < 40) return { bg: '#27ae60', text: 'white', label: '低风险' };
  if (riskScore < 70) return { bg: '#f39c12', text: 'white', label: '中风险' };
  return { bg: '#e74c3c', text: 'white', label: '高风险' };
}

function getRiskEmoji(riskScore) {
  if (riskScore < 40) return '🟢';
  if (riskScore < 70) return '🟡';
  return '🔴';
}

export default function StockSignals({
  symbol,
  signals,
  signalScore,
  riskScore,
  riskExplanations,
  scoreLabel,
  signalCount,
  dataMode,
  updateTime,
  hideHeader = false,
  forceExpanded = false,
}) {
  const [expanded, setExpanded] = useState(forceExpanded);

  const merged = useMemo(() => ({
    signals: signals ?? [],
    signalScore: signalScore || 0,
    signalCount: signalCount !== undefined ? signalCount : (signals?.length || 0),
    scoreLabel: scoreLabel || '',
    riskScore: riskScore || 0,
    riskExplanations: riskExplanations || [],
  }), [signals, signalScore, signalCount, scoreLabel, riskScore, riskExplanations]);

  const hasSignals = merged.signals && merged.signals.length > 0;

  const sStyle = getScoreStyle(merged.signalScore || 0);
  const rStyle = getRiskStyle(merged.riskScore || 0);
  const count = merged.signalCount;

  const periodMap = { daily: '日线', weekly: '周线', monthly: '月线' };
  const typeMap = { right: '右侧', left: '左侧' };

  return (
    <div className={hideHeader ? '' : 'mb-6'}>
      {!hideHeader && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between bg-white hover:bg-slate-50 px-5 py-4 rounded-xl border border-slate-200 transition-colors shadow-sm"
        >
          <div className="flex flex-wrap items-center gap-3">
            <span className="text-[15px] font-bold text-slate-800">
              📡 当前信号 ({count}个)
            </span>
            {(dataMode || updateTime) && (
              <span className="text-xs text-slate-500 font-medium">
                {dataMode ? `模式:${dataMode}` : ''}{dataMode && updateTime ? ' | ' : ''}{updateTime ? `时间:${updateTime}` : ''}
              </span>
            )}
            <span
              className="text-[11px] font-bold px-2.5 py-1 rounded-md"
              style={{ background: sStyle.bg, color: sStyle.text }}
            >
              信号分: {merged.signalScore || 0} · {merged.scoreLabel || '无信号'}
            </span>
            <span
              className="text-[11px] font-bold px-2.5 py-1 rounded-md"
              style={{ background: rStyle.bg, color: rStyle.text }}
            >
              风险分: {merged.riskScore || 0} {getRiskEmoji(merged.riskScore || 0)}
            </span>
          </div>
          <span className="text-sm font-medium text-slate-400">
            {expanded ? '▲ 收起' : '▼ 展开'}
          </span>
        </button>
      )}

      {expanded && (
        <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-5">
          {/* 左侧：风险详情 */}
          <div className="bg-white border border-slate-100 rounded-xl p-5 shadow-sm">
            <div className="text-[13px] font-bold tracking-wide uppercase text-slate-600 mb-4 flex items-center gap-2">
              ⚠️ 风险评估
            </div>
            {(!merged.riskExplanations || merged.riskExplanations.length === 0) ? (
              <div className="text-sm text-slate-500 py-4 text-center border border-dashed border-slate-200 rounded-lg">暂无风险评估</div>
            ) : (
              <div className="space-y-3">
                {merged.riskExplanations.slice(0, 5).map((exp, idx) => (
                  <div key={idx} className="text-sm px-4 py-3 rounded-lg bg-red-50 text-red-700 border border-red-100/50 leading-relaxed">{exp}</div>
                ))}
              </div>
            )}
          </div>

          {/* 右侧：信号详情 */}
          <div className="bg-white border border-slate-100 rounded-xl p-5 shadow-sm">
            <div className="text-[13px] font-bold tracking-wide uppercase text-slate-600 mb-4 flex items-center gap-2">
              📋 触发信号 <span className="text-slate-400 font-normal">({merged.signals.length})</span>
            </div>
            {!hasSignals ? (
              <div className="text-sm text-slate-500 py-4 text-center border border-dashed border-slate-200 rounded-lg">无买入信号</div>
            ) : (
              <div className="space-y-3">
                {merged.signals.map((sig, idx) => {
                  const sigScore = sig.score || 0;
                  return (
                    <div key={idx} className="bg-slate-50 rounded-xl p-4 border border-slate-100 transition-all hover:bg-slate-50/80">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2 flex-wrap">
                          {sig.signal_type === 'left' ? (
                            <span className="px-2 py-1 rounded-md text-[11px] font-medium bg-amber-50 text-amber-700 border border-amber-200/50">📉 左侧</span>
                          ) : (
                            <span className="px-2 py-1 rounded-md text-[11px] font-medium bg-emerald-50 text-emerald-700 border border-emerald-200/50">📈 右侧</span>
                          )}
                          <span className="font-bold text-[14px] text-slate-800 tracking-tight">{sig.signal_name}</span>
                          {sig.strength === 'strong' && <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-red-100 text-red-700">强</span>}
                          {sig.strength === 'medium' && <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-blue-100 text-blue-700">中</span>}
                          {sig.strength === 'weak' && <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-slate-200 text-slate-600">弱</span>}
                          <span className="text-[11px] px-2 py-1 rounded-md bg-white text-slate-500 border border-slate-200 font-medium">
                            {periodMap[sig.period] || sig.period}
                          </span>
                        </div>
                        <span className="text-[11px] text-slate-400 font-medium">
                          {dataMode === '实盘' && updateTime && sig.trigger_date && updateTime.startsWith(sig.trigger_date)
                            ? updateTime
                            : (sig.trigger_date || '')}
                        </span>
                      </div>
                      <p className="text-[13px] text-slate-600 leading-relaxed mb-3">{sig.description}</p>
                      <div className="flex flex-wrap gap-4 text-[12px] text-slate-500 bg-white px-3 py-2 rounded-lg border border-slate-100">
                        <span className="flex items-center gap-1">信号分: <b className={sigScore >= 70 ? 'text-emerald-600' : sigScore >= 50 ? 'text-amber-600' : 'text-red-600'}>{sigScore}</b></span>
                        {sig.volume_ratio != null && <span className="flex items-center gap-1">量比: <b className="text-slate-700">{Number(sig.volume_ratio).toFixed(2)}</b></span>}
                        {sig.close_price != null && <span className="flex items-center gap-1">价格: <span className="text-slate-700 font-mono">{Number(sig.close_price).toFixed(2)}</span></span>}
                        {sig.change_pct != null && (
                          <span className={`font-mono font-medium ${sig.change_pct > 0 ? 'text-red-500' : sig.change_pct < 0 ? 'text-emerald-500' : ''}`}>
                            {sig.change_pct > 0 ? '+' : ''}{Number(sig.change_pct).toFixed(2)}%
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

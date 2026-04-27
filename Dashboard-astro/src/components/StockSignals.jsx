import { useState } from 'react';

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

export default function StockSignals({ signals, signalScore, riskScore, riskExplanations, scoreLabel, signalCount }) {
  const [expanded, setExpanded] = useState(false);

  const hasSignals = signals && signals.length > 0;
  const showSection = hasSignals || riskScore >= 60;
  if (!showSection) return null;

  const sStyle = getScoreStyle(signalScore || 0);
  const rStyle = getRiskStyle(riskScore || 0);
  const count = signalCount !== undefined ? signalCount : (signals?.length || 0);

  const periodMap = { daily: '日线', weekly: '周线', monthly: '月线' };
  const typeMap = { right: '右侧', left: '左侧' };

  return (
    <div className="mb-6">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between bg-gray-50 hover:bg-gray-100 px-4 py-3 rounded-lg border border-gray-200 transition-colors"
      >
        <div className="flex items-center gap-3">
          <span className="text-base font-semibold text-gray-800">
            📡 当前信号 ({count}个)
          </span>
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded"
            style={{ background: sStyle.bg, color: sStyle.text }}
          >
            信号分:{signalScore || 0}·{scoreLabel || '无信号'}
          </span>
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded"
            style={{ background: rStyle.bg, color: rStyle.text }}
          >
            风险分:{riskScore || 0}{getRiskEmoji(riskScore || 0)}
          </span>
        </div>
        <span className="text-sm text-gray-500">
          {expanded ? '▲ 收起' : '▼ 展开'}
        </span>
      </button>

      {expanded && (
        <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* 信号详情 */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <div className="text-sm font-semibold text-gray-700 mb-3">📈 信号详情</div>
            {!hasSignals ? (
              <div className="text-xs text-gray-500">无买入信号</div>
            ) : (
              <div className="space-y-3">
                {signals.map((sig, idx) => {
                  const sigScore = sig.score || 0;
                  let scoreColor = '#e74c3c';
                  if (sigScore >= 70) scoreColor = '#27ae60';
                  else if (sigScore >= 50) scoreColor = '#f39c12';
                  return (
                    <div key={idx} className="border-b border-gray-100 last:border-0 pb-2 last:pb-0">
                      <div className="flex justify-between items-center text-xs">
                        <span>
                          {sig.signal_type === 'right' ? '📈' : '📉'} [{typeMap[sig.signal_type] || '未知'}] {sig.signal_name} ({periodMap[sig.period] || sig.period})
                        </span>
                        <span style={{ color: scoreColor, fontWeight: 600 }}>{sigScore}分</span>
                      </div>
                      <div className="text-xs text-gray-500 mt-1 leading-relaxed">
                        {sig.description}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* 风险评估 */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <div className="text-sm font-semibold text-gray-700 mb-3">⚠️ 风险评估</div>
            {(!riskExplanations || riskExplanations.length === 0) ? (
              <div className="text-xs text-gray-500">暂无风险评估</div>
            ) : (
              <div className="space-y-1">
                {riskExplanations.slice(0, 5).map((exp, idx) => (
                  <div key={idx} className="text-xs text-gray-600 py-0.5">{exp}</div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * 格式化工具函数
 */

export function formatPercent(value: number): string {
  if (value === undefined || value === null) return 'N/A';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

export function formatPrice(value: number): string {
  if (value === undefined || value === null) return 'N/A';
  return `¥${value.toFixed(2)}`;
}

export function formatNumber(value: number): string {
  if (value === undefined || value === null) return 'N/A';
  if (value >= 100000000) {
    return `${(value / 100000000).toFixed(1)}亿`;
  }
  if (value >= 10000) {
    return `${(value / 10000).toFixed(1)}万`;
  }
  return value.toLocaleString();
}

export function formatDate(dateStr: string): string {
  if (!dateStr) return '';
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString('zh-CN');
  } catch {
    return dateStr;
  }
}

export function getChangeColor(changePct: number): string {
  if (changePct > 0) return 'text-fin-up';
  if (changePct < 0) return 'text-fin-down';
  return 'text-gray-500';
}

export function getChangeBg(changePct: number): string {
  if (changePct > 0) return 'bg-red-50 text-red-600';
  if (changePct < 0) return 'bg-green-50 text-green-600';
  return 'bg-gray-50 text-gray-600';
}

export function getSignalTypeLabel(type: string): { text: string; emoji: string; className: string } {
  if (type === 'left') {
    return { text: '左侧', emoji: '📉', className: 'text-amber-600 bg-amber-50' };
  }
  return { text: '右侧', emoji: '📈', className: 'text-emerald-600 bg-emerald-50' };
}

export function getStrengthLabel(strength: string): { text: string; className: string } {
  const map: Record<string, { text: string; className: string }> = {
    strong: { text: '强', className: 'text-red-600 bg-red-50' },
    medium: { text: '中', className: 'text-orange-600 bg-orange-50' },
    weak: { text: '弱', className: 'text-gray-600 bg-gray-50' },
  };
  return map[strength] || { text: strength, className: 'text-gray-600 bg-gray-50' };
}

export function getRiskLevelColor(level: string): string {
  const map: Record<string, string> = {
    low: 'text-green-600',
    medium: 'text-yellow-600',
    high: 'text-orange-600',
    extreme: 'text-red-600',
    risky: 'text-red-600',
  };
  return map[level] || 'text-gray-600';
}

export function getHealthScoreColor(score: number): string {
  if (score >= 80) return 'text-green-600';
  if (score >= 60) return 'text-blue-600';
  if (score >= 40) return 'text-yellow-600';
  if (score >= 20) return 'text-orange-600';
  return 'text-red-600';
}

export function getHealthScoreBg(score: number): string {
  if (score >= 80) return 'bg-green-50';
  if (score >= 60) return 'bg-blue-50';
  if (score >= 40) return 'bg-yellow-50';
  if (score >= 20) return 'bg-orange-50';
  return 'bg-red-50';
}

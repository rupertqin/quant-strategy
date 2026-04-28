import { useState, useEffect, useRef } from 'react';
import { createChart, CrosshairMode } from 'lightweight-charts';

export default function IndexChart({ histData, intradayData, name }) {
  const [mode, setMode] = useState('daily');
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const hasIntraday = Array.isArray(intradayData) && intradayData.length > 0;

  useEffect(() => {
    if (!hasIntraday && mode === 'intraday') {
      setMode('daily');
    }
  }, [hasIntraday, mode]);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // 清理旧图表
    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }
    chartContainerRef.current.innerHTML = '';

    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: 'solid', color: '#ffffff' }, textColor: '#333' },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
      crosshair: { mode: CrosshairMode.Magnet },
      rightPriceScale: { borderColor: '#e0e0e0' },
      timeScale: { borderColor: '#e0e0e0', timeVisible: mode === 'intraday' },
      width: chartContainerRef.current.clientWidth,
      height: 280,
    });

    chartRef.current = chart;

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    if (mode === 'daily' && histData && histData.length > 0) {
      // 日线 - K线 + 均线
      const candleSeries = chart.addCandlestickSeries({
        upColor: '#ff4757',
        downColor: '#2ed573',
        borderUpColor: '#ff4757',
        borderDownColor: '#2ed573',
        wickUpColor: '#ff4757',
        wickDownColor: '#2ed573',
      });

      const candles = histData
        .map(row => ({
          time: row.date || row.trade_date,
          open: row.open,
          high: row.high,
          low: row.low,
          close: row.close,
        }))
        .filter(c => c.time != null && c.open != null && c.high != null && c.low != null && c.close != null);
      candleSeries.setData(candles);

      // 计算均线
      const closes = histData.map(d => d.close);
      const ma = (arr, n) => {
        return arr.map((_, i) => {
          if (i < n - 1) return null;
          const sum = arr.slice(i - n + 1, i + 1).reduce((a, b) => a + b, 0);
          return sum / n;
        });
      };

      const ma5 = ma(closes, 5);
      const ma10 = ma(closes, 10);
      const ma20 = ma(closes, 20);

      const addMaSeries = (data, color, title) => {
        const line = chart.addLineSeries({ color, lineWidth: 1, title });
        const lineData = histData.map((row, i) => ({
          time: row.date || row.trade_date,
          value: data[i],
        })).filter(d => d.value !== null);
        line.setData(lineData);
      };

      addMaSeries(ma5, '#333', 'MA5');
      addMaSeries(ma10, '#f59e0b', 'MA10');
      addMaSeries(ma20, '#8b5cf6', 'MA20');

    } else if (mode === 'intraday' && intradayData && intradayData.length > 0) {
      // 分时 - 蓝色折线
      const lineSeries = chart.addLineSeries({
        color: '#2196F3',
        lineWidth: 2,
      });

      const lineData = intradayData
        .map(row => ({
          time: row.time || row.trade_time || row.timestamp,
          value: row.price || row.close,
        }))
        .filter(d => d.time != null && d.value != null);
      lineSeries.setData(lineData);
    }

    chart.timeScale().fitContent();

    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [mode, histData, intradayData]);

  return (
    <div>
      {hasIntraday && (
        <div className="flex justify-end gap-1 mb-2">
          <button
            onClick={() => setMode('daily')}
            className={`px-3 py-1 text-xs rounded ${mode === 'daily' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            日线
          </button>
          <button
            onClick={() => setMode('intraday')}
            className={`px-3 py-1 text-xs rounded ${mode === 'intraday' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            分时
          </button>
        </div>
      )}
      <div ref={chartContainerRef} style={{ width: '100%', height: 280 }} />
      {mode === 'intraday' && intradayData && intradayData.length > 0 && (
        <div className="text-xs text-gray-500 mt-1">
          {(() => {
            const prices = intradayData.map(d => d.price || d.close);
            const max = Math.max(...prices);
            const min = Math.min(...prices);
            const last = prices[prices.length - 1];
            return `最新: ${last?.toFixed(2)} | 最高: ${max?.toFixed(2)} | 最低: ${min?.toFixed(2)}`;
          })()}
        </div>
      )}
    </div>
  );
}

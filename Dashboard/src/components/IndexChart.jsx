import { useState, useEffect, useRef } from 'react';
import { createChart, CrosshairMode, CandlestickSeries, LineSeries } from 'lightweight-charts';

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
      layout: { background: { type: 'solid', color: '#ffffff' }, textColor: '#333', attributionLogo: false },
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
      const candleSeries = chart.addSeries(CandlestickSeries, {
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

      const addMaSeries = (data, color) => {
        const line = chart.addSeries(LineSeries, { color, lineWidth: 1, title: '', lastValueVisible: false });
        const lineData = histData.map((row, i) => ({
          time: row.date || row.trade_date,
          value: data[i],
        })).filter(d => d.value !== null);
        line.setData(lineData);
      };

      addMaSeries(ma5, '#333');
      addMaSeries(ma10, '#f59e0b');
      addMaSeries(ma20, '#8b5cf6');

    } else if (mode === 'intraday' && intradayData && intradayData.length > 0) {
      // 分时 - 蓝色折线
      const lineSeries = chart.addSeries(LineSeries, {
        color: '#2196F3',
        lineWidth: 2,
      });

      const lineData = intradayData
        .map(row => {
          let t = row.time || row.trade_time || row.timestamp;
          // lightweight-charts 分时时间格式要求 YYYY-MM-DD HH:MM，去掉秒
          if (typeof t === 'string' && t.length > 16) {
            t = t.slice(0, 16);
          }
          return {
            time: t,
            value: row.price ?? row.close ?? null,
          };
        })
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
            const prices = intradayData.map(d => d.price ?? d.close ?? null).filter(v => v != null);
            if (prices.length === 0) return null;
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

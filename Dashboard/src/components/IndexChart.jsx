import { useEffect, useRef } from 'react';
import { createChart, CrosshairMode, CandlestickSeries, LineSeries } from 'lightweight-charts';

export default function IndexChart({ histData, name }) {
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);

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
      timeScale: { borderColor: '#e0e0e0' },
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

    if (histData && histData.length > 0) {
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
    }

    chart.timeScale().fitContent();

    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [histData, name]);

  return <div ref={chartContainerRef} style={{ width: '100%', height: 280 }} />;
}

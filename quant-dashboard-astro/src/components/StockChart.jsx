import { useEffect, useRef, useState } from 'react';

export default function StockChart({ symbol, dailyData: initialDaily, weeklyData: initialWeekly, monthlyData: initialMonthly }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const [period, setPeriod] = useState('daily');
  const [loadedData, setLoadedData] = useState({
    daily: initialDaily || [],
    weekly: initialWeekly || [],
    monthly: initialMonthly || [],
  });
  const [loading, setLoading] = useState(false);

  // 客户端加载数据
  useEffect(() => {
    if (!symbol) return;
    if (loadedData.daily.length > 0) return; // 已有数据

    async function fetchData() {
      setLoading(true);
      try {
        const res = await fetch(`/data/prices/${symbol}.json`);
        if (res.ok) {
          const allData = await res.json();
          // 假设数据是日线，通过 resample 计算周线和月线
          setLoadedData({
            daily: allData,
            weekly: resampleWeekly(allData),
            monthly: resampleMonthly(allData),
          });
        }
      } catch (e) {
        console.error('加载股票数据失败:', e);
      } finally {
        setLoading(false);
      }
    }
    fetchData();
  }, [symbol]);

  const dataMap = {
    daily: loadedData.daily,
    weekly: loadedData.weekly,
    monthly: loadedData.monthly,
  };

  useEffect(() => {
    if (!containerRef.current) return;
    const data = dataMap[period];
    if (!data || data.length === 0) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }
    containerRef.current.innerHTML = '';

    const tv = window.LightweightCharts;
    if (!tv) return;

    const chart = tv.createChart(containerRef.current, {
      layout: { background: { type: 'solid', color: '#ffffff' }, textColor: '#333' },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
      crosshair: { mode: tv.CrosshairMode.Magnet },
      rightPriceScale: { borderColor: '#e0e0e0' },
      timeScale: { borderColor: '#e0e0e0' },
      width: containerRef.current.clientWidth,
      height: 450,
    });

    chartRef.current = chart;

    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    // K线
    const candleSeries = chart.addCandlestickSeries({
      upColor: '#ff4757',
      downColor: '#2ed573',
      borderUpColor: '#ff4757',
      borderDownColor: '#2ed573',
      wickUpColor: '#ff4757',
      wickDownColor: '#2ed573',
    });

    const candles = data.map(row => ({
      time: row.date || row.trade_date,
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
    }));
    candleSeries.setData(candles);

    // 均线
    const closes = data.map(d => d.close);
    const ma = (arr, n) => arr.map((_, i) => {
      if (i < n - 1) return null;
      return arr.slice(i - n + 1, i + 1).reduce((a, b) => a + b, 0) / n;
    });

    const addMa = (vals, color, title) => {
      const line = chart.addLineSeries({ color, lineWidth: 1, title });
      line.setData(data.map((row, i) => ({
        time: row.date || row.trade_date,
        value: vals[i],
      })).filter(d => d.value !== null));
    };

    addMa(ma(closes, 5), '#333', 'MA5');
    addMa(ma(closes, 10), '#f59e0b', 'MA10');
    addMa(ma(closes, 20), '#8b5cf6', 'MA20');
    addMa(ma(closes, 60), '#06b6d4', 'MA60');

    // 成交量
    const volumes = data.map(d => d.volume || 0);
    const maxVol = Math.max(...volumes);
    if (maxVol > 0) {
      const volSeries = chart.addHistogramSeries({
        color: '#e2e8f0',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
      });
      volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
      volSeries.setData(data.map((row, i) => ({
        time: row.date || row.trade_date,
        value: row.volume || 0,
        color: (row.close || 0) >= (row.open || 0) ? '#ff4757' : '#2ed573',
      })));
    }

    chart.timeScale().fitContent();

    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [period, loadedData.daily, loadedData.weekly, loadedData.monthly]);

  return (
    <div>
      <div className="flex justify-end gap-1 mb-2">
        {['daily', 'weekly', 'monthly'].map(p => (
          <button
            key={p}
            onClick={() => setPeriod(p)}
            className={`px-3 py-1 text-xs rounded ${period === p ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            {p === 'daily' ? '日线' : p === 'weekly' ? '周线' : '月线'}
          </button>
        ))}
      </div>
      {loading && (
        <div className="flex items-center justify-center h-[450px] text-gray-400">
          <span>加载图表数据...</span>
        </div>
      )}
      <div ref={containerRef} style={{ width: '100%', height: 450, display: loading ? 'none' : 'block' }} />
    </div>
  );
}

// 周线重采样
function resampleWeekly(dailyData) {
  if (!dailyData || dailyData.length === 0) return [];
  const weeks = {};
  dailyData.forEach(d => {
    const date = new Date(d.date || d.trade_date);
    const weekKey = `${date.getFullYear()}-W${Math.ceil((date.getDate() + date.getDay()) / 7)}`;
    if (!weeks[weekKey]) {
      weeks[weekKey] = { ...d, time: d.date || d.trade_date, high: d.high, low: d.low, volume: 0 };
    } else {
      weeks[weekKey].high = Math.max(weeks[weekKey].high, d.high);
      weeks[weekKey].low = Math.min(weeks[weekKey].low, d.low);
      weeks[weekKey].close = d.close;
      weeks[weekKey].volume += (d.volume || 0);
    }
  });
  return Object.values(weeks);
}

// 月线重采样
function resampleMonthly(dailyData) {
  if (!dailyData || dailyData.length === 0) return [];
  const months = {};
  dailyData.forEach(d => {
    const date = new Date(d.date || d.trade_date);
    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
    if (!months[monthKey]) {
      months[monthKey] = { ...d, time: d.date || d.trade_date, high: d.high, low: d.low, volume: 0 };
    } else {
      months[monthKey].high = Math.max(months[monthKey].high, d.high);
      months[monthKey].low = Math.min(months[monthKey].low, d.low);
      months[monthKey].close = d.close;
      months[monthKey].volume += (d.volume || 0);
    }
  });
  return Object.values(months);
}

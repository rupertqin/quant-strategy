import { useEffect, useRef, useState } from 'react';

const MA_COLORS = {
  ma5: '#000000',
  ma10: '#f1c40f',
  ma20: '#9b59b6',
  ma60: '#fd79a8',
  ma120: '#6c5ce7',
};

export default function StockChart({
  symbol,
  dailyData: initialDaily,
  weeklyData: initialWeekly,
  monthlyData: initialMonthly,
  dailyLimit = null,
  weeklyLimit = null,
  monthlyLimit = null,
  chartWidth = 500,
  barSpacing = 8,
  zoomOutBars = 0,
  initialVisibleBars = null,
  isRealtimeData = false,
  realtimeTime = '',
}) {
  const wrapperRef = useRef(null);
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const tooltipRef = useRef(null);
  const tooltipHostRef = useRef(null);
  const [period, setPeriod] = useState('daily');
  const [loadedData, setLoadedData] = useState({
    daily: initialDaily || [],
    weekly: initialWeekly || [],
    monthly: initialMonthly || [],
  });
  const [loading, setLoading] = useState(false);
  const [chartBootTick, setChartBootTick] = useState(0);
  const [responsiveWidth, setResponsiveWidth] = useState(chartWidth);

  // 若通过 props 传入日线但未传入周线/月线，自动计算
  useEffect(() => {
    if (initialDaily && initialDaily.length > 0) {
      const weekly = initialWeekly?.length ? initialWeekly : resampleWeekly(initialDaily);
      const monthly = initialMonthly?.length ? initialMonthly : resampleMonthly(initialDaily);
      setLoadedData({
        daily: initialDaily,
        weekly,
        monthly,
      });
    }
  }, [initialDaily, initialWeekly, initialMonthly]);

  // 客户端加载数据（兜底）
  useEffect(() => {
    if (!symbol) return;
    if (loadedData.daily.length > 0) return;

    async function fetchData() {
      setLoading(true);
      try {
        const res = await fetch(`/data/prices/${symbol}.json`);
        if (res.ok) {
          const allData = await res.json();
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
    const updateWidth = () => {
      const nextWidth = wrapperRef.current?.clientWidth || chartWidth;
      if (nextWidth > 0) setResponsiveWidth(nextWidth);
    };

    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        updateWidth();
        setChartBootTick(t => t + 1);
      }
    };

    const onPageShow = () => {
      updateWidth();
      setChartBootTick(t => t + 1);
    };

    updateWidth();
    const timer = setTimeout(updateWidth, 120);
    const observer = typeof ResizeObserver !== 'undefined' && wrapperRef.current
      ? new ResizeObserver(updateWidth)
      : null;
    if (observer && wrapperRef.current) observer.observe(wrapperRef.current);

    window.addEventListener('resize', updateWidth);
    window.addEventListener('orientationchange', updateWidth);
    window.addEventListener('pageshow', onPageShow);
    document.addEventListener('visibilitychange', onVisibilityChange);

    return () => {
      clearTimeout(timer);
      if (observer) observer.disconnect();
      window.removeEventListener('resize', updateWidth);
      window.removeEventListener('orientationchange', updateWidth);
      window.removeEventListener('pageshow', onPageShow);
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, [chartWidth]);

  useEffect(() => {
    if (!containerRef.current) return;
    const effectiveWidth = Math.max(280, Math.floor(Math.min(chartWidth, responsiveWidth || chartWidth)));
    const data = dataMap[period];
    if (!data || data.length === 0) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }
    containerRef.current.innerHTML = '';

    const tv = window.LightweightCharts;
    if (!tv) return;

    // 创建 tooltip DOM
    const tooltipEl = document.createElement('div');
    tooltipEl.style.cssText = `
      position: static;
      background: rgba(255,255,255,0.98);
      border: 1px solid #d1d5db;
      border-radius: 4px;
      padding: 8px 10px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 12px;
      line-height: 1.35;
      box-shadow: 0 2px 6px rgba(0,0,0,0.08);
      pointer-events: none;
      width: ${effectiveWidth}px;
      margin: 0 auto 8px auto;
      display: none;
      box-sizing: border-box;
    `;
    tooltipEl.innerHTML = `
      <div style="margin-bottom:4px"><span style="font-weight:600;color:#666;margin-right:8px">日期:</span><span id="tt-date" style="font-weight:600">--</span></div>
      <div style="margin-bottom:4px"><span style="font-weight:600;color:#666;margin-right:8px">K线:</span>
        <span style="margin-right:12px">开:<span id="tt-open">--</span></span>
        <span style="margin-right:12px">高:<span id="tt-high">--</span></span>
        <span style="margin-right:12px">低:<span id="tt-low">--</span></span>
        <span>收:<span id="tt-close">--</span></span>
      </div>
      <div style="margin-bottom:4px"><span style="font-weight:600;color:#666;margin-right:8px">均线:</span>
        <span style="margin-right:10px;color:${MA_COLORS.ma5}">MA5:<span id="tt-ma5">--</span></span>
        <span style="margin-right:10px;color:${MA_COLORS.ma10}">MA10:<span id="tt-ma10">--</span></span>
        <span style="margin-right:10px;color:${MA_COLORS.ma20}">MA20:<span id="tt-ma20">--</span></span>
        <span style="margin-right:10px;color:${MA_COLORS.ma60}">MA60:<span id="tt-ma60">--</span></span>
        <span style="color:${MA_COLORS.ma120}">MA120:<span id="tt-ma120">--</span></span>
      </div>
      <div style="margin-bottom:4px"><span style="font-weight:600;color:#666;margin-right:8px">MACD:</span>
        <span style="margin-right:12px;color:#0066cc">DIF:<span id="tt-dif">--</span></span>
        <span style="color:#ff9900">DEA:<span id="tt-dea">--</span></span>
      </div>
      <div><span style="font-weight:600;color:#666;margin-right:8px">KDJ:</span>
        <span style="margin-right:12px;color:#ff6b6b">K:<span id="tt-k">--</span></span>
        <span style="margin-right:12px;color:#4ecdc4">D:<span id="tt-d">--</span></span>
        <span style="color:#45b7d1">J:<span id="tt-j">--</span></span>
      </div>
    `;
    if (tooltipHostRef.current) {
      tooltipHostRef.current.innerHTML = '';
      tooltipHostRef.current.appendChild(tooltipEl);
    }
    tooltipRef.current = tooltipEl;

    const chart = tv.createChart(containerRef.current, {
      layout: {
        background: { type: 'solid', color: '#ffffff' },
        textColor: '#333333',
      },
      grid: {
        vertLines: { color: '#f0f0f0' },
        horzLines: { color: '#f0f0f0' },
      },
      crosshair: {
        mode: tv.CrosshairMode.Magnet,
        vertLine: {
          color: '#758696',
          labelBackgroundColor: '#758696',
          width: 1,
          style: 2,
          visible: true,
          labelVisible: true,
        },
        horzLine: {
          color: '#758696',
          labelBackgroundColor: '#758696',
          width: 1,
          style: 2,
          visible: true,
          labelVisible: true,
        },
      },
      rightPriceScale: { borderColor: '#e0e0e0' },
      timeScale: {
        borderColor: '#e0e0e0',
        timeVisible: false,
        // 不锁边，允许左右留白；否则数据会被强制撑满宽度
        fixLeftEdge: false,
        fixRightEdge: false,
        lockVisibleTimeRangeOnResize: true,
      },
      width: effectiveWidth,
      height: 650,
    });

    chartRef.current = chart;

    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        const nextWidth = Math.max(280, Math.floor(Math.min(chartWidth, wrapperRef.current?.clientWidth || responsiveWidth || chartWidth)));
        chartRef.current.applyOptions({ width: nextWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    // 准备数据
    const timeField = data[0]?.date ? 'date' : 'trade_date';
    const closes = data.map(d => d.close);

    // 计算均线
    const ma5 = ma(closes, 5);
    const ma10 = ma(closes, 10);
    const ma20 = ma(closes, 20);
    const ma60 = ma(closes, 60);
    const ma120 = ma(closes, 120);

    // 计算MACD
    const { dif, dea, macdBar } = calculateMACD(closes);

    // 计算KDJ
    const { k, d, j } = calculateKDJ(data);

    // ========== 主图（K线+均线）==========
    const candleSeries = chart.addCandlestickSeries({
      upColor: '#ff4757',
      downColor: '#2ed573',
      borderUpColor: '#ff4757',
      borderDownColor: '#2ed573',
      wickUpColor: '#ff4757',
      wickDownColor: '#2ed573',
    });

    const candles = data.map(row => ({
      time: row[timeField],
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
    }));
    candleSeries.setData(candles);

    const maSeries = {};
    const addMaSeries = (vals, color, key) => {
      const s = chart.addLineSeries({
        color,
        lineWidth: 1,
        priceScaleId: 'right',
        lastValueVisible: false,
        priceLineVisible: false,
        title: '',
        crosshairMarkerVisible: false,
      });
      const seriesData = data.map((row, i) => ({
        time: row[timeField],
        value: vals[i],
      })).filter(d => d.value !== null && !isNaN(d.value));
      s.setData(seriesData);
      maSeries[key] = s;
      return s;
    };

    addMaSeries(ma5, MA_COLORS.ma5, 'ma5');
    addMaSeries(ma10, MA_COLORS.ma10, 'ma10');
    addMaSeries(ma20, MA_COLORS.ma20, 'ma20');
    addMaSeries(ma60, MA_COLORS.ma60, 'ma60');
    addMaSeries(ma120, MA_COLORS.ma120, 'ma120');

    candleSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.18, bottom: 0.55 },
    });

    // ========== 成交量（副图1）==========
    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    const volumeData = data.map(row => ({
      time: row[timeField],
      value: row.volume || 0,
      color: (row.close || 0) >= (row.open || 0) ? 'rgba(255, 71, 87, 0.5)' : 'rgba(46, 213, 115, 0.5)',
    }));
    volumeSeries.setData(volumeData);
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.48, bottom: 0.35 },
    });

    // ========== MACD（副图2）==========
    const macdDifSeries = chart.addLineSeries({
      color: '#0066cc', lineWidth: 1,
      priceScaleId: 'macd',
      lastValueVisible: false,
      priceLineVisible: false,
      title: '',
    });
    macdDifSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: dif[i],
    })).filter(d => d.value !== null && !isNaN(d.value)));

    const macdDeaSeries = chart.addLineSeries({
      color: '#ff9900', lineWidth: 1,
      priceScaleId: 'macd',
      lastValueVisible: false,
      priceLineVisible: false,
      title: '',
    });
    macdDeaSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: dea[i],
    })).filter(d => d.value !== null && !isNaN(d.value)));

    const macdBarSeries = chart.addHistogramSeries({
      priceScaleId: 'macd',
      lastValueVisible: false,
    });
    macdBarSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: macdBar[i],
      color: (macdBar[i] || 0) >= 0 ? '#ff4757' : '#2ed573',
    })).filter(d => d.value !== null && !isNaN(d.value)));

    chart.priceScale('macd').applyOptions({
      scaleMargins: { top: 0.68, bottom: 0.18 },
    });

    // ========== KDJ（副图3）==========
    const kdjKSeries = chart.addLineSeries({
      color: '#ff6b6b', lineWidth: 1,
      priceScaleId: 'kdj',
      lastValueVisible: false,
      priceLineVisible: false,
      title: '',
    });
    kdjKSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: k[i],
    })).filter(d => d.value !== null && !isNaN(d.value)));

    const kdjDSeries = chart.addLineSeries({
      color: '#4ecdc4', lineWidth: 1,
      priceScaleId: 'kdj',
      lastValueVisible: false,
      priceLineVisible: false,
      title: '',
    });
    kdjDSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: d[i],
    })).filter(d => d.value !== null && !isNaN(d.value)));

    const kdjJSeries = chart.addLineSeries({
      color: '#45b7d1', lineWidth: 1,
      priceScaleId: 'kdj',
      lastValueVisible: false,
      priceLineVisible: false,
      title: '',
    });
    kdjJSeries.setData(data.map((row, i) => ({
      time: row[timeField],
      value: j[i],
    })).filter(d => d.value !== null && !isNaN(d.value)));

    chart.priceScale('kdj').applyOptions({
      scaleMargins: { top: 0.85, bottom: 0.02 },
    });

    // K线宽度本质由 timeScale 决定：barSpacing 越大，K线实体通常越宽
    // 若显式 setVisibleLogicalRange(全部数据)，会把图强制压缩到满宽，barSpacing 体感会变弱
    const RIGHT_OFFSET = 2;
    chart.timeScale().applyOptions({
      barSpacing,
      minBarSpacing: 0.5,
      rightOffset: RIGHT_OFFSET,
    });

    const periodLimits = {
      daily: dailyLimit,
      weekly: weeklyLimit,
      monthly: monthlyLimit,
    };

    const totalBars = candles.length;
    const configuredLimit = periodLimits[period];
    const autoVisibleBars = Math.max(30, Math.floor(effectiveWidth / Math.max(1, barSpacing)));
    const requestedBaseBars = initialVisibleBars && initialVisibleBars > 0 ? initialVisibleBars : autoVisibleBars;
    const limitBaseBars = configuredLimit && configuredLimit > 0
      ? Math.min(configuredLimit, requestedBaseBars)
      : requestedBaseBars;
    const visibleBars = Math.max(10, Math.min(totalBars, limitBaseBars + zoomOutBars));

    // 始终设置初始可视范围，避免默认把全部数据压缩到固定宽度
    chart.timeScale().setVisibleLogicalRange({
      from: totalBars - visibleBars,
      to: totalBars + RIGHT_OFFSET,
    });

    // 启用鼠标滚轮缩放
    chart.applyOptions({
      handleScroll: { vertTouchDrag: false },
      handleScale: {
        axisPressedMouseMove: { time: true, price: false },
      },
    });

    // ========== Tooltip 更新 ==========
    function setText(id, text) {
      const el = tooltipEl.querySelector('#tt-' + id);
      if (el) el.textContent = text;
    }

    // 初始化显示最新数据
    if (candles.length > 0) {
      tooltipEl.style.display = 'block';
      const latest = candles[candles.length - 1];
      setText('date', isRealtimeData && realtimeTime ? realtimeTime : latest.time);
      setText('open', latest.open?.toFixed(2) ?? '--');
      setText('high', latest.high?.toFixed(2) ?? '--');
      setText('low', latest.low?.toFixed(2) ?? '--');
      setText('close', latest.close?.toFixed(2) ?? '--');

      const lastValid = (arr) => {
        for (let i = arr.length - 1; i >= 0; i--) {
          if (arr[i] !== null && !isNaN(arr[i])) return arr[i];
        }
        return null;
      };
      setText('ma5', lastValid(ma5)?.toFixed(2) ?? '--');
      setText('ma10', lastValid(ma10)?.toFixed(2) ?? '--');
      setText('ma20', lastValid(ma20)?.toFixed(2) ?? '--');
      setText('ma60', lastValid(ma60)?.toFixed(2) ?? '--');
      setText('ma120', lastValid(ma120)?.toFixed(2) ?? '--');
      setText('dif', lastValid(dif)?.toFixed(3) ?? '--');
      setText('dea', lastValid(dea)?.toFixed(3) ?? '--');
      setText('k', lastValid(k)?.toFixed(2) ?? '--');
      setText('d', lastValid(d)?.toFixed(2) ?? '--');
      setText('j', lastValid(j)?.toFixed(2) ?? '--');
    }

    chart.subscribeCrosshairMove(function(param) {
      if (!param.time || !param.seriesData) {
        // 鼠标移出时恢复最新数据
        if (candles.length > 0) {
          const latest = candles[candles.length - 1];
          setText('date', isRealtimeData && realtimeTime ? realtimeTime : latest.time);
          setText('open', latest.open?.toFixed(2) ?? '--');
          setText('high', latest.high?.toFixed(2) ?? '--');
          setText('low', latest.low?.toFixed(2) ?? '--');
          setText('close', latest.close?.toFixed(2) ?? '--');
        }
        return;
      }
      const sd = param.seriesData;

      // 如果选中的是最右边的一根K线，并且是实盘数据，则显示精确时间
      if (isRealtimeData && realtimeTime && param.time === candles[candles.length - 1].time) {
        setText('date', realtimeTime);
      } else {
        setText('date', param.time);
      }

      const candle = sd.get(candleSeries);
      if (candle) {
        setText('open', candle.open?.toFixed(2) ?? '--');
        setText('high', candle.high?.toFixed(2) ?? '--');
        setText('low', candle.low?.toFixed(2) ?? '--');
        setText('close', candle.close?.toFixed(2) ?? '--');
      }

      const ma5v = sd.get(maSeries.ma5);
      setText('ma5', (ma5v?.value)?.toFixed(2) ?? '--');
      const ma10v = sd.get(maSeries.ma10);
      setText('ma10', (ma10v?.value)?.toFixed(2) ?? '--');
      const ma20v = sd.get(maSeries.ma20);
      setText('ma20', (ma20v?.value)?.toFixed(2) ?? '--');
      const ma60v = sd.get(maSeries.ma60);
      setText('ma60', (ma60v?.value)?.toFixed(2) ?? '--');
      const ma120v = sd.get(maSeries.ma120);
      setText('ma120', (ma120v?.value)?.toFixed(2) ?? '--');

      const difv = sd.get(macdDifSeries);
      setText('dif', (difv?.value)?.toFixed(3) ?? '--');
      const deav = sd.get(macdDeaSeries);
      setText('dea', (deav?.value)?.toFixed(3) ?? '--');

      const kv = sd.get(kdjKSeries);
      setText('k', (kv?.value)?.toFixed(2) ?? '--');
      const dv = sd.get(kdjDSeries);
      setText('d', (dv?.value)?.toFixed(2) ?? '--');
      const jv = sd.get(kdjJSeries);
      setText('j', (jv?.value)?.toFixed(2) ?? '--');
    });

    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [
    period,
    loadedData.daily,
    loadedData.weekly,
    loadedData.monthly,
    chartWidth,
    responsiveWidth,
    chartBootTick,
    barSpacing,
    zoomOutBars,
    initialVisibleBars,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
  ]);

  const displayWidth = Math.max(280, Math.floor(Math.min(chartWidth, responsiveWidth || chartWidth)));

  return (
    <div ref={wrapperRef} style={{ width: '100%' }}>
      <div className="flex items-center justify-between mb-2 gap-2 flex-wrap">
        <div>
          {isRealtimeData ? (
            <span className="inline-flex items-center px-2 py-1 text-xs font-semibold rounded bg-red-100 text-red-700">
              实盘中 {realtimeTime ? `· ${realtimeTime}` : ''}
            </span>
          ) : (
            <span className="inline-flex items-center px-2 py-1 text-xs font-semibold rounded bg-gray-100 text-gray-600">
              收盘数据
            </span>
          )}
        </div>
        <div className="flex justify-end gap-1">
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
      </div>
      {loading && (
        <div className="flex items-center justify-center h-[650px] text-gray-400">
          <span>加载图表数据...</span>
        </div>
      )}
      <div ref={tooltipHostRef} />
      <div
        ref={containerRef}
        style={{ width: displayWidth, maxWidth: '100%', margin: '0 auto', height: 650, display: loading ? 'none' : 'block', position: 'relative' }}
      />
    </div>
  );
}

// 简单移动平均
function ma(arr, n) {
  return arr.map((_, i) => {
    if (i < n - 1) return null;
    return arr.slice(i - n + 1, i + 1).reduce((a, b) => a + b, 0) / n;
  });
}

// MACD 计算
function calculateMACD(closes, fast = 12, slow = 26, signal = 9) {
  const ema = (data, span) => {
    const k = 2 / (span + 1);
    const res = [data[0]];
    for (let i = 1; i < data.length; i++) {
      res.push(data[i] * k + res[i - 1] * (1 - k));
    }
    return res;
  };
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  const dif = emaFast.map((v, i) => v - emaSlow[i]);
  const dea = ema(dif, signal);
  const macdBar = dif.map((v, i) => (v - dea[i]) * 2);
  return { dif, dea, macdBar };
}

// KDJ 计算
function calculateKDJ(data, n = 9, m1 = 3, m2 = 3) {
  const lows = data.map(d => d.low);
  const highs = data.map(d => d.high);
  const closes = data.map(d => d.close);

  const rsv = [];
  for (let i = 0; i < data.length; i++) {
    if (i < n - 1) {
      rsv.push(null);
      continue;
    }
    const lowN = Math.min(...lows.slice(i - n + 1, i + 1));
    const highN = Math.max(...highs.slice(i - n + 1, i + 1));
    rsv.push(highN === lowN ? 50 : (closes[i] - lowN) / (highN - lowN) * 100);
  }

  const k = new Array(data.length).fill(null);
  const d = new Array(data.length).fill(null);
  const j = new Array(data.length).fill(null);

  const firstIdx = rsv.findIndex(v => v !== null);
  if (firstIdx === -1) return { k, d, j };

  k[firstIdx] = (2 / 3) * 50 + (1 / 3) * rsv[firstIdx];
  d[firstIdx] = (2 / 3) * 50 + (1 / 3) * k[firstIdx];
  j[firstIdx] = 3 * k[firstIdx] - 2 * d[firstIdx];

  for (let i = firstIdx + 1; i < data.length; i++) {
    k[i] = (2 / 3) * k[i - 1] + (1 / 3) * rsv[i];
    d[i] = (2 / 3) * d[i - 1] + (1 / 3) * k[i];
    j[i] = 3 * k[i] - 2 * d[i];
  }

  return { k, d, j };
}

// 周线重采样：以每周五为key
function resampleWeekly(dailyData) {
  if (!dailyData || dailyData.length === 0) return [];
  const weeks = {};
  dailyData.forEach(d => {
    const dateStr = d.date || d.trade_date;
    const date = new Date(dateStr);
    const day = date.getDay(); // 0=Sun, 1=Mon, ..., 6=Sat
    let diff;
    if (day === 0) diff = -2;      // Sunday -> last Friday
    else if (day === 6) diff = -1; // Saturday -> last Friday
    else diff = 5 - day;           // Mon-Fri -> this Friday
    const friday = new Date(date);
    friday.setDate(date.getDate() + diff);
    const weekKey = friday.toISOString().split('T')[0];

    if (!weeks[weekKey]) {
      weeks[weekKey] = {
        date: weekKey,
        trade_date: weekKey,
        time: weekKey,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
        volume: d.volume || 0,
      };
    } else {
      weeks[weekKey].high = Math.max(weeks[weekKey].high, d.high);
      weeks[weekKey].low = Math.min(weeks[weekKey].low, d.low);
      weeks[weekKey].close = d.close;
      weeks[weekKey].volume += (d.volume || 0);
    }
  });
  return Object.values(weeks);
}

// 月线重采样：以每月最后一个交易日为key
function resampleMonthly(dailyData) {
  if (!dailyData || dailyData.length === 0) return [];
  const months = {};
  dailyData.forEach(d => {
    const dateStr = d.date || d.trade_date;
    const date = new Date(dateStr);
    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

    if (!months[monthKey]) {
      months[monthKey] = {
        date: dateStr,
        trade_date: dateStr,
        time: dateStr,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
        volume: d.volume || 0,
      };
    } else {
      months[monthKey].high = Math.max(months[monthKey].high, d.high);
      months[monthKey].low = Math.min(months[monthKey].low, d.low);
      months[monthKey].close = d.close;
      months[monthKey].volume += (d.volume || 0);
      months[monthKey].date = dateStr;
      months[monthKey].trade_date = dateStr;
      months[monthKey].time = dateStr;
    }
  });
  return Object.values(months);
}

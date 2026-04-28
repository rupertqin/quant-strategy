import { useState, useEffect, useCallback, useMemo } from 'react';
import { DEFAULT_POOL } from '../utils/defaultPool.js';

const STORAGE_KEY = 'quant_stock_pool';

export function useStockPool() {
  const [pool, setPool] = useState([]);
  const [initialized, setInitialized] = useState(false);

  // 初始化：从 localStorage 读取，无数据则使用默认股票池
  useEffect(() => {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          setPool(parsed);
        } else {
          setPool(DEFAULT_POOL);
        }
      } catch {
        setPool(DEFAULT_POOL);
      }
    } else {
      setPool(DEFAULT_POOL);
    }
    setInitialized(true);
  }, []);

  // 持久化到 localStorage
  useEffect(() => {
    if (initialized) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(pool));
    }
  }, [pool, initialized]);

  const add = useCallback((symbol) => {
    const s = symbol.trim().toUpperCase();
    if (!s) return false;
    setPool(prev => {
      if (prev.includes(s)) return prev;
      return [...prev, s];
    });
    return true;
  }, []);

  const remove = useCallback((symbol) => {
    setPool(prev => prev.filter(s => s !== symbol));
  }, []);

  const reset = useCallback(() => {
    setPool(DEFAULT_POOL);
  }, []);

  const clear = useCallback(() => {
    setPool([]);
  }, []);

  const isInPool = useCallback((symbol) => {
    return pool.includes(symbol);
  }, [pool]);

  const poolSet = useMemo(() => new Set(pool), [pool]);

  return { pool, poolSet, add, remove, reset, clear, isInPool, initialized };
}

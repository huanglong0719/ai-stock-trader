/**
 * 股票数据管理 Hook
 * 统一管理股票数据的获取、缓存和更新逻辑
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import { cacheService } from '../utils/db';

/**
 * 股票数据 Hook
 * @param {string} symbol - 股票代码
 * @param {string} freq - K线周期 (D/W/M)
 * @returns {Object} 股票数据和操作方法
 */
export const useStockData = (symbol, freq = 'D') => {
  const [klineData, setKlineData] = useState([]);
  const [quoteData, setQuoteData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const abortControllerRef = useRef(null);
  const fetchSeqRef = useRef(0);

  /**
   * 获取K线数据
   */
  const fetchKlineData = useCallback(async () => {
    if (!symbol) return;

    // 取消之前的请求
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    const seq = ++fetchSeqRef.current;
    abortControllerRef.current = new AbortController();
    
    setLoading(true);
    setError(null);

    try {
      // 1. 尝试从缓存读取
      const cached = await cacheService.getKline(symbol, freq);
      if (cached && seq === fetchSeqRef.current) {
        setKlineData(cached.data);
      }

      // 2. 获取最新数据
      const response = await axios.get(`/api/market/kline/${symbol}`, {
        params: { freq },
        signal: abortControllerRef.current.signal
      });

      if (seq === fetchSeqRef.current) {
        const data = response.data || [];
        setKlineData(data);
        
        // 3. 更新缓存
        if (data.length > 0) {
          await cacheService.saveKline(symbol, freq, data);
        }
      }
    } catch (err) {
      if (err.name !== 'CanceledError' && seq === fetchSeqRef.current) {
        setError(err.message);
        console.error('Failed to fetch kline data:', err);
      }
    } finally {
      if (seq === fetchSeqRef.current) {
        setLoading(false);
      }
    }
  }, [symbol, freq]);

  /**
   * 获取实时报价
   */
  const fetchQuoteData = useCallback(async () => {
    if (!symbol) return;

    try {
      const response = await axios.get(`/api/market/quote/${symbol}`);
      setQuoteData(response.data);
      
      // 缓存报价数据
      await cacheService.saveQuote(symbol, response.data);
    } catch (err) {
      console.error('Failed to fetch quote data:', err);
    }
  }, [symbol]);

  /**
   * 刷新数据
   */
  const refresh = useCallback(() => {
    fetchKlineData();
    fetchQuoteData();
  }, [fetchKlineData, fetchQuoteData]);

  // 当 symbol 或 freq 变化时重新获取数据
  useEffect(() => {
    fetchKlineData();
    fetchQuoteData();

    return () => {
      // 清理：取消未完成的请求
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [fetchKlineData, fetchQuoteData]);

  return {
    klineData,
    quoteData,
    loading,
    error,
    refresh
  };
};

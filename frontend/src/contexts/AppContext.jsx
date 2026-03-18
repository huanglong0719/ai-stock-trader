/**
 * 全局应用状态管理
 * 使用 Context API 统一管理应用状态
 */
import React, { createContext, useState, useCallback } from 'react';
import { storageService } from '../utils/storage';

const AppContext = createContext(null);

/**
 * 应用状态提供者
 */
export const AppProvider = ({ children }) => {
  // 选中的股票
  const [selectedStock, setSelectedStock] = useState(
    localStorage.getItem('selectedStock') || '002353.SZ'
  );

  // K线周期
  const [freq, setFreq] = useState('D');

  // 自选股列表
  const [watchlist, setWatchlist] = useState(() => {
    const saved = storageService.load();
    return Array.isArray(saved) ? saved : [];
  });

  // 实时更新开关
  const [realtimeEnabled, setRealtimeEnabled] = useState(
    localStorage.getItem('realtimeEnabled') !== 'false'
  );

  /**
   * 选择股票
   */
  const selectStock = useCallback((symbol) => {
    setSelectedStock(symbol);
    setFreq('D');
    localStorage.setItem('selectedStock', symbol);
  }, []);

  /**
   * 切换K线周期
   */
  const changeFreq = useCallback((newFreq) => {
    setFreq(newFreq);
  }, []);

  /**
   * 添加到自选
   */
  const addToWatchlist = useCallback((stock) => {
    setWatchlist(prev => {
      // 去重
      const exists = prev.find(s => s.ts_code === stock.ts_code);
      if (exists) return prev;
      
      const newList = [stock, ...prev];
      storageService.save(newList);
      return newList;
    });
  }, []);

  /**
   * 从自选移除
   */
  const removeFromWatchlist = useCallback((ts_code) => {
    setWatchlist(prev => {
      const newList = prev.filter(s => s.ts_code !== ts_code);
      storageService.save(newList);
      return newList;
    });
  }, []);

  /**
   * 更新自选股列表
   */
  const updateWatchlist = useCallback((newList) => {
    setWatchlist(newList);
    storageService.save(newList);
  }, []);

  /**
   * 切换实时更新
   */
  const toggleRealtime = useCallback(() => {
    setRealtimeEnabled(prev => {
      const newValue = !prev;
      localStorage.setItem('realtimeEnabled', newValue);
      return newValue;
    });
  }, []);

  const value = {
    // 状态
    selectedStock,
    freq,
    watchlist,
    realtimeEnabled,
    
    // 操作方法
    selectStock,
    changeFreq,
    addToWatchlist,
    removeFromWatchlist,
    updateWatchlist,
    toggleRealtime
  };

  return (
    <AppContext.Provider value={value}>
      {children}
    </AppContext.Provider>
  );
};

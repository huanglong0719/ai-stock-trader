import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Layout, Card, Tag, Spin, message, Typography } from 'antd';
import { RobotOutlined } from '@ant-design/icons';
import { KlineChart } from './components/KlineChart';
import TopBar from './components/TopBar';
// DataStatusBar is now used in SidePanel
import TradingPanel from './components/TradingPanel';
import MarketIndices from './components/MarketIndices';
import SidePanel from './components/SidePanel';
import ReviewModal from './components/ReviewModal';
import StockSelector from './components/StockSelector';
import PlansModal from './components/PlansModal';
import AIChatWindow from './components/AIChatWindow';
import SystemNavigator from './components/SystemNavigator';
import MemoryModal from './components/MemoryModal';
import RewardPunishModal from './components/RewardPunishModal';

import axios from 'axios';
import ReactMarkdown from 'react-markdown';
import PinyinMatch from 'pinyin-match';
import { cacheService } from './utils/db';
import { storageService } from './utils/storage';

const { Header, Content } = Layout;
const { Text } = Typography;

const isRequestAborted = (err) => {
  if (!err) return false;
  if (err.code === 'ERR_CANCELED') return true;
  if (err.name === 'CanceledError') return true;
  if (err.message === 'canceled') return true;
  if (/ERR_ABORTED/i.test(String(err.message || ''))) return true;
  if (/ERR_ABORTED/i.test(String(err.stack || ''))) return true;
  if (err.config?.signal?.aborted) return true;
  if (err.request && err.request.status === 0 && /aborted/i.test(String(err.message || ''))) return true;
  if (err.cause && err.cause.name === 'AbortError') return true;
  return false;
};

const shouldSilenceAxiosError = (err) => {
  if (!err) return true;
  if (isRequestAborted(err)) return true;
  const hasResponse = !!err.response;
  if (hasResponse) return false;
  if (err.code === 'ERR_NETWORK') return true;
  if (err.request && err.request.status === 0) return true;
  if (/network error/i.test(String(err.message || ''))) return true;
  return false;
};

// Simple debounce utility
const debounce = (func, wait) => {
  let timeout;
  return function (...args) {
    const context = this;
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(context, args), wait);
  };
};

const isTradingTime = () => {
  const now = new Date();
  const day = now.getDay();
  const hour = now.getHours();
  const minute = now.getMinutes();
  const time = hour * 100 + minute;

  if (day === 0 || day === 6) return false;

  return (time >= 910 && time <= 1135) || (time >= 1255 && time <= 1505);
};

const isTradingSessionTime = () => {
  const now = new Date();
  const day = now.getDay();
  const hour = now.getHours();
  const minute = now.getMinutes();
  const time = hour * 100 + minute;

  if (day === 0 || day === 6) return false;

  return time >= 910 && time <= 1505;
};

const isPostMarketRefreshTime = () => {
  const now = new Date();
  const day = now.getDay();
  const hour = now.getHours();
  const minute = now.getMinutes();
  const time = hour * 100 + minute;
  if (day === 0 || day === 6) return false;
  return time > 1505 && time <= 1830;
};

const App = () => {
  const [selectedStock, setSelectedStock] = useState(localStorage.getItem('selectedStock') || '002353.SZ');
  const [freq, setFreq] = useState('D');
  const [klineData, setKlineData] = useState(null); // 修改为 null，表示初始未知状态
  const [quoteData, setQuoteData] = useState(null);
  const [realtimeEnabled, setRealtimeEnabled] = useState(localStorage.getItem('realtimeEnabled') !== 'false');
  const realtimeRef = useRef(localStorage.getItem('realtimeEnabled') !== 'false');
  const lastOverviewFetchTsRef = useRef(0);
  const lastPostMarketFetchTsRef = useRef(0);

  // AI 提供商管理
  const [availableProviders, setAvailableProviders] = useState([]);
  const [analysisProvider, setAnalysisProvider] = useState(localStorage.getItem('analysisProvider') || 'Xiaomi MiMo');
  const [chatProvider, setChatProvider] = useState(localStorage.getItem('chatProvider') || 'Xiaomi MiMo');
  const [analysisApiKey, setAnalysisApiKey] = useState(localStorage.getItem('analysisApiKey') || '');
  const [chatApiKey, setChatApiKey] = useState(localStorage.getItem('chatApiKey') || '');

  useEffect(() => {
    // 获取可用的 AI 提供商
    axios.get('/api/ai/providers')
      .then(res => {
        const providers = Array.isArray(res.data) ? res.data : [];
        setAvailableProviders(providers);
        setAnalysisProvider(prev => (providers.includes(prev) ? prev : (providers[0] || prev)));
        setChatProvider(prev => (providers.includes(prev) ? prev : (providers[0] || prev)));
      })
      .catch(err => {
        console.error('获取 AI 提供商列表失败:', err);
      });
  }, []);

  useEffect(() => {
    localStorage.setItem('analysisProvider', analysisProvider);
  }, [analysisProvider]);

  useEffect(() => {
    localStorage.setItem('chatProvider', chatProvider);
  }, [chatProvider]);

  useEffect(() => {
    localStorage.setItem('analysisApiKey', analysisApiKey);
  }, [analysisApiKey]);

  useEffect(() => {
    localStorage.setItem('chatApiKey', chatApiKey);
  }, [chatApiKey]);

  useEffect(() => {
    realtimeRef.current = realtimeEnabled;
    localStorage.setItem('realtimeEnabled', realtimeEnabled);
  }, [realtimeEnabled]);

  const [lastUpdateTime, setLastUpdateTime] = useState(null);

  // 股票列表管理
  const [allStocks, setAllStocks] = useState([]);
  const allStocksRef = useRef([]);

  useEffect(() => {
    allStocksRef.current = allStocks;
  }, [allStocks]); // 所有股票

  const [watchlist, setWatchlist] = useState([]); // 自选股
  const watchlistRef = useRef([]);

  useEffect(() => {
    watchlistRef.current = watchlist;
  }, [watchlist]);
  const [searchOptions, setSearchOptions] = useState([]); // 搜索结果

  const [loading, setLoading] = useState(false);
  const [klineLoading, setKlineLoading] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiAnalysis, setAiAnalysis] = useState(null);

  // 选股状态
  const [selectorLoading, setSelectorLoading] = useState(false);
  const [selectorResults, setSelectorResults] = useState({}); // 修改为对象存储不同策略结果
  const [selectorStrategy, setSelectorStrategy] = useState('default');
  const [syncStatus, setSyncStatus] = useState(storageService.get('sync_status') || null);
  const [showSelector, setShowSelector] = useState(false);
  const [selectorLogs, setSelectorLogs] = useState([]); // 选股执行日志
  const [marketOverview, setMarketOverview] = useState(storageService.get('market_overview') || null); // 大盘指数概览
  const marketOverviewRef = useRef(marketOverview);

  useEffect(() => {
    marketOverviewRef.current = marketOverview;
  }, [marketOverview]);

  // 复盘状态
  const [showReviewModal, setShowReviewModal] = useState(false);
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewData, setReviewData] = useState(null);
  const [reviewLogs, setReviewLogs] = useState([]);
  const reviewAbortRef = useRef(null);
  const reviewLogIntervalRef = useRef(null);
  const reviewEventSourceRef = useRef(null);

  // 交易计划状态
  const [plansModalVisible, setPlansModalVisible] = useState(false);
  const [plansData, setPlansData] = useState([]);
  const [plansLoading, setPlansLoading] = useState(false);
  const [navVisible, setNavVisible] = useState(false);
  const [memoryModalVisible, setMemoryModalVisible] = useState(false);
  const [rewardPunishVisible, setRewardPunishVisible] = useState(false);
  const [rewardPunishLoading, setRewardPunishLoading] = useState(false);
  const [rewardPunishData, setRewardPunishData] = useState(null);

  // 交易面板状态
  const [tradingPanelVisible, setTradingPanelVisible] = useState(false);

  const wsRef = useRef(null);
  const fetchSeqRef = useRef(0);
  const lastSelectedStockRef = useRef(selectedStock);

  useEffect(() => {
    // 优先从持久化缓存加载同步状态
    const cachedStatus = storageService.get('sync_status');
    if (cachedStatus) {
      setSyncStatus(cachedStatus);
    }
    checkSyncStatus();
  }, []);

  const checkSyncStatus = async () => {
    try {
      const res = await axios.get('/api/sync/status', { timeout: 5000 }); // 设置5秒超时
      if (res.data) {
        setSyncStatus(res.data);
        storageService.set('sync_status', res.data);
      }
    } catch (err) {
      if (!shouldSilenceAxiosError(err)) {
        console.warn("Failed to check sync status:", err.message);
      }
      // 如果没有旧状态，设置一个默认状态避免无限loading
      setSyncStatus(prev => prev || {
        status: "Unknown",
        data_quality: {
          status: "Unknown",
          latest_trade_date: "未知",
          latest_coverage: "未知"
        }
      });
    }
  };

  // 动态轮询同步状态：如果正在运行任务，则加快轮询频率
  useEffect(() => {
    const isRunning = syncStatus?.current_task?.status === 'running';
    const delay = isRunning ? 2000 : 60000;

    const timer = setInterval(checkSyncStatus, delay);
    return () => clearInterval(timer);
  }, [syncStatus?.current_task?.status]);

  // 初始化加载股票列表
  const initStocks = useCallback(async () => {
    try {
      // 1. 优先从本地缓存加载自选股和全部股票列表，立即展示 UI
      const savedWatchlistRaw = storageService.load();
      const cachedStocksRaw = storageService.getAllStocks();
      const savedSelector = storageService.loadSelector();

      const savedWatchlist = Array.isArray(savedWatchlistRaw) ? savedWatchlistRaw : [];
      const cachedStocks = Array.isArray(cachedStocksRaw) ? cachedStocksRaw : [];

      // 批量更新基础状态，减少重渲染次数
      if (savedWatchlist.length > 0) setWatchlist(savedWatchlist);
      if (cachedStocks.length > 0) setAllStocks(cachedStocks);
      if (savedSelector) {
        if (Array.isArray(savedSelector.data)) {
          setSelectorResults({ default: savedSelector });
        } else {
          setSelectorResults(savedSelector);
        }
      }

      // 如果完全没数据，才显示 loading
      if (cachedStocks.length === 0) {
        setLoading(true);
      }

      // 尝试用本地缓存的报价填充自选股，保证离线可见
      if (savedWatchlist.length > 0) {
        try {
          const cachedQuotes = await Promise.all(
            savedWatchlist.map(item => cacheService.getQuote(item.ts_code))
          );
          const quotesMap = {};
          cachedQuotes.forEach((q, idx) => {
            if (q && (q.ts_code || q.symbol)) {
              const key = q.ts_code || q.symbol || savedWatchlist[idx]?.ts_code;
              quotesMap[key] = q;
            }
          });
          if (Object.keys(quotesMap).length > 0) {
            setWatchlist(prev => prev.map(s => ({
              ...s,
              ...(quotesMap[s.ts_code] || {})
            })));
          }
        } catch (e) {
          console.warn('加载缓存报价失败', e);
        }
      }

      // 2. 异步并行获取最新数据
      const fetchAll = async () => {
        try {
          const [stocksRes] = await Promise.all([
            axios.get('/api/market/stocks', { timeout: 15000 }) // 设置 15 秒超时
          ]);

          const nextStocks = Array.isArray(stocksRes.data) ? stocksRes.data : [];
          setAllStocks(nextStocks);
          storageService.saveAllStocks(nextStocks);

          // 如果自选股为空，初始化默认值
          if (savedWatchlist.length === 0 && nextStocks.length > 0) {
            const initial = nextStocks.slice(0, 20);
            setWatchlist(initial);
            storageService.save(initial);
          }

          // 异步更新自选股最新行情
          const currentWatchlist = savedWatchlist.length > 0 ? savedWatchlist : (nextStocks.slice(0, 20));
          if (currentWatchlist.length > 0) {
            const codes = currentWatchlist.map(s => s.ts_code);
            const quotesRes = await axios.post('/api/market/quotes', codes, { timeout: 10000 });
            const quotesMap = {};
            (Array.isArray(quotesRes.data) ? quotesRes.data : []).forEach(q => {
              if (q.ts_code) {
                quotesMap[q.ts_code] = q;
              } else if (q.symbol) {
                quotesMap[q.symbol] = q;
              }
            });

            setWatchlist(prev => prev.map(s => ({
              ...s,
              ...(quotesMap[s.ts_code] || {})
            })));
          }
        } catch (err) {
          if (!shouldSilenceAxiosError(err)) {
            console.warn("Silent background update failed:", err.message);
          }
        } finally {
          setLoading(false);
        }
      };

      fetchAll();
    } catch (err) {
      if (!shouldSilenceAxiosError(err)) {
        console.error("Initialization failed:", err);
      }
    }
  }, []);

  useEffect(() => {
    initStocks();

    // 增加一个额外的检查，如果 allStocks 为空，则延迟 2 秒重试一次
    const timer = setTimeout(() => {
      if (allStocksRef.current.length === 0) {
        console.log("allStocks still empty, retrying init...");
        initStocks();
      }
    }, 2000);

    return () => clearTimeout(timer);
  }, [initStocks]);

  useEffect(() => {
    // 强制滚动到顶部，对抗浏览器的滚动恢复机制
    // [优化] 仅在初始加载时执行一次，不再随 marketOverview 更新而刷新，避免干扰用户阅读分析报告
    const forceScrollTop = () => {
      try {
        const main = document.querySelector('main');
        if (main) main.scrollTop = 0;
        const content = document.querySelector('.ant-layout-content');
        if (content) content.scrollTop = 0;
        window.scrollTo(0, 0);
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
      } catch (e) { void e; }
    };

    // 立即执行一次
    forceScrollTop();
    
    // 在不同时间点多次执行，确保覆盖浏览器的恢复行为
    const timers = [10, 50, 100, 300, 600, 1000].map(ms => setTimeout(forceScrollTop, ms));
    
    return () => timers.forEach(clearTimeout);
  }, [selectedStock]); // 仅在切换股票时触发滚动到顶部，避免在自动刷新行情时干扰阅读

  // 轮询大盘指数
  useEffect(() => {
    const isValidOverview = (v) => {
      if (!v || typeof v !== 'object' || Array.isArray(v)) return false;
      return Boolean(v.time || v.stats_source || v.sh || v.sz || v.cy || (Array.isArray(v.indices) && v.indices.length));
    };

    // 优先从持久化缓存加载大盘指数
    const cachedOverview = storageService.get('market_overview');
    if (isValidOverview(cachedOverview)) {
      setMarketOverview(cachedOverview);
    } else if (cachedOverview) {
      try { localStorage.removeItem('market_overview'); } catch (e) { void e; }
    }

    const fetchOverview = async () => {
      // 交易时间或没有数据时请求；非交易时间降低请求频率，避免只读取缓存不更新
      if (!isTradingTime() && isValidOverview(marketOverviewRef.current)) {
        const nowTs = Date.now();
        if (nowTs - (lastOverviewFetchTsRef.current || 0) < 5 * 60 * 1000) return;
      }

      try {
        const res = await axios.get('/api/market/overview', { timeout: 8000 });
        if (isValidOverview(res.data)) {
          setMarketOverview(res.data);
          // 保存到本地缓存，供下次冷启动快速加载
          storageService.set('market_overview', res.data);
          lastOverviewFetchTsRef.current = Date.now();
        }
      } catch (e) {
        if (!shouldSilenceAxiosError(e)) {
          console.warn("Failed to fetch market overview", e);
        }
        if (!isValidOverview(marketOverviewRef.current)) {
          const fallback = { time: new Date().toLocaleTimeString(), stats_source: 'OVERVIEW_UNAVAILABLE' };
          setMarketOverview(fallback);
          storageService.set('market_overview', fallback);
        }
      }
    };

    fetchOverview(); // Initial
    const timer = setInterval(fetchOverview, 10000); // 10s
    return () => clearInterval(timer);
  }, []);

  // 自选股价格定时刷新
  useEffect(() => {
    if (watchlist.length === 0) return;

    const timer = setInterval(async () => {
      // 如果暂停了实时更新，则不进行轮询
      if (!realtimeRef.current) return;

      // 非交易时间停止轮询 (仅在交易时段 09:10-11:35, 12:55-15:05 更新)
      if (!isTradingTime()) {
        if (!isPostMarketRefreshTime()) return;
        const nowTs = Date.now();
        if (nowTs - (lastPostMarketFetchTsRef.current || 0) < 10000) return;
        lastPostMarketFetchTsRef.current = nowTs;
      }

      try {
        const codes = watchlistRef.current.map(s => s.ts_code);
        const res = await axios.post('/api/market/quotes', codes);
        if (res.data && Array.isArray(res.data)) {
          const quotesMap = {};
          res.data.forEach(q => {
            // 后端返回的是 ts_code 字段，而不是 symbol
            if (q.ts_code) {
              quotesMap[q.ts_code] = q;
            } else if (q.symbol) {
              quotesMap[q.symbol] = q;
            }
          });

          setWatchlist(prev => prev.map(s => ({
            ...s,
            ...(quotesMap[s.ts_code] || {})
          })));
          try {
            await Promise.all(
              Object.values(quotesMap).map(q => {
                const code = q.ts_code || q.symbol;
                if (!code) return Promise.resolve();
                return cacheService.saveQuote(code, q);
              })
            );
          } catch (e) {
            console.warn('缓存自选股报价失败', e);
          }
        }
      } catch (err) {
        if (shouldSilenceAxiosError(err)) return;
        storageService.log('WARN', '自选股行情轮询失败', { message: err?.message, code: err?.code });
      }
    }, 3000); // 3秒刷新一次 (优化后加快频率)

    return () => clearInterval(timer);
  }, [watchlist.length]); // 只有长度变化时重置定时器，内部通过 prev 获取最新列表

  // 轮询选股日志
  useEffect(() => {
    let interval = null;
    if (selectorLoading) {
      interval = setInterval(async () => {
        try {
          const res = await axios.get('/api/strategy/logs');
          setSelectorLogs(res.data);
        } catch (err) {
          if (shouldSilenceAxiosError(err)) return;
          storageService.log('WARN', '获取选股日志失败', { message: err?.message, code: err?.code });
        }
      }, 800);
    } else {
      if (interval) clearInterval(interval);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [selectorLoading]);

  // WebSocket 实时行情
  useEffect(() => {
    if (!selectedStock) return;
    console.log('WebSocket Effect running for:', selectedStock, 'freq:', freq);

    // 定义 connect 函数的引用，用于在 cleanup 中清理
    let reconnectTimer = null;
    let connectTimer = null;
    let isUnmounted = false;

    const connect = () => {
      if (isUnmounted) return;

      // 增加一个小延时，避免在组件快速切换时频繁连接
      connectTimer = setTimeout(() => {
        if (isUnmounted) return;

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/quote/${selectedStock}`;

        console.log('Connecting to WebSocket:', wsUrl);

        // 确保关闭之前的连接
        if (wsRef.current) {
          try {
            wsRef.current.close();
          } catch (e) { void e; }
          wsRef.current = null;
        }

        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          if (isUnmounted) return;
          console.log('WebSocket Connected');
        };

        ws.onmessage = (event) => {
          if (isUnmounted) return;
          // 如果暂停了实时更新，则忽略消息
          if (!realtimeRef.current) return;

          try {
            const data = JSON.parse(event.data);
            if (data.type === 'heartbeat') return;

            setLastUpdateTime(new Date().toLocaleTimeString());

            // 1. 更新当前报价
            setQuoteData(prev => {
              const stockInfo = allStocksRef.current.find(s => s.ts_code === selectedStock) || {};
              return { ...prev, ...data, ...stockInfo };
            });

            // 2. 实时更新 K 线数据
            cacheService.saveQuote(selectedStock, data);
            if (freq === 'D') {
              setKlineData(currentKlineData => {
                if (!currentKlineData || !Array.isArray(currentKlineData) || currentKlineData.length === 0) return currentKlineData;
                if (!data.time) return currentKlineData;
                if (!isTradingSessionTime()) return currentKlineData;

                const quoteDate = data.time.split(' ')[0];
                const lastKline = { ...currentKlineData[currentKlineData.length - 1] };
                const lastKlineDate = String(lastKline.time || '').split(' ')[0];

                if (quoteDate === lastKlineDate) {
                  lastKline.volume = lastKline.volume ?? lastKline.vol ?? 0;
                  if (
                    Math.abs(Number(lastKline.close) - Number(data.price)) < 0.0001 &&
                    Math.abs(Number(lastKline.volume) - Number(data.vol)) < 1
                  ) {
                    return currentKlineData;
                  }

                  const updatedLastKline = {
                    ...lastKline,
                    close: Number(data.price),
                    high: Math.max(Number(lastKline.high), Number(data.high)),
                    low: Math.min(Number(lastKline.low), Number(data.low)),
                    volume: Number(data.vol)
                  };
                  const prevClose = Number(data.pre_close ?? lastKline.pre_close ?? 0);
                  if (prevClose > 0) {
                    updatedLastKline.pct_chg = ((updatedLastKline.close - prevClose) / prevClose) * 100;
                  }
                  const next = [...currentKlineData];
                  next[next.length - 1] = updatedLastKline;
                  return next;
                }

                return currentKlineData;
              });
            }
          } catch (e) {
            console.error("Error parsing WebSocket message:", e);
          }
        };

        ws.onclose = (event) => {
          if (isUnmounted) return;
          console.log('WebSocket Closed. Code:', event.code, 'Reason:', event.reason);
          // 1000 是正常关闭，1012 是服务端重启
          if (event.code !== 1000) {
            if (event.code === 1012) {
              console.log('Server is restarting, will reconnect in 5s...');
              reconnectTimer = setTimeout(connect, 5000);
            } else {
              reconnectTimer = setTimeout(connect, 3000);
            }
            storageService.log('WARN', 'WebSocket连接关闭，尝试重连', { code: event.code, symbol: selectedStock });
          }
        };

        ws.onerror = (error) => {
          if (isUnmounted) return;
          // 如果连接尚未建立就被关闭（比如后端重启），这是正常现象
          if (ws.readyState !== WebSocket.OPEN) {
            console.warn('WebSocket connection failed or interrupted (likely server restart)');
          } else {
            console.error('WebSocket Error:', error);
            storageService.log('ERROR', 'WebSocket连接异常', { symbol: selectedStock });
          }
        };
      }, 500); // 500ms 延时
    };

    connect();

    return () => {
      isUnmounted = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (connectTimer) clearTimeout(connectTimer);
      if (wsRef.current) {
        const ws = wsRef.current;
        ws.onopen = null;
        ws.onmessage = null;
        ws.onerror = null;
        ws.onclose = null;
        try {
          ws.close();
        } catch (e) { void e; }
        wsRef.current = null;
      }
    };
  }, [selectedStock, freq]);

  // 获取数据逻辑（含缓存）
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const fetchData = useCallback(debounce(async (symbol, frequency) => {
    if (!symbol) return;
    const seq = fetchSeqRef.current + 1;
    fetchSeqRef.current = seq;

    const normalizeKline = (arr) => {
      if (!Array.isArray(arr)) return [];
      return arr.map((item) => {
        if (!item || typeof item !== 'object') return item;
        const volume = item.volume ?? item.vol ?? 0;
        return { ...item, volume };
      });
    };
    
    // 1. 优先尝试从缓存加载，以最快速度显示图表
    const cached = await cacheService.getKline(symbol, frequency);
    if (fetchSeqRef.current !== seq) return;

    if (lastSelectedStockRef.current !== symbol || !quoteData) {
      try {
        const cachedQuote = await cacheService.getQuote(symbol);
        if (cachedQuote && fetchSeqRef.current === seq) {
          const currentStocks = allStocksRef.current;
          const stockInfo = currentStocks.find(s => s.ts_code === symbol) || {};
          setQuoteData({ ...cachedQuote, ...stockInfo });
        }
      } catch (e) {
        console.warn('读取缓存行情失败', e);
      }
    }

    const cachedData = cached && Array.isArray(cached.data) ? cached.data : null;
    const hasCache = !!(cachedData && cachedData.length > 0);
    let cacheIsValid = !!cached?.isValid;
    if (hasCache && typeof frequency === 'string' && frequency.includes('min')) {
      const anyNonZeroVol = cachedData.some((x) => Number(x?.volume ?? x?.vol ?? 0) > 0);
      if (!anyNonZeroVol) cacheIsValid = false;

      const lastTimeRaw = cachedData[cachedData.length - 1]?.time;
      const lastTimeStr = lastTimeRaw == null ? '' : String(lastTimeRaw);
      const lastDate = lastTimeStr.split(' ')[0] || '';
      const lastClock = lastTimeStr.split(' ')[1] || '';
      const now = new Date();
      const nowDateStr = now.toISOString().slice(0, 10);
      const nowMinutes = now.getHours() * 60 + now.getMinutes();
      if (lastDate === nowDateStr && nowMinutes >= 15 * 60 + 5) {
        if (!String(lastClock).startsWith('15:00')) cacheIsValid = false;
      }
    }
    if (hasCache && typeof frequency === 'string' && ['D', 'W', 'M'].includes(frequency)) {
      if (cachedData.length < 120) cacheIsValid = false;
    }
    if (hasCache) {
      setKlineData(normalizeKline(cachedData));
      // 有缓存时，不需要设置 loading，后台静默更新
    } else {
      // 没缓存时才显示加载中
      setKlineLoading(true);
      // 如果是切换了股票，则清空旧数据；如果是同一股票切换频率，保持旧数据直到新数据返回
      if (lastSelectedStockRef.current !== symbol) {
        setKlineData([]);
        setQuoteData(null);
      }
    }
    lastSelectedStockRef.current = symbol;

    // 并行请求：获取实时行情和最新 K 线
    const fetchQuote = async () => {
      try {
        const quoteRes = await axios.get(`/api/market/quote/${symbol}`, { timeout: 8000 });
        if (fetchSeqRef.current !== seq) return;

        const currentStocks = allStocksRef.current;
        const stockInfo = currentStocks.find(s => s.ts_code === symbol) || {};

        const rawQuote = quoteRes.data || {};
        const fullQuote = { ...rawQuote, ...stockInfo };
        if (Object.keys(rawQuote).length > 0) {
          setQuoteData(fullQuote);
          await cacheService.saveQuote(symbol, fullQuote);
        } else {
          throw new Error('empty_quote');
        }
      } catch (err) {
        if (!shouldSilenceAxiosError(err)) {
          console.error('获取实时行情失败:', err);
        }
        try {
          const cachedQuote = await cacheService.getQuote(symbol);
          if (cachedQuote && fetchSeqRef.current === seq) {
            const currentStocks = allStocksRef.current;
            const stockInfo = currentStocks.find(s => s.ts_code === symbol) || {};
            setQuoteData({ ...cachedQuote, ...stockInfo });
            return;
          }
        } catch (e) {
          console.warn('回退缓存行情失败', e);
        }
        if (fetchSeqRef.current === seq) {
          const wl = watchlistRef.current || [];
          const fallbackItem = wl.find(item => item.ts_code === symbol);
          if (fallbackItem) {
            setQuoteData(fallbackItem);
          }
        }
      }
    };

    const fetchKline = async () => {
      try {
        const res = await axios.get(`/api/market/kline/${symbol}`, {
          params: { freq: frequency },
          timeout: 12000
        });
        
        if (fetchSeqRef.current !== seq) return;

        if (res.data && res.data.length > 0) {
          const normalized = normalizeKline(res.data);
          // 比较新老数据，如果一致则不更新，减少渲染
          setKlineData(prev => {
            if (prev && prev.length === normalized.length && 
                prev[prev.length-1]?.time === normalized[normalized.length-1]?.time &&
                prev[prev.length-1]?.close === normalized[normalized.length-1]?.close) {
              return prev;
            }
            return normalized;
          });
          await cacheService.saveKline(symbol, frequency, normalized);
        } else {
          setKlineData([]);
        }
      } catch (err) {
        if (!shouldSilenceAxiosError(err)) {
          storageService.log('ERROR', '获取 K 线数据失败', { symbol, frequency, message: err?.message });
        }
      } finally {
        if (fetchSeqRef.current === seq) {
          setKlineLoading(false);
        }
      }
    };

    const tasks = [fetchQuote()];
    const shouldRefreshKline = isTradingSessionTime() || !hasCache || !cacheIsValid;
    if (shouldRefreshKline) tasks.push(fetchKline());
    await Promise.allSettled(tasks);
  }, 300), []); // 设置 300ms 防抖延时

  // 当选择股票或频率变化时刷新
  useEffect(() => {
    if (!selectedStock) return;

    // 尝试加载历史分析结果
    const saved = storageService.loadAnalysis(selectedStock);
    if (saved) {
      setAiAnalysis({
        ...saved.analysis,
        timestamp: saved.timestamp
      });
    } else {
      setAiAnalysis(null);
    }

    fetchData(selectedStock, freq);
  }, [selectedStock, freq, fetchData]);

  const handleStockClick = useCallback((symbol) => {
    setSelectedStock(symbol);
    setFreq('D');
    localStorage.setItem('selectedStock', symbol);
  }, []);

  const handleGenerateAnalysis = () => {
    setAiLoading(true);
    axios.post('/api/analysis/kline', { 
      symbol: selectedStock,
      preferred_provider: analysisProvider,
      api_key: analysisApiKey
    })
      .then(res => {
        const analysisData = {
          ...res.data,
          timestamp: new Date().getTime()
        };
        setAiAnalysis(analysisData);
        storageService.saveAnalysis(selectedStock, res.data);
        message.success('AI 分析报告生成成功');
      })
      .catch(err => {
        console.error(err);
        message.error('生成分析失败');
      })
      .finally(() => {
        setAiLoading(false);
      });
  };

  // 搜索功能
  const handleSearch = useCallback((value) => {
    console.log("Searching for:", value);
    if (!value) {
      setSearchOptions([]);
      return;
    }
    // 简单搜索：代码或名称包含
    const searchVal = value.toUpperCase();
    const results = allStocks.filter(stock =>
      (stock.ts_code && stock.ts_code.toUpperCase().includes(searchVal)) ||
      (stock.symbol && stock.symbol.includes(searchVal)) ||
      (stock.name && stock.name.includes(value)) ||
      (stock.name && PinyinMatch.match(stock.name, value))
    ).slice(0, 10); // 限制10条

    console.log("Search results:", results.length);

    setSearchOptions(results.map(stock => ({
      value: stock.ts_code,
      label: (
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>{stock.name}</span>
          <span style={{ color: '#888' }}>{stock.ts_code}</span>
        </div>
      ),
      stock: stock // 保存完整对象以便添加到自选
    })));
  }, [allStocks]);

  const handleSelectStock = useCallback((value) => {
    setSelectedStock(value);
    setFreq('D');
    // 检查是否在自选股中，如果不在提示添加
    const exists = watchlist.find(w => w.ts_code === value);
    if (!exists) {
      // 自动添加到自选 (可选逻辑，或者只跳转)
      // 这里我们暂时只跳转，不自动添加
    }
    setSearchOptions([]); // 选择后清空搜索建议
  }, [watchlist]);

  // 添加到自选
  const addToWatchlist = () => {
    // 找到当前选中的股票详情
    const selectedKey = selectedStock || '';
    const shortCode = selectedKey.split('.')[0];
    const quoteCode = quoteData?.ts_code || quoteData?.symbol || '';
    const stock = allStocks.find(s => s.ts_code === selectedKey)
      || allStocks.find(s => (s.ts_code || '').split('.')[0] === shortCode)
      || (quoteCode ? (allStocks.find(s => s.ts_code === quoteCode) || allStocks.find(s => (s.ts_code || '').split('.')[0] === quoteCode.split('.')[0])) : null);

    const toAdd = stock || (quoteCode ? {
      ts_code: quoteCode,
      name: quoteData?.name || quoteCode.split('.')[0],
      price: quoteData?.price || 0,
      pre_close: quoteData?.pre_close || 0
    } : null);
    if (!toAdd || !toAdd.ts_code) {
      message.warning('未找到股票信息');
      return;
    }
    const newWatchlist = [toAdd, ...watchlist];
    const uniqueWatchlist = Array.from(new Set(newWatchlist.map(s => s.ts_code)))
      .map(code => newWatchlist.find(s => s.ts_code === code));
    setWatchlist(uniqueWatchlist);
    storageService.save(uniqueWatchlist);
    message.success(`${toAdd.name} 已加入自选`);
  };

  // 从自选移除
  const removeFromWatchlist = useCallback((e, ts_code) => {
    e.stopPropagation(); // 阻止触发点击事件
    const newWatchlist = watchlist.filter(item => item.ts_code !== ts_code);
    setWatchlist(newWatchlist);
    storageService.save(newWatchlist);
    message.success('已从自选移除');
  }, [watchlist]);

  // 执行智能选股
  const handleSmartSelection = useCallback(async (strategyOverride = null, forceRefresh = false) => {
    const strategy = strategyOverride || selectorStrategy;

    // 如果不是强制刷新，且已经有该策略的结果，则直接返回
    if (!forceRefresh && selectorResults[strategy]) {
      setSelectorStrategy(strategy);
      setShowSelector(true);
      return;
    }

    setSelectorLoading(true);
    setShowSelector(true);
    setSelectorStrategy(strategy);
    setSelectorLogs([]); // 开始选股前清空旧日志

    // 建立轮询机制获取日志
    const logInterval = setInterval(async () => {
      try {
        const logRes = await axios.get('/api/strategy/logs');
        if (logRes.data && Array.isArray(logRes.data)) {
          setSelectorLogs(logRes.data);
        }
      } catch (e) {
        console.warn('获取日志失败', e);
      }
    }, 1000); // 每秒轮询一次

    try {
      const res = await axios.get('/api/strategy/selector', {
        params: {
          limit: 10,
          strategy: strategy
        }
      });
      clearInterval(logInterval); // 选股完成，清除轮询

      if (res.data && Array.isArray(res.data)) {
        const now = new Date().getTime();
        const newResult = {
          timestamp: now,
          data: res.data
        };

        setSelectorResults(prev => ({
          ...prev,
          [strategy]: newResult
        }));

        storageService.saveSelector(strategy, res.data);
        message.success(`${strategy === 'pullback' ? '强势回调' : '多维综合'}选股完成`);
      } else {
        message.warning('未能筛选出符合条件的潜力股');
        setSelectorResults(prev => ({
          ...prev,
          [strategy]: { timestamp: new Date().getTime(), data: [] }
        }));
      }
    } catch (err) {
      console.error(err);
      clearInterval(logInterval); // 出错也要清除轮询
      message.error('选股策略执行失败，请检查网络和 API 配置');
    } finally {
      setSelectorLoading(false);
    }
  }, [selectorResults, selectorStrategy]);

  const handleDailyReview = async () => {
    setReviewLoading(true);
    setShowReviewModal(true);
    setReviewLogs([]);
    setReviewData(null);
    const watchlistCodes = watchlist.map(item => item.ts_code);
    const qs = new URLSearchParams();
    if (watchlistCodes.length > 0) qs.set('watchlist', watchlistCodes.join(','));
    if (analysisProvider) qs.set('preferred_provider', analysisProvider);
    if (analysisApiKey) qs.set('api_key', analysisApiKey);

    if (reviewEventSourceRef.current) {
      try {
        reviewEventSourceRef.current.close();
      } catch (e) {
        console.warn('关闭旧流失败', e);
      }
      reviewEventSourceRef.current = null;
    }

    const source = new EventSource(`/api/trading/review/stream?${qs.toString()}`);
    reviewEventSourceRef.current = source;

    const appendLog = (line) => {
      setReviewLogs(prev => {
        const next = [...prev, line].slice(-2000);
        return next;
      });
    };

    source.addEventListener('placeholder', (e) => {
      try {
        const payload = JSON.parse(e.data || '{}');
        setReviewData(payload);
      } catch (err) {
        console.warn('解析复盘占位数据失败', err);
      }
    });

    source.addEventListener('log', (e) => {
      try {
        const payload = JSON.parse(e.data || '{}');
        if (payload?.line) appendLog(payload.line);
      } catch (err) {
        if (e?.data) appendLog(String(e.data));
      }
    });

    source.addEventListener('final', (e) => {
      try {
        const payload = JSON.parse(e.data || '{}');
        setReviewData(payload);
        storageService.saveReview(payload);
        message.success('复盘完成');
      } catch (err) {
        console.warn('解析复盘最终结果失败', err);
      } finally {
        setReviewLoading(false);
        try { source.close(); } catch (e2) { void e2; }
        reviewEventSourceRef.current = null;
      }
    });

    source.addEventListener('error', () => {
      message.error('复盘流中断或失败');
      setReviewLoading(false);
      try { source.close(); } catch (e2) { void e2; }
      reviewEventSourceRef.current = null;
    });
  };

  const handleCancelReviewModal = () => {
    if (reviewLogIntervalRef.current) {
      clearInterval(reviewLogIntervalRef.current);
      reviewLogIntervalRef.current = null;
    }
    if (reviewEventSourceRef.current) {
      try {
        reviewEventSourceRef.current.close();
      } catch (e) {
        console.warn('关闭复盘流失败', e);
      }
      reviewEventSourceRef.current = null;
    }
    if (reviewAbortRef.current) {
      try {
        reviewAbortRef.current.abort();
      } catch (e) {
        console.warn('取消复盘请求失败', e);
      }
      reviewAbortRef.current = null;
    }
    setReviewLoading(false);
    setShowReviewModal(false);
  };

  const handleGetLatestReview = async () => {
    setShowReviewModal(true);
    const cached = storageService.loadReview();
    if (cached?.data) {
      setReviewData(cached.data);
    }
    const hasLocalData = !!(cached?.data || reviewData);
    setReviewLoading(!hasLocalData);
    try {
      const res = await axios.get('/api/trading/review/latest');
      if (res.data) {
        setReviewData(res.data);
        storageService.saveReview(res.data);
        message.success('已加载最新复盘结果');
      }
    } catch (err) {
      if (err.response && err.response.status === 404) {
        message.info('暂无复盘数据，请点击“复盘”开始');
      } else {
        console.error(err);
        message.error('加载失败: ' + (err.response?.data?.detail || err.message));
      }
    } finally {
      setReviewLoading(false);
    }
  };

  const filterPlans = useCallback((items) => {
    const cancelKeywords = ['取消', '清除', '移出', '不再跟踪', '不再监控', '放弃跟踪'];
    return (items || []).filter((p) => {
      if (!p) return false;
      if (p.executed) return false;
      if (String(p.track_status || '').toUpperCase() === 'CANCELLED') return false;
      const review = String(p.review_content || '');
      if (review && cancelKeywords.some((k) => review.includes(k))) return false;
      return true;
    });
  }, []);

  const fetchPlans = useCallback(async () => {
    setPlansLoading(true);
    try {
      const res = await axios.get('/api/trading/plans/today');
      setPlansData(filterPlans(res.data || []));
    } catch (err) {
      console.error(err);
      message.error('获取交易计划失败');
    } finally {
      setPlansLoading(false);
    }
  }, [filterPlans]);

  const handleViewPlans = async () => {
    setPlansModalVisible(true);
    fetchPlans();
  };

  const handlePlanRemoved = useCallback((planId) => {
    setPlansData((prev) => filterPlans((prev || []).filter((p) => p.id !== planId)));
  }, [filterPlans]);

  const fetchRewardPunish = useCallback(async () => {
    setRewardPunishLoading(true);
    try {
      const res = await axios.get('/api/trading/reward-punish/summary', { timeout: 8000 });
      setRewardPunishData(res.data || null);
    } catch (err) {
      const msg = err.code === 'ECONNABORTED' ? '奖惩数据获取超时' : (err.response?.data?.detail || err.message);
      message.error('奖惩数据获取失败: ' + msg);
    } finally {
      setRewardPunishLoading(false);
    }
  }, []);

  const handleViewRewardPunish = useCallback(() => {
    setRewardPunishVisible(true);
    fetchRewardPunish();
  }, [fetchRewardPunish]);

  // 如果计划弹窗可见，自动轮询刷新 (每1秒)
  useEffect(() => {
    if (!plansModalVisible) return;
    fetchPlans();
    const es = new EventSource('/api/trading/plans/stream');
    es.addEventListener('plan', (evt) => {
      try {
        const data = JSON.parse(evt.data || '{}');
        if (data.type === 'plan_removed') {
          handlePlanRemoved(Number(data.plan_id));
        } else if (data.type === 'plan_refresh') {
          fetchPlans();
        }
      } catch (e) {
        console.error(e);
      }
    });
    es.onerror = () => {
      es.close();
    };
    return () => {
      es.close();
    };
  }, [plansModalVisible, fetchPlans, handlePlanRemoved]);

  // 打开选股结果弹窗
  const openSelectorModal = useCallback(() => {
    setShowSelector(true);
    const currentResult = selectorResults[selectorStrategy];
    if (!currentResult || currentResult.data.length === 0) {
      handleSmartSelection(selectorStrategy, false);
    }
  }, [selectorResults, selectorStrategy, handleSmartSelection]);

  const isSelectedInWatchlist = watchlist.some(s => s.ts_code === selectedStock);

  return (
    <Layout style={{ height: '100vh', overflow: 'hidden' }}>
      <ReviewModal
        visible={showReviewModal}
        onCancel={handleCancelReviewModal}
        loading={reviewLoading}
        data={reviewData}
        logs={reviewLogs}
        onSelectStock={(code) => {
          setSelectedStock(code);
          setShowReviewModal(false);
        }}
      />

      <StockSelector
        visible={showSelector}
        onCancel={() => setShowSelector(false)}
        strategy={selectorStrategy}
        onStrategyChange={handleSmartSelection}
        loading={selectorLoading}
        logs={selectorLogs}
        results={selectorResults}
        onSelectStock={(stock) => {
          const analysisData = {
            content: stock.analysis,
            score: stock.score,
            source: "AI_Selector",
            timestamp: new Date().getTime()
          };
          setAiAnalysis(analysisData);
          storageService.saveAnalysis(stock.ts_code, analysisData);
          setSelectedStock(stock.ts_code);
          setShowSelector(false);
        }}
      />

      <PlansModal
        visible={plansModalVisible}
        onCancel={() => setPlansModalVisible(false)}
        data={plansData}
        loading={plansLoading}
        onRefresh={fetchPlans}
        onPlanRemoved={handlePlanRemoved}
      />

      <RewardPunishModal
        visible={rewardPunishVisible}
        onCancel={() => setRewardPunishVisible(false)}
        loading={rewardPunishLoading}
        data={rewardPunishData}
        onRefresh={fetchRewardPunish}
      />

      <MemoryModal
        visible={memoryModalVisible}
        onCancel={() => setMemoryModalVisible(false)}
      />

      <SystemNavigator
        visible={navVisible}
        onClose={() => setNavVisible(false)}
      />

      <SidePanel
        searchOptions={searchOptions}
        onSearch={handleSearch}
        onSelectStock={handleSelectStock}
        allStocks={allStocks}
        onOpenSelector={openSelectorModal}
        watchlist={watchlist}
        loading={loading}
        onRefresh={initStocks}
        selectedStock={selectedStock}
        onStockClick={handleStockClick}
        onRemoveFromWatchlist={removeFromWatchlist}
        syncStatus={syncStatus}
      />

      {/* 模拟交易面板 */}
      <TradingPanel
        visible={tradingPanelVisible}
        onClose={() => setTradingPanelVisible(false)}
        onSelectStock={handleStockClick}
      />

      <Layout>
        <Header style={{
          background: '#141414',
          padding: 0,
          height: 'auto',
          borderBottom: '1px solid #303030'
        }}>
          <TopBar
            quoteData={quoteData}
            selectedStock={selectedStock}
            isSelectedInWatchlist={isSelectedInWatchlist}
            onAddToWatchlist={addToWatchlist}
            lastUpdateTime={lastUpdateTime}
            realtimeEnabled={realtimeEnabled}
            onToggleRealtime={() => setRealtimeEnabled(!realtimeEnabled)}
            onRefresh={() => fetchData(selectedStock, freq)}
            aiLoading={aiLoading}
            onGenerateAnalysis={handleGenerateAnalysis}
            marketOverview={marketOverview}
            onSelectStock={handleStockClick}
            onDailyReview={handleDailyReview}
            onViewReview={handleGetLatestReview}
            onViewPlans={handleViewPlans}
            onViewNav={() => setNavVisible(true)}
            onManageMemory={() => setMemoryModalVisible(true)}
            onViewTrading={() => setTradingPanelVisible(true)}
            onViewRewardPunish={handleViewRewardPunish}
            availableProviders={availableProviders}
            analysisProvider={analysisProvider}
            onAnalysisProviderChange={setAnalysisProvider}
            analysisApiKey={analysisApiKey}
            onAnalysisApiKeyChange={setAnalysisApiKey}
          />
        </Header>

        <Content style={{
          padding: '16px 24px',
          backgroundColor: '#0a0a0a',
          overflowY: 'auto',
          display: 'flex',
          flexDirection: 'column',
          gap: 16
        }}>
          {/* 将 MarketIndices 放在最顶部，总是渲染组件，由组件内部决定如何显示 */}
          <MarketIndices marketOverview={marketOverview} onSelectStock={handleStockClick} />
          
          {(klineLoading && (!klineData || klineData.length === 0)) || klineData === null ? (
            <div style={{ height: 600, display: 'flex', justifyContent: 'center', alignItems: 'center', backgroundColor: '#141414', borderRadius: 8 }}>
              <Spin size="large" tip={klineData === null ? "正在加载缓存..." : "正在获取数据..."}>
                <div style={{ padding: 50 }} />
              </Spin>
            </div>
          ) : (
            <Card
              variant="borderless"
              style={{ backgroundColor: '#141414', borderRadius: 8, minHeight: 600 }}
              styles={{ body: { padding: '16px' } }}
            >
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end', alignItems: 'center' }}>
                <div style={{ color: '#666', fontSize: 12 }}>
                  数据更新于: {quoteData?.time || '-'}
                </div>
              </div>
              <KlineChart
                data={klineData || []}
                symbol={selectedStock}
                freq={freq}
                preClose={quoteData?.pre_close}
                onFreqChange={(v) => {
                  setFreq(v);
                }}
              />
            </Card>
          )}

          {aiAnalysis && (
            <Card
              title={<div style={{ display: 'flex', alignItems: 'center', gap: 8 }}><RobotOutlined style={{ color: '#26a69a' }} /> AI 分析建议</div>}
              variant="borderless"
              style={{ backgroundColor: '#141414', borderRadius: 8 }}
            >
              <div className="markdown-body" style={{ color: '#ccc' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <Tag color={aiAnalysis.source === 'AI' ? 'blue' : 'orange'}>{aiAnalysis.source}</Tag>
                  {aiAnalysis.timestamp && (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      分析时间: {new Date(aiAnalysis.timestamp).toLocaleString()}
                    </Text>
                  )}
                </div>
                <div style={{ marginTop: 12 }}>
                  <ReactMarkdown>{aiAnalysis.analysis || aiAnalysis.content}</ReactMarkdown>
                </div>
              </div>
            </Card>
          )}
        </Content>
      </Layout>
      <AIChatWindow 
        availableProviders={availableProviders}
        chatProvider={chatProvider}
        onChatProviderChange={setChatProvider}
        chatApiKey={chatApiKey}
        onChatApiKeyChange={setChatApiKey}
      />
    </Layout>
  );
};

export default App;

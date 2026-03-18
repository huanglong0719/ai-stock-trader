/**
 * 股票数据存储服务
 * 负责自选股数据的持久化、验证、错误处理及性能优化
 */

const STORAGE_KEY = 'watchlist';
const SELECTOR_KEY = 'selector_results';
const STOCKS_KEY = 'all_stocks';
const WATCHLIST_BACKUP_KEY = 'watchlist_backup';
const ANALYSIS_PREFIX = 'analysis_';
const REVIEW_KEY = 'review_latest';
const STOCKS_EXPIRE_TIME = 24 * 60 * 60 * 1000; // 24小时过期
const CHUNK_SIZE = 1024 * 1024; // 1MB per chunk
const MAX_RETRIES = 3;
const DEBOUNCE_DELAY = 1000;

class StorageService {
    constructor() {
        this.debounceTimer = null;
        this.retryCount = 0;
    }

    /**
     * 通用获取方法
     */
    get(key) {
        try {
            const val = localStorage.getItem(key);
            if (!val) return null;
            return JSON.parse(val);
        } catch (e) {
            return null;
        }
    }

    /**
     * 通用设置方法
     */
    set(key, value) {
        try {
            localStorage.setItem(key, JSON.stringify(value));
        } catch (e) {
            this.log('ERROR', `设置缓存失败: ${key}`, { error: e.message });
        }
    }

    /**
     * 记录日志
     * @param {string} level - 级别: 'INFO' | 'WARN' | 'ERROR' | 'CRITICAL'
     * @param {string} message - 日志内容
     * @param {any} details - 详细信息
     */
    log(level, message, details = {}) {
        const timestamp = new Date().toISOString();
        const logEntry = {
            timestamp,
            level,
            message,
            ...details
        };
        console.log(`[StorageService][${level}] ${message}`, logEntry);
        
        // 可以在此处扩展到持久化日志或发送到后端
    }

    /**
     * 验证数据格式
     * @param {any} data - 待验证的数据
     * @returns {boolean}
     */
    validate(data) {
        if (!Array.isArray(data)) return false;
        return data.every(item => item && typeof item === 'object' && item.ts_code);
    }

    /**
     * 加载自选股数据 (支持分块加载)
     * @returns {Array}
     */
    load() {
        try {
            let combinedData = '';
            const meta = localStorage.getItem(`${STORAGE_KEY}_meta`);
            
            if (meta) {
                const { chunks } = JSON.parse(meta);
                for (let i = 0; i < chunks; i++) {
                    combinedData += localStorage.getItem(`${STORAGE_KEY}_chunk_${i}`) || '';
                }
            } else {
                // 回退到旧的存储方式
                combinedData = localStorage.getItem(STORAGE_KEY);
            }

            if (!combinedData) return [];

            const data = JSON.parse(combinedData);
            if (this.validate(data)) {
                this.log('INFO', '数据加载成功', { count: data.length, size: combinedData.length });
                return data;
            } else {
                this.log('WARN', '加载的数据格式无效，已重置');
                return this._loadBackup();
            }
        } catch (error) {
            this.log('ERROR', '加载数据失败', { error: error.message });
            return this._loadBackup();
        }
    }

    /**
     * 保存自选股数据 (防抖版本)
     * @param {Array} data 
     */
    save(data) {
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }

        this.debounceTimer = setTimeout(() => {
            this._performSave(data);
        }, DEBOUNCE_DELAY);
    }

    /**
     * 缓存全量股票列表
     */
    saveAllStocks(stocks) {
        try {
            const cacheData = {
                timestamp: Date.now(),
                data: stocks
            };
            localStorage.setItem(STOCKS_KEY, JSON.stringify(cacheData));
            this.log('INFO', '全量股票列表缓存成功', { count: stocks.length });
        } catch (error) {
            this.log('ERROR', '缓存全量股票列表失败', { error: error.message });
        }
    }

    /**
     * 获取缓存的全量股票列表
     */
    getAllStocks() {
        try {
            const cacheStr = localStorage.getItem(STOCKS_KEY);
            if (!cacheStr) return null;

            const cacheData = JSON.parse(cacheStr);
            const now = Date.now();

            if (now - cacheData.timestamp > STOCKS_EXPIRE_TIME) {
                this.log('INFO', '股票列表缓存已过期');
                return null;
            }

            return cacheData.data;
        } catch (error) {
            this.log('ERROR', '读取股票列表缓存失败', { error: error.message });
            return null;
        }
    }

    /**
     * 执行保存操作 (支持分块存储和压缩模拟)
     * @param {Array} data 
     * @private
     */
    _performSave(data) {
        if (!this.validate(data)) {
            this.log('ERROR', '尝试保存无效格式的数据', { data });
            return;
        }

        try {
            const serializedData = JSON.stringify(data);
            const totalSize = serializedData.length;
            const chunksCount = Math.ceil(totalSize / CHUNK_SIZE);

            const attemptSave = (retry = 0) => {
                try {
                    // 清理旧的分块
                    this._clearOldChunks();

                    if (chunksCount > 1) {
                        // 执行分块存储
                        for (let i = 0; i < chunksCount; i++) {
                            const chunk = serializedData.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                            localStorage.setItem(`${STORAGE_KEY}_chunk_${i}`, chunk);
                        }
                        localStorage.setItem(`${STORAGE_KEY}_meta`, JSON.stringify({ chunks: chunksCount, totalSize }));
                    } else {
                        // 小数据直接存储
                        localStorage.setItem(STORAGE_KEY, serializedData);
                    }
                    
                    localStorage.setItem(WATCHLIST_BACKUP_KEY, serializedData);
                    this.log('INFO', '保存成功', { size: totalSize, chunks: chunksCount, count: data.length });
                } catch (error) {
                    if (error.name === 'QuotaExceededError') {
                        this.log('CRITICAL', '存储空间已满，保存失败', { size: totalSize });
                    } else if (retry < MAX_RETRIES) {
                        this.log('WARN', `保存重试中 (${retry + 1}/${MAX_RETRIES})`, { error: error.message });
                        setTimeout(() => attemptSave(retry + 1), 500);
                    } else {
                        this.log('ERROR', '持久化最终失败', { error: error.message });
                    }
                }
            };

            attemptSave();
        } catch (error) {
            this.log('ERROR', '序列化数据失败', { error: error.message });
        }
    }

    _loadBackup() {
        try {
            const backupRaw = localStorage.getItem(WATCHLIST_BACKUP_KEY);
            if (!backupRaw) return [];
            const backupData = JSON.parse(backupRaw);
            if (this.validate(backupData)) {
                this.log('INFO', '自选股已从备份恢复', { count: backupData.length });
                return backupData;
            }
            return [];
        } catch (error) {
            this.log('ERROR', '自选股备份恢复失败', { error: error.message });
            return [];
        }
    }

    /**
     * 清理旧的分块数据
     * @private
     */
    _clearOldChunks() {
        const metaRaw = localStorage.getItem(`${STORAGE_KEY}_meta`);
        if (metaRaw) {
            try {
                const { chunks } = JSON.parse(metaRaw);
                for (let i = 0; i < chunks; i++) {
                    localStorage.removeItem(`${STORAGE_KEY}_chunk_${i}`);
                }
                localStorage.removeItem(`${STORAGE_KEY}_meta`);
            } catch (e) {
                this.log('WARN', '分块元数据解析失败，已忽略', { error: e?.message });
                localStorage.removeItem(`${STORAGE_KEY}_meta`);
            }
        }
        localStorage.removeItem(STORAGE_KEY);
    }

    /**
     * 保存选股结果
     * @param {string} strategy
     * @param {Array} data 
     */
    saveSelector(strategy, data) {
        try {
            const raw = localStorage.getItem(SELECTOR_KEY);
            let selectorData = {};
            if (raw) {
                try {
                    selectorData = JSON.parse(raw);
                    // 兼容旧格式
                    if (Array.isArray(selectorData.data)) {
                        selectorData = { default: selectorData };
                    }
                } catch (e) {
                    selectorData = {};
                }
            }

            selectorData[strategy] = {
                timestamp: new Date().getTime(),
                data: data
            };
            
            localStorage.setItem(SELECTOR_KEY, JSON.stringify(selectorData));
            this.log('INFO', '选股结果保存成功', { strategy, count: data.length });
        } catch (error) {
            this.log('ERROR', '保存选股结果失败', { error: error.message });
        }
    }

    /**
     * 加载选股结果
     * @param {string} strategy
     * @returns {Object|null} { timestamp, data }
     */
    loadSelector(strategy = null) {
        try {
            const raw = localStorage.getItem(SELECTOR_KEY);
            if (!raw) return null;
            let selectorData = JSON.parse(raw);
            
            // 兼容旧格式
            if (Array.isArray(selectorData.data)) {
                if (!strategy || strategy === 'default') return selectorData;
                return null;
            }

            if (strategy) {
                return selectorData[strategy] || null;
            }
            return selectorData;
        } catch (error) {
            this.log('ERROR', '加载选股结果失败', { error: error.message });
            return null;
        }
    }

    /**
     * 保存个股 AI 分析结果
     * @param {string} symbol 
     * @param {Object} analysis 
     */
    saveAnalysis(symbol, analysis) {
        try {
            const key = `${ANALYSIS_PREFIX}${symbol}`;
            localStorage.setItem(key, JSON.stringify({
                timestamp: new Date().getTime(),
                analysis: analysis
            }));
            this.log('INFO', '保存 AI 分析结果成功', { symbol });
        } catch (error) {
            this.log('ERROR', '保存 AI 分析结果失败', { symbol, error: error.message });
        }
    }

    /**
     * 加载个股 AI 分析结果
     * @param {string} symbol 
     * @returns {Object|null}
     */
    loadAnalysis(symbol) {
        try {
            const key = `${ANALYSIS_PREFIX}${symbol}`;
            const raw = localStorage.getItem(key);
            if (!raw) return null;
            return JSON.parse(raw);
        } catch (error) {
            this.log('ERROR', '加载 AI 分析结果失败', { symbol, error: error.message });
            return null;
        }
    }

    saveReview(data) {
        if (!data) return;
        this.set(REVIEW_KEY, { timestamp: Date.now(), data });
    }

    loadReview() {
        const v = this.get(REVIEW_KEY);
        if (!v || !v.data) return null;
        return v;
    }
}

export const storageService = new StorageService();

import Dexie from 'dexie';

export const db = new Dexie('AITraderDB');

db.version(1).stores({
  stocks: 'ts_code, symbol, name, industry',
  klines: '[symbol+freq], symbol, freq, last_updated',
  quotes: 'symbol, price, last_updated'
});

export const cacheService = {
  // 保存 K 线数据
  async saveKline(symbol, freq, data) {
    await db.klines.put({
      symbol,
      freq,
      data,
      last_updated: Date.now()
    });
  },

  // 获取 K 线数据
  async getKline(symbol, freq) {
    const cached = await db.klines.get([symbol, freq]);
    if (cached) {
      // 检查有效期，例如 5 分钟内认为有效（分时图需要更频繁更新，这里简化处理）
      const isValid = Date.now() - cached.last_updated < 5 * 60 * 1000;
      return { data: cached.data, isValid };
    }
    return null;
  },

  // 保存实时行情
  async saveQuote(symbol, quote) {
    await db.quotes.put({
      symbol,
      ...quote,
      last_updated: Date.now()
    });
  },

  // 获取缓存的行情
  async getQuote(symbol) {
    return await db.quotes.get(symbol);
  }
};

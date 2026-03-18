import React, { memo } from 'react';

const MarketIndices = memo(({ marketOverview, onSelectStock }) => {
  if (!marketOverview || typeof marketOverview !== 'object' || Array.isArray(marketOverview)) {
    return (
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        marginBottom: 16,
        gap: 12,
        padding: '4px 8px',
        borderRadius: 6,
        backgroundColor: '#141414',
        border: '1px solid #303030',
        color: '#666',
        fontSize: 12,
        minHeight: 32, // 防止高度塌陷
      }}>
        指数/涨跌统计加载中...
      </div>
    );
  }

  // 适配数据结构：后端可能返回 {sh:..., sz:..., cy:...} 对象，也可能返回 {indices: [...]}
  let indices = [];
  if (marketOverview.indices && Array.isArray(marketOverview.indices)) {
      indices = marketOverview.indices;
  } else if (marketOverview.sh || marketOverview.sz) {
      // 保证顺序：上证、深证、创业板
      indices = [marketOverview.sh, marketOverview.sz, marketOverview.cy].filter(Boolean);
  }

  const upCount = marketOverview.up_count ?? marketOverview.up ?? 0;
  const downCount = marketOverview.down_count ?? marketOverview.down ?? 0;
  const flatCount = marketOverview.flat_count ?? marketOverview.flat ?? 0;
  const totalCount = marketOverview.total_count ?? marketOverview.total ?? 0;
  const limitUpCount = marketOverview.limit_up_count ?? marketOverview.limit_up ?? 0;
  const limitDownCount = marketOverview.limit_down_count ?? marketOverview.limit_down ?? 0;
  const overviewTime = marketOverview.time || '';
  const statsSource = marketOverview.stats_source || '';
  const statsSourceDisplay = (() => {
    const s = String(statsSource || '');
    if (!s) return '';
    if (s.startsWith('CLOSE_CACHE_')) return s.slice('CLOSE_CACHE_'.length);
    if (s === 'CLOSE_CACHE') return '';
    if (s.startsWith('CLOSE_CACHE')) return s.replace(/^CLOSE_CACHE_?/, '');
    return s;
  })();

  const hasStats = Boolean(Number(upCount) || Number(downCount) || Number(flatCount) || Number(limitUpCount) || Number(limitDownCount) || Number(totalCount));

  const renderIndex = (index) => {
    if (!index) return null;
    
    // 优先显示名称，如果名称包含代码则处理掉
    let displayName = index.name || index.ts_code || index.symbol || '';
    
    // 缩减指数名称
    if (displayName.includes('上证指数')) displayName = "上证";
    else if (displayName.includes('深证成指')) displayName = "深证";
    else if (displayName.includes('创业板指')) displayName = "创业";
    else if (displayName.includes('沪深300')) displayName = "300";
    else if (displayName.includes('中证500')) displayName = "500";
    else if (displayName.includes('中证1000')) displayName = "1000";
    
    const price = Number(index.price) || 0;
    const pctChg = Number(index.pct_chg) || 0;
    const color = pctChg >= 0 ? '#ef5350' : '#26a69a';
    
    return (
        <div 
           key={index.ts_code || index.symbol || Math.random()} 
           onClick={() => onSelectStock && onSelectStock(index.ts_code || index.symbol)}
           style={{ 
               display: 'flex', 
               alignItems: 'center', 
               gap: 8, 
               fontSize: 14, 
               marginRight: 0,
               cursor: 'pointer',
               padding: '4px 8px',
               borderRadius: '6px',
               backgroundColor: '#1f1f1f',
               border: '1px solid #303030',
               transition: 'all 0.2s',
           }}
           onMouseEnter={(e) => e.currentTarget.style.borderColor = '#555'}
           onMouseLeave={(e) => e.currentTarget.style.borderColor = '#303030'}
        >
            <span style={{ color: '#aaa', fontWeight: 500 }}>{displayName}</span>
            <span style={{ color: color, fontWeight: 'bold', fontSize: 15 }}>{price.toFixed(2)}</span>
            <span style={{ color: color }}>{pctChg > 0 ? '+' : ''}{pctChg.toFixed(2)}%</span>
        </div>
    );
  };

  return (
    <div style={{ 
      display: 'flex', 
      alignItems: 'center', 
      marginBottom: 16,
      flexWrap: 'nowrap',
      justifyContent: 'flex-start',
      gap: 12,
      overflowX: 'auto',
      WebkitOverflowScrolling: 'touch',
      position: 'relative',
      zIndex: 10,
      minHeight: 32, // 防止高度塌陷
    }}>
        <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'nowrap', gap: 12, whiteSpace: 'nowrap', width: '100%' }}>
          {indices.length ? indices.map(renderIndex) : (
            <div style={{ color: '#666', fontSize: 12, padding: '0 4px' }}>
              大盘指数未加载
            </div>
          )}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 8px', borderRadius: 6, backgroundColor: '#1f1f1f', border: '1px solid #303030' }}>
            <span style={{ color: '#aaa', fontSize: 12 }}>涨跌停</span>
            <span style={{ color: '#ef5350', fontWeight: 700 }}>{Number(limitUpCount) || 0}</span>
            <span style={{ color: '#26a69a', fontWeight: 700 }}>{Number(limitDownCount) || 0}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 8px', borderRadius: 6, backgroundColor: '#1f1f1f', border: '1px solid #303030' }}>
            <span style={{ color: '#aaa', fontSize: 12 }}>涨跌家数</span>
            <span style={{ color: '#ef5350', fontWeight: 700 }}>{Number(upCount) || 0}</span>
            <span style={{ color: '#26a69a', fontWeight: 700 }}>{Number(downCount) || 0}</span>
            <span style={{ color: '#9e9e9e', fontWeight: 700 }}>{Number(flatCount) || 0}</span>
            {Number(totalCount) ? (
              <span style={{ color: '#777', fontWeight: 600, marginLeft: 4 }}>总 {Number(totalCount) || 0}</span>
            ) : null}
          </div>
          {(overviewTime || statsSource || hasStats) ? (
            <div style={{ color: '#666', fontSize: 12 }}>
              {overviewTime || '--'}{statsSourceDisplay ? ` · ${statsSourceDisplay}` : ''}
            </div>
          ) : null}
        </div>
    </div>
  );
});

MarketIndices.displayName = 'MarketIndices';

export default MarketIndices;

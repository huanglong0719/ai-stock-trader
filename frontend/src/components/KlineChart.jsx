import React, { useEffect, useRef, useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts';
import { Button, Space, Radio, Tooltip, Dropdown } from 'antd';
import { DownloadOutlined, ReloadOutlined, DownOutlined } from '@ant-design/icons';

const getDefaultZoomState = (len) => {
  if (!len || len <= 60) return { start: 0, end: 100 };
  // 对于长数据，默认显示最后 200 个点，而不是最后 30%
  // 假设总长度 1000，显示最后 200，则 start = 80, end = 100
  const showCount = 200;
  const start = Math.max(0, ((len - showCount) / len) * 100);
  return { start: start, end: 100 };
};

const normalizeZoomState = (state, len) => {
  if (!state || typeof state !== 'object') return getDefaultZoomState(len);
  const start = Number(state.start);
  const end = Number(state.end);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return getDefaultZoomState(len);
  if (start < 0 || end > 100 || start >= end) return getDefaultZoomState(len);
  return { start, end };
};

export const KlineChart = React.memo(function KlineChart({ data, symbol, freq = 'D', preClose, onFreqChange }) {
  const chartRef = useRef(null);
  const safeData = useMemo(() => (Array.isArray(data) ? data : []), [data]);
  const dayPeriodValue = useMemo(() => (['D', 'W', 'M'].includes(freq) ? freq : null), [freq]);

  const zoomKey = useMemo(() => `kline_zoom_${symbol}_${freq}`, [symbol, freq]);
  const zoomSaveRafRef = useRef(null);
  const zoomPendingRef = useRef(null);

  useEffect(() => {
    const saved = localStorage.getItem(zoomKey);
    let nextState = null;
    if (saved) {
      try {
        nextState = JSON.parse(saved);
      } catch {
        nextState = null;
      }
    }

    const inst = chartRef.current?.getEchartsInstance?.();
    if (!inst) return;
    const target = normalizeZoomState(nextState, safeData.length);
    requestAnimationFrame(() => {
      try {
        const currentOption = inst.getOption();
        const currentZoom = currentOption.dataZoom?.[0];
        if (currentZoom && (Math.abs(currentZoom.start - target.start) > 0.1 || Math.abs(currentZoom.end - target.end) > 0.1)) {
          inst.dispatchAction({ type: 'dataZoom', start: target.start, end: target.end });
        }
      } catch {
        void 0;
      }
    });
  }, [zoomKey, safeData.length]);

  const handleDataZoom = (params) => {
    let start, end;
    if (params?.batch?.length) {
      start = params.batch[0].start;
      end = params.batch[0].end;
    } else {
      start = params?.start;
      end = params?.end;
    }
    const nextState = normalizeZoomState({ start, end }, safeData.length);
    zoomPendingRef.current = nextState;
    if (zoomSaveRafRef.current) return;
    zoomSaveRafRef.current = requestAnimationFrame(() => {
      zoomSaveRafRef.current = null;
      const pending = zoomPendingRef.current;
      if (!pending) return;
      localStorage.setItem(zoomKey, JSON.stringify(pending));
    });
  };

  const resetView = () => {
    const defaultState = getDefaultZoomState(safeData.length);
    localStorage.setItem(zoomKey, JSON.stringify(defaultState));
    if (chartRef.current) {
      const chartInstance = chartRef.current.getEchartsInstance();
      chartInstance.dispatchAction({
        type: 'dataZoom',
        start: defaultState.start,
        end: defaultState.end
      });
    }
  };

  const processedData = useMemo(() => {
    const dates = safeData.map(item => item.time);
    const values = safeData.map(item => [
      item.open != null ? Number(item.open) : null,
      item.close != null ? Number(item.close) : null,
      item.low != null ? Number(item.low) : null,
      item.high != null ? Number(item.high) : null,
      item.volume != null ? Number(item.volume) : 0,
      item.pct_chg != null ? Number(item.pct_chg) : 0
    ]);
    const ma5 = safeData.map(item => item.ma5 != null ? Number(item.ma5) : null);
    const ma10 = safeData.map(item => item.ma10 != null ? Number(item.ma10) : null);
    const ma20 = safeData.map(item => item.ma20 != null ? Number(item.ma20) : null);
    const macdData = {
      diff: safeData.map(item => item.macd_diff != null ? Number(item.macd_diff) : null),
      dea: safeData.map(item => {
        const val = item.macd_dea ?? item.macd_signal;
        return val != null ? Number(val) : null;
      }),
      macd: safeData.map(item => item.macd != null ? Number(item.macd) : null)
    };

    const volumeData = safeData.map((item) => {
      const open = Number(item.open);
      const close = Number(item.close);
      const volume = Number(item.volume || 0);
      const color = close >= open ? '#ef5350' : '#26a69a';
      return {
        value: volume,
        itemStyle: {
          color: color,
          opacity: 0.8,
          borderColor: color,
          borderWidth: 1
        },
        emphasis: {
          itemStyle: {
            color: color,
            opacity: 1,
            borderColor: '#fff',
            borderWidth: 1
          }
        }
      };
    });
    return { dates, values, ma5, ma10, ma20, macdData, volumeData };
  }, [safeData]);

  const { dates, values, ma5, ma10, ma20, macdData, volumeData } = processedData;

  const exportToCSV = () => {
    const headers = ['日期', '开盘价', '最高价', '最低价', '收盘价', '成交量'];
    const rows = safeData.map(item => [item.time, item.open, item.high, item.low, item.close, item.volume].join(","));
    const csvContent = "\ufeff" + headers.join(",") + "\n" + rows.join("\n");
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${symbol}_${freq}_data.csv`;
    link.click();
    URL.revokeObjectURL(link.href); // 释放内存
  };

  const option = useMemo(() => ({
    backgroundColor: 'transparent',
    animation: false, // 禁用全局动画以提升性能
    // animationDuration: 0, // 移除未使用的动画配置
    // animationEasing: 'cubicOut', // 移除未使用的动画配置
    legend: {
      top: 10,
      left: 'center',
      data: ['K线', 'MA5', 'MA10', 'MA20', 'DIFF', 'DEA', 'MACD'],
      textStyle: { color: '#ccc' }
    },
    toolbox: {
      show: true,
      feature: {
        dataZoom: {
          yAxisIndex: 'none',
          title: {
            zoom: '区域缩放',
            back: '还原'
          }
        },
        restore: { show: false },
        saveAsImage: { show: false }
      },
      iconStyle: {
        borderColor: '#666'
      },
      emphasis: {
        iconStyle: {
          borderColor: '#26a69a'
        }
      },
      right: 20,
      top: 10
    },
    axisPointer: {
      type: 'line',
      link: [{ xAxisIndex: [0, 1, 2] }],
      label: { 
        backgroundColor: '#333',
        borderColor: '#555',
        borderWidth: 1,
        show: true 
      },
      snap: true,
      show: true,
      z: 1000,
      triggerTooltip: true, // 强制同步触发 tooltip
      value: null, // 不强制绑定 value，让 ECharts 自动处理
      status: 'show' // 强制保持显示状态
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { 
        type: 'cross',
        label: {
          show: true,
          precision: 2,
          backgroundColor: '#333'
        },
        lineStyle: {
          type: 'dashed',
          color: '#777',
          width: 1
        },
        animation: false // 禁用动画以减少延迟
      },
      backgroundColor: 'rgba(25, 25, 25, 0.8)',
      borderColor: '#444',
      borderWidth: 1,
      textStyle: { color: '#ccc', fontSize: 12 },
      confine: true,
      triggerOn: 'mousemove|click', // 增加 click 触发
      showContent: true,
      enterable: false, // 禁用鼠标进入 tooltip
      renderMode: 'html', // 使用 html 渲染模式
      appendToBody: true, // 将 tooltip 挂载到 body 以减少层级计算
      className: 'echarts-tooltip', // 自定义类名方便调试
      formatter: (params) => {
        if (!params || params.length === 0) return '';
        
        // 查找K线数据，确保无论params顺序如何都能正确获取
        const klineItem = params.find(item => item.seriesName === 'K线');
        if (!klineItem) return '';

        const date = klineItem.name;
        let res = date + '<br/>';
        
        // ECharts candlestick params.data format might include index at position 0 when using category axis
        // We detect this by checking if data length is 7 (index + 6 values) instead of 6
        let open, close, low, high, vol, pctChg;
        
        // 确保 data 是数组
        const dataArr = Array.isArray(klineItem.data) ? klineItem.data : [];
        
        if (dataArr.length >= 6) {
           // ECharts candlestick data order: [open, close, low, high] (index 0 if no category index)
           // But wait, our 'values' array in useMemo is: [open, close, low, high, volume, pct_chg]
           // When ECharts processes it for 'candlestick' series:
           // If category axis, usually data is [index, open, close, low, high, ...]
           // Let's debug by checking values.
           
           // If the first value is an integer index (e.g., 200), and second is price (e.g. 11.5)
           // It's likely [index, open, close, low, high, vol, pct]
           
           // However, if we see 0.09, that's likely pctChg or change.
           // In your image, Open=0.09, Close=0.09. 
           // If our data source was [open, close, low, high, vol, pct], and ECharts mapped it wrong?
           
           // ECharts series.encode default:
           // x: 0, y: [1, 2, 3, 4] (if index 0 is time/category)
           
           // In our processedData:
           // values = [open, close, low, high, vol, pct]
           // Dates are in xAxis data.
           // So series data is just the values array.
           // ECharts will add the category index at position 0 automatically?
           // Yes, usually params.data = [index, open, close, low, high, vol, pct]
           
           if (dataArr.length === 7) {
               open = dataArr[1];
               close = dataArr[2];
               low = dataArr[3];
               high = dataArr[4];
               vol = dataArr[5];
               pctChg = dataArr[6];
           } else {
               // [open, close, low, high, vol, pct]
               open = dataArr[0];
               close = dataArr[1];
               low = dataArr[2];
               high = dataArr[3];
               vol = dataArr[4];
               pctChg = dataArr[5];
           }
        } else {
           // Fallback or empty
           open = 0; close = 0; low = 0; high = 0; vol = 0; pctChg = 0;
        }
        
        let volStr = '';
        if (vol >= 100000000) {
          volStr = (vol / 100000000).toFixed(2) + '亿手';
        } else if (vol >= 10000) {
          volStr = (vol / 10000).toFixed(2) + '万手';
        } else {
          volStr = Math.round(vol) + '手';
        }
        
        const pctChgNum = Number(pctChg);
        // 如果 pctChgNum 是 0.09 这种小数值且没有百分号，可能是涨跌额而不是涨跌幅？
        // 实际上 ECharts 数据里的 pct_chg 通常是我们后端传过来的
        // 检查后端数据源，如果是涨跌额 (change)，需要除以昨收盘算 pct
        // 但这里我们假设后端传的是 pct_chg (百分比值)
        
        const pctChgStr = Number.isFinite(pctChgNum) ? (pctChgNum > 0 ? '+' : '') + pctChgNum.toFixed(2) + '%' : '-';
        const pctColor = pctChgNum > 0 ? '#ef5350' : (pctChgNum < 0 ? '#26a69a' : '#ccc');
        
        // 修复开盘/收盘价显示错误的问题
        // 确保 open/close 是价格，不是涨跌额
        // 增加数据校验：如果 open/close 远小于 low/high (例如 0.09 vs 11.5)，说明字段映射可能错了
        // 这里只是 formatting，不改变数据源。如果源数据错了，这里显示就错了。
        
        res += `开盘: ${Number(open).toFixed(2)}<br/>收盘: ${Number(close).toFixed(2)}<br/>`;
        res += `涨跌: <span style="color: ${pctColor}">${pctChgStr}</span><br/>`;
        res += `最低: ${Number(low).toFixed(2)}<br/>最高: ${Number(high).toFixed(2)}<br/>成交量: ${volStr}<br/>`;

        // 处理其他指标 (MA, MACD等)
        params.forEach(item => {
          if (item.seriesName !== 'K线' && item.seriesName !== '成交量') {
            const val = (item.data && typeof item.data === 'object') ? item.data.value : item.data;
            if (val !== undefined && val !== null) {
              const formattedVal = (typeof val === 'number') ? val.toFixed(2) : val;
              res += `${item.seriesName}: ${formattedVal}<br/>`;
            }
          }
        });
        return res;
      }
    },
    grid: [
      { left: '40', right: '60', top: '10%', height: '50%' },
      { left: '40', right: '60', top: '65%', height: '15%' }, 
      { left: '40', right: '60', top: '82%', height: '10%' }
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: true,
        axisLine: { onZero: false, lineStyle: { color: '#333' } },
        axisPointer: { show: true, snap: true },
        splitLine: { show: false }
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: true,
        axisLine: { onZero: false, lineStyle: { color: '#333' } },
        axisPointer: { show: true, snap: true },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false }
      },
      {
        type: 'category',
        gridIndex: 2,
        data: dates,
        boundaryGap: true,
        axisLine: { onZero: false, lineStyle: { color: '#333' } },
        axisPointer: { show: true, snap: true },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false }
      }
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: false },
        axisLine: { lineStyle: { color: '#333' } },
        splitLine: { lineStyle: { color: '#222' } },
        axisLabel: { color: '#ccc' },
        position: 'right',
        axisPointer: {
          show: true,
          label: { show: true, precision: 2 }
        }
      },
      {
        scale: false,
        min: 0,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: { 
          show: true,
          color: '#666',
          fontSize: 10,
          formatter: (value) => {
            if (value >= 100000000) return (value / 100000000).toFixed(1) + '亿';
            if (value >= 10000) return (value / 10000).toFixed(1) + '万';
            return value;
          }
        },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisPointer: {
          show: true,
          label: { show: true }
        }
      },
      {
        scale: true,
        gridIndex: 2,
        splitNumber: 2,
        axisLabel: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisPointer: {
          show: true,
          label: { show: true }
        }
      }
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1, 2],
        zoomOnMouseWheel: true,
        moveOnMouseMove: true,
        moveOnMouseWheel: true
      },
      {
        show: true,
        xAxisIndex: [0, 1, 2],
        type: 'slider',
        top: '95%',
        textStyle: { color: '#ccc' },
        handleIcon: 'path://M10.7,11.9v-1.3H9.3v1.3c-4.9,0.3-8.8,4.4-8.8,9.4c0,5,3.9,9.1,8.8,9.4v1.3h1.3v-1.3c4.9-0.3,8.8-4.4,8.8-9.4C19.5,16.3,15.6,12.2,10.7,11.9z M13.3,24.4H6.7V23h6.6V24.4z M13.3,19.6H6.7v-1.4h6.6V19.6z',
        handleSize: '80%',
        dataBackground: {
          lineStyle: { color: '#26a69a' },
          areaStyle: { color: '#26a69a', opacity: 0.1 }
        },
        selectedDataBackground: {
          lineStyle: { color: '#26a69a' },
          areaStyle: { color: '#26a69a', opacity: 0.3 }
        },
        borderColor: '#333'
      }
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: values,
        itemStyle: {
          color: '#ef5350',
          color0: '#26a69a',
          borderColor: '#ef5350',
          borderColor0: '#26a69a'
        },
        barMaxWidth: freq === 'M' ? 40 : (freq === 'W' ? 20 : 10), // 根据周期动态限制柱体最大宽度
        large: true,
        markLine: {
          symbol: ['none', 'none'],
          data: [
            ...(preClose ? [{
              yAxis: preClose,
              name: '昨收',
              lineStyle: {
                color: '#666',
                type: 'dashed',
                width: 1
              },
              label: {
                show: true,
                position: 'end',
                formatter: `昨收: ${preClose}`,
                color: '#666'
              }
            }] : []),
            ...(safeData.length > 0 ? [{
              yAxis: safeData[safeData.length - 1].close,
              name: '现价',
              lineStyle: {
                color: safeData[safeData.length - 1].close >= (preClose || 0) ? '#ef5350' : '#26a69a',
                type: 'solid',
                width: 1
              },
              label: {
                show: true,
                position: 'end',
                formatter: `现价: ${safeData[safeData.length - 1].close}`,
                backgroundColor: safeData[safeData.length - 1].close >= (preClose || 0) ? '#ef5350' : '#26a69a',
                color: '#fff',
                padding: [2, 4],
                borderRadius: 2
              }
            }] : [])
          ]
        },
        markPoint: {
          label: {
            formatter: function (param) {
              return param != null ? Math.round(param.value) + '' : '';
            }
          },
          data: [
            {
              name: 'highest value',
              type: 'max',
              valueDim: 'highest'
            },
            {
              name: 'lowest value',
              type: 'min',
              valueDim: 'lowest'
            }
          ],
          tooltip: {
            formatter: function (param) {
              return param.name + '<br/>' + (param.data.coord || '');
            }
          }
        }
      },
      {
        name: 'MA5',
        type: 'line',
        data: ma5,
        smooth: true,
        showSymbol: false,
        connectNulls: true,
        lineStyle: { width: 1, color: '#fff' }
      },
      {
        name: 'MA10',
        type: 'line',
        data: ma10,
        smooth: true,
        showSymbol: false,
        connectNulls: true,
        lineStyle: { width: 1, color: '#ffea00' }
      },
      {
        name: 'MA20',
        type: 'line',
        data: ma20,
        smooth: true,
        showSymbol: false,
        connectNulls: true,
        lineStyle: { width: 1, color: '#e040fb' }
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        large: false,
        barMaxWidth: freq === 'M' ? 40 : (freq === 'W' ? 20 : 10), // 同步限制成交量柱体宽度
        barMinWidth: 2,
        barMinHeight: 5, // 进一步增加最小高度，确保月线末端小成交量清晰可见
        barWidth: '65%',
        z: 10, // 确保在顶层渲染
        data: volumeData
      },
      {
        name: 'DIFF',
        type: 'line',
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: macdData.diff,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1, color: '#fff' }
      },
      {
        name: 'DEA',
        type: 'line',
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: macdData.dea,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1, color: '#ffea00' }
      },
      {
        name: 'MACD',
        type: 'bar',
        xAxisIndex: 2,
        yAxisIndex: 2,
        large: true,
        data: macdData.macd.map((val) => {
          const numVal = Number(val);
          if (isNaN(numVal)) return { value: 0 };
          return {
            value: numVal,
            itemStyle: {
              color: numVal >= 0 ? '#ef5350' : '#26a69a'
            }
          };
        })
      }
    ]
  }), [dates, values, ma5, ma10, ma20, macdData, volumeData, preClose, freq, safeData]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.echarts = echarts;
    let cancelled = false;
    let lastInst = null;
    const update = () => {
      if (cancelled) return false;
      const inst = chartRef.current?.getEchartsInstance?.() || null;
      if (!inst) return false;
      if (typeof inst.getOption !== 'function') return false;
      if (inst.getOption() === null) return false;
      if (typeof inst.isDisposed === 'function' && inst.isDisposed() === true) return false;
      window.__klineChart = inst;
      lastInst = inst;
      return true;
    };
    update();
    let tick = 0;
    const timer = setInterval(() => {
      tick += 1;
      const ok = update();
      if (ok && tick >= 5) clearInterval(timer);
      if (tick >= 80) clearInterval(timer);
    }, 100);
    return () => {
      cancelled = true;
      clearInterval(timer);
      if (lastInst && window.__klineChart === lastInst) {
        window.__klineChart = null;
      }
    };
  }, [symbol, freq, safeData.length]);

  if (safeData.length === 0) return <div style={{ height: 600, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#666' }}>暂无行情数据</div>;

  return (
    <div className="kline-chart-container" style={{ position: 'relative', height: '600px', minHeight: '500px' }}>
      <div style={{ position: 'absolute', top: 10, left: 20, zIndex: 100 }}>
        <Space>
          <Radio.Group 
            value={dayPeriodValue} 
            onChange={(e) => onFreqChange && onFreqChange(e.target.value)} 
            buttonStyle="solid" 
            size="small"
          >
            <Radio.Button value="D">日线</Radio.Button>
            <Radio.Button value="W">周线</Radio.Button>
            <Radio.Button value="M">月线</Radio.Button>
          </Radio.Group>
          <Dropdown
            menu={{
              items: [
                { key: '5min', label: '5分钟' },
                { key: '30min', label: '30分钟' },
              ],
              onClick: ({ key }) => onFreqChange && onFreqChange(key),
              selectedKeys: ['5min', '30min'].includes(freq) ? [freq] : []
            }}
            trigger={['click']}
          >
            <Button size="small">
              {freq === '5min' ? '分钟: 5' : freq === '30min' ? '分钟: 30' : '分钟'}
              <DownOutlined />
            </Button>
          </Dropdown>
          <Tooltip title="重置视图">
            <Button 
              size="small" 
              icon={<ReloadOutlined />} 
              onClick={resetView}
            />
          </Tooltip>
          <Tooltip title="导出 CSV 数据">
            <Button 
              size="small" 
              icon={<DownloadOutlined />} 
              onClick={exportToCSV}
            >
              导出
            </Button>
          </Tooltip>
        </Space>
      </div>
      <ReactECharts
        ref={chartRef}
        echarts={echarts}
        option={option}
        notMerge={true}
        lazyUpdate={true}
        style={{ height: '100%', width: '100%' }}
        theme="dark"
        onEvents={{
          'datazoom': handleDataZoom,
          'dblclick': resetView
        }}
        opts={{ renderer: 'canvas' }} // 强制使用 Canvas 渲染器
      />
    </div>
  );
});

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Modal, Row, Col, Table, Tabs, Tag, Button, message, Popconfirm, Tooltip } from 'antd';
import { 
    WalletOutlined, 
    TransactionOutlined, 
    HistoryOutlined, 
    ReloadOutlined, 
    SolutionOutlined, 
    WarningOutlined, 
    LineChartOutlined
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts';
import { formatAmount, formatHandsFromShares } from '../utils/format';

const { TabPane } = Tabs;

const TradingPanel = ({ visible, onClose, onSelectStock }) => {
  const [account, setAccount] = useState(null);
  const [positions, setPositions] = useState([]);
  const [records, setRecords] = useState([]);
  const [entrustments, setEntrustments] = useState([]);
  const [equityCurve, setEquityCurve] = useState([]);
  const [loading, setLoading] = useState(false);
  const fetchWithTimeout = useCallback(async (url, options = {}, timeoutMs = 8000) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...options, signal: controller.signal });
      return res;
    } finally {
      clearTimeout(timer);
    }
  }, []);

  const handleOpenKline = (record) => {
    const code = record?.ts_code;
    if (!code) return;
    if (typeof onSelectStock === 'function') onSelectStock(code);
    if (typeof onClose === 'function') onClose();
  };

  const handleCancelEntrustment = async (planId) => {
    try {
      const response = await fetch(`/api/trading/plan/${planId}/cancel`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ reason: '手动撤单' }) });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || '撤单失败');
      }
      message.success('撤单成功');
      fetchData(); // 刷新数据
    } catch (error) {
      console.error('Cancel entrustment failed:', error);
      message.error(error.message);
    }
  };

  const fetchData = useCallback(async (options = {}) => {
    const { silent = false } = options;
    if (!silent) setLoading(true);
    try {
      const dashboardRes = await fetchWithTimeout('/api/trading/refresh', { method: 'POST' }, 20000);
      if (!dashboardRes.ok) {
        let detail = '统一快照失败';
        try {
          const data = await dashboardRes.json();
          detail = data?.detail || data?.message || detail;
        } catch {
          detail = '统一快照失败';
        }
        throw new Error(detail);
      }
      const dashboard = await dashboardRes.json();
      setAccount(dashboard.account || null);
      setPositions(Array.isArray(dashboard.positions) ? dashboard.positions : []);
      setRecords(Array.isArray(dashboard.records) ? dashboard.records : []);
      setEntrustments(Array.isArray(dashboard.entrustments) ? dashboard.entrustments : []);

      const equityRes = await fetchWithTimeout('/api/trading/equity-curve', {}, 20000);
      if (equityRes.ok) {
        const eq = await equityRes.json();
        setEquityCurve(Array.isArray(eq) ? eq : []);
      }
    } catch (error) {
      console.error('Failed to fetch trading data:', error);
      if (!silent) {
        if (error?.name === 'AbortError') {
          message.warning('交易数据拉取超时，已保留上次结果');
        } else {
          message.error(error?.message || '获取交易数据失败');
        }
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, [fetchWithTimeout]);

  useEffect(() => {
    if (visible) {
      fetchData();
    }
  }, [visible, fetchData]);

  const isTradingTime = () => {
    const now = new Date();
    const day = now.getDay();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const time = hour * 100 + minute;
    if (day === 0 || day === 6) return false;
    return (time >= 910 && time <= 1135) || (time >= 1255 && time <= 1505);
  };

  const autoSyncRef = useRef(false);

  useEffect(() => {
    if (!visible) return;
    const timer = setInterval(async () => {
      if (autoSyncRef.current) return;
      if (!isTradingTime()) return;
      autoSyncRef.current = true;
      try {
        await fetchData({ silent: true });
      } catch (error) {
        console.error('Auto sync assets failed:', error);
      } finally {
        autoSyncRef.current = false;
      }
    }, 5000);
    return () => clearInterval(timer);
  }, [visible, fetchData]);

  const handleSync = async () => {
    setLoading(true);
    try {
        await fetchData();
        message.success('资产同步成功');
    } catch (error) {
        console.error('Sync assets failed:', error);
        message.error(error.message || '同步失败，请检查网络或稍后重试');
    } finally {
        setLoading(false);
    }
  };

  const positionColumns = [
    {
      title: '股票',
      key: 'name',
      render: (text, record) => (
        <Tooltip title="点击打开K线分析">
          <div>
            <div style={{ fontWeight: 'bold' }}>{record.name}</div>
            <div style={{ fontSize: 12, color: '#888' }}>{record.ts_code}</div>
          </div>
        </Tooltip>
      ),
    },
    {
      title: '持仓/可用',
      key: 'vol',
      render: (text, record) => (
        <span>{formatHandsFromShares(record.vol)} / <span style={{ color: '#888' }}>{formatHandsFromShares(record.available_vol)}</span></span>
      ),
    },
    {
      title: '现价/成本',
      key: 'price',
      render: (text, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>{record.current_price.toFixed(2)}</div>
          <div style={{ fontSize: 12, color: '#888' }}>{record.avg_price.toFixed(2)}</div>
        </div>
      ),
    },
    {
      title: '市值',
      dataIndex: 'market_value',
      key: 'market_value',
      render: (val) => formatAmount(val),
    },
    {
      title: '浮盈',
      key: 'float_pnl',
      render: (text, record) => {
        const color = record.float_pnl >= 0 ? '#ef5350' : '#26a69a';
        return (
          <div style={{ color, fontWeight: 'bold' }}>
            <div>{formatAmount(record.float_pnl)}</div>
            <div style={{ fontSize: 12 }}>{record.pnl_pct.toFixed(2)}%</div>
          </div>
        );
      },
    }
  ];

  const today = new Date().toISOString().split('T')[0];
  const todayEntrustments = entrustments.filter(e => String(e?.created_at || '').startsWith(today));
  const historyEntrustments = entrustments.filter(e => !String(e?.created_at || '').startsWith(today));

  const entrustmentColumns = [
    {
      title: '日期/时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (val) => {
        const d = new Date(val);
        const dateStr = d.toLocaleDateString();
        const timeStr = d.toLocaleTimeString();
        return (
          <div>
            {d.toISOString().startsWith(today) ? (
              <span style={{ fontWeight: 'bold', color: '#1890ff' }}>{timeStr}</span>
            ) : (
              <div>
                <div style={{ fontSize: 11 }}>{dateStr}</div>
                <div style={{ fontSize: 12, color: '#888' }}>{timeStr}</div>
              </div>
            )}
          </div>
        );
      },
      width: 100,
    },
    {
      title: '股票',
      key: 'name',
      render: (text, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>{record.name}</div>
          <div style={{ fontSize: 12, color: '#888' }}>{record.ts_code}</div>
        </div>
      ),
      width: 120,
    },
    {
      title: '操作/策略',
      key: 'strategy',
      render: (text, record) => (
        <div>
          <div style={{ marginBottom: 4 }}>
            <Tag color={record.action === 'BUY' ? 'red' : record.action === 'SELL' ? 'green' : 'blue'}>
              {record.action === 'BUY' ? '买入' : record.action === 'SELL' ? '卖出' : '观察'}
            </Tag>
            <Tag color={record.order_type === 'LIMIT' ? 'orange' : 'cyan'} style={{ fontSize: 10 }}>
              {record.order_type === 'LIMIT' ? '限价' : '市价'}
            </Tag>
          </div>
          <div style={{ fontSize: 12, color: '#1890ff' }}>{record.strategy_name}</div>
        </div>
      ),
      width: 140,
    },
    {
      title: '委托 (数量/价格)',
      key: 'target',
      render: (text, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>{record.target_vol ? formatHandsFromShares(record.target_vol) : '-'}</div>
          <div style={{ fontSize: 12, color: '#888' }}>{record.target_price ? record.target_price.toFixed(2) : '-'}</div>
        </div>
      ),
      width: 130,
    },
    {
      title: '当前价',
      dataIndex: 'current_price',
      key: 'current_price',
      render: (val, record) => {
        const color = val >= record.limit_price ? '#ef5350' : '#26a69a';
        return <span style={{ color, fontWeight: 'bold' }}>{val > 0 ? val.toFixed(2) : '-'}</span>;
      },
      width: 90,
    },
    {
      title: '成交 (数量/价格)',
      key: 'executed',
      render: (text, record) => (
        <div>
          <div style={{ fontWeight: 'bold', color: record.executed_vol ? '#52c41a' : '#888' }}>
            {record.executed_vol ? formatHandsFromShares(record.executed_vol) : '未成交'}
          </div>
          <div style={{ fontSize: 12, color: '#888' }}>
            {record.executed_price ? record.executed_price.toFixed(2) : '-'}
          </div>
        </div>
      ),
      width: 130,
    },
    {
      title: '冻结 (数量/金额)',
      key: 'frozen',
      render: (text, record) => (
        record.frozen_amount > 0 ? (
          <div>
            <div style={{ fontWeight: 'bold', color: '#fa8c16' }}>{formatHandsFromShares(record.frozen_vol)}</div>
            <div style={{ fontSize: 12, color: '#fa8c16' }}>{formatAmount(record.frozen_amount)}</div>
          </div>
        ) : (record.required_amount > 0 ? (
          <div>
            <div style={{ fontWeight: 'bold', color: '#666' }}>{formatHandsFromShares(record.required_vol)}</div>
            <div style={{ fontSize: 12, color: '#666' }}>{formatAmount(record.required_amount)}</div>
          </div>
        ) : '-')
      ),
      width: 130,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status, record) => {
        let color = 'default';
        if (status === '已成交') color = 'success';
        if (status === '排队中' || status === '已报待成') color = 'processing';
        if (status === '涨停排队') color = 'geekblue';
        if (status === '待成交') color = 'warning';
        if (status === '未冻结') color = 'default';
        if (status === '观察中') color = 'cyan';
        if (status === '废单') color = 'error';
        
        return (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <Tag color={color}>{status}</Tag>
              {record.warning && (
                <Tag color="error" icon={<WarningOutlined />}>审计异常</Tag>
              )}
            </div>
            {record.warning && (
              <div style={{ marginTop: 4 }}>
                {record.warning.split(' | ').map((warn, i) => (
                  <div key={i} style={{ fontSize: 12, color: '#ff4d4f', fontWeight: 'bold', display: 'flex', alignItems: 'flex-start', gap: 4 }}>
                    <span style={{ marginTop: 2 }}>•</span>
                    <span>{warn}</span>
                  </div>
                ))}
              </div>
            )}
            {record.review_content && (
              <Tooltip
                placement="topLeft"
                title={<div style={{ whiteSpace: 'pre-wrap' }}>{record.review_content}</div>}
              >
                <div
                  style={{
                    fontSize: 11,
                    color: '#999',
                    marginTop: 4,
                    maxWidth: 200,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    cursor: 'default',
                  }}
                >
                  {record.review_content}
                </div>
              </Tooltip>
            )}
          </div>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      render: (text, record) => (
        <div style={{ display: 'flex', gap: 8 }}>
          {['待成交', '排队中', '已报待成', '涨停排队', '未冻结'].includes(record.status) && (
            <Popconfirm
              title="确定要撤销该委托吗？"
              onConfirm={() => handleCancelEntrustment(record.id)}
              okText="确定"
              cancelText="取消"
            >
              <Button size="small" danger>撤单</Button>
            </Popconfirm>
          )}
        </div>
      ),
      width: 80,
    },
  ];

  const recordColumns = [
    {
      title: '时间',
      dataIndex: 'trade_time',
      key: 'trade_time',
      render: (val) => new Date(val).toLocaleString(),
    },
    {
      title: '股票',
      key: 'name',
      render: (text, record) => `${record.name} (${record.ts_code})`,
    },
    {
      title: '操作',
      dataIndex: 'trade_type',
      key: 'trade_type',
      render: (type) => (
        <Tag color={type === 'BUY' ? 'red' : 'green'}>
          {type === 'BUY' ? '买入' : '卖出'}
        </Tag>
      ),
    },
    {
      title: '价格',
      dataIndex: 'price',
      key: 'price',
      render: (val) => val.toFixed(2),
    },
    {
      title: '数量',
      dataIndex: 'vol',
      key: 'vol',
      render: (val) => formatHandsFromShares(val),
    },
    {
      title: '金额',
      dataIndex: 'amount',
      key: 'amount',
      render: (val) => formatAmount(val),
    },
    {
      title: '盈亏比例',
      dataIndex: 'pnl_pct',
      key: 'pnl_pct',
      render: (val, record) => {
        if (record.trade_type === 'BUY' || val === null || val === undefined) return '-';
        const color = val >= 0 ? '#ef5350' : '#26a69a';
        return <span style={{ color, fontWeight: 'bold' }}>{val.toFixed(2)}%</span>;
      },
    },
  ];

  const getEquityChartOption = () => {
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross', label: { backgroundColor: '#333' } },
        backgroundColor: 'rgba(30, 30, 30, 0.9)',
        borderColor: '#444',
        textStyle: { color: '#eee' },
        formatter: function (params) {
          const data = params[0].data;
          const date = data[0];
          const totalAssets = data[1];
          const dailyPnl = data[2];
          const dailyPnlPct = data[3];
          const totalPnl = data[4];
          const totalPnlPct = data[5];
          
          const pnlColor = dailyPnl >= 0 ? '#ef5350' : '#26a69a';
          const totalPnlColor = totalPnl >= 0 ? '#ef5350' : '#26a69a';

          return `<div style="padding: 4px 8px;">
            <div style="font-weight: bold; margin-bottom: 4px; border-bottom: 1px solid #444; color: #fff;">${date}</div>
            <div style="display: flex; justify-content: space-between; gap: 20px;">
              <span style="color: #888;">总资产:</span>
              <span style="font-weight: bold; color: #fff;">¥${totalAssets.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
            </div>
            <div style="display: flex; justify-content: space-between; gap: 20px;">
              <span style="color: #888;">当日盈亏:</span>
              <span style="color: ${pnlColor}; font-weight: bold;">${dailyPnl >= 0 ? '+' : ''}${dailyPnl.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} (${dailyPnlPct.toFixed(2)}%)</span>
            </div>
            <div style="display: flex; justify-content: space-between; gap: 20px;">
              <span style="color: #888;">累计盈亏:</span>
              <span style="color: ${totalPnlColor}; font-weight: bold;">${totalPnl >= 0 ? '+' : ''}${totalPnl.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} (${totalPnlPct.toFixed(2)}%)</span>
            </div>
          </div>`;
        }
      },
      legend: {
        data: ['总资产', '当日盈亏'],
        bottom: 0,
        textStyle: { color: '#888' }
      },
      grid: {
        top: '10%',
        left: '3%',
        right: '4%',
        bottom: '12%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        boundaryGap: true,
        data: equityCurve.map(item => item.date),
        axisLine: { lineStyle: { color: '#333' } },
        axisLabel: { color: '#888' }
      },
      yAxis: [
        {
          type: 'value',
          name: '资产',
          nameTextStyle: { color: '#888' },
          scale: true,
          splitLine: { lineStyle: { type: 'dashed', color: '#222' } },
          axisLabel: { 
            color: '#888',
            formatter: (val) => val >= 10000 ? (val/10000).toFixed(0) + '万' : val
          }
        },
        {
          type: 'value',
          name: '当日盈亏',
          nameTextStyle: { color: '#888' },
          splitLine: { show: false },
          axisLabel: { color: '#888' }
        }
      ],
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
        { 
          type: 'slider', 
          bottom: 30, 
          height: 20, 
          start: 0, 
          end: 100, 
          handleSize: '100%',
          backgroundColor: '#1a1a1a',
          borderColor: '#333',
          fillerColor: 'rgba(24, 144, 255, 0.1)',
          handleStyle: { color: '#444' }
        }
      ],
      series: [
        {
          name: '总资产',
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: equityCurve.map(item => [
            item.date, 
            item.total_assets, 
            item.daily_pnl, 
            item.daily_pnl_pct,
            item.total_pnl,
            item.total_pnl_pct
          ]),
          lineStyle: { width: 3, color: '#1890ff' },
          itemStyle: { color: '#1890ff' },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(24, 144, 255, 0.2)' },
              { offset: 1, color: 'rgba(24, 144, 255, 0)' }
            ])
          }
        },
        {
          name: '当日盈亏',
          type: 'bar',
          yAxisIndex: 1,
          data: equityCurve.map(item => ({
            value: item.daily_pnl,
            itemStyle: {
              color: item.daily_pnl >= 0 ? 'rgba(239, 83, 80, 0.5)' : 'rgba(38, 166, 154, 0.5)'
            }
          }))
        }
      ]
    };
  };

  return (
    <Modal
      title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <WalletOutlined /> 模拟交易账户
              <Button type="link" icon={<ReloadOutlined />} onClick={handleSync} loading={loading} />
          </div>
      }
      open={visible}
      onCancel={onClose}
      footer={null}
      width={900}
      styles={{ body: { padding: '20px' } }}
    >
      {account && (
        <div style={{ padding: '16px 0', marginBottom: '16px', borderBottom: '1px solid #333' }}>
          <Row gutter={16}>
            <Col span={5}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>总资产</div>
                <div style={{ color: '#1890ff', fontSize: '20px', fontWeight: 'bold' }}>
                  <span style={{ fontSize: '14px', marginRight: '2px' }}>¥</span>
                  {account.total_assets.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
              </div>
            </Col>
            <Col span={5}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>可用资金</div>
                <div style={{ color: '#ddd', fontSize: '18px' }}>
                  <span style={{ fontSize: '12px', marginRight: '2px' }}>¥</span>
                  {account.available_cash.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
              </div>
            </Col>
            <Col span={4}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>冻结资金</div>
                <div style={{ color: account.frozen_cash > 0 ? '#fa8c16' : '#ddd', fontSize: '18px' }}>
                  <span style={{ fontSize: '12px', marginRight: '2px' }}>¥</span>
                  {account.frozen_cash.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
              </div>
            </Col>
            <Col span={5}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>持仓市值</div>
                <div style={{ color: '#ddd', fontSize: '18px' }}>
                  <span style={{ fontSize: '12px', marginRight: '2px' }}>¥</span>
                  {account.market_value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
              </div>
            </Col>
            <Col span={5}>
              <div style={{ textAlign: 'center', borderLeft: '1px solid #333' }}>
                <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>总盈亏</div>
                <div style={{ color: account.total_pnl >= 0 ? '#ef5350' : '#26a69a', fontSize: '18px', fontWeight: 'bold' }}>
                  {account.total_pnl >= 0 ? '+' : ''}
                  {account.total_pnl.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                  <span style={{ fontSize: '12px', marginLeft: '4px' }}>({account.total_pnl_pct.toFixed(2)}%)</span>
                </div>
              </div>
            </Col>
          </Row>
        </div>
      )}

      <Tabs defaultActiveKey="positions">
        <TabPane tab={<span><LineChartOutlined /> 资金曲线</span>} key="equity">
          <div style={{ height: 400, marginTop: 10 }}>
            <ReactECharts 
              option={getEquityChartOption()} 
              style={{ height: '100%', width: '100%' }}
              notMerge={true}
              lazyUpdate={true}
            />
          </div>
        </TabPane>
        <TabPane tab={<span><TransactionOutlined /> 持仓列表 ({positions.length})</span>} key="positions">
          <Table 
            dataSource={positions} 
            columns={positionColumns} 
            rowKey="ts_code" 
            pagination={false}
            size="small"
            onRow={(record) => ({
              onClick: () => handleOpenKline(record),
              style: { cursor: 'pointer' }
            })}
          />
        </TabPane>
        <TabPane tab={<span><SolutionOutlined /> 委托详情</span>} key="entrustments">
          <Tabs size="small" type="card" style={{ marginTop: -10 }}>
            <TabPane tab={`今日委托 (${todayEntrustments.length})`} key="today_ent">
              <Table 
                dataSource={todayEntrustments} 
                columns={entrustmentColumns} 
                rowKey="id" 
                pagination={false}
                size="small"
              />
            </TabPane>
            <TabPane tab={`历史委托 (${historyEntrustments.length})`} key="history_ent">
              <Table 
                dataSource={historyEntrustments} 
                columns={entrustmentColumns} 
                rowKey="id" 
                pagination={{ pageSize: 10 }}
                size="small"
              />
            </TabPane>
          </Tabs>
        </TabPane>
        <TabPane tab={<span><HistoryOutlined /> 交易记录</span>} key="records">
          <Table 
            dataSource={records} 
            columns={recordColumns} 
            rowKey={(record, index) => index}
            pagination={{ pageSize: 10 }}
            size="small"
          />
        </TabPane>
      </Tabs>
    </Modal>
  );
};

export default TradingPanel;

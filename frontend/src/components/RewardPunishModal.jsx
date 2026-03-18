import React from 'react';
import { Modal, Card, Tag, Table, Button, Spin, Typography, Empty } from 'antd';

const { Text } = Typography;

const formatPct = (val) => {
  if (val === undefined || val === null || Number.isNaN(Number(val))) return '--';
  return `${(Number(val) * 100).toFixed(2)}%`;
};

const formatNum = (val, digits = 2) => {
  if (val === undefined || val === null || Number.isNaN(Number(val))) return '--';
  return Number(val).toFixed(digits);
};

const formatMetricLabel = (val) => {
  const map = {
    total_return: '收益率',
    max_drawdown: '最大回撤',
    sharpe: '夏普比率',
    win_rate: '胜率',
    consecutive_loss_pct: '连续亏损',
    daily_buy_count: '当日买入次数'
  };
  return map[val] || val || '--';
};

const formatActionLabel = (val) => {
  const map = {
    BONUS: '绩效奖金',
    BONUS_EXTRA: '额外分红',
    BONUS_SHARPE: '夏普奖励',
    BONUS_WINRATE: '胜率奖励',
    PAUSE_TRADING: '暂停交易',
    STRONG_PENALTY: '严惩'
  };
  return map[val] || val || '--';
};

const formatLevelLabel = (val) => {
  const map = {
    LEVEL1: '一级',
    LEVEL2: '二级',
    HIGH: '高',
    CRITICAL: '严重'
  };
  return map[val] || val || '--';
};

const formatStatusLabel = (val) => {
  const map = {
    TRIGGERED: '已触发',
    RESOLVED: '已处理',
    PENDING: '待处理'
  };
  return map[val] || val || '--';
};

const RewardPunishModal = ({ visible, onCancel, loading, data, onRefresh }) => {
  const metrics = data?.metrics || {};
  const events = Array.isArray(data?.recent_events) ? data.recent_events : [];
  const paused = !!data?.trading_paused;

  const columns = [
    { title: '日期', dataIndex: 'date', key: 'date', width: 110 },
    { title: '规则', dataIndex: 'rule_name', key: 'rule_name', width: 160 },
    { title: '指标', dataIndex: 'metric', key: 'metric', width: 120, render: (v) => formatMetricLabel(v) },
    { title: '数值', dataIndex: 'value', key: 'value', width: 120, render: (v) => formatNum(v, 4) },
    { title: '动作', dataIndex: 'action', key: 'action', width: 140, render: (v) => formatActionLabel(v) },
    { title: '级别', dataIndex: 'level', key: 'level', width: 100, render: (v) => formatLevelLabel(v) },
    { title: '状态', dataIndex: 'status', key: 'status', width: 110, render: (v) => formatStatusLabel(v) }
  ];

  return (
    <Modal
      title="奖惩制度与风控状态"
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={900}
      styles={{ body: { maxHeight: '70vh', overflowY: 'auto', padding: '16px' } }}
    >
      {loading ? (
        <div style={{ textAlign: 'center', padding: '30px 0' }}>
          <Spin />
        </div>
      ) : (
        <>
          <div style={{ display: 'flex', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
            <Card size="small" style={{ flex: 1, minWidth: 240 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Text>交易状态</Text>
                {paused ? <Tag color="red">暂停</Tag> : <Tag color="green">正常</Tag>}
              </div>
              <div style={{ marginTop: 8, color: '#888' }}>{data?.pause_reason || '无'}</div>
            </Card>
            <Card size="small" style={{ flex: 1, minWidth: 240 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>累计收益率</Text>
                <Text strong>{formatPct(metrics.total_return)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>最大回撤</Text>
                <Text>{formatPct(metrics.max_drawdown)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>夏普比率</Text>
                <Text>{formatNum(metrics.sharpe, 2)}</Text>
              </div>
            </Card>
            <Card size="small" style={{ flex: 1, minWidth: 240 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>胜率</Text>
                <Text>{formatPct(metrics.win_rate)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>连续亏损</Text>
                <Text>{formatPct(metrics.consecutive_loss_pct)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>当日买入次数</Text>
                <Text>{formatNum(metrics.daily_buy_count, 0)}</Text>
              </div>
            </Card>
          </div>
          <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
            <Button onClick={onRefresh}>刷新</Button>
          </div>
          {events.length > 0 ? (
            <Table
              size="small"
              rowKey="id"
              dataSource={events}
              columns={columns}
              pagination={false}
              scroll={{ x: 760 }}
            />
          ) : (
            <Empty description="暂无奖惩事件" />
          )}
        </>
      )}
    </Modal>
  );
};

export default RewardPunishModal;

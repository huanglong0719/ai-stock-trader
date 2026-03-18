import React, { memo } from 'react';
import { Badge, Tooltip, message, Typography, Spin, Progress } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import axios from 'axios';

const { Text } = Typography;

const DataStatusBar = memo(({ syncStatus }) => {
  const handleReload = () => {
    axios.post('/api/sync/backfill', { days: 3 });
    message.info('已触发增量数据同步...');
  };

  const currentTask = syncStatus?.data_quality?.current_task;
  const isRunning = currentTask?.status === 'running';
  const dataQuality = syncStatus?.data_quality;

  return (
    <div style={{ padding: '12px 16px', borderTop: '1px solid #303030', flexShrink: 0, backgroundColor: '#1a1a1a' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <Text type="secondary" style={{ fontSize: 11 }}>
            {isRunning ? '正在同步数据...' : '数据同步状态'}
        </Text>
        <Badge status={isRunning ? 'processing' : (dataQuality?.status === 'Healthy' ? 'success' : 'warning')} size="small" />
      </div>
      
      {isRunning ? (
        <div style={{ fontSize: 11, color: '#888' }}>
            <div style={{ marginBottom: 4 }}>{currentTask?.message || '正在处理...'}</div>
            <Progress percent={currentTask?.progress || 0} size="small" status="active" strokeColor="#26a69a" showInfo={false} />
        </div>
      ) : dataQuality ? (
        <div style={{ fontSize: 11, color: '#666' }}>
          <div>最新交易日: <span style={{ color: '#aaa' }}>{dataQuality.latest_trade_date}</span></div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>数据覆盖率: <span style={{ color: '#aaa' }}>{dataQuality.latest_coverage}</span></span>
            <Tooltip title="点击重新同步最近数据">
              <ReloadOutlined 
                style={{ cursor: 'pointer' }} 
                onClick={handleReload} 
              />
            </Tooltip>
          </div>
        </div>
      ) : (
        <div style={{ fontSize: 11, color: '#666', display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 }}>
           <Spin size="small" /> 
           <span>正在检查数据状态...</span>
        </div>
      )}
    </div>
  );
});

DataStatusBar.displayName = 'DataStatusBar';

export default DataStatusBar;

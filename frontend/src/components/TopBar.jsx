import React, { memo } from 'react';
import { Typography, Tag, Button, Badge, Spin, Select, Input, Popover, message, Tooltip } from 'antd';
import { RiseOutlined, FallOutlined, PlusOutlined, ReloadOutlined, RobotOutlined, FireOutlined, SettingOutlined, ToolOutlined, DatabaseOutlined, SafetyOutlined } from '@ant-design/icons';
import { formatVolume } from '../utils/format';
import axios from 'axios';

const { Title } = Typography;
const { Option } = Select;

const TopBar = memo(({
  quoteData,
  selectedStock,
  isSelectedInWatchlist,
  onAddToWatchlist,
  lastUpdateTime,
  realtimeEnabled,
  onRefresh,
  aiLoading,
  onGenerateAnalysis,
  onDailyReview,
  onViewReview,
  onViewPlans,
  onViewNav,
  onManageMemory,
  onViewTrading,
  onViewRewardPunish,
  availableProviders,
  analysisProvider,
  onAnalysisProviderChange,
  analysisApiKey,
  onAnalysisApiKeyChange
}) => {
  const getChangeColor = (val, base) => {
    if (val === undefined || base === undefined) return '#ccc';
    return val >= base ? '#ef5350' : '#26a69a';
  };

  const safeFixed = (val, digits = 2) => {
    if (val === undefined || val === null || isNaN(val)) return '--';
    return Number(val).toFixed(digits);
  };

  const handleFixStock = async () => {
    if (!selectedStock) return;
    message.loading({ content: `正在修复 ${selectedStock} 数据...`, key: 'fix_stock' });
    try {
      await axios.post('/api/sync/fix_stock', { ts_code: selectedStock });
      message.success({ content: `已触发 ${selectedStock} 修复任务，请稍候...`, key: 'fix_stock', duration: 3 });
    } catch (err) {
      message.error({ content: `修复失败: ${err.message}`, key: 'fix_stock' });
    }
  };

  return (
    <div style={{
      background: '#141414',
      padding: '0 8px',
      borderBottom: '1px solid #303030',
      height: 64,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between'
    }}>
      {quoteData ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', flex: 1, overflow: 'hidden' }}>
            {/* 股票基本信息 */}
            <div style={{ display: 'flex', flexDirection: 'column', marginRight: 8, minWidth: 80 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <Title level={5} style={{ margin: 0, color: '#fff', whiteSpace: 'nowrap', fontSize: 16, lineHeight: '24px' }}>
                  {quoteData.name || selectedStock}
                </Title>
                <span style={{ color: '#666', fontSize: 12, paddingTop: 2, lineHeight: '20px' }}>{selectedStock?.split('.')[0]}</span>
                {isSelectedInWatchlist ? (
                  <Tag color="success" style={{ fontSize: 10, lineHeight: '16px', padding: '0 2px', margin: 0 }}>自选</Tag>
                ) : (
                  <Button
                    size="small"
                    type="text"
                    icon={<PlusOutlined />}
                    onClick={onAddToWatchlist}
                    style={{ fontSize: 12, height: 20, padding: '0 2px', color: '#1890ff' }}
                  />
                )}
                <Tooltip title="修复数据 (清理缓存并重新下载)">
                  <Button
                    size="small"
                    type="text"
                    icon={<ToolOutlined />}
                    onClick={handleFixStock}
                    style={{ fontSize: 12, height: 20, padding: '0 2px', color: '#faad14', marginLeft: 4 }}
                  />
                </Tooltip>
              </div>
              <div style={{ fontSize: 11, color: '#666', marginTop: 2, whiteSpace: 'nowrap', lineHeight: '14px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {quoteData.industry || '--'} | {quoteData.area || '--'}
              </div>
            </div>

            {/* 核心行情数据 */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginRight: 'auto' }}>
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                <span style={{ fontSize: 11, color: '#888', marginBottom: 2, lineHeight: '14px', whiteSpace: 'nowrap' }}>最新价</span>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 4 }}>
                  <span style={{
                    color: getChangeColor(quoteData.price, quoteData.pre_close),
                    fontSize: 20,
                    fontWeight: 'bold',
                    lineHeight: '24px',
                    whiteSpace: 'nowrap'
                  }}>
                    {safeFixed(quoteData.price)}
                  </span>
                  {quoteData.price !== undefined && quoteData.pre_close !== undefined ? (
                    quoteData.price >= quoteData.pre_close ? <RiseOutlined style={{ fontSize: 12, color: '#ef5350' }} /> : <FallOutlined style={{ fontSize: 12, color: '#26a69a' }} />
                  ) : null}
                </div>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column' }}>
                <span style={{ fontSize: 11, color: '#888', marginBottom: 2, lineHeight: '14px', whiteSpace: 'nowrap' }}>涨跌幅</span>
                <span style={{
                  color: getChangeColor(quoteData.price, quoteData.pre_close),
                  fontSize: 16,
                  fontWeight: 'bold',
                  lineHeight: '24px',
                  whiteSpace: 'nowrap'
                }}>
                  {quoteData.price !== undefined && quoteData.pre_close ? (
                    ((quoteData.price - quoteData.pre_close) / quoteData.pre_close * 100).toFixed(2) + '%'
                  ) : '--'}
                </span>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column' }}>
                <span style={{ fontSize: 11, color: '#888', marginBottom: 2, lineHeight: '14px', whiteSpace: 'nowrap' }}>成交量</span>
                <span style={{ fontSize: 14, color: '#ccc', lineHeight: '24px', whiteSpace: 'nowrap' }}>
                  {formatVolume(quoteData.vol)}
                </span>
              </div>

              <div style={{ width: 1, height: 24, background: '#303030', margin: '0 2px' }} />

              <div style={{ display: 'flex', gap: 4 }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  <div style={{ display: 'flex', gap: 4, fontSize: 12, lineHeight: '14px' }}>
                    <span style={{ color: '#888' }}>今开</span>
                    <span style={{ color: getChangeColor(quoteData.open, quoteData.pre_close) }}>
                      {safeFixed(quoteData.open)}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, fontSize: 12, lineHeight: '14px' }}>
                    <span style={{ color: '#888' }}>换手</span>
                    <span style={{ color: '#ccc' }}>
                      {quoteData.turnover_rate ? `${Number(quoteData.turnover_rate).toFixed(2)}%` : '--'}
                    </span>
                  </div>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  <div style={{ display: 'flex', gap: 4, fontSize: 12, lineHeight: '14px' }}>
                    <span style={{ color: '#888' }}>最高</span>
                    <span style={{ color: getChangeColor(quoteData.high, quoteData.pre_close) }}>
                      {safeFixed(quoteData.high)}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, fontSize: 12, lineHeight: '14px' }}>
                    <span style={{ color: '#888' }}>最低</span>
                    <span style={{ color: getChangeColor(quoteData.low, quoteData.pre_close) }}>
                      {safeFixed(quoteData.low)}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* 右侧操作区 - Always visible regardless of quoteData */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, paddingLeft: 4, flexShrink: 0 }}>
            {lastUpdateTime && (
              <div style={{ color: '#666', fontSize: 11, marginRight: 4, display: 'flex', flexDirection: 'column', alignItems: 'flex-end', justifyContent: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', lineHeight: '14px', marginBottom: 2 }}>
                  <Badge status={realtimeEnabled ? "processing" : "default"} size="small" />
                  <span style={{ marginLeft: 4, lineHeight: '14px', whiteSpace: 'nowrap' }}>实时</span>
                </div>
                <span style={{ fontSize: 10, opacity: 0.8, lineHeight: '14px', whiteSpace: 'nowrap' }}>{lastUpdateTime.split(' ')[1] || lastUpdateTime}</span>
              </div>
            )}
            <Button size="small" icon={<ReloadOutlined />} onClick={onRefresh} style={{ fontSize: 12, padding: '0 4px' }} title="刷新" />
            <Button size="small" type="primary" danger ghost icon={<FireOutlined />} onClick={onDailyReview} style={{ fontSize: 12, padding: '0 6px' }}>复盘</Button>
            <Button size="small" onClick={onViewReview} style={{ fontSize: 12, padding: '0 6px' }}>结果</Button>
            <Button size="small" onClick={onViewPlans} style={{ fontSize: 12, color: '#1890ff', borderColor: '#1890ff', padding: '0 6px' }}>计划</Button>
            <Button size="small" onClick={onViewNav} style={{ fontSize: 12, color: '#722ed1', borderColor: '#722ed1', padding: '0 6px' }}>导航</Button>
            <Button size="small" onClick={onViewTrading} style={{ fontSize: 12, color: '#52c41a', borderColor: '#52c41a', padding: '0 6px' }}>交易</Button>
            <Button size="small" icon={<SafetyOutlined />} onClick={onViewRewardPunish} style={{ fontSize: 12, color: '#faad14', borderColor: '#faad14', padding: '0 6px' }}>奖惩</Button>
            
            <Tooltip title="记忆管理 (导出/导入)">
              <Button 
                size="small" 
                icon={<DatabaseOutlined />} 
                onClick={onManageMemory}
                style={{ fontSize: 12, color: '#fff', borderColor: '#303030', backgroundColor: '#1f1f1f', padding: '0 6px' }}
              >
                记忆库
              </Button>
            </Tooltip>
            
            <div style={{ display: 'flex', alignItems: 'center', background: '#262626', borderRadius: 4, padding: '0 2px', gap: 2 }}>
              <Select
                size="small"
                value={analysisProvider}
                onChange={onAnalysisProviderChange}
                variant="borderless"
                style={{ width: 90, fontSize: 12 }}
                styles={{ popup: { root: { backgroundColor: '#1f1f1f' } } }}
              >
                {availableProviders && availableProviders.map(p => (
                  <Option key={p} value={p}><span style={{ color: '#ccc', fontSize: 12 }}>{p}</span></Option>
                ))}
              </Select>
              
              <Popover
                content={
                  <div style={{ width: 250, padding: '8px 0' }}>
                    <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>自定义 APIKEY (可选):</div>
                    <Input.Password
                      size="small"
                      placeholder="输入该模型的 APIKEY"
                      value={analysisApiKey}
                      onChange={(e) => onAnalysisApiKeyChange(e.target.value)}
                      style={{ backgroundColor: '#303030', color: '#fff', border: '1px solid #434343' }}
                    />
                    <div style={{ marginTop: 8, fontSize: 10, color: '#666' }}>
                      留空则使用系统默认配置
                    </div>
                  </div>
                }
                title={<span style={{ color: '#fff' }}>AI 分析设置</span>}
                trigger="click"
                overlayStyle={{ padding: 0 }}
                color="#1f1f1f"
              >
                <Button 
                  size="small" 
                  type="text" 
                  icon={<SettingOutlined style={{ fontSize: 12, color: analysisApiKey ? '#1890ff' : '#666' }} />} 
                  style={{ height: 22, width: 22, padding: 0 }}
                />
              </Popover>

              <Button 
                size="small" 
                type="primary" 
                icon={<RobotOutlined />} 
                onClick={onGenerateAnalysis} 
                loading={aiLoading} 
                style={{ fontSize: 12, height: 22, padding: '0 8px' }} 
                disabled={!quoteData}
              >
                AI分析
              </Button>
            </div>
          </div>
        </>
      ) : (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
            <div style={{ color: '#666', display: 'flex', alignItems: 'center', gap: 10 }}>
            {selectedStock ? (
                <>
                <Spin size="small" />
                <span>正在加载 {selectedStock}...</span>
                </>
            ) : (
                "请从左侧选择股票"
            )}
            </div>
            
            {/* 右侧操作区 - Copied here to be visible during loading */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, paddingLeft: 12, flexShrink: 0 }}>
                <Button size="small" icon={<ReloadOutlined />} onClick={onRefresh} style={{ fontSize: 12, padding: '0 6px' }} title="刷新" />
                <Button size="small" type="primary" danger ghost icon={<FireOutlined />} onClick={onDailyReview} style={{ fontSize: 12, padding: '0 8px' }}>复盘</Button>
                <Button size="small" onClick={onViewReview} style={{ fontSize: 12, padding: '0 8px' }}>结果</Button>
                <Button size="small" onClick={onViewPlans} style={{ fontSize: 12, color: '#1890ff', borderColor: '#1890ff', padding: '0 8px' }}>计划</Button>
                <Button size="small" onClick={onViewNav} style={{ fontSize: 12, color: '#722ed1', borderColor: '#722ed1', padding: '0 8px' }}>导航</Button>
                <Button size="small" onClick={onViewTrading} style={{ fontSize: 12, color: '#52c41a', borderColor: '#52c41a', padding: '0 8px' }}>交易</Button>
                <Button size="small" icon={<SafetyOutlined />} onClick={onViewRewardPunish} style={{ fontSize: 12, color: '#faad14', borderColor: '#faad14', padding: '0 8px' }}>奖惩</Button>
            </div>
        </div>
      )}
    </div>
  );
});

TopBar.displayName = 'TopBar';

export default TopBar;

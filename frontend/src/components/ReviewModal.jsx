import React from 'react';
import { Modal, Row, Col, Card, Statistic, Tag, Button, Spin, Empty, Typography } from 'antd';
import { FireOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';

const { Text } = Typography;

const ReviewModal = ({ 
  visible, 
  onCancel, 
  loading, 
  data, 
  onSelectStock,
  logs = [] // Add logs prop
}) => {
  const logContainerRef = React.useRef(null);

  // Auto scroll logs
  React.useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logs]);

  const renderContent = () => {
    if (loading) {
      const upCount = data?.up_count ?? data?.up ?? 0;
      const downCount = data?.down_count ?? data?.down ?? 0;
      const limitUpCount = data?.limit_up_count ?? data?.limit_up ?? 0;
      const limitDownCount = data?.limit_down_count ?? data?.limit_down ?? 0;
      const totalVolume = data?.total_volume ?? 0;
      const marketTemperature = data?.market_temperature ?? data?.temp ?? 0;
      const createdAt = data?.created_at ?? null;
      const summaryText = String(data?.summary ?? '');
      return (
        <div style={{ textAlign: 'center', padding: '50px' }}>
          <Spin size="large" tip="正在进行深度复盘与推演..." />
          {(summaryText || createdAt) && (
            <div style={{ marginTop: 12, color: '#aaa', fontSize: 12 }}>
              {createdAt && (
                <div style={{ marginBottom: 6 }}>
                  系统生成时间: {new Date(createdAt).toLocaleString()}
                </div>
              )}
              {summaryText && (
                <div style={{ maxWidth: 720, margin: '0 auto', lineHeight: 1.6 }}>
                  {summaryText}
                </div>
              )}
            </div>
          )}
          <Row gutter={[12, 12]} justify="start" style={{ marginTop: 18 }}>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>市场温度</span>} value={marketTemperature} suffix={<span style={{ fontSize: 12, marginLeft: 2 }}>°C</span>} valueStyle={{ color: marketTemperature > 50 ? '#ef5350' : '#26a69a', fontSize: 18, fontWeight: 'bold' }} />
              </Card>
            </Col>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>全市场成交额</span>} value={totalVolume} suffix={<span style={{ fontSize: 12, color: '#faad14', marginLeft: 2 }}>(亿)</span>} precision={0} valueStyle={{ color: '#faad14', fontSize: 18, fontWeight: 'bold' }} />
              </Card>
            </Col>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>涨停家数</span>} value={limitUpCount} valueStyle={{ color: '#ef5350', fontSize: 18, fontWeight: 'bold' }} prefix={<FireOutlined style={{ fontSize: 14 }} />} />
              </Card>
            </Col>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>跌停家数</span>} value={limitDownCount || 0} valueStyle={{ color: '#26a69a', fontSize: 18, fontWeight: 'bold' }} />
              </Card>
            </Col>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>上涨家数</span>} value={upCount} valueStyle={{ color: '#ef5350', fontSize: 18, fontWeight: 'bold' }} />
              </Card>
            </Col>
            <Col span={4}>
              <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
                <Statistic title={<span style={{ fontSize: 12, color: '#aaa' }}>下跌家数</span>} value={downCount} valueStyle={{ color: '#26a69a', fontSize: 18, fontWeight: 'bold' }} />
              </Card>
            </Col>
          </Row>
          <div
              ref={logContainerRef}
              style={{
                  marginTop: 30,
                  padding: '15px',
                  backgroundColor: '#000',
                  borderRadius: '6px',
                  textAlign: 'left',
                  maxHeight: '300px',
                  overflowY: 'auto',
                  border: '1px solid #333',
                  fontFamily: 'monospace',
                  fontSize: '12px',
                  scrollBehavior: 'smooth'
              }}
          >
              {logs.length > 0 ? (
                  logs.map((log, i) => (
                      <div
                        key={i}
                        style={{
                          color: log.includes('[ERROR]')
                            ? '#ff4d4f'
                            : (log.includes('[WARN]') || log.includes('[WARNING]'))
                              ? '#faad14'
                              : '#00ff00',
                          marginBottom: 4
                        }}
                      >
                          {log}
                      </div>
                  ))
              ) : (
                  <div style={{ color: '#666' }}>准备开始复盘...</div>
              )}
          </div>
        </div>
      );
    }

    if (!data) return <Empty description="暂无复盘数据" />;
    
    const upCount = data.up_count ?? data.up ?? 0;
    const downCount = data.down_count ?? data.down ?? 0;
    const limitUpCount = data.limit_up_count ?? data.limit_up ?? 0;
    const limitDownCount = data.limit_down_count ?? data.limit_down ?? 0;
    const totalVolume = data.total_volume ?? 0;
    const marketTemperature = data.market_temperature ?? data.temp ?? 0;
    const highestPlate = data.highest_plate ?? data.highestPlate ?? 0;
    const summary = data.summary ?? '';
    const target_plan = data.target_plan ?? null;
    const target_plans = data.target_plans ?? null;
    const holding_plans = data.holding_plans ?? null;
    const created_at = data.created_at ?? null;
    
    // Use target_plans if available, otherwise fallback to [target_plan]
    const displayPlans = target_plans && target_plans.length > 0 
        ? target_plans 
        : (target_plan ? [target_plan] : []);

    const limitDownVal = limitDownCount || 0;
    const mainThemeText = data.main_theme ?? data.mainTheme ?? '';
    const isGenerating = mainThemeText === '生成中' || String(summary).includes('后台生成中');
    const highestPlateDisplay = isGenerating ? '-' : highestPlate;
    const ladder = data.ladder ?? null;
    const ladderTiers = ladder?.tiers ?? null;
    const ladderStocks = Array.isArray(ladder?.stocks) ? ladder.stocks : [];
    const tierEntries = ladderTiers ? Object.entries(ladderTiers) : [];
    const turnoverTop = Array.isArray(data.turnover_top) ? data.turnover_top : [];
    const ladderOpps = Array.isArray(data.ladder_opportunities) ? data.ladder_opportunities : [];
    const debugCandidates = data.debug_candidates === true;

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
        {created_at && (
           <div style={{ textAlign: 'right', color: '#666', fontSize: 12 }}>
              系统生成时间: {new Date(created_at).toLocaleString()}
           </div>
        )}
        {/* 1. 市场核心指标 */}
        <Row gutter={[12, 12]} justify="start">
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>市场温度</span>} 
                value={marketTemperature} 
                suffix={<span style={{ fontSize: 12, marginLeft: 2 }}>°C</span>} 
                valueStyle={{ color: marketTemperature > 50 ? '#ef5350' : '#26a69a', fontSize: 18, fontWeight: 'bold' }} 
              />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>全市场成交额</span>} 
                value={totalVolume} 
                suffix={<span style={{ fontSize: 12, color: '#faad14', marginLeft: 2 }}>(亿)</span>} 
                precision={0} 
                valueStyle={{ color: '#faad14', fontSize: 18, fontWeight: 'bold' }} 
              />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>涨停家数</span>} 
                value={limitUpCount} 
                valueStyle={{ color: '#ef5350', fontSize: 18, fontWeight: 'bold' }} 
                prefix={<FireOutlined style={{ fontSize: 14 }} />} 
              />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>跌停家数</span>} 
                value={limitDownVal} 
                valueStyle={{ color: '#26a69a', fontSize: 18, fontWeight: 'bold' }} 
              />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>上涨家数</span>} 
                value={upCount} 
                valueStyle={{ color: '#ef5350', fontSize: 18, fontWeight: 'bold' }} 
              />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', height: '100%' }}>
              <Statistic 
                title={<span style={{ fontSize: 12, color: '#aaa' }}>下跌家数</span>} 
                value={downCount} 
                valueStyle={{ color: '#26a69a', fontSize: 18, fontWeight: 'bold' }} 
              />
            </Card>
          </Col>
        </Row>

        {/* 2. 复盘总结 */}
        <Card title="📈 顶级游资复盘笔记" size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030' }} headStyle={{ color: '#faad14' }}>
          <div style={{ marginBottom: 10, color: '#aaa', fontSize: 12 }}>
            连板天梯最高: <Text style={{ color: '#fff' }}>{highestPlateDisplay}</Text> 板
          </div>
          {!isGenerating && tierEntries.length > 0 && (
            <div style={{ marginBottom: 10, color: '#aaa', fontSize: 12, lineHeight: 1.6 }}>
              <div style={{ marginBottom: 6 }}>
                连板梯队: {tierEntries.slice(0, 12).map(([k, v]) => `${k}板×${v}`).join(' / ')}
              </div>
              {ladderStocks.length > 0 && (
                <div>
                  代表股: {ladderStocks.slice(0, 12).map((s) => `${s.ts_code}${s.name ? `(${s.name})` : ''}-${s.height}板`).join('，')}
                </div>
              )}
            </div>
          )}
          <div className="markdown-body" style={{ color: '#ccc', fontSize: 13, maxHeight: 300, overflowY: 'auto' }}>
             <ReactMarkdown>{summary}</ReactMarkdown>
          </div>
        </Card>

        {debugCandidates && !isGenerating && ladderOpps.length > 0 && (
          <Card
            title="🧩 梯队联动机会"
            size="small"
            style={{ backgroundColor: '#1f1f1f', borderColor: '#303030' }}
            headStyle={{ color: '#52c41a' }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {ladderOpps.slice(0, 12).map((x, idx) => (
                <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <Text strong style={{ color: '#fff', marginRight: 8 }}>{x.ts_code}</Text>
                    {x.name && <Text style={{ color: '#aaa', marginRight: 8 }}>{x.name}</Text>}
                    {x.industry && <Tag color="green">{x.industry}</Tag>}
                    <div style={{ color: '#aaa', fontSize: 12, marginTop: 4, wordBreak: 'break-word' }}>
                      {x.reason}
                    </div>
                  </div>
                  <Button type="primary" size="small" onClick={() => onSelectStock(x.ts_code)}>查看K线</Button>
                </div>
              ))}
            </div>
          </Card>
        )}

        {debugCandidates && turnoverTop.length > 0 && (
          <Card
            title="💰 成交额排行 Top"
            size="small"
            style={{ backgroundColor: '#1f1f1f', borderColor: '#303030' }}
            headStyle={{ color: '#faad14' }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {turnoverTop.slice(0, 20).map((x, idx) => {
                const amt = Number(x.turnover_amount || 0);
                return (
                  <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <Text strong style={{ color: '#fff', marginRight: 8 }}>{x.ts_code}</Text>
                      {x.name && <Text style={{ color: '#aaa', marginRight: 8 }}>{x.name}</Text>}
                      {x.industry && <Tag color="gold">{x.industry}</Tag>}
                      <Text style={{ color: '#faad14' }}>成交额: {amt.toFixed(1)} 亿</Text>
                    </div>
                    <Button size="small" onClick={() => onSelectStock(x.ts_code)}>查看K线</Button>
                  </div>
                );
              })}
            </div>
          </Card>
        )}

        {/* 3. 明日核心标的 (Multiple) */}
        {displayPlans.length > 0 ? (
          <Card 
            title={
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span style={{ color: '#ef5350' }}>⚔️ 明日核心博弈标的 ({displayPlans.length})</span>
                    <Tag color="red">AI 精选</Tag>
                </div>
            } 
            size="small" 
            style={{ backgroundColor: '#2a1215', borderColor: '#5c1c21' }}
            headStyle={{ borderBottom: '1px solid #5c1c21' }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                {displayPlans.map((plan, index) => (
                    <div key={index} style={{ borderBottom: index < displayPlans.length - 1 ? '1px solid #5c1c21' : 'none', paddingBottom: index < displayPlans.length - 1 ? 12 : 0 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <div>
                                <Text strong style={{ fontSize: 18, color: '#fff', marginRight: 10 }}>{plan.ts_code}</Text>
                                {plan.name && <Text style={{ color: '#aaa', marginRight: 10 }}>{plan.name}</Text>}
                                <Tag color="gold">{plan.strategy}</Tag>
                            </div>
                            <Button type="primary" size="small" onClick={() => {
                                onSelectStock(plan.ts_code);
                            }}>查看K线</Button>
                        </div>
                        
                        <div style={{ backgroundColor: '#00000033', padding: 10, borderRadius: 4, marginTop: 8 }}>
                            <Text style={{ color: '#ffccc7', whiteSpace: 'pre-wrap' }}>{plan.reason}</Text>
                        </div>
                        
                        <div style={{ display: 'flex', gap: 20, fontSize: 12, color: '#aaa', marginTop: 8 }}>
                            <span>建议仓位: <Text style={{ color: '#fff' }}>{plan.position_pct * 100}%</Text></span>
                            {plan.buy_price > 0 && <span>建议买入: <Text style={{ color: '#fff' }}>{plan.buy_price}</Text></span>}
                        </div>
                    </div>
                ))}
            </div>
          </Card>
        ) : (
          <Card size="small" style={{ backgroundColor: '#1f1f1f', borderColor: '#303030', textAlign: 'center', padding: '20px 0' }}>
            <Empty 
              image={Empty.PRESENTED_IMAGE_SIMPLE} 
              description={
                <div style={{ color: '#888' }}>
                  <p>当前市场环境分歧较大或炸板率过高，AI 建议空仓观望。</p>
                  <p style={{ fontSize: '12px' }}>(系统仅在市场温度 &gt; 40 且 炸板率 &lt; 40% 时生成进攻计划)</p>
                </div>
              } 
            />
          </Card>
        )}

        {/* 4. 持仓跟踪与计划 */}
        {holding_plans && holding_plans.length > 0 && (
          <Card 
            title={
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span style={{ color: '#1890ff' }}>🛡️ 持仓跟踪与计划</span>
                    <Tag color="blue">持仓管理</Tag>
                </div>
            } 
            size="small" 
            style={{ backgroundColor: '#111d2c', borderColor: '#15395b' }}
            headStyle={{ borderBottom: '1px solid #15395b' }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                {holding_plans.map((plan, idx) => (
                    <div key={idx} style={{ borderBottom: idx < holding_plans.length - 1 ? '1px solid #303030' : 'none', paddingBottom: 10 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 5 }}>
                            <div>
                                <Text strong style={{ fontSize: 16, color: '#fff', marginRight: 10 }}>{plan.name || plan.ts_code}</Text>
                                <Text style={{ color: '#aaa', fontSize: 12 }}>{plan.ts_code}</Text>
                            </div>
                            <Button type="link" size="small" onClick={() => onSelectStock(plan.ts_code)}>查看</Button>
                        </div>
                         <div style={{ backgroundColor: '#00000033', padding: 8, borderRadius: 4, marginBottom: 5 }}>
                            <Text style={{ color: '#bae7ff', fontSize: 13, whiteSpace: 'pre-wrap' }}>{plan.reason}</Text>
                        </div>
                        <div style={{ display: 'flex', gap: 15, fontSize: 12, color: '#888' }}>
                             {plan.buy_price > 0 && <span>补仓: <Text style={{ color: '#fff' }}>{plan.buy_price}</Text></span>}
                             {plan.take_profit > 0 && <span>止盈: <Text style={{ color: '#f5222d' }}>{plan.take_profit}</Text></span>}
                             {plan.stop_loss > 0 && <span>止损: <Text style={{ color: '#52c41a' }}>{plan.stop_loss}</Text></span>}
                        </div>
                    </div>
                ))}
            </div>
          </Card>
        )}
      </div>
    );
  };

  return (
    <Modal
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <FireOutlined style={{ color: '#ef5350' }} />
          <span>市场深度复盘与推演</span>
        </div>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={800}
      styles={{ body: { maxHeight: '70vh', overflowY: 'auto', padding: '20px' } }}
    >
      {renderContent()}
    </Modal>
  );
};

export default ReviewModal;

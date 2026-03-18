import React, { useRef, useEffect, memo } from 'react';
import { Modal, Radio, Spin, Button, Card, Row, Col, Space, Typography, Tag, Empty, Table, Progress, Badge, Popover, Divider } from 'antd';
import { ThunderboltOutlined, FireOutlined, BarChartOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import { formatAmount } from '../utils/format';

const { Text } = Typography;

const StockSelector = memo(({
    visible,
    onCancel,
    strategy,
    onStrategyChange,
    loading,
    logs,
    results,
    onSelectStock
}) => {
    const logContainerRef = useRef(null);

    // 自动滚动日志到末尾
    useEffect(() => {
        if (logContainerRef.current) {
            logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
        }
    }, [logs]);

    // 渲染基本面 5 步得分详情
    const renderFinaPopover = (fina_details) => {
        if (!fina_details || Object.keys(fina_details).length === 0) return "暂无基本面详情";

        const steps = [
            { key: 'step1_safety', label: '1. 财务安全' },
            { key: 'step2_profitability', label: '2. 盈利能力' },
            { key: 'step3_business', label: '3. 商业模式' },
            { key: 'step4_growth', label: '4. 成长潜力' },
            { key: 'step5_valuation', label: '5. 估值水平' }
        ];

        return (
            <div style={{ width: 300 }}>
                <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Text strong style={{ fontSize: 14 }}>5 步基本面筛选详情</Text>
                    <Tag color={(fina_details.total_score || 0) >= 80 ? 'success' : (fina_details.total_score || 0) >= 60 ? 'warning' : 'error'}>
                        总分: {(fina_details.total_score || 0).toFixed(1)}
                    </Tag>
                </div>
                <Divider style={{ margin: '8px 0', borderColor: '#303030' }} />
                {steps.map(step => {
                    const data = fina_details[step.key] || { score: 0, details: [] };
                    return (
                        <div key={step.key} style={{ marginBottom: 10 }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                                <Text size="small" style={{ color: '#aaa' }}>{step.label}</Text>
                                <Text strong style={{ color: data.score >= 60 ? '#52c41a' : '#faad14' }}>{data.score}分</Text>
                            </div>
                            {data.details && data.details.length > 0 && (
                                <div style={{ paddingLeft: 8, borderLeft: '2px solid #303030' }}>
                                    {data.details.map((detail, idx) => (
                                        <div key={idx} style={{ fontSize: 11, color: '#888' }}>• {detail}</div>
                                    ))}
                                </div>
                            )}
                        </div>
                    );
                })}
                <Divider style={{ margin: '8px 0', borderColor: '#303030' }} />
                <div style={{ color: '#aaa', fontSize: 12, fontStyle: 'italic' }}>
                    结论：{fina_details.conclusion}
                </div>
            </div>
        );
    };

    // 渲染板块分析详情 (Helper function)
    const renderSectorAnalysis = (sector, selectedStockCode) => {
        if (!sector || sector.error) return null;

        const { rising_wave_status, comparison, strategy: aiStrategy, industry } = sector;
        const isSectorActive = rising_wave_status?.is_sector_active;

        return (
            <div style={{ marginTop: 16, padding: '12px', backgroundColor: '#1a1a1a', borderRadius: '8px', border: '1px solid #303030' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <BarChartOutlined style={{ color: '#faad14' }} />
                        <Text strong style={{ color: '#fff' }}>板块横向对比分析 ({industry})</Text>
                    </div>
                    <Tag color={isSectorActive ? 'error' : 'default'} icon={isSectorActive ? <FireOutlined /> : null}>
                        {isSectorActive ? `板块主升浪共振 (池内${rising_wave_status.active_stock_count}只)` : '板块暂未形成合力'}
                    </Tag>
                </div>

                <Row gutter={[16, 16]}>
                    <Col span={24}>
                        <div style={{ backgroundColor: '#141414', padding: '10px', borderRadius: '4px', borderLeft: '3px solid #faad14' }}>
                            <div style={{ fontSize: 12, color: '#aaa', marginBottom: 4 }}>策略建议</div>
                            <div style={{ fontSize: 13, color: '#ddd', whiteSpace: 'pre-wrap' }}>{aiStrategy}</div>
                        </div>
                    </Col>

                    <Col span={24}>
                        <div style={{ fontSize: 12, color: '#aaa', marginBottom: 8 }}>板块核心标对比</div>
                        <Table
                            size="small"
                            dataSource={comparison?.stock_data || []}
                            pagination={false}
                            rowKey="ts_code"
                            columns={[
                                { title: '股票', dataIndex: 'name', key: 'name', width: 80, render: (text, record) => <Text style={{ color: record.ts_code === selectedStockCode ? '#faad14' : '#ccc', fontSize: 12 }}>{text}</Text> },
                                { title: '涨跌', dataIndex: 'pct_chg', key: 'pct_chg', width: 70, render: val => <Text style={{ color: val >= 0 ? '#ef5350' : '#26a69a', fontSize: 12 }}>{val > 0 ? '+' : ''}{val}%</Text> },
                                { title: '突破强度', dataIndex: 'breakout_strength', key: 'breakout_strength', width: 80, render: val => <Text style={{ color: val >= 0 ? '#ef5350' : '#26a69a', fontSize: 12 }}>{val > 0 ? '+' : ''}{val}%</Text> },
                                { title: '主力流入', dataIndex: 'net_mf_5d', key: 'net_mf_5d', width: 90, render: val => <Text style={{ color: val >= 0 ? '#ef5350' : '#26a69a', fontSize: 12 }}>{formatAmount(val * 10000)}</Text> },
                                { title: '大单占比', dataIndex: 'large_order_ratio', key: 'large_order_ratio', width: 80, render: val => <div style={{ width: 60 }}><Progress percent={val} size="small" showInfo={false} strokeColor={val > 30 ? '#ef5350' : '#1890ff'} /><Text style={{ fontSize: 10, color: '#888' }}>{val}%</Text></div> },
                                { title: 'PE折溢价', dataIndex: 'pe_premium', key: 'pe_premium', width: 80, render: val => <Tag color={val < 0 ? 'success' : 'warning'} style={{ fontSize: 10 }}>{val > 0 ? '+' : ''}{val}%</Tag> },
                                { title: '量价评分', dataIndex: 'vol_price_score', key: 'vol_price_score', width: 80, render: val => <Badge count={val} overflowCount={100} style={{ backgroundColor: val >= 80 ? '#ef5350' : '#52c41a', fontSize: 10 }} /> },
                            ]}
                            style={{ backgroundColor: 'transparent' }}
                            className="custom-table"
                            onRow={(record) => ({
                                onClick: (e) => {
                                    e.stopPropagation();
                                    onSelectStock(record);
                                },
                                style: { cursor: 'pointer' }
                            })}
                        />
                    </Col>
                </Row>
            </div>
        );
    };

    const renderContent = () => {
        if (loading) {
            // Limit logs to last 200 items to improve performance
            const displayLogs = logs.length > 200 ? logs.slice(-200) : logs;
            return (
                <div style={{ padding: '40px 0', textAlign: 'center' }}>
                    <Spin size="large" tip="正在通过 Tushare + AI + 实时搜索 进行多维筛选分析..." />
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
                        {displayLogs.length > 0 ? (
                            displayLogs.map((log, i) => (
                                <div key={i} style={{ color: log.includes('[ERROR]') ? '#ff4d4f' : log.includes('[WARN]') ? '#faad14' : '#00ff00', marginBottom: 4 }}>
                                    {log}
                                </div>
                            ))
                        ) : (
                            <div style={{ color: '#666' }}>准备初始化选股引擎...</div>
                        )}
                    </div>
                </div>
            );
        }

        const currentResult = results[strategy] || { data: [] };

        if (currentResult.data.length > 0) {
            return (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
                    <div style={{ backgroundColor: '#1f1f1f', padding: '12px', borderRadius: '8px', border: '1px solid #303030', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div style={{ flex: 1 }}>
                            <Text type="secondary">
                                {strategy === 'pullback'
                                    ? '策略逻辑：筛选阶段涨幅超12%或有涨停基因的强势股，且当前处于缩量回调至 MA20 支撑位区间，结合 AI 判断洗盘质量。'
                                    : '策略逻辑：基本面(PE/ROE) + 资金面(主力净流入) + 技术面(放量) 初选，AI + 实时搜索 进行题材及政策深度二次筛选。'}
                            </Text>
                        </div>
                        <div style={{ marginLeft: 20, textAlign: 'right', minWidth: 150 }}>
                            <Text type="secondary" style={{ fontSize: 12 }}>上次更新: {currentResult.timestamp ? new Date(currentResult.timestamp).toLocaleString() : '未知'}</Text>
                            <br />
                            <Button size="small" type="link" onClick={() => onStrategyChange(strategy, true)} style={{ padding: 0 }}>重新筛选</Button>
                        </div>
                    </div>
                    {currentResult.data.map((stock) => (
                        <Card
                            key={stock.ts_code}
                            hoverable
                            style={{ backgroundColor: '#141414', borderColor: '#303030' }}
                            onClick={() => onSelectStock(stock)}
                        >
                            <Row gutter={24} align="middle">
                                <Col span={6}>
                                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                                        <Text strong style={{ fontSize: 18, color: '#fff' }}>{stock.name}</Text>
                                        <Text type="secondary">{stock.ts_code}</Text>
                                        <Tag color="blue" style={{ marginTop: 8, width: 'fit-content' }}>{stock.industry}</Tag>
                                    </div>
                                </Col>
                                <Col span={6}>
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                                        <Text type="secondary" style={{ fontSize: 12 }}>主力净流入</Text>
                                        <Text style={{ color: (stock.metrics?.net_mf || 0) >= 0 ? '#ef5350' : '#26a69a', fontSize: 16 }}>
                                            {formatAmount((stock.metrics?.net_mf || 0) * 10000)}
                                        </Text>
                                        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                                            <Text type="secondary" style={{ fontSize: 12 }}>PE: {(stock.metrics?.pe || 0).toFixed(2)}</Text>
                                            <Popover content={renderFinaPopover(stock.metrics?.fina_details)} trigger="hover" placement="right">
                                                <Tag color={(stock.metrics?.fina_score || 0) >= 60 ? 'success' : (stock.metrics?.fina_score || 0) >= 40 ? 'warning' : 'error'} style={{ fontSize: 10, cursor: 'help' }}>
                                                    基本面: {(stock.metrics?.fina_score || 0).toFixed(1)}
                                                </Tag>
                                            </Popover>
                                            <Tag color="purple" style={{ fontSize: 10 }}>AI评分: {stock.score || 0}</Tag>
                                        </div>
                                    </div>
                                </Col>
                                <Col span={12}>
                                    <div style={{ borderLeft: '1px solid #303030', paddingLeft: 20 }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                            <Text strong style={{ color: '#26a69a' }}>AI 选股建议：</Text>
                                            <Text type="secondary" style={{ fontSize: 11 }}>
                                                分析时间: {currentResult.timestamp ? new Date(currentResult.timestamp).toLocaleString() : '未知'}
                                            </Text>
                                        </div>
                                        <div style={{ marginTop: 8, maxHeight: 100, overflow: 'hidden', position: 'relative' }}>
                                            <div style={{ color: '#aaa', fontSize: 13, lineHeight: '1.6' }}>
                                                <ReactMarkdown>{stock.analysis}</ReactMarkdown>
                                            </div>
                                            <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: 40, background: 'linear-gradient(transparent, #141414)' }}></div>
                                        </div>
                                    </div>
                                </Col>
                            </Row>
                            {/* 注入板块横向对比分析 */}
                            {renderSectorAnalysis(stock.sector_analysis, stock.ts_code)}
                        </Card>
                    ))}
                </div>
            );
        }

        return (
            <div style={{ padding: '60px 0' }}>
                <Empty
                    description={
                        <Space direction="vertical">
                            <Text type="secondary">当前策略下暂无保存的筛选结果</Text>
                            <Button type="primary" onClick={() => onStrategyChange(strategy, true)}>立即筛选</Button>
                        </Space>
                    }
                />
            </div>
        );
    };

    return (
        <Modal
            title={
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '95%' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <ThunderboltOutlined style={{ color: '#faad14' }} />
                        <span>智能量化选股</span>
                    </div>
                    <Radio.Group
                        value={strategy}
                        onChange={(e) => {
                            onStrategyChange(e.target.value, false);
                        }}
                        optionType="button"
                        buttonStyle="solid"
                        size="small"
                    >
                        <Radio.Button value="default">多维综合</Radio.Button>
                        <Radio.Button value="pullback">强势回调</Radio.Button>
                    </Radio.Group>
                </div>
            }
            open={visible}
            onCancel={onCancel}
            footer={null}
            width={1000}
            styles={{ body: { maxHeight: '70vh', overflowY: 'auto', padding: '20px' } }}
            forceRender={false} // Ensure it's not pre-rendered
        >
            <style>{`
        .custom-table .ant-table {
          background: transparent !important;
          color: #ccc !important;
        }
        .custom-table .ant-table-thead > tr > th {
          background: #1a1a1a !important;
          color: #888 !important;
          border-bottom: 1px solid #303030 !important;
          font-size: 11px;
        }
        .custom-table .ant-table-tbody > tr > td {
          border-bottom: 1px solid #262626 !important;
          .ant-empty-description { color: #666; }
        }
        .custom-table .ant-table-tbody > tr:hover > td {
          background: #262626 !important;
        }
      `}</style>
            {visible ? renderContent() : null}
        </Modal>
    );
});

StockSelector.displayName = 'StockSelector';

export default StockSelector;

import React, { memo } from 'react';
import { Layout, Input, List, Typography, Button, Spin, Avatar, AutoComplete } from 'antd';
import { SearchOutlined, RobotOutlined, MinusOutlined, ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons';
import DataStatusBar from './DataStatusBar';

const { Sider } = Layout;
const { Title } = Typography;

const SidePanel = memo(({
    searchOptions,
    onSearch,
    onSelectStock,
    allStocks,
    onOpenSelector,
    watchlist,
    loading,
    onRefresh,
    selectedStock,
    onStockClick,
    onRemoveFromWatchlist,
    syncStatus
}) => {
    return (
        <Sider width={170} style={{ borderRight: '1px solid #303030', height: '100vh', backgroundColor: '#141414' }}>
            <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                <div style={{ padding: '12px 8px', borderBottom: '1px solid #303030', flexShrink: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', marginBottom: 12 }}>
                        <Avatar size="small" icon={<RobotOutlined />} style={{ backgroundColor: '#26a69a', marginRight: 8 }} />
                        <div>
                            <Title level={5} style={{ margin: 0, lineHeight: 1, fontSize: 14 }}>AI Trader</Title>
                        </div>
                    </div>
                    <AutoComplete
                        style={{ width: '100%' }}
                        options={searchOptions}
                        onSearch={onSearch}
                        onSelect={onSelectStock}
                        backfill={true}
                        popupMatchSelectWidth={false}
                        listHeight={400}
                    >
                        <Input
                            placeholder={allStocks.length > 0 ? "代码/名称" : "加载中..."}
                            prefix={allStocks.length > 0 ? <SearchOutlined style={{ color: '#555' }} /> : <Spin size="small" />}
                            variant="borderless"
                            style={{ background: '#1f1f1f', borderRadius: 6, padding: '4px 8px' }}
                            disabled={allStocks.length === 0}
                        />
                    </AutoComplete>

                    <Button
                        type="primary"
                        block
                        icon={<ThunderboltOutlined />}
                        style={{ marginTop: 12, height: 32, borderRadius: 6, backgroundColor: '#faad14', borderColor: '#faad14' }}
                        onClick={onOpenSelector}
                    >
                        智能选股
                    </Button>
                </div>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
                    <div style={{ padding: '8px 8px', color: '#666', fontSize: 12, flexShrink: 0, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>自选股 ({watchlist.length})</span>
                        <Button
                            type="text"
                            size="small"
                            icon={<ReloadOutlined spin={loading} />}
                            onClick={onRefresh}
                            style={{ color: '#666' }}
                        />
                    </div>
                    <div style={{ flex: 1, overflowY: 'auto' }}>
                        <List
                            dataSource={watchlist}
                            renderItem={(item) => (
                                <List.Item
                                    onClick={() => onStockClick(item.ts_code)}
                                    style={{
                                        cursor: 'pointer',
                                        padding: '10px 8px',
                                        borderBottom: '1px solid #222',
                                        backgroundColor: selectedStock === item.ts_code ? '#26a69a22' : 'transparent',
                                        transition: 'all 0.2s',
                                        position: 'relative'
                                    }}
                                    className="stock-item-hover"
                                >
                                    <div style={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                                            <div style={{ color: selectedStock === item.ts_code ? '#26a69a' : '#fff', fontWeight: 500, fontSize: 14 }}>{item.name}</div>
                                            <div style={{
                                                color: item.price >= item.pre_close ? '#ef5350' : '#26a69a',
                                                fontWeight: 'bold',
                                                fontSize: 15
                                            }}>
                                                {item.price ? item.price.toFixed(2) : '--'}
                                            </div>
                                        </div>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                            <div style={{ color: '#666', fontSize: 12 }}>{item.ts_code}</div>
                                            <div style={{ display: 'flex', alignItems: 'center' }}>
                                                <span style={{ 
                                                    color: (item.pct_chg || 0) >= 0 ? '#ef5350' : '#26a69a',
                                                    fontSize: 12,
                                                    marginRight: 8,
                                                    fontWeight: 500
                                                }}>
                                                    {item.pct_chg !== undefined ? (Number(item.pct_chg) > 0 ? '+' : '') + Number(item.pct_chg).toFixed(2) + '%' : '--'}
                                                </span>
                                                {/* 删除按钮 - 鼠标悬停显示 */}
                                                <Button
                                                    type="text"
                                                    size="small"
                                                    icon={<MinusOutlined />}
                                                    onClick={(e) => onRemoveFromWatchlist(e, item.ts_code)}
                                                    style={{ color: '#666', padding: 0, height: 20, minWidth: 20 }}
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </List.Item>
                            )}
                        />
                    </div>
                </div>
                {/* 底部状态栏 */}
                <DataStatusBar syncStatus={syncStatus} />
            </div>
        </Sider>
    );
});

SidePanel.displayName = 'SidePanel';

export default SidePanel;

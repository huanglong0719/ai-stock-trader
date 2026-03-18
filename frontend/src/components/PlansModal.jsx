import React, { useState } from 'react';
import { Modal, Table, Tag, Button, Tooltip, Popconfirm, Form, Input, InputNumber, Space, message } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, LockOutlined, ReloadOutlined } from '@ant-design/icons';
import axios from 'axios';

const PlansModal = ({ visible, onCancel, data, loading, onRefresh, onPlanRemoved }) => {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [editingPlan, setEditingPlan] = useState(null);
    const [form] = Form.useForm();
    const [submitting, setSubmitting] = useState(false);

    const handleAdd = () => {
        setEditingPlan(null);
        form.resetFields();
        // Set default values
        form.setFieldsValue({
            position_pct: 0.1,
            strategy_name: '自定义策略',
            buy_price: 0,
            stop_loss: 0,
            take_profit: 0
        });
        setIsEditModalVisible(true);
    };

    const handleEdit = (record) => {
        setEditingPlan(record);
        // Map backend fields to form fields
        form.setFieldsValue({
            ts_code: record.ts_code,
            strategy_name: record.strategy_name,
            buy_price: record.buy_price_limit, // Note: backend field name mapping
            stop_loss: record.stop_loss_price,
            take_profit: record.take_profit_price,
            position_pct: 0.1, // API doesn't return this in list usually, default 0.1
            reason: record.reason
        });
        setIsEditModalVisible(true);
    };

    const handleCancel = async (id) => {
        try {
            await axios.post(`/api/trading/plan/${id}/cancel`, { reason: '手动撤单' });
            message.success('计划已撤销');
            if (onPlanRemoved) onPlanRemoved(id);
            if (onRefresh) onRefresh();
        } catch (err) {
            message.error('撤销失败: ' + (err.response?.data?.detail || err.message));
        }
    };

    const handleFinish = async (values) => {
        setSubmitting(true);
        try {
            // Map form fields to backend expected fields
            const payload = {
                ts_code: values.ts_code,
                strategy_name: values.strategy_name,
                buy_price: values.buy_price, // Maps to buy_price_limit
                stop_loss: values.stop_loss,
                take_profit: values.take_profit,
                position_pct: values.position_pct,
                reason: values.reason,
                source: 'user'
            };

            if (editingPlan) {
                await axios.put(`/api/trading/plan/${editingPlan.id}`, payload);
                message.success('计划已更新');
            } else {
                await axios.post('/api/trading/plan', payload);
                message.success('计划已创建');
            }
            setIsEditModalVisible(false);
            if (onRefresh) onRefresh();
        } catch (err) {
            message.error('操作失败: ' + (err.response?.data?.detail || err.message));
        } finally {
            setSubmitting(false);
        }
    };

    const columns = [
        { title: '代码', dataIndex: 'ts_code', key: 'ts_code', width: 100 },
        { title: '策略', dataIndex: 'strategy_name', key: 'strategy_name', width: 100 },
        { 
            title: '参考价', 
            dataIndex: 'decision_price', 
            key: 'decision_price', 
            width: 80,
            render: (val) => (val && val > 0 ? Number(val).toFixed(2) : '-')
        },
        { 
            title: '计划价', 
            dataIndex: 'plan_price', 
            key: 'plan_price', 
            width: 80,
            render: (val) => (val && val > 0 ? Number(val).toFixed(2) : '-')
        },
        {
            title: '状态',
            dataIndex: 'executed',
            key: 'executed',
            width: 80,
            render: (_, record) => {
                if ((record.track_status || '').toUpperCase() === 'CANCELLED') {
                    return <Tag color="error">已撤销</Tag>;
                }
                return record.executed ? <Tag color="success">已执行</Tag> : <Tag color="default">监控中</Tag>;
            }
        },
        { 
            title: '生成时间', 
            dataIndex: 'created_at', 
            key: 'created_at', 
            width: 150, 
            render: (val) => val ? new Date(val).toLocaleString() : '-' 
        },
        { 
            title: 'AI执行理由', 
            dataIndex: 'review_content', 
            key: 'review_content', 
            ellipsis: true,
            render: (val) => val ? (
                <Tooltip title={val}>
                    <span>{val}</span>
                </Tooltip>
            ) : '-'
        },
        {
            title: '操作',
            key: 'action',
            width: 100,
            render: (_, record) => (
                <Space size="small">
                    {record.source === 'system' ? (
                        <Tooltip title="系统自动生成的计划，不可编辑参数">
                            <Button type="text" icon={<LockOutlined />} disabled size="small" />
                        </Tooltip>
                    ) : (
                        <Button type="text" icon={<EditOutlined />} onClick={() => handleEdit(record)} size="small" />
                    )}
                    <Popconfirm title="确定撤销吗?" onConfirm={() => handleCancel(record.id)} okText="是" cancelText="否">
                         <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>
                </Space>
            )
        }
    ];

    return (
        <>
            <Modal
                title={
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginRight: 24 }}>
                        <span>今日交易计划监控</span>
                        <Space>
                            <Button icon={<ReloadOutlined />} onClick={onRefresh} loading={loading}>刷新</Button>
                            <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>添加计划</Button>
                        </Space>
                    </div>
                }
                open={visible}
                onCancel={onCancel}
                footer={null}
                width={1000}
            >
                <Table
                    dataSource={data}
                    loading={loading}
                    rowKey="id"
                    pagination={false}
                    size="small"
                    columns={columns}
                    scroll={{ x: 900 }}
                />
            </Modal>

            <Modal
                title={editingPlan ? "编辑计划" : "添加计划"}
                open={isEditModalVisible}
                onCancel={() => setIsEditModalVisible(false)}
                onOk={() => form.submit()}
                confirmLoading={submitting}
                destroyOnHidden
            >
                <Form form={form} layout="vertical" onFinish={handleFinish}>
                    <Form.Item name="ts_code" label="股票代码" rules={[{ required: true, message: '请输入代码' }]}>
                        <Input disabled={!!editingPlan} placeholder="例如: 000001.SZ" />
                    </Form.Item>
                    <Form.Item name="strategy_name" label="策略名称" rules={[{ required: true }]}>
                        <Input />
                    </Form.Item>
                    <div style={{ display: 'flex', gap: 16 }}>
                        <Form.Item name="buy_price" label="买入价格 (0为不限)" rules={[{ required: true }]} style={{ flex: 1 }}>
                            <InputNumber style={{ width: '100%' }} min={0} step={0.01} />
                        </Form.Item>
                        <Form.Item name="position_pct" label="仓位比例 (0-1)" rules={[{ required: true }]} style={{ flex: 1 }}>
                            <InputNumber style={{ width: '100%' }} min={0} max={1} step={0.1} />
                        </Form.Item>
                    </div>
                    <div style={{ display: 'flex', gap: 16 }}>
                        <Form.Item name="stop_loss" label="止损价格" rules={[{ required: true }]} style={{ flex: 1 }}>
                            <InputNumber style={{ width: '100%' }} min={0} step={0.01} />
                        </Form.Item>
                        <Form.Item name="take_profit" label="止盈价格" rules={[{ required: true }]} style={{ flex: 1 }}>
                            <InputNumber style={{ width: '100%' }} min={0} step={0.01} />
                        </Form.Item>
                    </div>
                    <Form.Item name="reason" label="选股理由">
                        <Input.TextArea rows={2} placeholder="输入选股理由，AI将根据此理由进行跟踪" />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};

export default PlansModal;

import React from 'react';
import { Modal, List, Card, Typography, Space, Divider, message } from 'antd';
import { 
  DatabaseOutlined, 
  ThunderboltOutlined, 
  RobotOutlined, 
  BarChartOutlined, 
  MonitorOutlined,
  CompassOutlined,
  FileTextOutlined
} from '@ant-design/icons';

const { Text, Paragraph } = Typography;

const modules = [
  {
    title: '数据采集层 (Data Layer)',
    icon: <DatabaseOutlined style={{ color: '#1890ff' }} />,
    description: '负责与 Tushare、通达信等外部接口对接，抓取 K 线、实时行情及基础数据。',
    files: [
      { name: 'tushare_client.py', path: 'backend/app/services/market/tushare_client.py', desc: 'Tushare 异步 API 客户端' },
      { name: 'market_data_service.py', path: 'backend/app/services/market/market_data_service.py', desc: '核心行情聚合与缓存服务' },
      { name: 'stock_models.py', path: 'backend/app/models/stock_models.py', desc: '数据库模型定义 (DailyBar, StockIndicator)' }
    ]
  },
  {
    title: '数据加工层 (Processing Layer)',
    icon: <ThunderboltOutlined style={{ color: '#faad14' }} />,
    description: '对原始行情进行清洗、复权，计算 MA、MACD、BIAS 等技术指标。',
    files: [
      { name: 'indicator_service.py', path: 'backend/app/services/indicator_service.py', desc: '指标计算核心逻辑' },
      { name: 'data_sync.py', path: 'backend/app/services/data_sync.py', desc: '全市场数据同步任务' },
      { name: 'scheduler.py', path: 'backend/app/services/scheduler.py', desc: '定时同步与维护任务调度' }
    ]
  },
  {
    title: '选股与分析层 (Analysis Layer)',
    icon: <BarChartOutlined style={{ color: '#52c41a' }} />,
    description: '执行趋势筛选、基本面过滤、复盘统计及连板梯队分析。',
    files: [
      { name: 'stock_selector.py', path: 'backend/app/services/stock_selector.py', desc: '选股池生成与筛选' },
      { name: 'review_service.py', path: 'backend/app/services/review_service.py', desc: '每日复盘与统计逻辑' },
      { name: 'analysis_service.py', path: 'backend/app/services/ai/analysis_service.py', desc: 'AI 分析调度与结果解析' }
    ]
  },
  {
    title: 'AI 核心引擎 (AI Engine)',
    icon: <RobotOutlined style={{ color: '#eb2f96' }} />,
    description: '处理 Prompt 构建、多模型切换及交易计划生成。',
    files: [
      { name: 'prompt_builder.py', path: 'backend/app/services/ai/prompt_builder.py', desc: '提示词构建与约束管理' },
      { name: 'chat_service.py', path: 'backend/app/services/ai/chat_service.py', desc: 'AI 上下文与对话服务' }
    ]
  },
  {
    title: '系统监控与基础 (System Infrastructure)',
    icon: <MonitorOutlined style={{ color: '#722ed1' }} />,
    description: '日志管理、健康检查、系统配置及错误追踪。',
    files: [
      { name: 'logger_config.py', path: 'backend/app/utils/logger_config.py', desc: '全局日志滚动与格式配置' },
      { name: 'monitor_service.py', path: 'backend/app/services/monitor_service.py', desc: '任务执行监控与日志记录' },
      { name: 'config.py', path: 'backend/app/core/config.py', desc: '系统环境变量与 API 配置' }
    ]
  }
];

const SystemNavigator = ({ visible, onClose }) => {
  const copyPath = (path) => {
    navigator.clipboard.writeText(path);
    message.success(`已复制路径: ${path}`);
  };

  return (
    <Modal
      title={
        <Space>
          <CompassOutlined />
          <span>系统架构导航 (System Navigator)</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      footer={null}
      width={900}
      styles={{ body: { padding: '24px', backgroundColor: '#f0f2f5' } }}
    >
      <div style={{ 
        backgroundColor: '#fff', 
        padding: '12px 16px', 
        borderRadius: '8px', 
        marginBottom: 24,
        border: '1px solid #d9d9d9',
        color: '#000'
      }}>
        <div style={{ fontWeight: 'bold', marginBottom: 4 }}>使用说明：</div>
        <div style={{ fontSize: '13px', lineHeight: '1.6' }}>
          1. <b>点击文件名</b>：自动复制该文件的相对路径到剪贴板。<br />
          2. <b>快速定位</b>：在 IDE 中按下 <kbd style={{ background: '#eee', padding: '2px 4px', borderRadius: '3px', border: '1px solid #ccc' }}>Ctrl + P</kbd>，粘贴路径并回车，即可直达代码。
        </div>
      </div>

      <List
        grid={{ gutter: 16, column: 1 }}
        dataSource={modules}
        renderItem={module => (
          <Card 
            title={
              <Space>
                {module.icon}
                <Text strong style={{ fontSize: 16 }}>{module.title}</Text>
              </Space>
            }
            style={{ marginBottom: 16, borderRadius: 8 }}
            size="small"
          >
            <Paragraph style={{ color: '#666', marginBottom: 12 }}>{module.description}</Paragraph>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {module.files.map(file => (
                <Card.Grid 
                  key={file.path} 
                  style={{ 
                    width: '33.33%', 
                    padding: '12px',
                    cursor: 'pointer',
                    transition: 'all 0.3s'
                  }}
                  className="nav-grid-item"
                  onClick={() => copyPath(file.path)}
                >
                  <Space direction="vertical" size={0}>
                    <Space>
                    <FileTextOutlined style={{ color: '#1890ff' }} />
                    <Text strong style={{ fontSize: 13 }}>{file.name}</Text>
                  </Space>
                    <Text type="secondary" style={{ fontSize: 11 }}>{file.desc}</Text>
                  </Space>
                </Card.Grid>
              ))}
            </div>
          </Card>
        )}
      />
      
      <Divider orientation="left" plain>
        <Text type="secondary" style={{ fontSize: 12 }}>快捷提示: 在 IDE 中按 Ctrl+P 输入路径即可直达代码</Text>
      </Divider>
    </Modal>
  );
};

export default SystemNavigator;

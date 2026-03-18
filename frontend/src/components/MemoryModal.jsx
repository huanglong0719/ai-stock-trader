import React, { useState } from 'react';
import { Modal, Button, Upload, Space, message, Typography, Radio } from 'antd';
import { UploadOutlined, DownloadOutlined, FileTextOutlined } from '@ant-design/icons';
import axios from 'axios';

const { Text, Paragraph } = Typography;

const MemoryModal = ({ visible, onCancel }) => {
  const [exportFormat, setExportFormat] = useState('json');
  const [loading, setLoading] = useState(false);
  const [importing, setImporting] = useState(false);

  const handleExport = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`/api/memory/export?format=${exportFormat}`, {
        responseType: 'blob',
      });
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      const filename = response.headers['content-disposition']?.split('filename=')[1] || `memory_export.${exportFormat}`;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      message.success('记忆数据导出成功');
    } catch (error) {
      message.error('导出失败: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const uploadProps = {
    name: 'file',
    action: '/api/memory/import',
    showUploadList: false,
    beforeUpload: (file) => {
      const isJsonOrCsv = file.type === 'application/json' || file.type === 'text/csv' || file.name.endsWith('.json') || file.name.endsWith('.csv');
      if (!isJsonOrCsv) {
        message.error('只能上传 JSON 或 CSV 文件!');
      }
      return isJsonOrCsv;
    },
    customRequest: async ({ file, onSuccess, onError }) => {
      setImporting(true);
      const formData = new FormData();
      formData.append('file', file);
      
      try {
        const response = await axios.post('/api/memory/import', formData, {
          headers: { 'Content-Type': 'multipart/form-data' },
        });
        
        if (response.data.status === 'success') {
          const { added, updated, skipped } = response.data.details;
          message.success(`导入成功: 新增 ${added}, 更新 ${updated}, 跳过 ${skipped}`);
          onSuccess(response.data);
        } else {
          throw new Error('Import status not success');
        }
      } catch (error) {
        message.error('导入失败: ' + (error.response?.data?.detail || error.message));
        onError(error);
      } finally {
        setImporting(false);
      }
    },
  };

  return (
    <Modal
      title={
        <Space>
          <FileTextOutlined />
          <span>记忆数据管理 (Memory Management)</span>
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={500}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
        
        {/* 导出区域 */}
        <div style={{ background: '#f5f5f5', padding: 16, borderRadius: 8 }}>
          <Text strong style={{ display: 'block', marginBottom: 12 }}>导出记忆 (Backup)</Text>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Space>
              <Text>格式:</Text>
              <Radio.Group value={exportFormat} onChange={e => setExportFormat(e.target.value)}>
                <Radio.Button value="json">JSON</Radio.Button>
                <Radio.Button value="csv">CSV</Radio.Button>
              </Radio.Group>
            </Space>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              onClick={handleExport} 
              loading={loading}
              block
            >
              导出当前记忆库
            </Button>
          </Space>
        </div>

        {/* 导入区域 */}
        <div style={{ background: '#f5f5f5', padding: 16, borderRadius: 8 }}>
          <Text strong style={{ display: 'block', marginBottom: 12 }}>导入记忆 (Restore/Merge)</Text>
          <Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 12 }}>
            支持 JSON/CSV 格式。导入时将自动合并数据，相同策略+条件+动作的记录将根据权重更新。
          </Paragraph>
          <Upload {...uploadProps}>
            <Button icon={<UploadOutlined />} loading={importing} block>
              选择文件并导入
            </Button>
          </Upload>
        </div>

      </div>
    </Modal>
  );
};

export default MemoryModal;

import React, { useState, useEffect, useRef } from 'react';
import { Input, Button, List, Avatar, Spin, Drawer, FloatButton, Select } from 'antd';
import { SendOutlined, RobotOutlined, UserOutlined, MessageOutlined, SettingOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';

const { Option } = Select;

// Extract MessageItem to prevent re-renders when input changes
const MessageItem = React.memo(({ item }) => (
  <div style={{ 
    display: 'flex', 
    justifyContent: item.role === 'user' ? 'flex-end' : 'flex-start',
    marginBottom: 20 
  }}>
    {item.role === 'assistant' && (
       <Avatar style={{ backgroundColor: '#1890ff', marginRight: 10 }} icon={<RobotOutlined />} />
    )}
    <div style={{ 
      maxWidth: '80%',
      padding: '10px 15px',
      borderRadius: '10px',
      backgroundColor: item.role === 'user' ? '#1890ff' : '#303030',
      color: '#fff',
      borderTopLeftRadius: item.role === 'assistant' ? 0 : 10,
      borderTopRightRadius: item.role === 'user' ? 0 : 10,
    }}>
      <div className="markdown-body" style={{ fontSize: 14 }}>
        <ReactMarkdown>{item.content}</ReactMarkdown>
      </div>
      <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.5)', marginTop: 5, textAlign: 'right' }}>
        {item.created_at}
      </div>
    </div>
    {item.role === 'user' && (
       <Avatar style={{ backgroundColor: '#87d068', marginLeft: 10 }} icon={<UserOutlined />} />
    )}
  </div>
));
MessageItem.displayName = 'MessageItem';

// Extract MessageList to prevent re-renders when input changes
const MessageList = React.memo(({ messages }) => (
  <List
    dataSource={messages}
    renderItem={(item) => <MessageItem item={item} />}
  />
));
MessageList.displayName = 'MessageList';

const AIChatWindow = ({ 
  availableProviders, 
  chatProvider, 
  onChatProviderChange,
  chatApiKey,
  onChatApiKeyChange
}) => {
  const [visible, setVisible] = useState(false);
  const [messages, setMessages] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    if (visible) {
      fetchHistory();
    }
  }, [visible]);

  useEffect(() => {
    scrollToBottom();
  }, [messages, visible]);

  const fetchHistory = async () => {
    try {
      const res = await fetch('/api/chat/history');
      const data = await res.json();
      if (Array.isArray(data)) {
        setMessages(data);
      }
    } catch (error) {
      console.error("Failed to fetch chat history:", error);
    }
  };

  const handleSend = async () => {
    if (!inputValue.trim()) return;

    const userMsg = { role: 'user', content: inputValue, created_at: new Date().toLocaleString() };
    setMessages(prev => [...prev, userMsg]);
    setInputValue('');
    setLoading(true);

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 80000);
      const res = await fetch('/api/chat/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({ 
          content: userMsg.content,
          preferred_provider: chatProvider,
          api_key: chatApiKey
        })
      });
      clearTimeout(timeoutId);
      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(errorText || '请求失败');
      }
      const data = await res.json();
      if (data && data.content) {
        setMessages(prev => [...prev, data]);
      } else if (data && data.detail) {
        setMessages(prev => [...prev, { role: 'assistant', content: String(data.detail) }]);
      } else {
        setMessages(prev => [...prev, { role: 'assistant', content: "AI 返回内容为空，请重试。" }]);
      }
    } catch (error) {
      const isTimeout = error && error.name === 'AbortError';
      const msg = isTimeout ? "AI 请求超时，请稍后重试。" : "网络错误，请重试。";
      console.error("Failed to send message:", error);
      setMessages(prev => [...prev, { role: 'assistant', content: msg }]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      <FloatButton 
        icon={<MessageOutlined />} 
        type="primary" 
        style={{ right: 24, bottom: 80, width: 50, height: 50 }} 
        onClick={() => setVisible(true)}
        tooltip="与 AI 交易员沟通"
      />
      
      <Drawer
        title={
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <Avatar style={{ backgroundColor: '#1890ff' }} icon={<RobotOutlined />} />
              <div>
                <div style={{ fontSize: 16, fontWeight: 'bold' }}>AI 交易员</div>
                <div style={{ fontSize: 12, color: '#999', fontWeight: 'normal' }}>您的专属基金经理</div>
              </div>
            </div>
            <Button 
              type="text" 
              icon={<SettingOutlined style={{ color: showSettings ? '#1890ff' : '#666' }} />} 
              onClick={() => setShowSettings(!showSettings)}
            />
          </div>
        }
        placement="right"
        width={500}
        onClose={() => setVisible(false)}
        open={visible}
        styles={{
          body: { padding: 0, display: 'flex', flexDirection: 'column', backgroundColor: '#1f1f1f' },
          header: { backgroundColor: '#141414', borderBottom: '1px solid #303030', color: '#fff' }
        }}
      >
        {showSettings && (
          <div style={{ padding: '12px 20px', backgroundColor: '#262626', borderBottom: '1px solid #303030' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <span style={{ color: '#aaa', fontSize: 12 }}>切换 AI 模型:</span>
              <Select
                size="small"
                value={chatProvider}
                onChange={onChatProviderChange}
                style={{ width: 150 }}
                styles={{ popup: { root: { backgroundColor: '#1f1f1f' } } }}
              >
                {availableProviders && availableProviders.map(p => (
                  <Option key={p} value={p}><span style={{ color: '#ccc', fontSize: 12 }}>{p}</span></Option>
                ))}
              </Select>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span style={{ color: '#aaa', fontSize: 12 }}>自定义 APIKEY:</span>
              <Input.Password
                size="small"
                placeholder="留空使用系统默认"
                value={chatApiKey}
                onChange={e => onChatApiKeyChange(e.target.value)}
                style={{ width: 200, backgroundColor: '#303030', color: '#fff', border: '1px solid #434343' }}
              />
            </div>
          </div>
        )}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px' }}>
          <MessageList messages={messages} />
          {loading && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, color: '#999', marginLeft: 10 }}>
               <Spin size="small" /> AI 正在思考...
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>

        <div style={{ 
          padding: '15px', 
          backgroundColor: '#141414', 
          borderTop: '1px solid #303030',
          display: 'flex',
          gap: 10
        }}>
          <Input.TextArea 
            value={inputValue}
            onChange={e => setInputValue(e.target.value)}
            onPressEnter={(e) => {
              if (!e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            placeholder="输入消息与 AI 沟通..."
            autoSize={{ minRows: 1, maxRows: 4 }}
            style={{ backgroundColor: '#303030', color: '#fff', border: 'none' }}
          />
          <Button 
            type="primary" 
            icon={<SendOutlined />} 
            onClick={handleSend}
            loading={loading}
          />
        </div>
      </Drawer>
    </>
  );
};

AIChatWindow.displayName = 'AIChatWindow';

export default AIChatWindow;

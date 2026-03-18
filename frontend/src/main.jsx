import React from 'react'
import ReactDOM from 'react-dom/client'
import { ConfigProvider, theme } from 'antd'
import axios from 'axios'
import App from './App.jsx'
import ErrorBoundary from './components/ErrorBoundary.jsx'
import './index.css'

// 解决 localhost 和 127.0.0.1 数据不互通的问题
// 强制重定向到 localhost，确保 localStorage 数据一致
if (window.location.hostname === '127.0.0.1') {
  const url = new URL(window.location.href);
  url.hostname = 'localhost';
  window.location.href = url.href;
}

// 彻底禁用浏览器滚动恢复，强制每次刷新都在顶部
try {
  if ('scrollRestoration' in window.history) {
    window.history.scrollRestoration = 'manual';
  }
} catch (e) { void e; }

const configuredApiBase = import.meta.env?.VITE_API_BASE_URL;
const inferredApiBase =
  window.location.protocol === 'file:'
    ? 'http://localhost:8000'
    : '';

axios.defaults.baseURL = configuredApiBase || inferredApiBase;

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ConfigProvider
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: '#26a69a',
          colorBgContainer: '#141414',
          colorBgLayout: '#000000',
        },
      }}
    >
      <ErrorBoundary>
        <App />
      </ErrorBoundary>
    </ConfigProvider>
  </React.StrictMode>,
)

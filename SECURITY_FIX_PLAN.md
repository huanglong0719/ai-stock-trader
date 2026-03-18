# 🔒 安全修复行动计划

**创建日期**: 2026-01-09  
**优先级**: P0 (立即执行)  
**预计完成时间**: 24 小时内

---

## 📋 修复清单

### ✅ 任务 1: 密钥安全处理

#### 1.1 立即撤销暴露的密钥
- [ ] Tushare Token: `46af14e3...`
- [ ] DeepSeek API Key: `sk-41ea4ec9...`
- [ ] MiMo API Key: `sk-cprgktp3...`
- [ ] Serper Search Key: `fcedb198...`

#### 1.2 重新生成密钥
访问以下平台重新生成：
1. Tushare: https://tushare.pro/user/token
2. DeepSeek: https://platform.deepseek.com/api_keys
3. MiMo: https://xiaomimimo.com/dashboard
4. Serper: https://serper.dev/api-key

#### 1.3 创建 .env.example 模板
```bash
# .env.example
TUSHARE_TOKEN=your_tushare_token_here
DEEPSEEK_API_KEY=your_deepseek_key_here
DEEPSEEK_BASE_URL=https://api.deepseek.com

MIMO_API_KEY=your_mimo_key_here
MIMO_BASE_URL=https://api.xiaomimimo.com/v1
MIMO_MODEL=mimo-v2-flash

SEARCH_API_KEY=your_search_key_here
SEARCH_ENGINE=serper

ENABLE_AUTO_TRADE=true
```

#### 1.4 更新 .gitignore
```bash
# 确保以下内容在 .gitignore 中
.env
.env.local
.env.*.local
*.db
*.db-shm
*.db-wal
__pycache__/
*.pyc
node_modules/
dist/
```

#### 1.5 清理 Git 历史（如果已提交）
```bash
# 使用 BFG Repo-Cleaner 清理敏感文件
git clone --mirror git://example.com/repo.git
java -jar bfg.jar --delete-files .env repo.git
cd repo.git
git reflog expire --expire=now --all && git gc --prune=now --aggressive
git push
```

---

### ✅ 任务 2: API 速率限制

#### 2.1 当前落地方案（已实现）

**目标**：优先解决“429 限流 + 连接不稳定 + 并发过高导致线程池耗尽”三类风险，确保服务可用性优先于吞吐量。

**已落地策略**：
1. **服务层并发控制**：`chat_service` / `analysis_service` / `review_service` 使用 `asyncio.Semaphore(3)` 限制并发 AI 请求。
2. **模型层并发门**：`ai_client` 内部对不同模型分别使用 `threading.Semaphore(2)` 控制并发（MiMo / DeepSeek 各 2）。
3. **失败自动降级**：优先 MiMo，遇到 `RateLimitError`（429）或其他异常时自动尝试 DeepSeek。
4. **网络兼容性**：HTTP 客户端默认禁用 HTTP/2，降低部分网络环境下的连接失败概率。

#### 2.2 备选增强方案（待评估）

如需要更强的“频率维度”控制（按分钟/小时次数），可在后续引入统一 `RateLimiter` 装饰器（需配合业务指标与成本监控设计），避免在高峰期被动触发上游限流。

#### 2.3 添加成本监控
```python
# backend/app/core/cost_monitor.py
import json
from datetime import datetime
from pathlib import Path

class CostMonitor:
    def __init__(self, log_file="logs/api_costs.jsonl"):
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(exist_ok=True)
    
    def log_call(self, service: str, model: str, tokens: int = 0):
        """记录 API 调用"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "service": service,
            "model": model,
            "tokens": tokens,
            "estimated_cost": self._estimate_cost(service, model, tokens)
        }
        
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(record) + '\n')
    
    def _estimate_cost(self, service: str, model: str, tokens: int) -> float:
        """估算成本（根据实际定价调整）"""
        pricing = {
            "deepseek": 0.0001,  # 每 1K tokens
            "mimo": 0.0002,
        }
        return (tokens / 1000) * pricing.get(service, 0)
    
    def get_daily_cost(self) -> float:
        """获取今日成本"""
        today = datetime.now().date().isoformat()
        total = 0.0
        
        if not self.log_file.exists():
            return 0.0
        
        with open(self.log_file, 'r') as f:
            for line in f:
                record = json.loads(line)
                if record['timestamp'].startswith(today):
                    total += record['estimated_cost']
        
        return total

cost_monitor = CostMonitor()
```

---

### ✅ 任务 3: 交易风控增强

#### 3.1 创建交易前置检查器
```python
# backend/app/services/validators/trade_validator.py
from typing import Dict, List, Optional
from datetime import datetime, time as dt_time

class TradeValidator:
    """交易前置风控检查"""
    
    @staticmethod
    def validate_trading_time() -> Dict[str, any]:
        """检查是否在交易时间"""
        now = datetime.now()
        current_time = now.time()
        
        # 交易时间: 9:30-11:30, 13:00-15:00
        morning_start = dt_time(9, 30)
        morning_end = dt_time(11, 30)
        afternoon_start = dt_time(13, 0)
        afternoon_end = dt_time(15, 0)
        
        is_trading = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        return {
            "valid": is_trading,
            "reason": "" if is_trading else "Not in trading hours"
        }
    
    @staticmethod
    def validate_position_limit(
        current_positions: int,
        max_positions: int = 10
    ) -> Dict[str, any]:
        """检查持仓数量限制"""
        valid = current_positions < max_positions
        return {
            "valid": valid,
            "reason": "" if valid else f"Max {max_positions} positions allowed"
        }
    
    @staticmethod
    def validate_single_position_size(
        position_value: float,
        total_assets: float,
        max_pct: float = 0.3
    ) -> Dict[str, any]:
        """检查单只股票仓位限制"""
        pct = position_value / total_assets if total_assets > 0 else 0
        valid = pct <= max_pct
        return {
            "valid": valid,
            "reason": "" if valid else f"Single position exceeds {max_pct*100}%"
        }
    
    @staticmethod
    def validate_price_deviation(
        current_price: float,
        target_price: float,
        max_deviation: float = 0.02
    ) -> Dict[str, any]:
        """检查价格偏离度"""
        deviation = abs(current_price - target_price) / target_price
        valid = deviation <= max_deviation
        return {
            "valid": valid,
            "reason": "" if valid else f"Price deviation {deviation*100:.1f}% > {max_deviation*100}%"
        }
    
    @classmethod
    def validate_buy(
        cls,
        ts_code: str,
        price: float,
        volume: int,
        account_info: Dict,
        current_positions: List
    ) -> Dict[str, any]:
        """综合买入检查"""
        checks = []
        
        # 1. 交易时间检查
        checks.append(cls.validate_trading_time())
        
        # 2. 持仓数量检查
        checks.append(cls.validate_position_limit(len(current_positions)))
        
        # 3. 单只仓位检查
        position_value = price * volume
        checks.append(cls.validate_single_position_size(
            position_value,
            account_info.get('total_assets', 0)
        ))
        
        # 4. 资金充足检查
        available_cash = account_info.get('available_cash', 0)
        need_cash = position_value * 1.001  # 含手续费
        checks.append({
            "valid": available_cash >= need_cash,
            "reason": "" if available_cash >= need_cash else "Insufficient funds"
        })
        
        # 汇总结果
        all_valid = all(c['valid'] for c in checks)
        reasons = [c['reason'] for c in checks if not c['valid']]
        
        return {
            "valid": all_valid,
            "reasons": reasons,
            "checks": checks
        }

trade_validator = TradeValidator()
```

#### 3.2 集成到交易服务
```python
# backend/app/services/trading_service.py
from app.services.validators.trade_validator import trade_validator

async def execute_buy(self, db: Session, plan: TradingPlan, suggested_price: float, volume: int = None) -> bool:
    try:
        # 前置风控检查
        account = await self._get_or_create_account(db)
        positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
        
        validation = trade_validator.validate_buy(
            ts_code=plan.ts_code,
            price=suggested_price,
            volume=volume or 100,
            account_info={
                "total_assets": account.total_assets,
                "available_cash": account.available_cash
            },
            current_positions=positions
        )
        
        if not validation['valid']:
            logger.warning(f"Buy validation failed for {plan.ts_code}: {validation['reasons']}")
            return False
        
        # 原有买入逻辑...
        ...
```

---

### ✅ 任务 4: WebSocket 安全加固

#### 4.1 强制 WSS 协议（生产环境）
```python
# backend/app/main.py
from fastapi import WebSocket, WebSocketDisconnect, HTTPException, Header
import secrets

# WebSocket 认证 Token 管理
ws_tokens = set()

@app.get("/api/ws/token")
async def get_ws_token():
    """获取 WebSocket 连接令牌"""
    token = secrets.token_urlsafe(32)
    ws_tokens.add(token)
    return {"token": token, "expires_in": 3600}

@app.websocket("/ws/quote/{symbol}")
async def websocket_endpoint(
    websocket: WebSocket,
    symbol: str,
    token: str = None
):
    # 验证 Token
    if token not in ws_tokens:
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    await manager.connect(websocket, symbol)
    # ... 原有逻辑
```

#### 4.2 前端适配
```javascript
// frontend/src/App.jsx
const connectWebSocket = async (symbol) => {
  // 1. 获取 Token
  const tokenRes = await axios.get('/api/ws/token');
  const token = tokenRes.data.token;
  
  // 2. 建立连接
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/quote/${symbol}?token=${token}`;
  
  const ws = new WebSocket(wsUrl);
  // ... 原有逻辑
};
```

---

## 📊 验证清单

完成修复后，请逐项验证：

- [ ] 所有旧密钥已撤销
- [ ] 新密钥已配置且正常工作
- [ ] `.env` 文件已添加到 `.gitignore`
- [ ] Git 历史中的敏感信息已清理
- [ ] AI API 调用受速率限制保护
- [ ] 成本监控日志正常记录
- [ ] 交易前置检查正常工作
- [ ] WebSocket 连接需要有效 Token
- [ ] 生产环境强制使用 HTTPS/WSS

---

## 🚀 部署步骤

1. **备份数据库**
```bash
cp backend/aitrader.db backend/aitrader.db.backup
```

2. **更新代码**
```bash
git pull origin main
```

3. **安装依赖**
```bash
cd backend && pip install -r requirements.txt
cd ../frontend && npm install
```

4. **配置新密钥**
```bash
cp backend/.env.example backend/.env
# 编辑 .env 填入新密钥
```

5. **重启服务**
```bash
# 后端
cd backend && uvicorn app.main:app --reload

# 前端
cd frontend && npm run dev
```

6. **验证功能**
- 访问 http://localhost:5173
- 测试选股功能
- 测试 AI 分析
- 测试实时行情

---

**负责人**: 开发团队  
**审核人**: 安全负责人  
**完成期限**: 2026-01-10 23:59

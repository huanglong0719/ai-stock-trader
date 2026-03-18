import time
import threading
from typing import Any, Optional
from openai import OpenAI
from openai import RateLimitError, AuthenticationError
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from app.core.config import settings
from app.services.logger import logger

class AIClient:
    def __init__(self):
        self._mimo_client = None
        self._nim_client = None
        self._ds_client = None
        self._mimo_gate = threading.Semaphore(3)
        self._nim_gate = threading.Semaphore(3)
        self._ds_gate = threading.Semaphore(3)
        self._provider_cooldowns = {} # {provider_name: cooldown_until_timestamp}
        self._initialized = False
        self._init_lock = threading.Lock()

    def _build_http_client(self) -> httpx.Client:
        return httpx.Client(
            http2=False,
            timeout=httpx.Timeout(90.0, connect=15.0, read=90.0),
            headers={"Connection": "close"},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=0),
        )

    def _normalize_base_url(self, base_url: str) -> str:
        s = str(base_url or "").strip()
        if not s:
            return s
        s = s.rstrip("/")
        if not s.endswith("/v1"):
            s = f"{s}/v1"
        return s

    def _ensure_initialized(self):
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self._init_clients()
            self._initialized = True

    def _init_clients(self):
        # 1. 初始化小米 MiMo
        if settings.MIMO_API_KEY:
            try:
                self._mimo_client = OpenAI(
                    api_key=settings.MIMO_API_KEY,
                    base_url=self._normalize_base_url(settings.MIMO_BASE_URL),
                    http_client=self._build_http_client(),
                    max_retries=0,
                    timeout=90.0
                )
                logger.info("AI Client: Xiaomi MiMo initialized.")
            except Exception as e:
                logger.error(f"AI Client: Failed to initialize MiMo: {e}")
            
        if settings.NVIDIA_NIM_API_KEY:
            try:
                self._nim_client = OpenAI(
                    api_key=settings.NVIDIA_NIM_API_KEY,
                    base_url=self._normalize_base_url(settings.NVIDIA_NIM_BASE_URL),
                    http_client=self._build_http_client(),
                    max_retries=0,
                    timeout=90.0
                )
                logger.info("AI Client: NVIDIA NIM initialized.")
            except Exception as e:
                logger.error(f"AI Client: Failed to initialize NVIDIA NIM: {e}")

        # 3. 初始化 DeepSeek
        if settings.DEEPSEEK_API_KEY:
            try:
                self._ds_client = OpenAI(
                    api_key=settings.DEEPSEEK_API_KEY,
                    base_url=self._normalize_base_url(settings.DEEPSEEK_BASE_URL),
                    http_client=self._build_http_client(),
                    max_retries=0,
                    timeout=90.0
                )
                logger.info("AI Client: DeepSeek initialized.")
            except Exception as e:
                logger.error(f"AI Client: Failed to initialize DeepSeek: {e}")

    @property
    def mimo_client(self):
        self._ensure_initialized()
        return self._mimo_client

    @property
    def nim_client(self):
        self._ensure_initialized()
        return self._nim_client

    @property
    def ds_client(self):
        self._ensure_initialized()
        return self._ds_client

    def _get_gate(self, client: Any) -> threading.Semaphore:
        if client is self._mimo_client:
            return self._mimo_gate
        if client is self._nim_client:
            return self._nim_gate
        return self._ds_gate

    def _get_provider_name(self, client: Any) -> str:
        if client is self._mimo_client:
            return "Xiaomi MiMo"
        if client is self._nim_client:
            return "NVIDIA NIM"
        return "DeepSeek"

    def _get_provider_base_url(self, client: Any) -> str:
        base_url_raw = getattr(client, "base_url", "") if client else ""
        return self._normalize_base_url(str(base_url_raw))

    def _get_provider_api_key(self, client: Any) -> str:
        if client is self._mimo_client:
            return settings.MIMO_API_KEY
        if client is self._nim_client:
            return settings.NVIDIA_NIM_API_KEY
        return settings.DEEPSEEK_API_KEY

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(lambda e: not isinstance(e, (RateLimitError, AuthenticationError))),
        before_sleep=lambda retry_state: logger.info(f"AI API 调用失败，正在重试... (第 {retry_state.attempt_number} 次)"),
        reraise=True
    )
    def call_ai_api(self, client: Any, model: str, prompt: str, system_prompt: Optional[str] = None, api_key: Optional[str] = None) -> str:
        """
        带重试机制的 AI 调用
        """
        if not system_prompt:
            system_prompt = "你是一个专业的 A 股量化交易助手，擅长结合技术面和实时资讯给出精准分析；基本面只作为退市风险过滤参考。你的回复应当逻辑严密、专业客观，严禁出现任何乱码、字符重复、复读或无意义的符号串。若用户/系统上下文已提供行情数据（含日/周/月/分钟K线或实时行情），严禁声称该数据缺失、不可用或被禁用，必须直接引用并基于其分析；仅当上下文确实未提供该维度数据时，才允许说明“本次上下文未提供该维度精确数据”。请直接输出 analysis 内容，不要带有任何开场白。"
            
        base_url = self._get_provider_base_url(client)
        
        # 如果提供了自定义 APIKEY，则使用临时客户端
        active_api_key = self._get_provider_api_key(client)
        if api_key and api_key.strip():
            active_api_key = api_key.strip()
            logger.info(f"Using custom APIKEY for model {model}")

        prompt_len = len(prompt)
        sys_len = len(system_prompt) if system_prompt else 0
        logger.info(f"Calling AI model: {model} (base_url: {base_url}, timeout: 90s, prompt_len={prompt_len}, sys_len={sys_len})...")
        start_time = time.time()

        gate = self._get_gate(client)
        acquired = gate.acquire(timeout=30)
        if not acquired:
            logger.error(f"AI并发拥堵：等待 {model} 令牌超时 (30s)")
            raise TimeoutError(f"AI并发拥堵：等待 {model} 令牌超时")

        http_client: httpx.Client | None = None
        try:
            http_client = self._build_http_client()
            active_client = OpenAI(
                api_key=active_api_key,
                base_url=base_url,
                http_client=http_client,
                max_retries=0,
                timeout=90.0
            )

            response = active_client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                stream=False,
                temperature=0.7,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                max_tokens=4000, # 增加 max_tokens 以防长报告被截断 (1500 -> 4000)
                timeout=120.0 # 增加超时到 120s
            )
            duration = time.time() - start_time
            logger.info(f"AI response received in {duration:.2f}s")
            return response.choices[0].message.content or ""
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as e:
            logger.warning(f"AI API 连接/超时错误: {type(e).__name__}: {e} (model={model})")
            raise e
        except RateLimitError as e:
            # 记录冷却时间，30秒内不再请求该提供商
            provider_name = self._get_provider_name(client)
            
            self._provider_cooldowns[provider_name] = time.time() + 30
            logger.error(f"AI API rate limited: {type(e).__name__}: {e} (model={model}, provider={provider_name}). Cooldown for 30s.")
            raise e
        except Exception as e:
            logger.error(f"AI API 调用异常: {type(e).__name__}: {e} (model={model}, base_url={base_url})")
            raise e
        finally:
            try:
                gate.release()
            except Exception:
                pass
            try:
                if http_client is not None:
                    http_client.close()
            except Exception:
                pass

    def get_available_providers(self) -> list[str]:
        """返回当前已配置的 AI 提供商列表"""
        providers = []
        if self.mimo_client:
            providers.append("Xiaomi MiMo")
        if self.ds_client:
            providers.append("DeepSeek")
        if self.nim_client:
            providers.append("NVIDIA NIM")
        return providers

    def call_ai_best_effort(self, prompt: str, system_prompt: Optional[str] = None, preferred_provider: Optional[str] = None, api_key: Optional[str] = None) -> str:
        all_candidates = []
        if self.mimo_client:
            all_candidates.append((self.mimo_client, settings.MIMO_MODEL, "Xiaomi MiMo"))
        if self.ds_client:
            all_candidates.append((self.ds_client, "deepseek-chat", "DeepSeek"))
        if self.nim_client:
            all_candidates.append((self.nim_client, settings.NVIDIA_NIM_MODEL, "NVIDIA NIM"))

        # 如果指定了首选提供商，将其移到最前面
        clients_to_try = []
        if preferred_provider:
            match = next((c for c in all_candidates if c[2] == preferred_provider), None)
            if match:
                clients_to_try.append(match)
                # 剩余的按原顺序排列
                for c in all_candidates:
                    if c[2] != preferred_provider:
                        clients_to_try.append(c)
        
        # 如果没有指定或者指定的不存在，使用默认顺序
        if not clients_to_try:
            clients_to_try = list(all_candidates)

        last_err: Optional[Exception] = None
        now = time.time()
        for client, model, p_name in clients_to_try:
            # 检查冷却时间
            if p_name in self._provider_cooldowns:
                if now < self._provider_cooldowns[p_name]:
                    logger.info(f"AI Provider {p_name} is in cooldown, skipping...")
                    continue
                else:
                    del self._provider_cooldowns[p_name]

            try:
                return self.call_ai_api(client, model, prompt, system_prompt=system_prompt, api_key=api_key)
            except RateLimitError as e:
                last_err = e
                continue
            except Exception as e:
                last_err = e
                continue

        if last_err:
            raise last_err
        raise Exception("No AI client initialized")

# Global instance
ai_client = AIClient()

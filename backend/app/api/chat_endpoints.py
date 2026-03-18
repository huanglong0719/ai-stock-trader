from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from app.services.chat_service import chat_service

router = APIRouter()

class ChatMessageDTO(BaseModel):
    role: str
    content: str
    created_at: Optional[str] = None

class SendMessageRequest(BaseModel):
    content: str
    preferred_provider: Optional[str] = None
    api_key: Optional[str] = None

@router.get("/chat/history", response_model=List[ChatMessageDTO])
async def get_chat_history():
    """
    获取聊天记录
    """
    msgs = await chat_service.get_history(limit=50)
    return [
        ChatMessageDTO(
            role=m.role,
            content=m.content,
            created_at=m.created_at.strftime("%Y-%m-%d %H:%M:%S") if m.created_at else ""
        ) for m in msgs
    ]

@router.post("/chat/send", response_model=ChatMessageDTO)
async def send_message(request: SendMessageRequest):
    """
    发送消息给 AI 基金经理
    """
    # Process user message and get response (this involves async AI call)
    response_content = await chat_service.process_user_message(request.content, preferred_provider=request.preferred_provider, api_key=request.api_key)
    
    return ChatMessageDTO(
        role="assistant",
        content=response_content,
        created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

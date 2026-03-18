from sqlalchemy import Column, Integer, String, DateTime, Text
from app.db.session import Base
import datetime

class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    role = Column(String)  # 'user', 'assistant', 'system'
    content = Column(Text)
    context_data = Column(Text, nullable=True)  # JSON string for extra context (e.g. market snapshot)
    created_at = Column(DateTime, default=datetime.datetime.now)

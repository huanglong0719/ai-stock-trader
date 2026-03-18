import datetime
from typing import Optional
from sqlalchemy import Integer, String, DateTime, Float, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base

class SystemJobLog(Base):
    __tablename__ = "system_job_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_name: Mapped[str] = mapped_column(String, index=True)
    start_time: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now())
    end_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String, default="RUNNING")
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

class SystemHeartbeat(Base):
    __tablename__ = "system_heartbeats"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    component: Mapped[str] = mapped_column(String, index=True)
    last_beat: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    status: Mapped[str] = mapped_column(String, default="OK")
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

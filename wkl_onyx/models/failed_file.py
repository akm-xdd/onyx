from datetime import datetime
from sqlalchemy import Integer, String, JSON, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class RetryStatus:
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"

class FailedFile(Base):
    __tablename__ = "failed_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    crawl_run_id: Mapped[int] = mapped_column(Integer, ForeignKey("crawl_runs.id", ondelete="CASCADE"))
    file_meta: Mapped[dict] = mapped_column(JSON, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String, default=RetryStatus.PENDING)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    crawl_run = relationship("CrawlRun")

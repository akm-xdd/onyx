from datetime import datetime
from sqlalchemy import Integer, String, JSON, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class RunStatus:
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    CANCELLED = "cancelled"


class RunType:
    FULL = "full"
    INCREMENTAL = "incremental"


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    crawl_job_id: Mapped[int] = mapped_column(Integer, ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(String, default=RunStatus.PENDING)
    run_type: Mapped[str] = mapped_column(String, default=RunType.INCREMENTAL)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    docs_processed: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    crawl_job = relationship("CrawlJob", back_populates="runs")
from datetime import datetime
from sqlalchemy import Integer, JSON, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class Checkpoint(Base):
    __tablename__ = "checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    crawl_job_id: Mapped[int] = mapped_column(Integer, ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    checkpoint_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    crawl_job = relationship("CrawlJob", back_populates="checkpoints")
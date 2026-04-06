from datetime import datetime
from sqlalchemy import Integer, String, JSON, DateTime, func, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class Credential(Base):
    __tablename__ = "credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_type: Mapped[str] = mapped_column(String, nullable=False)  # "google_drive", "slack", etc.
    credential_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_email: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
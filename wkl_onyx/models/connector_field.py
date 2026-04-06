# models/connector_field.py
from datetime import datetime
from sqlalchemy import Integer, String, Boolean, JSON, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class ConnectorFieldSchema(Base):
    __tablename__ = "connector_field_schemas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type_id: Mapped[int] = mapped_column(Integer, ForeignKey("connector_types.id", ondelete="CASCADE"))
    field_category: Mapped[str] = mapped_column(String, nullable=False)  # "credential" or "config"
    field_name: Mapped[str] = mapped_column(String, nullable=False)  # "github_access_token", "repo_owner"
    field_type: Mapped[str] = mapped_column(String, nullable=False)  # "str", "bool", "list[str]"
    label: Mapped[str] = mapped_column(String, nullable=False)
    required: Mapped[bool] = mapped_column(Boolean, default=False)
    default_value: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # {"value": True} or None
    options: Mapped[list | None] = mapped_column(JSON, nullable=True)  # ["all", "open", "closed"]
    display_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    connector_type = relationship("ConnectorType", back_populates="fields")
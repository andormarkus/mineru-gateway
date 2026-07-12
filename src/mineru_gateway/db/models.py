"""ORM models — workers, tasks, cache_entries, scaling_events.

Schema per SIMPLE_REFACTORING_PLAN.md. Timestamps are timezone-aware UTC.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from mineru_gateway.db.base import Base


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Worker(Base):
    """Controller-managed worker — durable intent and observed state on one row."""

    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    deployment_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    instance_id: Mapped[str | None] = mapped_column(String(256), nullable=True, unique=True)
    base_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    generation: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    desired_state: Mapped[str] = mapped_column(String(32), default="stopped", nullable=False)
    cloud_state: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    healthy: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    draining: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    drain_target: Mapped[str | None] = mapped_column(String(16), nullable=True)
    replacement_for: Mapped[str | None] = mapped_column(String(128), ForeignKey("workers.id"), nullable=True)
    rotation_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    failure_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stalled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    ready_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    start_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_health_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    scaling_events: Mapped[list[ScalingEvent]] = relationship(back_populates="worker")

    def __repr__(self) -> str:
        return f"<Worker {self.id} desired={self.desired_state} cloud={self.cloud_state}>"


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (Index("ix_tasks_status_created", "status", "created_at"),)

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), ForeignKey("workers.id"), nullable=True, index=True)
    upstream_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    upstream_base_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    backend: Mapped[str] = mapped_column(String(64), default="hybrid-engine", nullable=False)
    parse_method: Mapped[str] = mapped_column(String(16), default="auto", nullable=False)
    file_names: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    source: Mapped[str] = mapped_column(String(16), default="tasks", nullable=False)

    status: Mapped[str] = mapped_column(String(16), default="queued", nullable=False, index=True)
    payload_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    result_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    result_format: Mapped[str | None] = mapped_column(String(16), nullable=True)
    cache_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    options_blob: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    client_expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    worker: Mapped[Worker | None] = relationship(foreign_keys=[worker_id])


class CacheEntry(Base):
    """Content-addressed cache entry — no FKs to tasks."""

    __tablename__ = "cache_entries"

    cache_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    options_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    backend: Mapped[str] = mapped_column(String(64), nullable=False)
    parse_method: Mapped[str] = mapped_column(String(16), nullable=False)
    effort: Mapped[str] = mapped_column(String(16), nullable=False)

    object_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    object_generation: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    result_format: Mapped[str | None] = mapped_column(String(16), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    hit_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class ScalingEvent(Base):
    """Append-only scaling audit record — not workflow state."""

    __tablename__ = "scaling_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), ForeignKey("workers.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str] = mapped_column(String(256), nullable=False)
    triggered_by: Mapped[str] = mapped_column(String(16), default="autoscaler", nullable=False)
    requester: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    worker: Mapped[Worker | None] = relationship(back_populates="scaling_events")

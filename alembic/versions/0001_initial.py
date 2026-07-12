"""initial schema (simplified)

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("deployment_id", sa.String(128), nullable=False),
        sa.Column("instance_id", sa.String(256), nullable=True, unique=True),
        sa.Column("base_url", sa.String(512), nullable=True),
        sa.Column("generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("desired_state", sa.String(32), nullable=False, server_default="stopped"),
        sa.Column("cloud_state", sa.String(32), nullable=False, server_default="unknown"),
        sa.Column("healthy", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("draining", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("drain_target", sa.String(16), nullable=True),
        sa.Column("replacement_for", sa.String(128), nullable=True),
        sa.Column("rotation_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stalled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("start_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_active_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_health_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("terminated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.ForeignKeyConstraint(["replacement_for"], ["workers.id"], name="fk_workers_replacement_for"),
    )
    op.create_index("ix_workers_deployment_id", "workers", ["deployment_id"])
    op.create_index("ix_workers_desired_state", "workers", ["desired_state"])

    op.create_table(
        "cache_entries",
        sa.Column("cache_key", sa.String(64), primary_key=True),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("options_hash", sa.String(64), nullable=False),
        sa.Column("backend", sa.String(64), nullable=False),
        sa.Column("parse_method", sa.String(16), nullable=False),
        sa.Column("effort", sa.String(16), nullable=False),
        sa.Column("object_key", sa.String(512), nullable=True),
        sa.Column("object_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("result_format", sa.String(16), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("hit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
    )
    op.create_index("ix_cache_entries_content_sha256", "cache_entries", ["content_sha256"])
    op.create_index("ix_cache_entries_expires_at", "cache_entries", ["expires_at"])

    op.create_table(
        "tasks",
        sa.Column("task_id", sa.String(64), primary_key=True),
        sa.Column("worker_id", sa.String(128), nullable=True),
        sa.Column("upstream_task_id", sa.String(64), nullable=True),
        sa.Column("upstream_base_url", sa.String(512), nullable=True),
        sa.Column("backend", sa.String(64), nullable=False, server_default="hybrid-engine"),
        sa.Column("parse_method", sa.String(16), nullable=False, server_default="auto"),
        sa.Column("file_names", sa.JSON(), nullable=False),
        sa.Column("source", sa.String(16), nullable=False, server_default="tasks"),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("payload_key", sa.String(512), nullable=True),
        sa.Column("result_key", sa.String(512), nullable=True),
        sa.Column("result_format", sa.String(16), nullable=True),
        sa.Column("cache_key", sa.String(64), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("options_blob", sa.JSON(), nullable=True),
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("client_expired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], name="fk_tasks_worker"),
    )
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_worker_id", "tasks", ["worker_id"])
    op.create_index("ix_tasks_status_created", "tasks", ["status", "created_at"])

    op.create_table(
        "scaling_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("worker_id", sa.String(128), nullable=True),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("triggered_by", sa.String(16), nullable=False, server_default="autoscaler"),
        sa.Column("requester", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], name="fk_scaling_events_worker"),
    )
    op.create_index("ix_scaling_events_worker_id", "scaling_events", ["worker_id"])
    op.create_index("ix_scaling_events_created_at", "scaling_events", ["created_at"])


def downgrade() -> None:
    op.drop_table("scaling_events")
    op.drop_index("ix_tasks_status_created", table_name="tasks")
    op.drop_table("tasks")
    op.drop_table("cache_entries")
    op.drop_index("ix_workers_desired_state", table_name="workers")
    op.drop_index("ix_workers_deployment_id", table_name="workers")
    op.drop_table("workers")

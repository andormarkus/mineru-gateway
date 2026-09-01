"""Helpers for seeding external mineru-api workers into the gateway DB."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.models import Worker


async def seed_worker_row(
    session: AsyncSession,
    *,
    settings: GatewaySettings,
    worker_id: str,
    base_url: str,
    healthy: bool = True,
    ready: bool = True,
) -> Worker:
    """Insert a dispatchable worker row pointing at an external mineru-api base URL."""
    now = datetime.now(UTC)
    worker = Worker(
        id=worker_id,
        provider=settings.cloud.provider,
        deployment_id=settings.deployment_id,
        base_url=base_url.rstrip("/"),
        desired_state="running",
        cloud_state="running",
        healthy=healthy,
        ready_at=now if ready else None,
    )
    session.add(worker)
    await session.commit()
    return worker

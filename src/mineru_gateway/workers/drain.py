"""Shared drain protocol — DB polling then provider suspend (CLOUD_WORKERS.md)."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Protocol

from sqlalchemy import select

from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker

logger = logging.getLogger(__name__)


class SuspendCapable(Protocol):
    """Minimal provider surface required by the shared drain protocol."""

    async def suspend_instance(self, instance_id: str) -> None: ...


async def drain_and_suspend(provider: SuspendCapable, instance_id: str, *, timeout: float) -> None:
    """Mark worker draining, wait for in-flight tasks, then suspend the VM."""
    worker_id = await _find_worker_id(instance_id)
    if worker_id is None:
        logger.warning("drain_instance: no worker found for instance %s", instance_id)
        await provider.suspend_instance(instance_id)
        return

    await _set_worker_state(worker_id, "draining")
    await _wait_for_inflight(worker_id, timeout)
    await provider.suspend_instance(instance_id)
    logger.info("Drain complete: instance_id=%s worker_id=%s — suspended", instance_id, worker_id)


async def _find_worker_id(instance_id: str) -> str | None:
    async with get_db_session() as session:
        stmt = select(Worker).where(Worker.cloud_instance_id == instance_id)
        row = (await session.execute(stmt)).scalar_one_or_none()
        return row.id if row else None


async def _set_worker_state(worker_id: str, state: str) -> None:
    async with get_db_session() as session:
        row = await session.get(Worker, worker_id)
        if row is not None:
            row.state = state
            await session.commit()


async def _wait_for_inflight(worker_id: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    count = -1
    while time.monotonic() < deadline:
        async with get_db_session() as session:
            stmt = select(Task).where(
                Task.upstream_server_id == worker_id, Task.status.in_(["pending", "processing"])
            )
            inflight = (await session.execute(stmt)).scalars().all()
            count = len(inflight)
        if count == 0:
            return
        await asyncio.sleep(5.0)
    logger.warning("drain timeout: worker %s still has %d in-flight tasks after %.0fs", worker_id, count, timeout)

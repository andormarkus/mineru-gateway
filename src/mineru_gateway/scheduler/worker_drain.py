"""Scheduler drain loop — process workers in ``draining`` state (CLOUD_WORKERS.md drain protocol).

For each draining worker: check if in-flight tasks are done, if so ``suspend_instance`` (Tier A) or
``terminate_instance`` (Tier B rotation). If timeout: force-stop, mark affected tasks failed.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select

from mineru_gateway.cloud.base import CloudWorkerProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scaling.audit import record_scaling_event
from mineru_gateway.util.datetime import ensure_aware_utc

logger = logging.getLogger(__name__)


async def process_draining_workers(provider: CloudWorkerProvider | None) -> int:
    """Process all workers in ``draining`` state. Returns count fully drained."""
    settings = get_settings()
    drain_timeout = settings.rotation.drain_timeout_seconds

    async with get_db_session() as session:
        stmt = select(Worker).where(Worker.state == "draining")
        draining = (await session.execute(stmt)).scalars().all()

    if not draining:
        return 0

    logger.debug("Drain loop: processing %d draining worker(s)", len(draining))
    count = 0
    for worker in draining:
        done = await _check_drain_complete(worker.id, drain_timeout)
        if done:
            await _finalize_drain(worker, provider)
            count += 1
        else:
            logger.debug("Worker %s still draining — waiting for in-flight tasks", worker.id)

    return count


async def _check_drain_complete(worker_id: str, drain_timeout: int) -> bool:
    """Check if a worker has no in-flight tasks, or the drain timeout has expired."""
    async with get_db_session() as session:
        inflight_stmt = select(func.count(Task.task_id)).where(
            Task.upstream_server_id == worker_id, Task.status.in_(["pending", "processing"])
        )
        inflight = (await session.execute(inflight_stmt)).scalar() or 0

        if inflight == 0:
            logger.debug("Worker %s drain complete — no in-flight tasks", worker_id)
            return True

        # Check if drain timeout has expired (look at when the worker was set to draining).
        worker = await session.get(Worker, worker_id)
        if worker is not None:
            updated = ensure_aware_utc(worker.updated_at)
            if datetime.now(UTC) - updated > timedelta(seconds=drain_timeout):
                logger.warning("Drain timeout for worker %s — force-stopping with %d in-flight", worker_id, inflight)
                await _fail_inflight_tasks(worker_id)
                return True

    return False


async def _finalize_drain(worker: Worker, provider: CloudWorkerProvider | None) -> None:
    """Stop or terminate a drained worker."""
    async with get_db_session() as session:
        row = await session.get(Worker, worker.id)
        if row is None:
            return

        if provider is not None and row.cloud_instance_id:
            try:
                # Use stop (Tier A) — the instance may be reused later.
                await provider.suspend_instance(row.cloud_instance_id)
            except Exception:
                logger.exception("Failed to stop drained worker %s", row.id)
                return

        row.state = "stopped"
        row.enabled = False
        await record_scaling_event(
            session, action="stop", reason="drain complete", worker_id=row.id, triggered_by="autoscaler"
        )
        await session.commit()
        logger.info("Worker %s drained and stopped", row.id)


async def _fail_inflight_tasks(worker_id: str) -> None:
    """Mark in-flight tasks on a force-drained worker as failed."""
    async with get_db_session() as session:
        stmt = select(Task).where(Task.upstream_server_id == worker_id, Task.status.in_(["pending", "processing"]))
        tasks = (await session.execute(stmt)).scalars().all()
        for task in tasks:
            task.status = "failed"
            task.dispatch_state = "terminal"
            task.error = "Worker drained (force-stop after timeout)"
            task.completed_at = datetime.now(UTC)
        await session.commit()
        if tasks:
            logger.warning("Marked %d in-flight task(s) failed on force-drained worker %s", len(tasks), worker_id)

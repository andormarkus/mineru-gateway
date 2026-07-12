"""Background completion watcher — stores results to S3 for async tasks (CONCERNS.md §4/§7).

The async ``POST /tasks`` flow returns immediately with a task_id. The client polls ``GET /tasks/{id}`` for
status. But if the client never polls (or polls too late), the result never gets stored to S3 — and when the
worker is scaled down, the result is lost.

This background loop fixes that: it periodically scans the DB for tasks that are ``completed`` but have no
``result_key`` (no S3 pointer), fetches the result from the worker, stores it to S3, and sets the pointer.
This makes result storage reliable for the async path, independent of client polling behavior.

Leader-elected on Postgres (only one replica runs this loop). On sqlite, always runs (single-process).
"""

from __future__ import annotations

import logging

import httpx
from sqlalchemy import select

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.tasks.results import fetch_and_store_result
from mineru_gateway.tasks.status import sync_dispatched_task_statuses

logger = logging.getLogger(__name__)


async def store_unstored_results(store: CloudStorageProvider, client: httpx.AsyncClient) -> int:
    """Sync upstream status, then fetch + store results for completed tasks missing S3 pointers.

    Returns the count of results stored. Called by the background loop.
    """
    await sync_dispatched_task_statuses(client)

    async with get_db_session() as session:
        stmt = select(Task).where(
            Task.status == "completed",
            Task.result_key.is_(None),
            Task.upstream_task_id.isnot(None),
            Task.upstream_base_url.isnot(None),
        )
        unstored = (await session.execute(stmt)).scalars().all()

    if not unstored:
        return 0

    logger.debug("Completion watcher: %d unstored result(s) to fetch", len(unstored))
    count = 0
    for task in unstored:
        try:
            await fetch_and_store_result(
                task_id=task.task_id,
                upstream_task_id=task.upstream_task_id,
                upstream_base_url=task.upstream_base_url,
                store=store,
                client=client,
            )
            count += 1
            metrics.record_result_stored()
            logger.info("Completion watcher: stored result for task %s", task.task_id)
        except (httpx.HTTPError, OSError, KeyError) as exc:
            metrics.record_result_store_failed()
            logger.warning("Completion watcher: could not store result for task %s: %s", task.task_id, exc)

    return count

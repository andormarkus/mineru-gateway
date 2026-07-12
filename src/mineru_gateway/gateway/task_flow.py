"""Shared task-flow helpers for the synchronous routes (``/file_parse``, ``/v1/ocr``).

These are about *retrieving* a task's outcome after ingest, not about intake — so they live here
rather than in ``ingest.py``. ``poll_task_until_terminal`` blocks until the scheduler finishes a
queued task; ``fetch_result_or_none`` reads the stored result from S3.
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx
from fastapi import Request

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.tasks.results import fetch_and_store_result, read_result
from mineru_gateway.tasks.status import refresh_task_from_upstream

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({"completed", "failed"})

# Bound on how long a synchronous route polls the DB for the scheduler to finish a task.
_POLL_ITERATIONS = 300
_POLL_INTERVAL_SECONDS = 1.0


def get_store(request: Request) -> CloudStorageProvider | None:
    """Get the object store from app.state (None when no S3 backend is configured)."""
    return getattr(request.app.state, "object_store", None)


async def store_upstream_result_if_needed(task: Task, store: CloudStorageProvider, *, client: httpx.AsyncClient) -> None:
    """Fetch a completed task's result from the worker and persist it to S3 when missing."""
    if task.status != "completed" or task.result_key is not None:
        return
    if not task.upstream_task_id or not task.upstream_base_url:
        return
    try:
        await fetch_and_store_result(
            task_id=task.task_id,
            upstream_task_id=task.upstream_task_id,
            upstream_base_url=task.upstream_base_url,
            store=store,
            client=client,
        )
    except (httpx.HTTPError, OSError, KeyError) as exc:
        logger.debug("Could not store upstream result for task %s: %s", task.task_id, exc)


async def poll_task_until_terminal(
    task_id: str, *, store: CloudStorageProvider | None = None, route: str = "unknown"
) -> Task | None:
    """Poll until the task reaches a terminal state (``completed``/``failed``).

    After dispatch, the scheduler only records upstream identifiers — this loop refreshes status from
    the worker and, when ``store`` is provided, stores completed results to S3.

    Returns the terminal row, or ``None`` if the task vanishes (deleted mid-flight). Does not raise
    — the caller decides how to render a failure. Bounded by ``_POLL_ITERATIONS`` *
    ``_POLL_INTERVAL_SECONDS`` (~5 min ceiling); on timeout returns the last-seen row (which is
    non-terminal) so the caller can render "not ready yet".
    """
    start = time.perf_counter()
    row: Task | None = None
    outcome = "timeout"
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        for i in range(_POLL_ITERATIONS):
            row = await refresh_task_from_upstream(task_id, client=client)
            if row is None:
                logger.warning("Task %s disappeared while polling", task_id)
                outcome = "disappeared"
                metrics.record_poll_duration(
                    route=route, outcome=outcome, duration_ms=(time.perf_counter() - start) * 1000
                )
                return None
            if row.status in _TERMINAL_STATUSES:
                if store is not None:
                    await store_upstream_result_if_needed(row, store, client=client)
                    async with get_db_session() as session:
                        row = await session.get(Task, task_id)
                logger.debug("Task %s reached terminal state %s after %d polls", task_id, row.status, i + 1)
                outcome = row.status if row is not None else "unknown"
                metrics.record_poll_duration(
                    route=route, outcome=outcome, duration_ms=(time.perf_counter() - start) * 1000
                )
                return row
            if i > 0 and i % 30 == 0:
                logger.debug("Still polling task %s (status=%s, poll=%d)", task_id, row.status, i + 1)
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    logger.warning(
        "Poll timeout for task %s — last status=%s dispatch_state=%s",
        task_id,
        row.status if row else "unknown",
        row.dispatch_state if row else "unknown",
    )
    metrics.record_poll_duration(route=route, outcome=outcome, duration_ms=(time.perf_counter() - start) * 1000)
    return row


async def fetch_result_or_none(task_id: str, store: CloudStorageProvider) -> bytes | None:
    """Read a task's result ZIP from S3, or ``None`` if not stored yet (``KeyError`` swallowed).

    Thin convenience wrapper around :func:`read_result` for the synchronous routes, which want a
    soft "not ready" rather than an exception when the result pointer isn't set yet.
    """
    try:
        data, fmt = await read_result(task_id=task_id, store=store)
        logger.debug("Fetched result for task %s (%d bytes, format=%s)", task_id, len(data), fmt)
        return data
    except KeyError:
        logger.debug("Result not yet stored for task %s", task_id)
        return None

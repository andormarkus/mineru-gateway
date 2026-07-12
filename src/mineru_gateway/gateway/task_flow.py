"""Shared task-flow helpers for the synchronous routes (``/file_parse``, ``/v1/ocr``).

Polls the central task queue in PostgreSQL until the scheduler marks a task terminal.
Worker status sync and result persistence are owned by the scheduler — gateway routes
read task rows only.
"""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import Request
from fastapi.responses import JSONResponse

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.tasks.results import read_result
from mineru_gateway.tasks.status import TASK_COMPLETED, TASK_EXPIRED, TASK_FAILED

logger = logging.getLogger(__name__)

_FAILURE_STATUSES = frozenset({TASK_FAILED, TASK_EXPIRED})

_POLL_ITERATIONS = 300
_POLL_INTERVAL_SECONDS = 1.0


def get_store(request: Request) -> CloudStorageProvider | None:
    """Get the object store from app.state (None when no S3 backend is configured)."""
    return getattr(request.app.state, "object_store", None)


def is_poll_complete(row: Task) -> bool:
    """True when the synchronous route can stop polling."""
    if row.status == TASK_COMPLETED and bool(row.result_key):
        return True
    if row.status in _FAILURE_STATUSES:
        return True
    return row.client_expired_at is not None


def client_sla_expired_response(task_id: str, *, status: str) -> JSONResponse:
    """Return 202 when the client SLA expired but execution may still continue."""
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": status,
            "client_sla_expired": True,
            "message": "Client SLA expired; task execution continues. Check status and result endpoints.",
            "status_url": f"/tasks/{task_id}",
            "result_url": f"/tasks/{task_id}/result",
        },
    )


async def try_fetch_result_bytes(task_id: str, row: Task, store: CloudStorageProvider | None) -> bytes | None:
    """Return durable result bytes when the task completed with a stored pointer."""
    if store is None or row.status != TASK_COMPLETED or not row.result_key:
        return None
    return await fetch_result_or_none(task_id=task_id, store=store)


async def _load_task(task_id: str) -> Task | None:
    async with get_db_session() as session:
        return await session.get(Task, task_id)


async def poll_task_until_terminal(task_id: str, *, route: str = "unknown") -> Task | None:
    """Poll the database until the task is ready to return to the client.

    Returns a completed row with ``result_key``, a failed/expired row, ``None`` if the
    task disappears, or the last-seen nonterminal row on timeout.
    """
    start = time.perf_counter()
    row: Task | None = None
    outcome = "timeout"
    for i in range(_POLL_ITERATIONS):
        row = await _load_task(task_id)
        if row is None:
            logger.warning("Task %s disappeared while polling", task_id)
            outcome = "disappeared"
            break
        if is_poll_complete(row):
            logger.debug("Task %s poll complete (status=%s) after %d polls", task_id, row.status, i + 1)
            outcome = row.status
            break
        if i > 0 and i % 30 == 0:
            logger.debug("Still polling task %s (status=%s, poll=%d)", task_id, row.status, i + 1)
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
    else:
        logger.warning("Poll timeout for task %s — last status=%s", task_id, row.status if row else "unknown")

    metrics.record_poll_duration(route=route, outcome=outcome, duration_ms=(time.perf_counter() - start) * 1000)
    return row


async def fetch_result_or_none(task_id: str, store: CloudStorageProvider) -> bytes | None:
    """Read a task's result ZIP from object storage, or ``None`` if not stored yet."""
    try:
        data, fmt = await read_result(task_id=task_id, store=store)
        logger.debug("Fetched result for task %s (%d bytes, format=%s)", task_id, len(data), fmt)
        return data
    except KeyError:
        logger.debug("Result not yet stored for task %s", task_id)
        return None

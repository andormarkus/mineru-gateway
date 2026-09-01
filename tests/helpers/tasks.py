"""Task polling helpers for integration and slow tests."""

from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient

from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from tests.helpers.e2e_log import E2eProgress, e2e_log, e2e_verbose


async def wait_for_task_status(
    task_id: str, *, expected: str, timeout_seconds: float, poll_interval: float = 1.0
) -> Task:
    """Poll the DB until the task reaches ``expected`` status."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    row: Task | None = None
    while asyncio.get_running_loop().time() < deadline:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
        if row is not None and row.status == expected:
            return row
        await asyncio.sleep(poll_interval)
    pytest.fail(f"task {task_id} did not reach {expected!r} within {timeout_seconds}s (last={row})")


async def wait_for_task_status_api(
    client: AsyncClient, task_id: str, *, expected: str, timeout_seconds: float, poll_interval: float = 0.5
) -> dict:
    """Poll GET /tasks/{id} until the API reports ``expected`` status."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    body: dict = {}
    progress = E2eProgress(f"task {task_id[:8]}→{expected}") if e2e_verbose() else None
    if progress is not None:
        e2e_log(f"waiting for task {task_id} status={expected} (timeout={timeout_seconds:.0f}s)")
    while asyncio.get_running_loop().time() < deadline:
        resp = await client.get(f"/tasks/{task_id}")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        if body["status"] == expected:
            if progress is not None:
                e2e_log(f"task {task_id} reached {expected}", always=True)
            return body
        if progress is not None:
            progress.report(f"status={body.get('status', '?')}", signature=body.get("status"))
        await asyncio.sleep(poll_interval)
    pytest.fail(f"task {task_id} API status did not reach {expected!r} within {timeout_seconds}s (last={body})")

"""Phase 2 tests: central-queue ingest + DB status polling.

The gateway ingests tasks to the DB (status=queued). The scheduler dispatches them.
These tests verify the gateway-side: ingest, status polling from DB, and persistence.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ingest_returns_202_with_task_id(gateway_with_worker: tuple[AsyncClient, object, str]) -> None:
    """POST /tasks ingests and returns 202 + task_id with status=queued."""
    client, _, _ = gateway_with_worker

    resp = await client.post(
        "/tasks",
        files=[("files", ("doc.pdf", b"%PDF-1.4 fake content", "application/pdf"))],
        data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
    )
    assert resp.status_code == 202, resp.text
    task = resp.json()
    assert task["status"] == "queued"
    assert "task_id" in task
    assert task["source"] == "tasks"


@pytest.mark.asyncio
async def test_task_persists_to_db(gateway_with_worker: tuple[AsyncClient, object, str]) -> None:
    """The ingested task must appear in the DB with status=queued."""
    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import Task

    client, _, _ = gateway_with_worker

    resp = await client.post(
        "/tasks", files=[("files", ("note.pdf", b"data", "application/pdf"))], data={"backend": "pipeline"}
    )
    task_id = resp.json()["task_id"]

    async with get_db_session() as session:
        row = await session.get(Task, task_id)
    assert row is not None, "task did not persist to DB"
    assert row.backend == "pipeline"
    assert row.file_names == ["note.pdf"]
    assert row.source == "tasks"
    assert row.status == "queued"


@pytest.mark.asyncio
async def test_status_poll_reads_from_db(gateway_with_worker: tuple[AsyncClient, object, str]) -> None:
    """GET /tasks/{id} reads the task status from the DB."""
    client, _, _ = gateway_with_worker

    resp = await client.post(
        "/tasks", files=[("files", ("doc.pdf", b"%PDF-1.4 fake", "application/pdf"))], data={"backend": "pipeline"}
    )
    task_id = resp.json()["task_id"]

    status_resp = await client.get(f"/tasks/{task_id}")
    assert status_resp.status_code == 200
    body = status_resp.json()
    assert body["task_id"] == task_id
    assert body["status"] == "queued"
    assert "dispatch_state" not in body


@pytest.mark.asyncio
async def test_get_unknown_task_404(gateway_with_worker: tuple[AsyncClient, object, str]) -> None:
    client, _, _ = gateway_with_worker
    resp = await client.get("/tasks/nonexistent-id")
    assert resp.status_code == 404

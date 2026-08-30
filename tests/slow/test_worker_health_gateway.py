"""Scheduler worker /health probing through the gateway stack (real mineru-api)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import httpx
import pytest
from httpx import AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scheduler.scheduler import Scheduler
from mineru_gateway.tasks.status import TASK_QUEUED
from tests.helpers.scheduler import pre_autoscaling_scheduler_loop
from tests.slow.conftest import FIXTURES_DIR

pytestmark = pytest.mark.slow

_PDF_SAMPLE = FIXTURES_DIR / "pdf_sample_1.pdf"


@pytest.mark.asyncio
async def test_scheduler_health_probe_updates_worker_row(
    gateway_with_unprobed_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
    worker_url: str,
) -> None:
    """Scheduler polls GET {worker}/health and marks the DB row healthy + ready."""
    _client, worker_id, store = gateway_with_unprobed_external_worker
    settings = get_settings()

    async with get_db_session() as session:
        before = await session.get(Worker, worker_id)
    assert before is not None
    assert before.healthy is False
    assert before.ready_at is None
    assert before.last_health_checked_at is None

    async with httpx.AsyncClient(timeout=10.0) as http_client:
        scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=None)
        with patch.object(metrics, "record_health_check") as record_health:
            await scheduler._refresh_worker_health()
            record_health.assert_called_once_with(outcome="healthy")

    async with get_db_session() as session:
        after = await session.get(Worker, worker_id)
    assert after is not None
    assert after.healthy is True
    assert after.ready_at is not None
    assert after.last_health_checked_at is not None
    assert after.last_error is None
    assert after.base_url == worker_url


@pytest.mark.asyncio
async def test_admin_workers_reflects_health_probe(
    gateway_with_unprobed_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
    worker_url: str,
) -> None:
    """GET /admin/workers exposes healthy=true after the scheduler health probe."""
    client, worker_id, store = gateway_with_unprobed_external_worker
    settings = get_settings()

    async with httpx.AsyncClient(timeout=10.0) as http_client:
        scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=None)
        await scheduler._refresh_worker_health()

    resp = await client.get("/admin/workers")
    assert resp.status_code == 200, resp.text
    workers = resp.json()["workers"]
    worker = next(w for w in workers if w["id"] == worker_id)
    assert worker["healthy"] is True
    assert worker["base_url"] == worker_url
    assert worker["ready_at"] is not None

    detail = await client.get(f"/admin/workers/{worker_id}")
    assert detail.status_code == 200
    assert detail.json()["healthy"] is True


@pytest.mark.asyncio
async def test_dispatch_blocked_until_health_probe(
    gateway_with_unprobed_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
    worker_timeout_seconds: float,
) -> None:
    """Unhealthy workers are not dispatched until scheduler refreshes /health."""
    client, worker_id, store = gateway_with_unprobed_external_worker
    settings = get_settings()
    pdf_bytes = _PDF_SAMPLE.read_bytes()

    resp = await client.post(
        "/tasks",
        files=[("files", ("pdf_sample_1.pdf", pdf_bytes, "application/pdf"))],
        data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
    )
    assert resp.status_code == 202, resp.text
    task_id = resp.json()["task_id"]

    # Dispatch loop without health checks — task must stay queued.
    async with pre_autoscaling_scheduler_loop(
        settings,
        store,
        client_timeout=worker_timeout_seconds,
        include_health_checks=False,
        interval_seconds=0.1,
    ):
        await asyncio.sleep(1.0)

    async with get_db_session() as session:
        queued = await session.get(Task, task_id)
    assert queued is not None
    assert queued.status == TASK_QUEUED

    # Production-like loop: health probe then dispatch.
    deadline = asyncio.get_running_loop().time() + worker_timeout_seconds
    async with pre_autoscaling_scheduler_loop(
        settings,
        store,
        client_timeout=worker_timeout_seconds,
        interval_seconds=0.1,
    ):
        while asyncio.get_running_loop().time() < deadline:
            async with get_db_session() as session:
                row = await session.get(Task, task_id)
            if row is not None and row.status == "completed":
                break
            await asyncio.sleep(0.5)
        else:
            pytest.fail(f"task {task_id} never completed after health probe")

    async with get_db_session() as session:
        worker = await session.get(Worker, worker_id)
    assert worker is not None
    assert worker.healthy is True

    result_resp = await client.get(f"/tasks/{task_id}/result")
    assert result_resp.status_code == 200, result_resp.text
    assert len(result_resp.content) > 0

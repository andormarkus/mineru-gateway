"""Pre-autoscaling gateway surface tests (static worker, no EC2 autoscale).

Covers gateway HTTP APIs and scheduler paths that run before cloud_workers_enabled
would launch or terminate instances. See tests/slow/__init__.py for the full matrix.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import CacheEntry
from mineru_gateway.scheduler.cache_service import compute_cache_key, compute_file_records
from tests.helpers.scheduler import pre_autoscaling_scheduler_loop
from tests.helpers.tasks import wait_for_task_status, wait_for_task_status_api
from tests.slow.conftest import FIXTURES_DIR

pytestmark = pytest.mark.slow

_PDF_NAME = "pdf_sample_1.pdf"
_PDF_BYTES = (FIXTURES_DIR / _PDF_NAME).read_bytes()
_FORM = {"backend": "pipeline", "effort": "medium", "parse_method": "auto"}


@pytest.mark.asyncio
async def test_gateway_meta_endpoints(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
) -> None:
    """Liveness, readiness, and attribution root."""
    client, _, _ = gateway_with_external_worker

    health = await client.get("/health")
    assert health.status_code == 200
    health_body = health.json()
    assert health_body["status"] == "ok"
    assert "upstream" in health_body

    ready = await client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"

    root = await client.get("/")
    assert root.status_code == 200
    assert root.json()["service"] == "mineru-gateway"
    assert "upstream" in root.json()


@pytest.mark.asyncio
async def test_get_task_status_api(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider], worker_timeout_seconds: float
) -> None:
    """GET /tasks/{id} reflects queued → completed lifecycle."""
    client, _, store = gateway_with_external_worker
    settings = get_settings()

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        submit = await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        assert submit.status_code == 202, submit.text
        task_id = submit.json()["task_id"]

        body = await wait_for_task_status_api(
            client, task_id, expected="completed", timeout_seconds=worker_timeout_seconds
        )
        assert body["task_id"] == task_id
        assert body["file_names"]
        assert "pdf_sample_1" in body["file_names"][0]
        assert body["result_url"].endswith(f"/tasks/{task_id}/result")


@pytest.mark.asyncio
async def test_file_parse_sync(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider], worker_timeout_seconds: float
) -> None:
    """POST /file_parse blocks until the worker result ZIP is ready."""
    client, _, store = gateway_with_external_worker
    settings = get_settings()

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        resp = await client.post(
            "/file_parse",
            files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))],
            data=_FORM,
            timeout=worker_timeout_seconds + 30.0,
        )
    assert resp.status_code == 200, resp.text
    assert resp.headers.get("content-type", "").startswith("application/")
    assert len(resp.content) > 0


@pytest.mark.asyncio
async def test_cache_dedup_miss_then_hit(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider], worker_timeout_seconds: float
) -> None:
    """Identical submit twice → second request completes from cache without re-dispatch."""
    client, _, store = gateway_with_external_worker
    settings = get_settings()
    file_records = compute_file_records([_PDF_NAME], [_PDF_BYTES])
    cache_key, _ = compute_cache_key(file_records, _FORM)

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        resp1 = await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        assert resp1.status_code == 202
        task1_id = resp1.json()["task_id"]
        await wait_for_task_status(task1_id, expected="completed", timeout_seconds=worker_timeout_seconds)

        async with get_db_session() as session:
            cache_row = await session.get(CacheEntry, cache_key)
        assert cache_row is not None
        assert cache_row.object_key
        assert await store.exists(cache_row.object_key)

        resp2 = await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        assert resp2.status_code == 202
        task2 = resp2.json()
        assert task2["status"] == "completed"
        assert task2["task_id"] != task1_id

        async with get_db_session() as session:
            cache_row2 = await session.get(CacheEntry, cache_key)
        assert cache_row2 is not None
        assert cache_row2.object_key == cache_row.object_key


@pytest.mark.asyncio
async def test_cache_force_bypass(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider], worker_timeout_seconds: float
) -> None:
    """force=true bypasses cache and returns queued (not instant completed)."""
    client, _, store = gateway_with_external_worker
    settings = get_settings()

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        resp = await client.post(
            "/tasks?force=true", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM
        )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_admin_drain_sets_intent(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
) -> None:
    """Admin drain (stop) marks the worker as draining without EC2 calls."""
    client, worker_id, _ = gateway_with_external_worker

    drain = await client.post(f"/admin/workers/{worker_id}/drain", json={"reason": "slow-test drain"})
    assert drain.status_code == 200, drain.text
    drained = drain.json()
    assert drained["draining"] is True
    assert drained["drain_target"] == "stopped"

    listed = await client.get("/admin/workers")
    worker = next(w for w in listed.json()["workers"] if w["id"] == worker_id)
    assert worker["draining"] is True


@pytest.mark.asyncio
async def test_admin_recover_clears_stalled_drain(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
) -> None:
    """Admin recover clears drain intent on a stalled worker and resets failure_count."""
    from mineru_gateway.util.datetime import now_utc

    client, worker_id, _ = gateway_with_external_worker

    async with get_db_session() as session:
        from mineru_gateway.db.models import Worker

        row = await session.get(Worker, worker_id)
        assert row is not None
        row.failure_count = 5
        row.last_error = "slow-test stalled"
        row.draining = True
        row.drain_target = "terminated"
        row.stalled_at = now_utc()
        await session.commit()

    recover = await client.post(f"/admin/workers/{worker_id}/recover", json={"reason": "slow-test recover"})
    assert recover.status_code == 200, recover.text
    recovered = recover.json()
    assert recovered["failure_count"] == 0
    assert recovered["draining"] is False
    assert recovered["drain_target"] is None
    assert recovered["stalled_at"] is None


@pytest.mark.asyncio
async def test_admin_cache_invalidate_and_sweep(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider], worker_timeout_seconds: float
) -> None:
    """Admin can invalidate a cache entry and sweep expired rows."""
    client, _, store = gateway_with_external_worker
    settings = get_settings()
    file_records = compute_file_records([_PDF_NAME], [_PDF_BYTES])
    cache_key, _ = compute_cache_key(file_records, _FORM)

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        resp = await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        task_id = resp.json()["task_id"]
        await wait_for_task_status(task_id, expected="completed", timeout_seconds=worker_timeout_seconds)

    invalidate = await client.delete(f"/admin/cache/{cache_key}")
    assert invalidate.status_code == 200, invalidate.text
    assert invalidate.json()["invalidated"] == cache_key

    missing = await client.delete(f"/admin/cache/{cache_key}")
    assert missing.status_code == 404

    sweep = await client.post("/admin/cache/sweep")
    assert sweep.status_code == 200
    assert "removed" in sweep.json()

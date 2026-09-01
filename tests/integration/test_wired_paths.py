"""Integration tests: exercise the real wired paths against moto S3 + sqlite.

These tests verify the gaps that can't be covered by unit tests with in-memory
doubles:

1. Real S3ObjectStore (aioboto3) against a ThreadedMotoServer S3 endpoint.
2. dispatch_with_dedup end-to-end: submit → dedup-miss → populate cache →
   dedup-hit (second identical request returns instantly from cache).
3. Result durability: /tasks → worker → S3 → read back after "worker gone".
4. /v1/ocr full flow: build payload → submit to FakeWorker → normalize → response.

No Docker needed — moto's ThreadedMotoServer provides a real S3 HTTP endpoint
that aioboto3's async transport works with (unlike the in-process mock_aws()).

Marked as integration so `task test` (unit-only) skips them; run with
`task test-integration` or `pytest -m integration`.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
import uvicorn
from httpx import ASGITransport, AsyncClient

from mineru_gateway.cloud.aws.s3 import S3ObjectStore

pytestmark = pytest.mark.integration


async def _create_moto_bucket(endpoint: str, bucket: str) -> None:
    """Create bucket in moto for test setup (app never auto-creates buckets)."""
    import aioboto3

    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=endpoint) as s3:  # pyright: ignore[reportGeneralTypeIssues]
        await s3.create_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Fixtures: moto S3 server + gateway app with real S3 + FakeWorker
# ---------------------------------------------------------------------------


@pytest.fixture
def moto_s3_endpoint() -> Iterator[str]:
    """Start a ThreadedMotoServer for S3. Returns the endpoint URL."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    assert server._server is not None
    port = server._server.server_address[1]
    endpoint = f"http://127.0.0.1:{port}"
    yield endpoint
    server.stop()


@pytest_asyncio.fixture
async def s3_store(moto_s3_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[S3ObjectStore]:
    """A real S3ObjectStore connected to the moto S3 server."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    store = S3ObjectStore(bucket="gateway-test", endpoint_url=moto_s3_endpoint, region="us-east-1")
    await _create_moto_bucket(moto_s3_endpoint, "gateway-test")
    await store.prepare()
    yield store  # type: ignore[misc]


@pytest_asyncio.fixture
async def app_with_store_and_worker(
    moto_s3_endpoint: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> AsyncIterator[tuple[AsyncClient, object, str, S3ObjectStore]]:
    """Full gateway app with real S3 + a FakeWorker on a real port.

    Yields ``(client, fake_worker_state, worker_id, s3_store)``.

    The DB is a per-test file-backed sqlite: these tests run the app and a
    background scheduler loop concurrently, and in-memory sqlite (StaticPool,
    one shared connection) cannot carry concurrent sessions — they interleave
    transactions on a single connection and intermittently fail refreshes.
    """
    from tests.fakes.worker import FakeWorkerState, create_fake_worker_app

    # --- Start FakeWorker on a real port ---
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    worker_port = sock.getsockname()[1]
    sock.close()

    fake_state = FakeWorkerState()
    fake_app = create_fake_worker_app(fake_state)
    config = uvicorn.Config(fake_app, host="127.0.0.1", port=worker_port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.4)

    database_url = f"sqlite+aiosqlite:///{tmp_path / 'gateway-integration.db'}"

    # --- Set AWS credentials via env (default credential chain) ---
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MINERU_GATEWAY_DATABASE_URL", database_url)

    from unittest.mock import AsyncMock, patch

    from mineru_gateway.config import load_settings, reset_settings_cache

    reset_settings_cache()
    load_settings()

    # --- Build the real S3ObjectStore ---
    store = S3ObjectStore(bucket="gateway-dispatch-test", endpoint_url=moto_s3_endpoint, region="us-east-1")
    await _create_moto_bucket(moto_s3_endpoint, "gateway-dispatch-test")
    await store.prepare()

    try:
        from mineru_gateway.db.base import init_engine, shutdown_engine
        from mineru_gateway.gateway.app import create_app

        await shutdown_engine()
        init_engine(database_url)
        from tests.db_helpers import create_all_tables

        await create_all_tables()
        with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=store)):
            application = create_app()
            async with application.router.lifespan_context(application):
                # Register the FakeWorker directly in the DB (no in-memory pool anymore).
                worker_id = "fake-1"
                worker_url = f"http://127.0.0.1:{worker_port}"
                from mineru_gateway.config import get_settings
                from mineru_gateway.db.base import get_db_session
                from mineru_gateway.db.models import Worker

                settings = get_settings()
                async with get_db_session() as session:
                    session.add(
                        Worker(
                            id=worker_id,
                            provider=settings.cloud.provider,
                            deployment_id=settings.deployment_id,
                            base_url=worker_url,
                            desired_state="running",
                            cloud_state="running",
                            healthy=True,
                        )
                    )
                    await session.commit()

                transport = ASGITransport(app=application)
                async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                    yield client, fake_state, worker_id, store

        await shutdown_engine()
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


# ---------------------------------------------------------------------------
# 1. Real S3ObjectStore against moto S3
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_real_s3_put_get_delete(s3_store: S3ObjectStore) -> None:
    """The real S3ObjectStore round-trips objects through moto S3."""
    await s3_store.put("test-key", b"hello s3", content_type="text/plain")
    assert await s3_store.exists("test-key")

    data = await s3_store.get("test-key")
    assert data == b"hello s3"

    head = await s3_store.head("test-key")
    assert head["size"] == 8

    await s3_store.delete("test-key")
    assert not await s3_store.exists("test-key")


@pytest.mark.asyncio
async def test_real_s3_streaming(s3_store: S3ObjectStore) -> None:
    """Streaming read returns the same data."""
    payload = b"x" * 10000
    await s3_store.put("stream-key", payload)

    chunks = []
    async for chunk in s3_store.stream("stream-key", chunk_size=1024):
        chunks.append(chunk)
    assert b"".join(chunks) == payload


@pytest.mark.asyncio
async def test_real_s3_missing_keyerror(s3_store: S3ObjectStore) -> None:
    with pytest.raises(KeyError):
        await s3_store.get("nonexistent-integration")


# ---------------------------------------------------------------------------
# 2. dispatch_with_dedup end-to-end (cache miss → hit)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedup_cache_miss_then_hit(
    app_with_store_and_worker: tuple[AsyncClient, object, str, S3ObjectStore],
) -> None:
    """Submit the same file twice → second request hits the populated cache object."""
    import httpx

    from mineru_gateway.config import get_settings
    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry, Task
    from mineru_gateway.scheduler.cache_service import compute_cache_key, compute_file_records
    from mineru_gateway.scheduler.scheduler import Scheduler
    from mineru_gateway.scheduler.task_repository import TaskRepository
    from mineru_gateway.scheduler.worker_repository import WorkerRepository
    from mineru_gateway.tasks.storage import cache_object_key

    client, _worker_state, _, store = app_with_store_and_worker
    settings = get_settings()

    file_content = b"%PDF-1.4 identical content for dedup"
    form_data = {"backend": "pipeline", "effort": "medium", "parse_method": "auto"}
    file_records = compute_file_records(["doc.pdf"], [file_content])
    cache_key, _ = compute_cache_key(file_records, {"backend": "pipeline", "effort": "medium", "parse_method": "auto"})

    stop = asyncio.Event()

    async def run_scheduler_ticks() -> None:
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            workers = WorkerRepository(settings)
            task_repo = TaskRepository(settings, store, http_client, workers)
            scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=None)
            while not stop.is_set():
                await task_repo.dispatch_queued_tasks()
                await scheduler._synchronize_tasks_and_results()
                await asyncio.sleep(0.05)

    scheduler_task = asyncio.create_task(run_scheduler_ticks())
    try:
        resp1 = await client.post(
            "/tasks", files=[("files", ("doc.pdf", file_content, "application/pdf"))], data=form_data
        )
        assert resp1.status_code == 202
        task1_id = resp1.json()["task_id"]

        for _ in range(60):
            async with get_db_session() as session:
                row = await session.get(Task, task1_id)
            if row is not None and row.status == "completed" and row.result_key:
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("first task did not complete with stored result")

        # complete_with_result() commits before populate_from_task() copies the
        # result into the cache, so a completed task does not imply a populated
        # cache entry yet — poll until the CAS lands.
        for _ in range(60):
            async with get_db_session() as session:
                cache_row = await session.get(CacheEntry, cache_key)
            if cache_row is not None and cache_row.object_key:
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("cache entry was not populated after task completion")
        expected_object_key = cache_row.object_key
        assert expected_object_key.startswith(cache_object_key(cache_key))
        assert await store.exists(expected_object_key), f"cache object missing at {expected_object_key}"

        resp2 = await client.post(
            "/tasks", files=[("files", ("doc.pdf", file_content, "application/pdf"))], data=form_data
        )
        assert resp2.status_code == 202
        task2 = resp2.json()
        assert task2["status"] == "completed"
        assert task2["task_id"] != task1_id

        async with get_db_session() as session:
            cache_row = await session.get(CacheEntry, cache_key)
        assert cache_row is not None
        assert cache_row.object_key == expected_object_key
    finally:
        stop.set()
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task


@pytest.mark.asyncio
async def test_dedup_force_bypass(app_with_store_and_worker: tuple[AsyncClient, object, str, S3ObjectStore]) -> None:
    """force=true bypasses the cache lookup."""
    client, _, _, _ = app_with_store_and_worker

    file_content = b"%PDF-1.4 force bypass test"
    data = {"backend": "pipeline"}

    # First submit.
    await client.post("/tasks", files=[("files", ("f.pdf", file_content, "application/pdf"))], data=data)

    # Second with force=true — should dispatch, not hit cache.
    resp = await client.post(
        "/tasks?force=true", files=[("files", ("f.pdf", file_content, "application/pdf"))], data=data
    )
    assert resp.status_code == 202


# ---------------------------------------------------------------------------
# 3. Result durability through real dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_durability_through_dispatch(
    app_with_store_and_worker: tuple[AsyncClient, object, str, S3ObjectStore],
) -> None:
    """A submitted task's result gets stored to S3 and can be retrieved."""
    client, _worker_state, _, store = app_with_store_and_worker

    resp = await client.post(
        "/tasks",
        files=[("files", ("durable.pdf", b"%PDF-1.4 durable", "application/pdf"))],
        data={"backend": "pipeline"},
    )
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]

    import httpx

    from mineru_gateway.config import get_settings
    from mineru_gateway.scheduler.scheduler import Scheduler
    from mineru_gateway.scheduler.task_repository import TaskRepository
    from mineru_gateway.scheduler.worker_repository import WorkerRepository

    settings = get_settings()
    workers = WorkerRepository(settings)
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        task_repo = TaskRepository(settings, store, http_client, workers)
        scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=None)

        body: dict = {}
        for _ in range(30):
            await task_repo.dispatch_queued_tasks()
            await scheduler._synchronize_tasks_and_results()
            status = await client.get(f"/tasks/{task_id}")
            body = status.json()
            if body["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.2)

    assert body["status"] == "completed", f"Task didn't complete: {body}"

    result_resp = await client.get(f"/tasks/{task_id}/result")
    assert result_resp.status_code == 200, f"Result not retrievable: {result_resp.status_code}"
    assert len(result_resp.content) > 0


# ---------------------------------------------------------------------------
# 4. /v1/ocr full flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_v1_ocr_with_base64_document(
    app_with_store_and_worker: tuple[AsyncClient, object, str, S3ObjectStore],
) -> None:
    """/v1/ocr accepts a base64-encoded document, ingests to the queue, and returns normalized pages."""
    import base64

    import httpx

    from mineru_gateway.config import get_settings
    from mineru_gateway.scheduler.scheduler import Scheduler
    from mineru_gateway.scheduler.task_repository import TaskRepository
    from mineru_gateway.scheduler.worker_repository import WorkerRepository

    client, _, _, store = app_with_store_and_worker
    settings = get_settings()
    stop = asyncio.Event()

    async def run_scheduler_ticks() -> None:
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            workers = WorkerRepository(settings)
            task_repo = TaskRepository(settings, store, http_client, workers)
            scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=None)
            while not stop.is_set():
                await task_repo.dispatch_queued_tasks()
                await scheduler._synchronize_tasks_and_results()
                await asyncio.sleep(0.05)

    scheduler_task = asyncio.create_task(run_scheduler_ticks())
    try:
        pdf_bytes = b"%PDF-1.4 ocr integration test"
        resp = await client.post(
            "/v1/ocr",
            json={
                "model": "mineru",
                "document": {"type": "file", "file": base64.b64encode(pdf_bytes).decode(), "file_name": "ocr.pdf"},
                "backend": "pipeline",
            },
            timeout=60.0,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["model"] == "mineru"
        assert isinstance(body["pages"], list)
    finally:
        stop.set()
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task

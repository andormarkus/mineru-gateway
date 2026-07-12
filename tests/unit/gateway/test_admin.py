"""Phase 6 tests: admin API (read-only workers + emergency intent updates + cache)."""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import uvicorn
from httpx import ASGITransport, AsyncClient
from tests.fakes.worker import create_fake_worker_app

from mineru_gateway.config import get_settings
from mineru_gateway.db.base import init_engine, shutdown_engine
from mineru_gateway.db.models import Worker


@pytest_asyncio.fixture
async def admin_client() -> AsyncIterator[AsyncClient]:
    """A gateway client with a running FakeWorker for admin tests."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    fake_state = type("S", (), {"fail_next": 0})()
    fake_app = create_fake_worker_app(fake_state)  # type: ignore[arg-type]
    config = uvicorn.Config(fake_app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.3)

    try:
        import os
        from unittest.mock import AsyncMock, patch

        from tests.fakes.store import InMemoryStore

        from mineru_gateway.config import load_settings, reset_settings_cache
        from mineru_gateway.gateway.app import create_app

        os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        reset_settings_cache()
        load_settings()

        init_engine("sqlite+aiosqlite:///:memory:")
        from tests.db_helpers import create_all_tables

        await create_all_tables()
        with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=InMemoryStore())):
            application = create_app()
            async with application.router.lifespan_context(application):
                transport = ASGITransport(app=application)
                async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                    yield client
        await shutdown_engine()
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


async def _seed_controller_worker(client: AsyncClient, worker_id: str) -> None:
    from mineru_gateway.db.base import get_db_session

    settings = get_settings()
    async with get_db_session() as session:
        session.add(
            Worker(
                id=worker_id,
                provider=settings.cloud.provider,
                deployment_id=settings.deployment_id,
                base_url="http://localhost:9001",
                desired_state="running",
                cloud_state="running",
                healthy=True,
                instance_id=f"i-{worker_id}",
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_list_workers_read_only(admin_client: AsyncClient) -> None:
    await _seed_controller_worker(admin_client, "w-admin-1")

    resp = await admin_client.get("/admin/workers")
    assert resp.status_code == 200
    workers = resp.json()["workers"]
    assert any(w["id"] == "w-admin-1" for w in workers)
    worker = next(w for w in workers if w["id"] == "w-admin-1")
    assert worker["desired_state"] == "running"
    assert worker["cloud_state"] == "running"


@pytest.mark.asyncio
async def test_register_worker_removed(admin_client: AsyncClient) -> None:
    resp = await admin_client.post(
        "/admin/workers", json={"worker_id": "w-admin-1", "base_url": "http://localhost:9001", "source": "static"}
    )
    assert resp.status_code == 405


@pytest.mark.asyncio
async def test_drain_worker_sets_draining_flag(admin_client: AsyncClient) -> None:
    await _seed_controller_worker(admin_client, "w-drain")

    resp = await admin_client.post("/admin/workers/w-drain/drain", json={"reason": "test drain"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == "w-drain"
    assert body["draining"] is True
    assert body["drain_target"] == "stopped"
    assert body["desired_state"] == "running"


@pytest.mark.asyncio
async def test_drain_terminate_defers_desired_state(admin_client: AsyncClient) -> None:
    await _seed_controller_worker(admin_client, "w-term")

    resp = await admin_client.post("/admin/workers/w-term/drain", json={"reason": "emergency", "terminate": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["draining"] is True
    assert body["drain_target"] == "terminated"
    assert body["desired_state"] == "running"


@pytest.mark.asyncio
async def test_rotate_worker_sets_rotation_requested(admin_client: AsyncClient) -> None:
    await _seed_controller_worker(admin_client, "w-rotate")

    resp = await admin_client.post("/admin/workers/w-rotate/rotate", json={"reason": "test rotate"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == "w-rotate"
    assert body["rotation_requested"] is True


@pytest.mark.asyncio
async def test_recover_worker_resets_failures(admin_client: AsyncClient) -> None:
    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import Worker

    await _seed_controller_worker(admin_client, "w-recover")
    async with get_db_session() as session:
        row = await session.get(Worker, "w-recover")
        assert row is not None
        row.failure_count = 5
        row.last_error = "boom"
        row.healthy = False
        row.stalled_at = row.created_at
        row.draining = True
        row.drain_target = "terminated"
        await session.commit()

    resp = await admin_client.post("/admin/workers/w-recover/recover", json={"reason": "manual reset"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["failure_count"] == 0
    assert body["last_error"] == "boom"
    assert body["healthy"] is False
    assert body["stalled_at"] is None
    assert body["draining"] is False
    assert body["drain_target"] is None


@pytest.mark.asyncio
async def test_cache_sweep_empty(admin_client: AsyncClient) -> None:
    resp = await admin_client.post("/admin/cache/sweep")
    assert resp.status_code == 200
    assert resp.json()["removed"] == 0


@pytest.mark.asyncio
async def test_get_unknown_worker_404(admin_client: AsyncClient) -> None:
    resp = await admin_client.get("/admin/workers/nonexistent")
    assert resp.status_code == 404

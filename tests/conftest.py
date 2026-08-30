"""Shared pytest fixtures for mineru-gateway.

Tier-1 (FakeWorker) lives in tests/fakes/worker.py. This file wires up the
FastAPI test client, an isolated settings singleton per test, and the FakeWorker
ASGI app. Later phases add: sqlite engine, moto (AWS), testcontainers, respx.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.db.base import get_db_session, init_engine, shutdown_engine
from tests.db_helpers import create_all_tables
from tests.fakes.store import InMemoryStore

# --- event loop --------------------------------------------------------------
# pytest-asyncio is in auto mode (pyproject.toml). One loop per test function.


@pytest.fixture
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Fresh event loop per test (avoids cross-test async state leakage)."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# --- settings isolation ------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_settings(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure each test starts with an uncached settings singleton.

    Uses ``config.example.yaml`` so a local ``config.yaml`` (sandbox/prod) does not
    leak into unit tests. Tests that need specific config call ``reset_settings_cache()``
    then ``load_settings(...)`` or set env vars before importing the app.

    Auth stays off for the test base even though the example config ships it on
    (the example models a production-safe deployment; tests do not send keys).

    Skipped for E2E tests — they manage settings via session fixtures and env.
    """
    if request.node.get_closest_marker("e2e") is not None:
        yield
        return

    from pathlib import Path

    from mineru_gateway.config import load_settings, reset_settings_cache

    monkeypatch.setenv("MINERU_GATEWAY_AUTH__ENABLED", "false")
    test_config = Path(__file__).resolve().parent.parent / "config.example.yaml"
    reset_settings_cache()
    load_settings(config_path=test_config)
    yield
    reset_settings_cache()


# --- in-memory DB session ----------------------------------------------------


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    """Isolated sqlite schema + session for unit tests."""
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()
    session = get_db_session()
    try:
        yield session
    finally:
        await session.close()
        await shutdown_engine()


@pytest.fixture
def memory_store() -> InMemoryStore:
    """Fresh in-memory object store."""
    return InMemoryStore()


# --- Postgres (testcontainers or explicit URL) --------------------------------


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """Async PostgreSQL URL for migration and advisory-lock tests.

    Uses ``MINERU_GATEWAY_TEST_POSTGRES_URL`` when set; otherwise starts a
    ``postgres:16-alpine`` testcontainer (requires Docker + integration extra).
    """
    import os

    explicit = os.environ.get("MINERU_GATEWAY_TEST_POSTGRES_URL")
    if explicit:
        yield explicit
        return

    pytest.importorskip("testcontainers")
    from testcontainers.postgres import PostgresContainer

    try:
        with PostgresContainer("postgres:16-alpine") as postgres:
            sync_url = postgres.get_connection_url()
            if "+asyncpg" not in sync_url and "://" in sync_url:
                _scheme, rest = sync_url.split("://", 1)
                sync_url = f"postgresql+asyncpg://{rest}"
            yield sync_url
    except Exception as exc:
        pytest.skip(f"PostgreSQL testcontainer unavailable: {exc}")


# --- Gateway app + async client ---------------------------------------------


@pytest_asyncio.fixture
async def app() -> AsyncIterator[Any]:
    """A fresh gateway FastAPI app with default (test) settings."""
    import os
    from unittest.mock import AsyncMock, patch

    from mineru_gateway.config import load_settings, reset_settings_cache
    from mineru_gateway.db.base import init_engine
    from mineru_gateway.gateway.app import create_app
    from tests.db_helpers import create_all_tables
    from tests.fakes.store import InMemoryStore

    os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    reset_settings_cache()
    load_settings()

    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()

    with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=InMemoryStore())):
        application = create_app()
        # Run the lifespan so app.state.settings is populated.
        async with application.router.lifespan_context(application):
            yield application


@pytest_asyncio.fixture
async def client(app: Any) -> AsyncIterator[AsyncClient]:
    """Async HTTP client wired to the gateway via ASGI transport (no socket)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


@pytest_asyncio.fixture
async def app_no_store() -> AsyncIterator[Any]:
    """Gateway app fixture with object store unavailable (degraded ingest mode)."""
    import os
    from unittest.mock import AsyncMock, patch

    from mineru_gateway.config import load_settings, reset_settings_cache
    from mineru_gateway.db.base import init_engine
    from mineru_gateway.gateway.app import create_app
    from tests.db_helpers import create_all_tables

    os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    reset_settings_cache()
    load_settings()

    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()

    with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=None)):
        application = create_app()
        async with application.router.lifespan_context(application):
            yield application


# --- FakeWorker (Tier-1) -----------------------------------------------------


@pytest.fixture
def fake_worker_state() -> Any:
    """A fresh, scriptable FakeWorkerState. Tests mutate this directly."""
    from tests.fakes.worker import FakeWorkerState

    return FakeWorkerState()


@pytest_asyncio.fixture
async def fake_worker_client(fake_worker_state: Any) -> AsyncIterator[AsyncClient]:
    """Async HTTP client talking to an in-process FakeWorker (no socket).

    Use this to test dispatch logic: register the worker's base_url with the
    gateway's worker pool, then drive it through this client.
    """
    from tests.fakes.worker import create_fake_worker_app

    app = create_fake_worker_app(fake_worker_state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://fake-worker") as ac:
        yield ac


# --- Gateway app with a registered FakeWorker (full dispatch test) -----------


@pytest_asyncio.fixture
async def gateway_with_worker(fake_worker_state: Any) -> AsyncIterator[tuple[AsyncClient, Any, str]]:
    """Boot the full gateway with one FakeWorker registered.

    Yields ``(client, worker_state, worker_id)``. The gateway talks to the
    FakeWorker over a real socket (the worker needs a reachable URL the
    gateway's httpx client can hit).
    """
    import contextlib
    import socket

    import uvicorn

    from tests.fakes.worker import create_fake_worker_app

    # Bind a free port for the FakeWorker, then run uvicorn in a background task.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    bound_port = sock.getsockname()[1]
    sock.close()
    worker_base_url = f"http://127.0.0.1:{bound_port}"

    fake_app = create_fake_worker_app(fake_worker_state)
    config = uvicorn.Config(fake_app, host="127.0.0.1", port=bound_port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # Give the server a moment to bind.
    await asyncio.sleep(0.3)

    try:
        # Boot the gateway app (uses lifespan → inits DB, registry, object store).
        import os
        from unittest.mock import AsyncMock, patch

        from mineru_gateway.config import load_settings, reset_settings_cache
        from mineru_gateway.db.base import init_engine
        from mineru_gateway.gateway.app import create_app

        os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        reset_settings_cache()
        load_settings()

        init_engine("sqlite+aiosqlite:///:memory:")
        from tests.db_helpers import create_all_tables

        await create_all_tables()
        with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=InMemoryStore(name="test"))):
            application = create_app()
            async with application.router.lifespan_context(application):
                application.state.object_store = InMemoryStore(name="test")
                worker_id = "fake-worker-1"

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
                            base_url=worker_base_url,
                            desired_state="running",
                            cloud_state="running",
                            healthy=True,
                        )
                    )
                    await session.commit()

                transport = ASGITransport(app=application)
                async with AsyncClient(transport=transport, base_url="http://testserver") as gw_client:
                    yield gw_client, fake_worker_state, worker_id
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await server_task

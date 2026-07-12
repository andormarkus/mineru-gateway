"""API-key auth middleware tests."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from tests.db_helpers import create_all_tables
from tests.fakes.store import InMemoryStore

from mineru_gateway.config import load_settings, reset_settings_cache
from mineru_gateway.db.base import init_engine
from mineru_gateway.gateway.app import create_app


@pytest.mark.asyncio
async def test_auth_rejects_missing_key() -> None:
    os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    os.environ["MINERU_GATEWAY_AUTH__ENABLED"] = "true"
    os.environ["MINERU_GATEWAY_AUTH__API_KEY"] = "secret-key"
    reset_settings_cache()
    load_settings()
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()

    with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=InMemoryStore())):
        application = create_app()
        async with application.router.lifespan_context(application):
            transport = ASGITransport(app=application)
            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                resp = await client.get("/tasks/some-id")
                assert resp.status_code == 401

    os.environ.pop("MINERU_GATEWAY_AUTH__ENABLED", None)
    os.environ.pop("MINERU_GATEWAY_AUTH__API_KEY", None)


@pytest.mark.asyncio
async def test_auth_allows_key_and_public_health() -> None:
    os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    os.environ["MINERU_GATEWAY_AUTH__ENABLED"] = "true"
    os.environ["MINERU_GATEWAY_AUTH__API_KEY"] = "secret-key"
    reset_settings_cache()
    load_settings()
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()

    with patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=InMemoryStore())):
        application = create_app()
        async with application.router.lifespan_context(application):
            transport = ASGITransport(app=application)
            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                health = await client.get("/health")
                assert health.status_code == 200

                missing = await client.get("/tasks/some-id")
                assert missing.status_code == 401

                authed = await client.get("/tasks/some-id", headers={"x-api-key": "secret-key"})
                assert authed.status_code == 404

    os.environ.pop("MINERU_GATEWAY_AUTH__ENABLED", None)
    os.environ.pop("MINERU_GATEWAY_AUTH__API_KEY", None)

"""Readiness probe tests."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from tests.db_helpers import create_all_tables
from tests.fakes.store import InMemoryStore

from mineru_gateway.config import get_settings, load_settings, reset_settings_cache
from mineru_gateway.db.base import init_engine
from mineru_gateway.gateway.readiness import check_readiness


@pytest.mark.asyncio
async def test_readiness_ok(client: AsyncClient) -> None:
    resp = await client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


@pytest.mark.asyncio
async def test_readiness_fails_without_storage() -> None:
    reset_settings_cache()
    load_settings()
    settings = get_settings()
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()
    ready, detail = await check_readiness(settings=settings, store=None)
    assert ready is False
    assert "storage" in detail


@pytest.mark.asyncio
async def test_readiness_ok_with_store() -> None:
    reset_settings_cache()
    load_settings()
    settings = get_settings()
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()
    store = InMemoryStore()
    ready, detail = await check_readiness(settings=settings, store=store)
    assert ready is True
    assert detail == "ok"

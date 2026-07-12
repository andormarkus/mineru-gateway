"""CacheService unit tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore

from mineru_gateway.config import get_settings
from mineru_gateway.db.models import CacheEntry
from mineru_gateway.scheduler.cache_service import CacheService
from mineru_gateway.tasks.storage import cache_object_key


@pytest.mark.asyncio
async def test_invalidate_removes_object_and_row(db_session: AsyncSession) -> None:
    store = InMemoryStore(name="cache-svc")
    cache_key = "abc123"
    object_key = cache_object_key(cache_key)
    await store.put(object_key, b"zip-bytes")

    db_session.add(
        CacheEntry(
            cache_key=cache_key,
            content_sha256="deadbeef",
            options_hash="opts",
            backend="pipeline",
            parse_method="auto",
            effort="medium",
            object_key=object_key,
            result_format="zip",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    await db_session.commit()
    await db_session.close()

    svc = CacheService(get_settings(), store)
    assert await svc.invalidate(cache_key) is True
    assert not await store.exists(object_key)

    from mineru_gateway.db.base import get_db_session

    async with get_db_session() as session:
        assert await session.get(CacheEntry, cache_key) is None


@pytest.mark.asyncio
async def test_sweep_removes_expired_object_and_row(db_session: AsyncSession) -> None:
    store = InMemoryStore(name="cache-sweep")
    cache_key = "expired1"
    object_key = cache_object_key(cache_key)
    await store.put(object_key, b"old")

    db_session.add(
        CacheEntry(
            cache_key=cache_key,
            content_sha256="deadbeef",
            options_hash="opts",
            backend="pipeline",
            parse_method="auto",
            effort="medium",
            object_key=object_key,
            result_format="zip",
            expires_at=datetime.now(UTC) - timedelta(minutes=1),
        )
    )
    await db_session.commit()
    await db_session.close()

    removed = await CacheService(get_settings(), store).sweep_expired()
    assert removed == 1
    assert not await store.exists(object_key)

    from mineru_gateway.db.base import get_db_session

    async with get_db_session() as session:
        assert await session.get(CacheEntry, cache_key) is None

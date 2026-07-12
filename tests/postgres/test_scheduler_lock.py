"""PostgreSQL scheduler advisory lock tests."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from mineru_gateway.scheduler.lock import PostgresAdvisoryLock

pytestmark = pytest.mark.postgres


@pytest.mark.asyncio
async def test_postgres_advisory_lock_exclusive(postgres_url: str) -> None:
    engine_a = create_async_engine(postgres_url)
    engine_b = create_async_engine(postgres_url)
    lock_a = PostgresAdvisoryLock(engine_a)
    lock_b = PostgresAdvisoryLock(engine_b)
    try:
        assert await lock_a.acquire() is True
        assert await lock_b.acquire() is False
        assert await lock_a.verify() is True
        await lock_a.release()
        assert await lock_b.acquire() is True
        await lock_b.release()
    finally:
        await engine_a.dispose()
        await engine_b.dispose()


@pytest.mark.asyncio
async def test_postgres_advisory_lock_lost_on_connection_close(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    lock = PostgresAdvisoryLock(engine)
    try:
        assert await lock.acquire() is True
        assert lock._conn is not None
        await lock._conn.close()
        lock._conn = None
        assert await lock.verify() is False
    finally:
        await engine.dispose()

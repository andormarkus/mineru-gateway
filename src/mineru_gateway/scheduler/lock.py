"""Scheduler ownership — PostgreSQL advisory lock or SQLite no-op."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from mineru_gateway.config import get_settings

SCHEDULER_LOCK_KEY = 0x4D475F534348  # stable key: "MG_SCH"

logger = logging.getLogger(__name__)


class _NoOpLock:
    """SQLite / single-process: assume one scheduler."""

    @property
    def held(self) -> bool:
        return True

    async def release(self) -> None:
        return


class PostgresAdvisoryLock:
    """Session-level PostgreSQL advisory lock on a dedicated connection."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._conn: AsyncConnection | None = None
        self._held = False

    @property
    def held(self) -> bool:
        return self._held and self._conn is not None

    async def acquire(self) -> bool:
        if self._held:
            return True
        self._conn = await self._engine.connect()
        result = await self._conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": SCHEDULER_LOCK_KEY})
        acquired = bool(result.scalar())
        if acquired:
            self._held = True
            logger.info("Scheduler advisory lock acquired")
        else:
            await self._conn.close()
            self._conn = None
            logger.debug("Scheduler advisory lock not acquired — another scheduler is active")
        return acquired

    async def release(self) -> None:
        if self._conn is None:
            return
        if self._held:
            await self._conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": SCHEDULER_LOCK_KEY})
            self._held = False
            logger.info("Scheduler advisory lock released")
        await self._conn.close()
        self._conn = None

    async def verify(self) -> bool:
        """Return False if the dedicated lock connection is dead."""
        if not self._held or self._conn is None:
            return False
        try:
            await self._conn.execute(text("SELECT 1"))
            return True
        except Exception:
            self._held = False
            return False


@asynccontextmanager
async def scheduler_lock(
    database_url: str | None = None, *, retry_interval: float = 5.0
) -> AsyncIterator[PostgresAdvisoryLock | _NoOpLock]:
    """Acquire scheduler lock, retrying until acquired. Yields lock handle."""
    url = database_url or get_settings().database_url
    if url.startswith("sqlite"):
        yield _NoOpLock()
        return

    engine = create_async_engine(url)
    lock = PostgresAdvisoryLock(engine)
    try:
        while not await lock.acquire():
            logger.info("Waiting for scheduler advisory lock (retry in %.0fs)", retry_interval)
            await asyncio.sleep(retry_interval)
        yield lock
    finally:
        await lock.release()
        await engine.dispose()

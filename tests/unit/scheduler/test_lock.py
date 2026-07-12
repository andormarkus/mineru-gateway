"""Scheduler ownership — SQLite no-op lock."""

from __future__ import annotations

import pytest

from mineru_gateway.scheduler.lock import scheduler_lock


@pytest.mark.asyncio
async def test_sqlite_scheduler_lock_is_noop() -> None:
    """On sqlite, the scheduler lock is a no-op that always reports held."""
    async with scheduler_lock("sqlite+aiosqlite:///:memory:") as lock:
        assert lock.held is True

    async with scheduler_lock("sqlite+aiosqlite:///:memory:") as lock2:
        assert lock2.held is True

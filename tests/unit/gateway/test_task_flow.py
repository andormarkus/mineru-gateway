"""Tests for synchronous route DB polling."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.gateway.task_flow import is_poll_complete, poll_task_until_terminal


@pytest.mark.asyncio
async def test_poll_returns_immediately_on_completed(db_session: AsyncSession) -> None:
    async with get_db_session() as session:
        session.add(
            Task(
                task_id="done-fast",
                status="completed",
                backend="pipeline",
                file_names=["a.pdf"],
                result_key="results/done-fast.zip",
                completed_at=datetime.now(UTC),
            )
        )
        await session.commit()
    await db_session.close()

    row = await poll_task_until_terminal("done-fast", route="test")
    assert row is not None
    assert row.status == "completed"
    assert row.result_key == "results/done-fast.zip"


@pytest.mark.asyncio
async def test_poll_returns_immediately_on_failed(db_session: AsyncSession) -> None:
    async with get_db_session() as session:
        session.add(
            Task(
                task_id="fail-fast",
                status="failed",
                backend="pipeline",
                file_names=["a.pdf"],
                error="boom",
                completed_at=datetime.now(UTC),
            )
        )
        await session.commit()
    await db_session.close()

    row = await poll_task_until_terminal("fail-fast", route="test")
    assert row is not None
    assert row.status == "failed"


@pytest.mark.asyncio
async def test_poll_returns_immediately_on_expired(db_session: AsyncSession) -> None:
    async with get_db_session() as session:
        session.add(
            Task(
                task_id="expired-fast",
                status="expired",
                backend="pipeline",
                file_names=["a.pdf"],
                error="sla",
                completed_at=datetime.now(UTC),
            )
        )
        await session.commit()
    await db_session.close()

    row = await poll_task_until_terminal("expired-fast", route="test")
    assert row is not None
    assert row.status == "expired"


@pytest.mark.asyncio
async def test_poll_returns_none_when_task_disappears(db_session: AsyncSession) -> None:
    await db_session.close()
    row = await poll_task_until_terminal("missing-task", route="test")
    assert row is None


@pytest.mark.asyncio
async def test_poll_does_not_sleep_when_already_terminal(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Terminal tasks return on the first DB read — no polling sleep."""
    import asyncio

    async with get_db_session() as session:
        session.add(
            Task(
                task_id="done-no-sleep",
                status="completed",
                backend="pipeline",
                file_names=["a.pdf"],
                result_key="results/done-no-sleep.zip",
                completed_at=datetime.now(UTC),
            )
        )
        await session.commit()
    await db_session.close()

    slept: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    row = await poll_task_until_terminal("done-no-sleep", route="file_parse")
    assert row is not None
    assert row.status == "completed"
    assert slept == []


def test_is_poll_complete_requires_result_key() -> None:
    row = Task(task_id="t", status="completed", backend="pipeline", file_names=["a.pdf"])
    assert not is_poll_complete(row)
    row.result_key = "results/t.zip"
    assert is_poll_complete(row)


def test_is_poll_complete_on_client_sla_expiry() -> None:
    row = Task(
        task_id="sla",
        status="processing",
        backend="pipeline",
        file_names=["a.pdf"],
        client_expired_at=datetime.now(UTC),
    )
    assert is_poll_complete(row)

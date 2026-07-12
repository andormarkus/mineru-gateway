"""Task error classification and SLA expiry."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_session_factory
from mineru_gateway.db.models import Task
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.tasks.errors import ErrorClass, classify_error, should_retry


def test_classify_timeout_is_infra() -> None:
    assert classify_error(httpx.TimeoutException("timeout")) == ErrorClass.INFRA


def test_classify_connection_error_is_infra() -> None:
    assert classify_error(httpx.ConnectError("refused")) == ErrorClass.INFRA


def test_classify_404_is_content() -> None:
    resp = httpx.Response(404, request=httpx.Request("GET", "http://x"))
    err = httpx.HTTPStatusError("not found", request=resp.request, response=resp)
    assert classify_error(err) == ErrorClass.CONTENT


def test_classify_500_is_infra() -> None:
    resp = httpx.Response(503, request=httpx.Request("GET", "http://x"))
    err = httpx.HTTPStatusError("server error", request=resp.request, response=resp)
    assert classify_error(err) == ErrorClass.INFRA


def test_should_retry_infra_only() -> None:
    assert should_retry(httpx.TimeoutException("t")) is True
    resp = httpx.Response(400, request=httpx.Request("GET", "http://x"))
    err = httpx.HTTPStatusError("bad request", request=resp.request, response=resp)
    assert should_retry(err) is False


@pytest.mark.asyncio
async def test_expire_client_sla_tasks(db_session: AsyncSession) -> None:
    """SLA expiry sets client_expired_at without changing execution status."""
    old = datetime.now(UTC) - timedelta(hours=2)
    t1 = Task(task_id="stale-1", status="processing", backend="pipeline", file_names=["a.pdf"], dispatch_started_at=old)
    t2 = Task(
        task_id="fresh-1",
        status="processing",
        backend="pipeline",
        file_names=["b.pdf"],
        dispatch_started_at=datetime.now(UTC),
    )
    async with get_session_factory()() as session:
        session.add_all([t1, t2])
        await session.commit()

    settings = get_settings()
    repo = TaskRepository.for_gateway(settings)
    expired = await repo.expire_client_sla_tasks(sla_seconds=3600)
    assert expired == 1

    async with get_session_factory()() as session:
        stale_row = await session.get(Task, "stale-1")
        fresh_row = await session.get(Task, "fresh-1")
    assert stale_row is not None
    assert stale_row.status == "processing"
    assert stale_row.client_expired_at is not None
    assert fresh_row is not None
    assert fresh_row.client_expired_at is None


@pytest.mark.asyncio
async def test_expire_client_sla_tasks_includes_storing_result(db_session: AsyncSession) -> None:
    old = datetime.now(UTC) - timedelta(hours=2)
    async with get_session_factory()() as session:
        session.add(
            Task(
                task_id="storing-sla",
                status="storing_result",
                backend="pipeline",
                file_names=["a.pdf"],
                dispatch_started_at=old,
            )
        )
        await session.commit()

    settings = get_settings()
    repo = TaskRepository.for_gateway(settings)
    expired = await repo.expire_client_sla_tasks(sla_seconds=3600)
    assert expired == 1

    async with get_session_factory()() as session:
        row = await session.get(Task, "storing-sla")
    assert row is not None
    assert row.status == "storing_result"
    assert row.client_expired_at is not None
    assert row.error is None

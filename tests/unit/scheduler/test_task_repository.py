"""TaskRepository unit tests."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore

from mineru_gateway.config import get_settings
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.tasks.status import TASK_DISPATCHING, TASK_QUEUED
from mineru_gateway.util.datetime import ensure_aware_utc, now_utc


async def _add_worker(db: AsyncSession, *, worker_id: str = "w1") -> None:
    settings = get_settings()
    db.add(
        Worker(
            id=worker_id,
            provider=settings.cloud.provider,
            deployment_id=settings.deployment_id,
            base_url="http://w:8000",
            desired_state="running",
            cloud_state="running",
            healthy=True,
        )
    )
    await db.commit()


@pytest.mark.asyncio
async def test_claim_assigns_worker_atomically(db_session: AsyncSession) -> None:
    await _add_worker(db_session)
    db_session.add(
        Task(task_id="queued-1", status=TASK_QUEUED, backend="pipeline", file_names=["doc.pdf"], payload_key="p1")
    )
    await db_session.commit()

    repo = TaskRepository(get_settings(), InMemoryStore(), workers=WorkerRepository(get_settings()))
    claim = await repo._claim_next_dispatch()
    assert claim is not None
    task, worker = claim
    assert task.status == TASK_DISPATCHING
    assert task.worker_id == worker.id
    assert task.dispatch_started_at is not None

    row = await db_session.get(Task, "queued-1")
    assert row is not None
    assert row.status == TASK_DISPATCHING
    assert row.worker_id == worker.id


@pytest.mark.asyncio
async def test_claim_skips_when_no_worker_available(db_session: AsyncSession) -> None:
    db_session.add(
        Task(task_id="queued-2", status=TASK_QUEUED, backend="pipeline", file_names=["doc.pdf"], payload_key="p2")
    )
    await db_session.commit()

    repo = TaskRepository(get_settings(), InMemoryStore(), workers=WorkerRepository(get_settings()))
    assert await repo._claim_next_dispatch() is None

    row = await db_session.get(Task, "queued-2")
    assert row is not None
    assert row.status == TASK_QUEUED
    assert row.worker_id is None


@pytest.mark.asyncio
async def test_recover_stale_dispatch_claims(db_session: AsyncSession) -> None:
    """Dispatching tasks without upstream IDs are re-queued after restart."""
    stale_at = now_utc() - timedelta(seconds=300)
    db_session.add(
        Task(
            task_id="stale-1",
            status=TASK_DISPATCHING,
            dispatch_started_at=stale_at,
            upstream_task_id=None,
            backend="pipeline",
            file_names=["doc.pdf"],
        )
    )
    await db_session.commit()

    repo = TaskRepository(get_settings(), InMemoryStore(), workers=WorkerRepository(get_settings()))
    recovered = await repo.recover_stale_dispatch_claims()
    assert recovered == 1

    await db_session.refresh(await db_session.get(Task, "stale-1"))
    row = await db_session.get(Task, "stale-1")
    assert row is not None
    assert row.status == TASK_QUEUED
    assert row.dispatch_started_at is None
    assert row.worker_id is None


@pytest.mark.asyncio
async def test_defer_task_poll_advances_next_poll_at(db_session: AsyncSession) -> None:
    db_session.add(Task(task_id="poll-1", status="processing", backend="pipeline", file_names=["doc.pdf"]))
    await db_session.commit()

    repo = TaskRepository.for_gateway(get_settings())
    await repo.defer_task_poll("poll-1")

    row = await db_session.get(Task, "poll-1")
    assert row is not None
    assert row.next_poll_at is not None
    assert ensure_aware_utc(row.next_poll_at) > now_utc()

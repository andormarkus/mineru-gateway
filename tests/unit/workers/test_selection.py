"""Worker selection dispatchability tests."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.config import get_settings
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scheduler.worker_repository import WorkerRepository


async def _add_worker(db: AsyncSession, **kwargs: object) -> Worker:
    settings = get_settings()
    defaults = {
        "id": "w1",
        "provider": settings.cloud.provider,
        "deployment_id": settings.deployment_id,
        "base_url": "http://w:8000",
        "desired_state": "running",
        "cloud_state": "running",
        "healthy": True,
    }
    defaults.update(kwargs)
    worker = Worker(**defaults)  # type: ignore[arg-type]
    db.add(worker)
    await db.commit()
    return worker


@pytest.mark.asyncio
async def test_selects_dispatchable_worker(db_session: AsyncSession) -> None:
    await _add_worker(db_session)
    repo = WorkerRepository(get_settings())
    chosen = await repo.acquire_dispatchable(db_session)
    assert chosen is not None
    assert chosen.id == "w1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"desired_state": "stopped"},
        {"cloud_state": "pending"},
        {"healthy": False},
        {"base_url": None},
        {"draining": True},
        {"failure_count": get_settings().reconciliation.max_failure_count, "healthy": False, "draining": True},
    ],
)
async def test_rejects_non_dispatchable(db_session: AsyncSession, overrides: dict) -> None:
    await _add_worker(db_session, id="bad", **overrides)
    repo = WorkerRepository(get_settings())
    assert await repo.acquire_dispatchable(db_session) is None


@pytest.mark.asyncio
async def test_prefers_worker_with_fewer_active_tasks(db_session: AsyncSession) -> None:
    await _add_worker(db_session, id="busy", last_active_at=None)
    await _add_worker(db_session, id="idle", last_active_at=None)
    db_session.add(Task(task_id="t1", worker_id="busy", status="processing", backend="pipeline", file_names=["a.pdf"]))
    await db_session.commit()
    repo = WorkerRepository(get_settings())
    chosen = await repo.acquire_dispatchable(db_session)
    assert chosen is not None
    assert chosen.id == "idle"


@pytest.mark.asyncio
async def test_rejects_worker_at_capacity(db_session: AsyncSession) -> None:
    settings = get_settings()
    target = settings.scaling.target_per_worker
    await _add_worker(db_session, id="full")
    for i in range(target):
        db_session.add(
            Task(task_id=f"t{i}", worker_id="full", status="processing", backend="pipeline", file_names=["a.pdf"])
        )
    await db_session.commit()
    repo = WorkerRepository(settings)
    assert await repo.acquire_dispatchable(db_session) is None

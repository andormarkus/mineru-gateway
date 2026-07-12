"""Phase 1 tests: DB models, async engine/session, CRUD round-trips.

Uses an in-memory sqlite database (no Docker, no network).
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.db.models import CacheEntry, ScalingEvent, Task, Worker


@pytest.mark.asyncio
async def test_worker_round_trip(db_session: AsyncSession) -> None:
    worker = Worker(
        id="cloud-1",
        provider="aws",
        deployment_id="dev-local",
        base_url="http://localhost:8001",
        desired_state="running",
        cloud_state="running",
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(worker)

    fetched = (await db_session.execute(select(Worker).where(Worker.id == "cloud-1"))).scalar_one()
    assert fetched.provider == "aws"
    assert fetched.deployment_id == "dev-local"
    assert fetched.desired_state == "running"
    assert fetched.healthy is False
    assert fetched.draining is False


@pytest.mark.asyncio
async def test_task_round_trip(db_session: AsyncSession) -> None:
    worker = Worker(id="w1", provider="aws", deployment_id="dev-local", base_url="http://w1")
    db_session.add(worker)

    task = Task(
        task_id="task-uuid-1",
        worker_id="w1",
        upstream_task_id="upstream-1",
        backend="hybrid-engine",
        parse_method="auto",
        file_names=["doc.pdf"],
        status="queued",
        source="tasks",
    )
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)

    fetched = (await db_session.execute(select(Task).where(Task.task_id == "task-uuid-1"))).scalar_one()
    assert fetched.status == "queued"
    assert fetched.source == "tasks"
    assert fetched.file_names == ["doc.pdf"]
    assert fetched.worker is not None
    assert fetched.worker.id == "w1"


@pytest.mark.asyncio
async def test_cache_entry_round_trip(db_session: AsyncSession) -> None:
    cache = CacheEntry(
        cache_key="abc123hash",
        content_sha256="rawsha256",
        options_hash="ophash",
        backend="hybrid-engine",
        parse_method="auto",
        effort="medium",
        object_key="cache/abc123hash",
        size_bytes=1024,
    )
    db_session.add(cache)
    await db_session.commit()

    fetched = (await db_session.execute(select(CacheEntry).where(CacheEntry.cache_key == "abc123hash"))).scalar_one()
    assert fetched.hit_count == 0
    assert fetched.size_bytes == 1024


@pytest.mark.asyncio
async def test_scaling_event_round_trip(db_session: AsyncSession) -> None:
    worker = Worker(id="cloud-1", provider="aws", deployment_id="dev-local")
    db_session.add(worker)
    await db_session.flush()

    event = ScalingEvent(
        worker_id="cloud-1", action="start", reason="queue depth 8 > target 4", triggered_by="autoscaler"
    )
    db_session.add(event)
    await db_session.commit()

    events = (await db_session.execute(select(ScalingEvent).where(ScalingEvent.worker_id == "cloud-1"))).scalars().all()
    assert len(events) == 1
    assert events[0].action == "start"
    assert events[0].triggered_by == "autoscaler"

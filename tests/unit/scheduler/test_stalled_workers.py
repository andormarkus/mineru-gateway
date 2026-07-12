"""Stalled-worker force-termination: failure transition, grace period, reconciliation."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore
from tests.unit.scheduler.test_scheduler import _FakeProvider

from mineru_gateway.cloud.types import (
    TAG_DEPLOYMENT,
    TAG_GENERATION,
    TAG_MANAGED,
    TAG_ROLE,
    TAG_ROLE_WORKER,
    TAG_WORKER_ID,
    DiscoveredInstance,
    InstanceState,
)
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scheduler.scheduler import Scheduler
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.util.datetime import now_utc


async def _seed_stalled_worker(
    db: AsyncSession,
    wid: str,
    *,
    failure_count: int | None = None,
    stalled_at=None,
    desired_state: str = "running",
    draining: bool = True,
) -> None:
    settings = get_settings()
    max_failures = settings.reconciliation.max_failure_count
    count = failure_count if failure_count is not None else max_failures
    worker = Worker(
        id=wid,
        provider=settings.cloud.provider,
        deployment_id=settings.deployment_id,
        base_url=f"http://{wid}:8000",
        instance_id=f"i-{wid}",
        desired_state=desired_state,
        cloud_state="running",
        healthy=False,
        draining=draining,
        drain_target="terminated" if draining else None,
        failure_count=count,
        stalled_at=stalled_at if stalled_at is not None else (now_utc() if count >= max_failures else None),
    )
    db.add(worker)
    await db.commit()


@pytest.mark.asyncio
async def test_record_failure_enters_stalled_state(db_session: AsyncSession) -> None:
    settings = get_settings()
    max_failures = settings.reconciliation.max_failure_count
    await _seed_stalled_worker(
        db_session, "w-fail", failure_count=max_failures - 1, draining=False, desired_state="running"
    )
    await db_session.close()

    repo = WorkerRepository(settings)
    await repo.record_failure("w-fail", "boom", retryable=True)

    async with get_db_session() as session:
        row = await session.get(Worker, "w-fail")
    assert row is not None
    assert row.failure_count == max_failures
    assert row.stalled_at is not None
    assert row.healthy is False
    assert row.draining is True
    assert row.drain_target == "terminated"
    assert row.retry_after is None

    async with get_db_session() as session:
        assert await repo.acquire_dispatchable(session) is None


@pytest.mark.asyncio
async def test_recover_worker_clears_stalled_drain(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_stalled_worker(db_session, "w-recover")
    await db_session.close()

    repo = WorkerRepository(settings)
    recovered = await repo.recover_worker("w-recover", reason="manual", requester="test")
    assert recovered is not None
    assert recovered.failure_count == 0
    assert recovered.stalled_at is None
    assert recovered.draining is False
    assert recovered.drain_target is None


@pytest.mark.asyncio
async def test_converge_terminates_stalled_worker_without_work(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_stalled_worker(db_session, "w-idle")
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=None)
        await scheduler._converge_stalled_workers()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "w-idle")
    assert row is not None
    assert row.desired_state == "terminated"


@pytest.mark.asyncio
async def test_converge_waits_during_grace_with_active_work(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_stalled_worker(db_session, "w-busy", stalled_at=now_utc() - timedelta(seconds=60))
    async with get_db_session() as session:
        session.add(
            Task(task_id="t-busy", worker_id="w-busy", status="processing", backend="pipeline", file_names=["a.pdf"])
        )
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=None)
        await scheduler._converge_stalled_workers()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "w-busy")
    assert row is not None
    assert row.desired_state == "running"


@pytest.mark.asyncio
async def test_converge_force_terminates_after_grace(db_session: AsyncSession) -> None:
    settings = get_settings()
    grace = settings.reconciliation.stalled_worker_grace_seconds
    await _seed_stalled_worker(db_session, "w-expired", stalled_at=now_utc() - timedelta(seconds=grace + 30))
    async with get_db_session() as session:
        session.add(
            Task(
                task_id="t-expired",
                worker_id="w-expired",
                status="processing",
                backend="pipeline",
                file_names=["a.pdf"],
            )
        )
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=None)
        await scheduler._converge_stalled_workers()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "w-expired")
    assert row is not None
    assert row.desired_state == "terminated"


class _StalledTerminateProvider(_FakeProvider):
    def __init__(self) -> None:
        self.terminated: list[str] = []

    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        settings = get_settings()
        tags = {
            TAG_MANAGED: "true",
            TAG_ROLE: TAG_ROLE_WORKER,
            TAG_DEPLOYMENT: settings.deployment_id,
            TAG_WORKER_ID: "w-term",
            TAG_GENERATION: "0",
        }
        return [DiscoveredInstance(instance_id="i-w-term", worker_id="w-term", state=InstanceState.RUNNING, tags=tags)]

    async def terminate(self, instance_id: str) -> None:
        self.terminated.append(instance_id)


@pytest.mark.asyncio
async def test_reconcile_terminates_stalled_worker_with_termination_intent(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_stalled_worker(db_session, "w-term", desired_state="terminated")
    await db_session.close()

    provider = _StalledTerminateProvider()
    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=provider)
        await scheduler._reconcile_workers()
    finally:
        await client.aclose()

    assert provider.terminated == ["i-w-term"]


@pytest.mark.asyncio
async def test_stalled_worker_not_serviceable(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_stalled_worker(db_session, "w-stalled")
    await db_session.close()

    repo = WorkerRepository(settings)
    assert await repo.count_serviceable_workers() == 0
    assert await repo.count_starting_workers() == 0

"""Scheduler tick sub-steps: health polling and task/result synchronization."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore

from mineru_gateway.cloud.base import ComputeProvider
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
from mineru_gateway.config import get_settings, load_settings, reset_settings_cache
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scheduler.scheduler import Scheduler
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.util.datetime import now_utc


async def _seed_worker(db: AsyncSession, wid: str, base_url: str | None = None, **kwargs: object) -> None:
    settings = get_settings()
    worker = Worker(
        id=wid,
        provider=settings.cloud.provider,
        deployment_id=settings.deployment_id,
        base_url=base_url,
        instance_id=kwargs.get("instance_id", f"i-{wid}"),
        desired_state=str(kwargs.get("desired_state", "running")),
        cloud_state=str(kwargs.get("cloud_state", "running")),
        healthy=bool(kwargs.get("healthy", True)),
        draining=bool(kwargs.get("draining", False)),
        drain_target=kwargs.get("drain_target"),  # type: ignore[arg-type]
        rotation_requested=bool(kwargs.get("rotation_requested", False)),
        ready_at=kwargs.get("ready_at"),  # type: ignore[arg-type]
    )
    db.add(worker)
    await db.commit()


def _make_scheduler(client: httpx.AsyncClient) -> Scheduler:
    return Scheduler(settings=get_settings(), store=InMemoryStore(name="test"), client=client, provider=None)


@pytest.mark.asyncio
async def test_health_check_candidates_exclude_stopping_and_draining_workers(db_session: AsyncSession) -> None:
    """Workers winding down must not be probed — EC2 stop kills the API before cloud_state clears."""
    await _seed_worker(db_session, "active", "http://active:8000", ready_at=now_utc())
    await _seed_worker(
        db_session,
        "stopping",
        "http://stopping:8000",
        desired_state="stopped",
        cloud_state="stopping",
        ready_at=now_utc(),
    )
    await _seed_worker(
        db_session, "draining", "http://draining:8000", draining=True, drain_target="stopped", ready_at=now_utc()
    )
    await db_session.close()

    repo = WorkerRepository(get_settings())
    candidates = await repo.list_health_check_candidates(limit=32)
    assert [w.id for w in candidates] == ["active"]


@pytest.mark.asyncio
async def test_scheduler_health_checks_run_concurrently(db_session: AsyncSession) -> None:
    """One slow worker health probe must not block the rest of the batch."""
    now = now_utc()
    await _seed_worker(db_session, "fast-1", "http://fast1:8000", ready_at=now)
    await _seed_worker(db_session, "slow-1", "http://slow1:8000", ready_at=now)
    await _seed_worker(db_session, "fast-2", "http://fast2:8000", ready_at=now)
    await db_session.close()

    async def slow_health(_request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.4)
        return httpx.Response(200, json={"status": "healthy"})

    with respx.mock(assert_all_called=True) as mock:
        mock.get(url__regex=r"http://fast1:8000/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        mock.get(url__regex=r"http://fast2:8000/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        mock.get(url__regex=r"http://slow1:8000/health").mock(side_effect=slow_health)

        client = httpx.AsyncClient()
        start = time.perf_counter()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    assert time.perf_counter() - start < 0.75


@pytest.mark.asyncio
async def test_scheduler_health_marks_worker_healthy(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w1", "http://w1:8000", ready_at=now_utc())
    await db_session.close()

    with respx.mock(base_url="http://w1:8000") as mock:
        mock.get("/health").mock(return_value=httpx.Response(200, json={"status": "healthy"}))
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "w1")
    assert row is not None
    assert row.healthy is True
    assert row.last_health_checked_at is not None
    assert row.last_error is None


@pytest.mark.asyncio
async def test_scheduler_health_marks_unreachable_worker_unhealthy(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w2", "http://w2:8000", ready_at=now_utc())
    await db_session.close()

    with respx.mock(base_url="http://w2:8000") as mock:
        mock.get("/health").mock(return_value=httpx.Response(503))
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "w2")
    assert row is not None
    assert row.healthy is False
    assert row.last_error is not None


@pytest.mark.asyncio
async def test_scheduler_health_skips_stopped_intent_workers(db_session: AsyncSession) -> None:
    await _seed_worker(
        db_session,
        "w-stop",
        "http://w:8000",
        desired_state="stopped",
        cloud_state="stopped",
        healthy=False,
        ready_at=now_utc(),
    )
    await db_session.close()

    with respx.mock(base_url="http://w:8000", assert_all_called=False) as mock:
        mock.get("/health").mock(return_value=httpx.Response(200, json={"status": "healthy"}))
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()
        assert not mock.calls

    async with get_db_session() as session:
        row = await session.get(Worker, "w-stop")
    assert row is not None
    assert row.desired_state == "stopped"
    assert row.cloud_state == "stopped"
    assert row.healthy is False
    assert row.last_health_checked_at is None


@pytest.mark.asyncio
async def test_scheduler_health_polls_status_port_while_not_ready(db_session: AsyncSession) -> None:
    """Never-ready workers are probed on the bootstrap-status port, not mineru-api's own port."""
    await _seed_worker(db_session, "boot-1", "http://boot1:8000")
    await db_session.close()

    with respx.mock(assert_all_called=True) as mock:
        mock.get(url__regex=r"http://boot1:8001/health").mock(
            return_value=httpx.Response(
                503, json={"status": "starting", "stage": "building_docker_image", "detail": "42s elapsed"}
            )
        )
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "boot-1")
    assert row is not None
    assert row.healthy is False
    assert row.ready_at is None
    assert row.last_error is None
    assert row.provisioning_detail is not None
    assert "building_docker_image" in row.provisioning_detail
    assert "42s elapsed" in row.provisioning_detail


@pytest.mark.asyncio
async def test_scheduler_health_marks_ready_from_status_port(db_session: AsyncSession) -> None:
    """Status port reporting "ready" flips healthy=True and sets ready_at for the first time."""
    await _seed_worker(db_session, "boot-2", "http://boot2:8000")
    await db_session.close()

    with respx.mock(assert_all_called=True) as mock:
        mock.get(url__regex=r"http://boot2:8001/health").mock(
            return_value=httpx.Response(200, json={"status": "ready", "stage": "ready", "detail": "mineru-api healthy"})
        )
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "boot-2")
    assert row is not None
    assert row.healthy is True
    assert row.ready_at is not None
    assert row.last_error is None
    assert row.provisioning_detail is None


@pytest.mark.asyncio
async def test_scheduler_health_switches_to_mineru_port_once_ready(db_session: AsyncSession) -> None:
    """Once ready_at is set, later checks poll mineru-api's own port, not the status port."""
    await _seed_worker(db_session, "boot-3", "http://boot3:8000", ready_at=now_utc())
    await db_session.close()

    with respx.mock(assert_all_called=True) as mock:
        mock.get(url__regex=r"http://boot3:8000/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        client = httpx.AsyncClient()
        try:
            await _make_scheduler(client)._refresh_worker_health()
        finally:
            await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "boot-3")
    assert row is not None
    assert row.healthy is True


@pytest.mark.asyncio
async def test_scheduler_sync_skips_tasks_with_result_key(db_session: AsyncSession) -> None:
    """Completed tasks that already have a result_key are not re-fetched."""
    reset_settings_cache()
    load_settings()

    async with get_db_session() as session:
        session.add(
            Task(
                task_id="done-1",
                status="completed",
                upstream_task_id="up-2",
                upstream_base_url="http://worker",
                backend="pipeline",
                file_names=["x.pdf"],
                result_key="results/already-done.zip",
            )
        )
        await session.commit()

    client = httpx.AsyncClient(timeout=httpx.Timeout(2.0))
    try:
        await _make_scheduler(client)._synchronize_tasks_and_results()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Task, "done-1")
    assert row is not None
    assert row.result_key == "results/already-done.zip"


class _FakeProvider(ComputeProvider):
    @property
    def name(self) -> str:
        return "aws"

    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        return []

    async def launch(self, worker_id: str, *, deployment_id: str, generation: int) -> str:
        return f"i-{worker_id}"

    async def get_state(self, instance_id: str) -> InstanceState:
        return InstanceState.RUNNING

    async def get_private_ip(self, instance_id: str) -> str | None:
        return "10.0.0.1"

    async def get_public_ip(self, instance_id: str) -> str | None:
        return "203.0.113.1"

    async def start(self, instance_id: str) -> None:
        return None

    async def stop(self, instance_id: str) -> None:
        return None

    async def terminate(self, instance_id: str) -> None:
        return None


class _OrphanProvider(_FakeProvider):
    def __init__(self) -> None:
        self.terminated: list[str] = []

    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        settings = get_settings()
        return [
            DiscoveredInstance(
                instance_id="i-orphan",
                worker_id="ghost-worker",
                state=InstanceState.RUNNING,
                tags={
                    TAG_MANAGED: "true",
                    TAG_ROLE: TAG_ROLE_WORKER,
                    TAG_DEPLOYMENT: settings.deployment_id,
                    TAG_WORKER_ID: "ghost-worker",
                    TAG_GENERATION: "0",
                },
            )
        ]

    async def terminate(self, instance_id: str) -> None:
        self.terminated.append(instance_id)


@pytest.mark.asyncio
async def test_reconcile_terminates_orphan_vms(db_session: AsyncSession) -> None:
    settings = get_settings()
    await db_session.close()

    provider = _OrphanProvider()
    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=provider)
        await scheduler._reconcile_workers()
    finally:
        await client.aclose()

    assert provider.terminated == ["i-orphan"]


class _DuplicateProvider(_FakeProvider):
    def __init__(self) -> None:
        self.terminated: list[str] = []

    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        settings = get_settings()
        tags = {
            TAG_MANAGED: "true",
            TAG_ROLE: TAG_ROLE_WORKER,
            TAG_DEPLOYMENT: settings.deployment_id,
            TAG_WORKER_ID: "dup-worker",
            TAG_GENERATION: "0",
        }
        return [
            DiscoveredInstance(
                instance_id="i-canonical", worker_id="dup-worker", state=InstanceState.RUNNING, tags=tags
            ),
            DiscoveredInstance(
                instance_id="i-duplicate", worker_id="dup-worker", state=InstanceState.RUNNING, tags=tags
            ),
        ]

    async def terminate(self, instance_id: str) -> None:
        self.terminated.append(instance_id)


@pytest.mark.asyncio
async def test_reconcile_terminates_duplicate_vms(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_worker(db_session, "dup-worker", "http://dup:8000", instance_id="i-canonical")
    await db_session.close()

    provider = _DuplicateProvider()
    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=provider)
        await scheduler._reconcile_workers()
    finally:
        await client.aclose()

    assert provider.terminated == ["i-duplicate"]


@pytest.mark.asyncio
async def test_reconcile_sets_public_base_url(db_session: AsyncSession) -> None:
    reset_settings_cache()
    settings = load_settings(cloud={"provider": "aws", "aws": {"worker_address": "public"}})
    await _seed_worker(db_session, "pub-1", base_url=None, instance_id="i-pub-1")
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._reconcile_workers()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "pub-1")
    assert row is not None
    assert row.base_url == "http://203.0.113.1:8000"


@pytest.mark.asyncio
async def test_autoscale_drains_idle_worker_after_cooldown(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.util.datetime import now_utc

    settings = get_settings()
    await _seed_worker(db_session, "idle-w", "http://idle:8000")
    async with get_db_session() as session:
        row = await session.get(Worker, "idle-w")
        assert row is not None
        row.ready_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        row.last_active_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._apply_autoscaling()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "idle-w")
    assert row is not None
    assert row.draining is True
    assert row.drain_target == "stopped"


@pytest.mark.asyncio
async def test_autoscale_does_not_drain_while_worker_starting(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.util.datetime import now_utc

    settings = get_settings()
    await _seed_worker(db_session, "idle-w", "http://idle:8000")
    await _seed_worker(db_session, "boot-w", "http://boot:8000", cloud_state="running", healthy=False)
    async with get_db_session() as session:
        row = await session.get(Worker, "idle-w")
        assert row is not None
        row.ready_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        row.last_active_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._apply_autoscaling()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        idle = await session.get(Worker, "idle-w")
    assert idle is not None
    assert idle.draining is False


@pytest.mark.asyncio
async def test_autoscale_does_not_drain_while_another_worker_draining(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.util.datetime import now_utc

    settings = get_settings()
    await _seed_worker(db_session, "idle-w", "http://idle:8000")
    await _seed_worker(db_session, "drain-w", "http://drain:8000", draining=True, drain_target="stopped")
    async with get_db_session() as session:
        row = await session.get(Worker, "idle-w")
        assert row is not None
        row.ready_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        row.last_active_at = now_utc() - timedelta(seconds=settings.scaling.idle_cooldown_seconds + 30)
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._apply_autoscaling()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        idle = await session.get(Worker, "idle-w")
    assert idle is not None
    assert idle.draining is False


@pytest.mark.asyncio
async def test_autoscale_drains_long_idle_not_newly_ready_worker(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.util.datetime import now_utc

    settings = get_settings()
    cooldown = settings.scaling.idle_cooldown_seconds
    await _seed_worker(db_session, "busy-w", "http://busy:8000")
    await _seed_worker(db_session, "new-w", "http://new:8000")
    async with get_db_session() as session:
        busy = await session.get(Worker, "busy-w")
        new = await session.get(Worker, "new-w")
        assert busy is not None and new is not None
        busy.ready_at = now_utc() - timedelta(hours=1)
        busy.last_active_at = now_utc() - timedelta(seconds=cooldown + 30)
        new.ready_at = now_utc() - timedelta(seconds=30)
        new.last_active_at = None
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._apply_autoscaling()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        busy = await session.get(Worker, "busy-w")
        new = await session.get(Worker, "new-w")
    assert busy is not None and new is not None
    assert busy.draining is True
    assert new.draining is False


@pytest.mark.asyncio
async def test_drain_waits_for_inflight_before_terminate(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_worker(db_session, "drain-1", "http://drain:8000", draining=True, drain_target="terminated")
    async with get_db_session() as session:
        session.add(
            Task(
                task_id="t-inflight", worker_id="drain-1", status="processing", backend="pipeline", file_names=["a.pdf"]
            )
        )
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=None)
        await scheduler._advance_drains()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "drain-1")
    assert row is not None
    assert row.desired_state == "running"

    async with get_db_session() as session:
        task = await session.get(Task, "t-inflight")
        assert task is not None
        task.status = "completed"
        task.result_key = "results/t-inflight.zip"
        await session.commit()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=settings, store=InMemoryStore(name="test"), client=client, provider=None)
        await scheduler._advance_drains()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Worker, "drain-1")
    assert row is not None
    assert row.desired_state == "terminated"


@pytest.mark.asyncio
async def test_rotation_starts_replacement_for_emergency_request(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_worker(db_session, "old-1", "http://old:8000", rotation_requested=True, created_at=None)
    async with get_db_session() as session:
        row = await session.get(Worker, "old-1")
        assert row is not None
        row.ready_at = row.created_at
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._start_new_rotations()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        replacements = (await session.execute(select(Worker).where(Worker.replacement_for == "old-1"))).scalars().all()
        old = await session.get(Worker, "old-1")
    assert len(replacements) == 1
    assert old is not None
    assert old.rotation_requested is False
    assert replacements[0].generation == old.generation + 1


@pytest.mark.asyncio
async def test_rotation_drains_old_when_replacement_ready(db_session: AsyncSession) -> None:
    settings = get_settings()
    await _seed_worker(db_session, "old-2", "http://old:8000")
    async with get_db_session() as session:
        old = await session.get(Worker, "old-2")
        assert old is not None
        old.ready_at = old.created_at
        repl = Worker(
            id="repl-2",
            provider=settings.cloud.provider,
            deployment_id=settings.deployment_id,
            desired_state="running",
            cloud_state="running",
            healthy=True,
            base_url="http://repl:8000",
            ready_at=old.created_at,
            replacement_for="old-2",
            generation=old.generation + 1,
        )
        session.add(repl)
        await session.commit()
    await db_session.close()

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(
            settings=settings, store=InMemoryStore(name="test"), client=client, provider=_FakeProvider()
        )
        await scheduler._progress_rotations()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        old = await session.get(Worker, "old-2")
    assert old is not None
    assert old.draining is True


@pytest.mark.asyncio
async def test_retention_skips_db_delete_when_object_delete_fails(db_session: AsyncSession) -> None:
    from datetime import UTC, datetime, timedelta

    async with get_db_session() as session:
        session.add(
            Task(
                task_id="stale-1",
                status="completed",
                backend="pipeline",
                file_names=["x.pdf"],
                payload_key="payloads/stale-1.bin",
                result_key="results/stale-1.zip",
                completed_at=datetime.now(UTC) - timedelta(days=60),
            )
        )
        await session.commit()
    await db_session.close()

    class FailingStore(InMemoryStore):
        async def delete(self, key: str) -> bool:
            raise OSError("delete failed")

    client = httpx.AsyncClient()
    try:
        scheduler = Scheduler(settings=get_settings(), store=FailingStore(name="fail"), client=client, provider=None)
        await scheduler._cleanup_expired_tasks()
    finally:
        await client.aclose()

    async with get_db_session() as session:
        row = await session.get(Task, "stale-1")
    assert row is not None


@pytest.mark.asyncio
async def test_fast_tick_invokes_dispatch_steps_only() -> None:
    client = httpx.AsyncClient()
    try:
        scheduler = _make_scheduler(client)
        scheduler._tasks.recover_stale_dispatch_claims = AsyncMock(return_value=0)
        scheduler._synchronize_tasks_and_results = AsyncMock()
        scheduler._tasks.expire_client_sla_tasks = AsyncMock(return_value=0)
        scheduler._dispatch_queued_tasks = AsyncMock()
        scheduler._reconcile_workers = AsyncMock()
        scheduler._refresh_worker_health = AsyncMock()

        await scheduler._fast_tick()

        scheduler._tasks.recover_stale_dispatch_claims.assert_awaited_once()
        scheduler._synchronize_tasks_and_results.assert_awaited_once()
        scheduler._tasks.expire_client_sla_tasks.assert_awaited_once()
        scheduler._dispatch_queued_tasks.assert_awaited_once()
        scheduler._reconcile_workers.assert_not_awaited()
        scheduler._refresh_worker_health.assert_not_awaited()
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_slow_tick_invokes_reconcile_steps_only() -> None:
    client = httpx.AsyncClient()
    try:
        scheduler = _make_scheduler(client)
        scheduler._reconcile_workers = AsyncMock()
        scheduler._converge_stalled_workers = AsyncMock()
        scheduler._refresh_worker_health = AsyncMock()
        scheduler._apply_autoscaling = AsyncMock()
        scheduler._advance_drains_and_rotations = AsyncMock()
        scheduler._cleanup_if_due = AsyncMock()
        scheduler._dispatch_queued_tasks = AsyncMock()

        await scheduler._slow_tick()

        scheduler._reconcile_workers.assert_awaited_once()
        scheduler._converge_stalled_workers.assert_awaited_once()
        scheduler._refresh_worker_health.assert_awaited_once()
        scheduler._apply_autoscaling.assert_awaited_once()
        scheduler._advance_drains_and_rotations.assert_awaited_once()
        scheduler._cleanup_if_due.assert_awaited_once()
        scheduler._dispatch_queued_tasks.assert_not_awaited()
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_run_drives_both_loops_at_different_cadences() -> None:
    client = httpx.AsyncClient()
    try:
        scheduler = _make_scheduler(client)
        scheduler._dispatch_poll_interval = 0.05
        scheduler._reconcile_poll_interval = 0.2
        fast_calls = 0
        slow_calls = 0

        async def counting_fast() -> None:
            nonlocal fast_calls
            fast_calls += 1
            if fast_calls >= 3:
                scheduler._lock.held = False  # type: ignore[union-attr]

        async def counting_slow() -> None:
            nonlocal slow_calls
            slow_calls += 1

        scheduler._fast_tick = counting_fast
        scheduler._slow_tick = counting_slow

        async def noop_sleep(interval: float) -> None:
            await asyncio.sleep(0)

        scheduler._sleep_until_next_boundary = noop_sleep

        await scheduler.run()

        assert fast_calls >= 3
        assert slow_calls >= 1
        assert fast_calls > slow_calls
    finally:
        await client.aclose()

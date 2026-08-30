"""Scheduler scaling signal tests — pure desired-capacity from queue depth."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.config import get_settings
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.scheduler.scaling import ScalingInputs, ScalingSignal, compute_scaling_signal
from mineru_gateway.scheduler.worker_repository import WorkerRepository


async def _signal(db: AsyncSession) -> ScalingSignal:
    settings = get_settings()
    repo = WorkerRepository(settings)
    cfg = settings.scaling
    inputs = await repo.collect_scaling_inputs()
    return compute_scaling_signal(
        inputs=inputs, target_per_worker=cfg.target_per_worker, min_workers=cfg.min_workers, max_workers=cfg.max_workers
    )


async def _seed_worker(
    db: AsyncSession,
    wid: str,
    *,
    healthy: bool = True,
    desired_state: str = "running",
    cloud_state: str = "running",
    base_url: str | None = "http://w:8000",
    draining: bool = False,
) -> None:
    settings = get_settings()
    w = Worker(
        id=wid,
        provider=settings.cloud.provider,
        deployment_id=settings.deployment_id,
        base_url=base_url,
        desired_state=desired_state,
        cloud_state=cloud_state,
        healthy=healthy,
        draining=draining,
    )
    db.add(w)
    await db.commit()


async def _seed_task(db: AsyncSession, tid: str, status: str = "queued") -> None:
    db.add(Task(task_id=tid, status=status, backend="pipeline", file_names=["x.pdf"]))
    await db.commit()


@pytest.mark.asyncio
async def test_zero_workers_one_task_desires_capacity(db_session: AsyncSession) -> None:
    await _seed_task(db_session, "t0")
    sig = await _signal(db_session)
    assert sig.queue_depth == 1
    assert sig.serviceable_workers == 0
    assert sig.desired_workers == 1
    assert sig.should_scale_up


@pytest.mark.asyncio
async def test_exact_target_load_does_not_scale(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w1")
    for i in range(4):
        await _seed_task(db_session, f"t{i}")
    sig = await _signal(db_session)
    assert sig.queue_depth == 4
    assert sig.serviceable_workers == 1
    assert sig.desired_workers == 1
    assert not sig.should_scale_up
    assert not sig.should_scale_down


@pytest.mark.asyncio
async def test_max_workers_cap(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w1")
    for i in range(20):
        await _seed_task(db_session, f"t{i}")
    sig = compute_scaling_signal(
        inputs=ScalingInputs(
            queue_depth=20, serviceable_workers=1, starting_workers=0, draining_workers=0, stopping_workers=0
        ),
        target_per_worker=4,
        min_workers=0,
        max_workers=1,
    )
    assert sig.desired_workers == 1
    assert not sig.should_scale_up


@pytest.mark.asyncio
async def test_min_workers_floor_with_empty_queue(db_session: AsyncSession) -> None:
    sig = compute_scaling_signal(
        inputs=ScalingInputs(
            queue_depth=0, serviceable_workers=0, starting_workers=0, draining_workers=0, stopping_workers=0
        ),
        target_per_worker=4,
        min_workers=2,
        max_workers=12,
    )
    assert sig.desired_workers == 2
    assert sig.should_scale_up


@pytest.mark.asyncio
async def test_scale_down_when_overprovisioned(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w-idle")
    worker = await db_session.get(Worker, "w-idle")
    assert worker is not None
    worker.last_active_at = datetime.now(UTC) - timedelta(seconds=600)
    await db_session.commit()
    sig = await _signal(db_session)
    assert sig.queue_depth == 0
    assert sig.serviceable_workers == 1
    assert sig.desired_workers == 0
    assert sig.should_scale_down


@pytest.mark.asyncio
async def test_no_scale_down_while_workers_starting(db_session: AsyncSession) -> None:
    """Empty queue must not trigger idle drain while another worker is still booting."""
    await _seed_worker(db_session, "w-idle")
    await _seed_worker(db_session, "w-boot", cloud_state="running", healthy=False, base_url="http://203.0.113.9:8000")
    worker = await db_session.get(Worker, "w-idle")
    assert worker is not None
    worker.last_active_at = datetime.now(UTC) - timedelta(seconds=600)
    await db_session.commit()
    sig = await _signal(db_session)
    assert sig.queue_depth == 0
    assert sig.serviceable_workers == 1
    assert sig.starting_workers == 1
    assert sig.desired_workers == 0
    assert sig.scaling_in_progress
    assert not sig.should_scale_down


@pytest.mark.asyncio
async def test_no_scale_down_while_worker_draining(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w-active")
    await _seed_worker(db_session, "w-drain", draining=True)
    worker = await db_session.get(Worker, "w-active")
    assert worker is not None
    worker.last_active_at = datetime.now(UTC) - timedelta(seconds=600)
    await db_session.commit()
    sig = await _signal(db_session)
    assert sig.queue_depth == 0
    assert sig.serviceable_workers == 1
    assert sig.draining_workers == 1
    assert sig.desired_workers == 0
    assert sig.scaling_in_progress
    assert not sig.should_scale_down


@pytest.mark.asyncio
async def test_no_scale_down_while_worker_stopping(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w-active")
    await _seed_worker(db_session, "w-stop", cloud_state="stopping", healthy=False, base_url="http://stop:8000")
    worker = await db_session.get(Worker, "w-active")
    assert worker is not None
    worker.last_active_at = datetime.now(UTC) - timedelta(seconds=600)
    await db_session.commit()
    sig = await _signal(db_session)
    assert sig.stopping_workers == 1
    assert sig.scaling_in_progress
    assert not sig.should_scale_down


@pytest.mark.asyncio
async def test_queue_depth_three_keeps_desired_two_at_target_two(db_session: AsyncSession) -> None:
    """E2E keepalive uses depth=3 to hold desired=2 when target_per_worker=2."""
    sig = compute_scaling_signal(
        inputs=ScalingInputs(
            queue_depth=3, serviceable_workers=2, starting_workers=0, draining_workers=0, stopping_workers=0
        ),
        target_per_worker=2,
        min_workers=0,
        max_workers=2,
    )
    assert sig.desired_workers == 2
    assert not sig.should_scale_down


@pytest.mark.asyncio
async def test_queue_depth_one_scales_down_with_two_serviceable(db_session: AsyncSession) -> None:
    sig = compute_scaling_signal(
        inputs=ScalingInputs(
            queue_depth=1, serviceable_workers=2, starting_workers=0, draining_workers=0, stopping_workers=0
        ),
        target_per_worker=2,
        min_workers=0,
        max_workers=2,
    )
    assert sig.desired_workers == 1
    assert sig.should_scale_down


@pytest.mark.asyncio
async def test_starting_workers_count_toward_provisioned(db_session: AsyncSession) -> None:
    await _seed_worker(db_session, "w-boot", cloud_state="pending", healthy=False, base_url=None)
    await _seed_task(db_session, "t1")
    sig = await _signal(db_session)
    assert sig.starting_workers == 1
    assert sig.desired_workers == 1
    assert not sig.should_scale_up


@pytest.mark.asyncio
async def test_bootstrapping_workers_count_toward_provisioned(db_session: AsyncSession) -> None:
    """EC2 running but MinerU not healthy yet must not trigger a duplicate scale-up."""
    await _seed_worker(db_session, "w-warm", cloud_state="running", healthy=False, base_url="http://203.0.113.9:8000")
    await _seed_task(db_session, "t1")
    sig = await _signal(db_session)
    assert sig.starting_workers == 1
    assert sig.desired_workers == 1
    assert not sig.should_scale_up

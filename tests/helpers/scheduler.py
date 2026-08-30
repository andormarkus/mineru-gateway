"""Background scheduler loop for pre-autoscaling (static worker) tests.

Runs the scheduler steps that matter when ``cloud_workers_enabled=false`` and
workers are registered manually. EC2 reconcile, autoscale, rotation, and
retention cleanup are intentionally excluded.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.cloud.registry import init_provider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scheduler.scheduler import Scheduler
from tests.helpers.e2e_log import E2eProgress, e2e_log, format_gateway_snapshot

# Scheduler tick steps that launch/terminate EC2 instances or depend on a compute provider.
_EXCLUDED_TICK_STEPS = (
    "_reconcile_workers",
    "_apply_autoscaling",
    "_advance_drains_and_rotations",
    "_cleanup_if_due",
)


async def run_pre_autoscaling_tick(scheduler: Scheduler, *, include_health_checks: bool) -> None:
    """One scheduler iteration without cloud autoscaling or worker provisioning."""
    if include_health_checks:
        await scheduler._refresh_worker_health()
    await scheduler._tasks.recover_stale_dispatch_claims()
    await scheduler._synchronize_tasks_and_results()
    expired = await scheduler._tasks.expire_client_sla_tasks(sla_seconds=scheduler._settings.task_sla_seconds)
    if expired:
        metrics.record_sla_expired(expired)
    await scheduler._dispatch_queued_tasks()


@asynccontextmanager
async def pre_autoscaling_scheduler_loop(
    settings: GatewaySettings,
    store: CloudStorageProvider,
    *,
    interval_seconds: float = 0.05,
    client_timeout: float = 120.0,
    include_health_checks: bool = True,
) -> AsyncIterator[None]:
    """Run pre-autoscaling scheduler ticks until the context exits."""
    stop = asyncio.Event()
    provider = init_provider(settings)
    assert provider is None, "pre_autoscaling_scheduler_loop requires cloud_workers_enabled=false"

    async def _run() -> None:
        async with httpx.AsyncClient(timeout=client_timeout) as http_client:
            scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=provider)
            while not stop.is_set():
                await run_pre_autoscaling_tick(scheduler, include_health_checks=include_health_checks)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
                except TimeoutError:
                    continue

    task = asyncio.create_task(_run())
    try:
        yield
    finally:
        stop.set()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=client_timeout + interval_seconds + 5.0)
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# Backward-compatible alias used by existing slow tests.
scheduler_tick_loop = pre_autoscaling_scheduler_loop


@asynccontextmanager
async def full_scheduler_loop(
    settings: GatewaySettings,
    store: CloudStorageProvider,
    *,
    interval_seconds: float | None = None,
    client_timeout: float = 300.0,
) -> AsyncIterator[Scheduler]:
    """Run the full scheduler tick loop (reconcile, autoscale, dispatch, …)."""
    stop = asyncio.Event()
    provider = init_provider(settings)
    if provider is None:
        raise RuntimeError("full_scheduler_loop requires cloud_workers_enabled=true")

    poll = interval_seconds if interval_seconds is not None else settings.scheduler.reconcile_poll_interval_seconds
    holder: list[Scheduler] = []
    progress = E2eProgress("scheduler")

    async def _run() -> None:
        async with httpx.AsyncClient(timeout=client_timeout) as http_client:
            scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=provider)
            holder.append(scheduler)
            e2e_log("scheduler loop started", always=True)
            while not stop.is_set():
                await scheduler.tick()
                progress.report(
                    await format_gateway_snapshot(settings, provider),
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll)
                except TimeoutError:
                    continue

    task = asyncio.create_task(_run())
    try:
        for _ in range(100):
            if holder:
                break
            await asyncio.sleep(0.05)
        if not holder:
            raise RuntimeError("scheduler failed to start")
        yield holder[0]
    finally:
        stop.set()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=client_timeout + poll + 30.0)
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

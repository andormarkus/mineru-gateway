"""AWS scheduler E2E — autoscale, rotation, and drain on real EC2.

Tests run in order (test_01 → test_05) sharing one session worker pool.
Scheduler session uses a 60s idle cooldown (override via ``MINERU_GATEWAY_SCALING__IDLE_COOLDOWN_SECONDS``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import GatewaySettings, get_settings
from tests.e2e.conftest import SAMPLE_PDF
from tests.helpers.cloud import scheduler_poll_interval_seconds, wait_for_serviceable_workers
from tests.helpers.e2e import (
    fetch_admin_workers,
    is_serviceable_admin_worker,
    maintain_min_queue_depth,
    min_queue_depth_for_desired_workers,
    submit_pdf_task,
    wait_for_admin_workers,
    wait_for_autoscaler_idle_drain,
    wait_for_provisioned_workers,
    wait_for_queue_empty,
    wait_for_rotation_complete,
)
from tests.helpers.scheduler import full_scheduler_loop
from tests.helpers.tasks import wait_for_task_status_api

pytestmark = pytest.mark.e2e

_PDF_BYTES = SAMPLE_PDF.read_bytes()
_PDF_NAME = SAMPLE_PDF.name


@asynccontextmanager
async def _e2e_scheduler(
    settings: GatewaySettings, store: CloudStorageProvider, *, client_timeout: float
) -> AsyncIterator[None]:
    """Per-test scheduler loop (reuses EC2 workers from the session DB)."""
    async with full_scheduler_loop(
        settings,
        store,
        interval_seconds=settings.scheduler.reconcile_poll_interval_seconds,
        client_timeout=client_timeout,
    ):
        yield


@pytest.mark.asyncio
async def test_01_scale_from_zero_launch_and_complete(
    gateway_cloud_e2e_scheduler_session: tuple[AsyncClient, CloudStorageProvider],
    e2e_launch_timeout_seconds: float,
    e2e_worker_timeout_seconds: float,
) -> None:
    """Queue depth triggers first EC2 launch (min_workers=0) and task completes."""
    client, store = gateway_cloud_e2e_scheduler_session
    settings = get_settings()
    poll = scheduler_poll_interval_seconds()

    async with _e2e_scheduler(settings, store, client_timeout=e2e_worker_timeout_seconds):
        workers = await fetch_admin_workers(client)
        assert not any(w.get("healthy") for w in workers), workers

        task_id = await submit_pdf_task(client, filename=_PDF_NAME, pdf_bytes=_PDF_BYTES)
        await wait_for_serviceable_workers(
            settings, count=1, timeout_seconds=e2e_launch_timeout_seconds, poll_interval=poll
        )
        await wait_for_task_status_api(
            client, task_id, expected="completed", timeout_seconds=e2e_worker_timeout_seconds, poll_interval=poll
        )


@pytest.mark.asyncio
async def test_02_autoscale_up_provisions_second_worker(
    gateway_cloud_e2e_scheduler_session: tuple[AsyncClient, CloudStorageProvider],
    e2e_launch_timeout_seconds: float,
    e2e_worker_timeout_seconds: float,
) -> None:
    """Enough queued work provisions a second GPU worker; both must become healthy."""
    client, store = gateway_cloud_e2e_scheduler_session
    settings = get_settings()
    poll = scheduler_poll_interval_seconds()
    target = settings.scaling.target_per_worker
    keepalive_depth = min_queue_depth_for_desired_workers(2, target_per_worker=target)

    async with (
        _e2e_scheduler(settings, store, client_timeout=e2e_worker_timeout_seconds),
        # Hold desired>=2 until both workers are healthy (depth=1 → desired=1 @ target=2).
        maintain_min_queue_depth(client, settings, pdf_bytes=_PDF_BYTES, min_depth=keepalive_depth),
    ):
        # ceil((target+1)/target) == 2 when target_per_worker == target (e.g. 3 tasks @ 2 → desired=2).
        for i in range(target + 1):
            await submit_pdf_task(client, filename=f"pdf_sample_{(i % 3) + 1}.pdf", pdf_bytes=_PDF_BYTES)

        await wait_for_provisioned_workers(
            settings, count=2, timeout_seconds=e2e_launch_timeout_seconds, poll_interval=poll
        )
        await wait_for_serviceable_workers(
            settings, count=2, timeout_seconds=e2e_launch_timeout_seconds, poll_interval=poll
        )


@pytest.mark.asyncio
async def test_03_worker_rotation_replaces_instance(
    gateway_cloud_e2e_scheduler_session: tuple[AsyncClient, CloudStorageProvider],
    e2e_launch_timeout_seconds: float,
    e2e_worker_timeout_seconds: float,
) -> None:
    """Admin rotate launches a replacement VM and drains the old worker."""
    client, store = gateway_cloud_e2e_scheduler_session
    settings = get_settings()
    poll = scheduler_poll_interval_seconds()
    target = settings.scaling.target_per_worker
    keepalive_depth = min_queue_depth_for_desired_workers(2, target_per_worker=target)

    # The feeder context must exit before the scheduler context: the backlog can
    # only drain while the scheduler loop is still running.
    async with _e2e_scheduler(settings, store, client_timeout=e2e_worker_timeout_seconds):
        async with maintain_min_queue_depth(client, settings, pdf_bytes=_PDF_BYTES, min_depth=keepalive_depth):
            await wait_for_serviceable_workers(
                settings, count=2, timeout_seconds=e2e_launch_timeout_seconds, poll_interval=poll
            )

            workers = await wait_for_admin_workers(
                client,
                predicate=lambda ws: any(is_serviceable_admin_worker(w) for w in ws),
                timeout_seconds=60.0,
                poll_interval=poll,
                description="at least one serviceable worker to rotate",
            )
            serviceable = [w for w in workers if is_serviceable_admin_worker(w)]
            worker_id = serviceable[0]["id"]

            rotate = await client.post(f"/admin/workers/{worker_id}/rotate", json={"reason": "e2e rotation"})
            assert rotate.status_code == 200, rotate.text

            await wait_for_admin_workers(
                client,
                predicate=lambda ws, oid=worker_id: any(w.get("replacement_for") == oid for w in ws),
                timeout_seconds=120.0,
                poll_interval=poll,
                description="replacement worker row created",
            )
            old, replacement = await wait_for_rotation_complete(
                client,
                worker_id,
                timeout_seconds=e2e_launch_timeout_seconds,
                poll_interval=poll,
                settings=settings,
                maintain_queue=True,
                queue_feed_bytes=_PDF_BYTES,
            )
            assert replacement.get("replacement_for") == worker_id
            assert old.get("draining") is True

        # Stop keepalive feeder first, then drain backlog so test_04 starts at queue=0.
        await wait_for_queue_empty(settings, timeout_seconds=e2e_launch_timeout_seconds, poll_interval=poll)


@pytest.mark.asyncio
async def test_04_autoscale_idle_drain_stops_worker(
    gateway_cloud_e2e_scheduler_session: tuple[AsyncClient, CloudStorageProvider],
    e2e_launch_timeout_seconds: float,
    e2e_worker_timeout_seconds: float,
) -> None:
    """Empty queue + idle cooldown causes autoscaler to drain a worker (stop intent)."""
    client, store = gateway_cloud_e2e_scheduler_session
    settings = get_settings()
    poll = scheduler_poll_interval_seconds()
    idle_wait = settings.scaling.idle_cooldown_seconds + 30

    async with _e2e_scheduler(settings, store, client_timeout=e2e_worker_timeout_seconds):
        await wait_for_autoscaler_idle_drain(
            client,
            settings,
            idle_timeout_seconds=idle_wait,
            precondition_timeout_seconds=e2e_launch_timeout_seconds,
            poll_interval=poll,
        )


@pytest.mark.asyncio
async def test_05_admin_drain_terminate_clears_pool(
    gateway_cloud_e2e_scheduler_session: tuple[AsyncClient, CloudStorageProvider],
    e2e_launch_timeout_seconds: float,
    e2e_worker_timeout_seconds: float,
) -> None:
    """Admin drain(terminate) converges remaining workers to terminated intent."""
    client, store = gateway_cloud_e2e_scheduler_session
    poll = scheduler_poll_interval_seconds()

    async with _e2e_scheduler(
        get_settings(), store, client_timeout=max(e2e_worker_timeout_seconds, e2e_launch_timeout_seconds)
    ):
        workers = await fetch_admin_workers(client)
        for worker in workers:
            if worker.get("desired_state") == "terminated":
                continue
            drain = await client.post(
                f"/admin/workers/{worker['id']}/drain", json={"reason": "e2e cleanup", "terminate": True}
            )
            assert drain.status_code == 200, drain.text

        await wait_for_admin_workers(
            client,
            predicate=lambda ws: bool(ws) and all(w.get("desired_state") == "terminated" for w in ws),
            timeout_seconds=e2e_launch_timeout_seconds,
            poll_interval=poll,
            description="all workers desired_state=terminated",
        )

"""Fixtures for AWS EC2 end-to-end tests."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from tests.helpers.cloud import (
    E2E_SCHEDULER_IDLE_COOLDOWN_SECONDS,
    E2E_TEARDOWN_WAIT_SECONDS,
    E2E_WORKER_HTTP_TIMEOUT_SECONDS,
    E2eCloudConfig,
    build_e2e_settings,
    e2e_aws_configured,
    init_e2e_provider,
    launch_readiness_timeout_seconds,
    load_e2e_cloud_config,
    log_e2e_config,
    mark_all_workers_terminated,
    prime_e2e_cloud_env,
    prime_e2e_os_environ,
    require_e2e_cloud_config,
    scheduler_poll_interval_seconds,
    terminate_discovered_vms,
    wait_for_serviceable_workers,
    wait_for_workers_gone,
)
from tests.helpers.e2e_log import e2e_log, log_gateway_snapshot
from tests.helpers.scheduler import full_scheduler_loop

pytestmark = pytest.mark.e2e

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
SAMPLE_PDF = FIXTURES_DIR / "pdf_sample_1.pdf"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "e2e: tests requiring real AWS EC2 worker launch/stop and mineru-api (Tier-4)",
    )
    if e2e_aws_configured():
        from mineru_gateway.logging_config import configure_logging

        configure_logging(os.environ.get("MINERU_GATEWAY_LOG_LEVEL", "INFO"))
        e2e_log("E2E progress logging enabled", always=True)


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    if e2e_aws_configured() and "tests/e2e/" in nodeid:
        e2e_log(f"▶ TEST {nodeid}", always=True)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Run test_01_, test_02_, … in order within scheduler E2E module."""
    scheduler_items = [i for i in items if "test_cloud_scheduler.py" in str(i.fspath)]
    scheduler_items.sort(key=lambda item: item.name)
    other = [i for i in items if i not in scheduler_items]
    items[:] = other + scheduler_items


@pytest.fixture(scope="session")
def e2e_worker_timeout_seconds() -> float:
    return E2E_WORKER_HTTP_TIMEOUT_SECONDS


@pytest.fixture(scope="session")
def e2e_launch_timeout_seconds() -> float:
    return float(launch_readiness_timeout_seconds())


@pytest.fixture(scope="session")
def e2e_teardown_timeout_seconds() -> float:
    return E2E_TEARDOWN_WAIT_SECONDS


@pytest_asyncio.fixture(scope="session", autouse=True)
async def e2e_session_vm_cleanup(e2e_teardown_timeout_seconds: float) -> AsyncIterator[None]:
    if not e2e_aws_configured():
        yield
        return
    cfg = load_e2e_cloud_config(database_url="sqlite+aiosqlite:///:memory:")
    if cfg is None:
        yield
        return
    # Prime cloud env only — do not set DATABASE_URL (gateway session uses a temp file).
    prime_e2e_cloud_env(cfg)
    settings = build_e2e_settings(cfg)
    provider = init_e2e_provider(settings)
    await terminate_discovered_vms(provider, cfg.deployment_id)
    yield
    await terminate_discovered_vms(provider, cfg.deployment_id)
    await wait_for_workers_gone(
        provider,
        settings,
        timeout_seconds=e2e_teardown_timeout_seconds,
        poll_interval=scheduler_poll_interval_seconds(),
    )


async def _teardown_session(
    settings,
    store: CloudStorageProvider,
    provider,
    *,
    poll: float,
) -> None:
    e2e_log("session teardown: drain workers + terminate VMs", always=True)
    await mark_all_workers_terminated(settings)
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        from mineru_gateway.scheduler.scheduler import Scheduler

        scheduler = Scheduler(settings=settings, store=store, client=http_client, provider=provider)
        for _ in range(6):
            await scheduler.tick()
            await asyncio.sleep(poll)
    await terminate_discovered_vms(provider, settings.deployment_id)


async def _gateway_cloud_session(
    tmp_path_factory: pytest.TempPathFactory,
    cfg: E2eCloudConfig,
    *,
    e2e_worker_timeout_seconds: float,
    e2e_launch_timeout_seconds: float,
    warm_workers: int,
) -> AsyncIterator[tuple[AsyncClient, CloudStorageProvider]]:
    from mineru_gateway.db.base import shutdown_engine
    from mineru_gateway.gateway.app import create_app

    prime_e2e_os_environ(cfg)
    settings = build_e2e_settings(cfg)
    log_e2e_config(cfg, label="session start")

    await shutdown_engine()

    provider = init_e2e_provider(settings)
    await terminate_discovered_vms(provider, settings.deployment_id)

    application = create_app(settings)
    poll = settings.scheduler.reconcile_poll_interval_seconds
    e2e_log(f"starting gateway (db={settings.database_url})", always=True)
    async with application.router.lifespan_context(application):
        store: CloudStorageProvider = application.state.object_store
        if warm_workers > 0:
            e2e_log(f"warming {warm_workers} worker(s) with scheduler", always=True)
            async with full_scheduler_loop(
                settings,
                store,
                interval_seconds=poll,
                client_timeout=e2e_worker_timeout_seconds,
            ):
                await log_gateway_snapshot(settings, provider, label="initial")
                await wait_for_serviceable_workers(
                    settings,
                    count=warm_workers,
                    timeout_seconds=e2e_launch_timeout_seconds,
                    provider=provider,
                )
        else:
            await log_gateway_snapshot(settings, provider, label="initial")
        # Scheduler runs per-test (same loop as the test body) so dispatch keeps
        # working after fixture yield. EC2 workers stay up — only the local tick
        # loop restarts; reconcile reuses existing instances.
        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, store
        await _teardown_session(settings, store, provider, poll=poll)
    e2e_log("session teardown complete", always=True)
    await shutdown_engine()


@pytest_asyncio.fixture(scope="session")
async def gateway_cloud_e2e_session(
    tmp_path_factory: pytest.TempPathFactory,
    e2e_worker_timeout_seconds: float,
    e2e_launch_timeout_seconds: float,
) -> AsyncIterator[tuple[AsyncClient, CloudStorageProvider]]:
    """Quick smoke: one warm worker for health + single task."""
    db_dir = tmp_path_factory.mktemp("e2e-smoke")
    cfg = require_e2e_cloud_config(database_url=f"sqlite+aiosqlite:///{db_dir / 'gateway.db'}")
    cfg = replace(cfg, min_workers=1, max_workers=1)
    async for stack in _gateway_cloud_session(
        tmp_path_factory,
        cfg,
        e2e_worker_timeout_seconds=e2e_worker_timeout_seconds,
        e2e_launch_timeout_seconds=e2e_launch_timeout_seconds,
        warm_workers=1,
    ):
        yield stack


@pytest_asyncio.fixture(scope="session")
async def gateway_cloud_e2e_scheduler_session(
    tmp_path_factory: pytest.TempPathFactory,
    e2e_worker_timeout_seconds: float,
    e2e_launch_timeout_seconds: float,
) -> AsyncIterator[tuple[AsyncClient, CloudStorageProvider]]:
    """Full scheduler E2E: scale-from-zero, autoscale, rotation, drain on real AWS."""
    db_dir = tmp_path_factory.mktemp("e2e-scheduler")
    cfg = require_e2e_cloud_config(database_url=f"sqlite+aiosqlite:///{db_dir / 'gateway.db'}")
    cfg = replace(
        cfg,
        min_workers=0,
        max_workers=2,
        target_per_worker=2,
        idle_cooldown_seconds=E2E_SCHEDULER_IDLE_COOLDOWN_SECONDS,
    )
    async for stack in _gateway_cloud_session(
        tmp_path_factory,
        cfg,
        e2e_worker_timeout_seconds=e2e_worker_timeout_seconds,
        e2e_launch_timeout_seconds=e2e_launch_timeout_seconds,
        warm_workers=0,
    ):
        yield stack

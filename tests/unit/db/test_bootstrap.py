"""Tests for sqlite schema bootstrap."""

from __future__ import annotations

import pytest
from tests.helpers.cloud import E2eCloudConfig, build_e2e_settings, prime_e2e_os_environ

from mineru_gateway.db.base import init_engine, shutdown_engine
from mineru_gateway.db.bootstrap import ensure_sqlite_schema
from mineru_gateway.startup_guard import _validate_worker_provider_ownership


@pytest.mark.asyncio
async def test_ensure_sqlite_schema_creates_workers_table() -> None:
    await shutdown_engine()
    init_engine("sqlite+aiosqlite:///:memory:")
    await ensure_sqlite_schema()

    cfg = E2eCloudConfig(
        deployment_id="e2e-test",
        region="eu-central-1",
        launch_template_id="lt-x",
        launch_template_version="$Latest",
        bucket="bucket",
        min_workers=0,
        max_workers=1,
        target_per_worker=2,
        idle_cooldown_seconds=120,
        launch_readiness_timeout_seconds=3600,
        scheduler_poll_interval_seconds=2,
        database_url="sqlite+aiosqlite:///:memory:",
    )
    prime_e2e_os_environ(cfg)
    settings = build_e2e_settings(cfg)
    await _validate_worker_provider_ownership(settings)
    await shutdown_engine()

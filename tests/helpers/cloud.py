"""AWS EC2 helpers for end-to-end tests."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from mineru_gateway.cloud.base import ComputeProvider
from mineru_gateway.cloud.registry import init_provider
from mineru_gateway.cloud.types import CLOUD_STATE_TERMINATED, InstanceState
from mineru_gateway.config import (
    GatewaySettings,
    ReconciliationConfig,
    ScalingConfig,
    SchedulerConfig,
    load_settings,
    reset_settings_cache,
)
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from tests.helpers.e2e_log import E2eProgress, e2e_log, e2e_verbose, format_gateway_snapshot

# Instances in these states no longer accept work; don't block test teardown on AWS
# finishing the shutdown (can take many minutes while status is shutting-down).
_GONE_INSTANCE_STATES = frozenset({InstanceState.TERMINATED, InstanceState.TERMINATING})

# Test-harness constants (not gateway settings — not env-configurable).
E2E_WORKER_HTTP_TIMEOUT_SECONDS = 900.0
E2E_TEARDOWN_WAIT_SECONDS = 120.0
E2E_SCHEDULER_IDLE_COOLDOWN_SECONDS = 60


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def e2e_aws_configured() -> bool:
    """True when real EC2 E2E prerequisites are present in the environment.

    Beyond launch template + bucket, ``MINERU_GATEWAY_E2E=1`` must be set
    explicitly: these tests launch real GPU instances and cost money, so a
    shell that merely still exports the cloud config must never trip them.
    """
    return bool(
        os.environ.get("MINERU_GATEWAY_E2E", "").strip() == "1"
        and os.environ.get("MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID", "").strip()
        and os.environ.get("MINERU_GATEWAY_CLOUD__AWS__BUCKET", "").strip()
    )


def scheduler_poll_interval_seconds() -> float:
    return _env_float(
        "MINERU_GATEWAY_SCHEDULER__RECONCILE_POLL_INTERVAL_SECONDS", SchedulerConfig().reconcile_poll_interval_seconds
    )


def scaling_idle_cooldown_seconds() -> int:
    return _env_int("MINERU_GATEWAY_SCALING__IDLE_COOLDOWN_SECONDS", ScalingConfig().idle_cooldown_seconds)


def launch_readiness_timeout_seconds() -> int:
    return _env_int(
        "MINERU_GATEWAY_RECONCILIATION__LAUNCH_READINESS_TIMEOUT_SECONDS",
        ReconciliationConfig().launch_readiness_timeout_seconds,
    )


def log_e2e_config(cfg: E2eCloudConfig, *, label: str) -> None:
    e2e_log(
        f"{label}: deployment={cfg.deployment_id} region={cfg.region} "
        f"template={cfg.launch_template_id} bucket={cfg.bucket} "
        f"workers={cfg.min_workers}-{cfg.max_workers} target_per_worker={cfg.target_per_worker} "
        f"idle_cooldown={cfg.idle_cooldown_seconds}s poll={cfg.scheduler_poll_interval_seconds}s",
        always=True,
    )


@dataclass(frozen=True)
class E2eCloudConfig:
    deployment_id: str
    region: str
    launch_template_id: str
    launch_template_version: str
    bucket: str
    worker_address: str
    min_workers: int
    max_workers: int
    target_per_worker: int
    idle_cooldown_seconds: int
    launch_readiness_timeout_seconds: int
    scheduler_poll_interval_seconds: float
    database_url: str


def load_e2e_cloud_config(*, database_url: str) -> E2eCloudConfig | None:
    launch_template_id = os.environ.get("MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID", "").strip()
    bucket = os.environ.get("MINERU_GATEWAY_CLOUD__AWS__BUCKET", "").strip()
    if not launch_template_id or not bucket:
        return None
    return E2eCloudConfig(
        deployment_id=os.environ.get("MINERU_GATEWAY_DEPLOYMENT_ID", "e2e-pytest").strip(),
        region=os.environ.get("MINERU_GATEWAY_CLOUD__AWS__REGION", os.environ.get("AWS_REGION", "us-east-1")).strip(),
        launch_template_id=launch_template_id,
        launch_template_version=os.environ.get("MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_VERSION", "$Latest").strip(),
        bucket=bucket,
        worker_address=os.environ.get("MINERU_GATEWAY_CLOUD__AWS__WORKER_ADDRESS", "public").strip(),
        min_workers=_env_int("MINERU_GATEWAY_SCALING__MIN_WORKERS", 1),
        max_workers=_env_int("MINERU_GATEWAY_SCALING__MAX_WORKERS", 1),
        target_per_worker=_env_int("MINERU_GATEWAY_SCALING__TARGET_PER_WORKER", ScalingConfig().target_per_worker),
        idle_cooldown_seconds=scaling_idle_cooldown_seconds(),
        launch_readiness_timeout_seconds=launch_readiness_timeout_seconds(),
        scheduler_poll_interval_seconds=scheduler_poll_interval_seconds(),
        database_url=database_url,
    )


def apply_e2e_env(monkeypatch: pytest.MonkeyPatch, cfg: E2eCloudConfig) -> None:
    """Push E2E settings into env (highest precedence for load_settings)."""
    monkeypatch.setenv("MINERU_GATEWAY_DEPLOYMENT_ID", cfg.deployment_id)
    monkeypatch.setenv("MINERU_GATEWAY_DATABASE_URL", cfg.database_url)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD_WORKERS_ENABLED", "true")
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__PROVIDER", "aws")
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__REGION", cfg.region)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__BUCKET", cfg.bucket)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID", cfg.launch_template_id)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_VERSION", cfg.launch_template_version)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__WORKER_ADDRESS", cfg.worker_address)
    monkeypatch.delenv("MINERU_GATEWAY_CLOUD__AWS__ENDPOINT_URL", raising=False)
    monkeypatch.delenv("MINERU_GATEWAY_CLOUD__AWS__EC2_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__MIN_WORKERS", str(cfg.min_workers))
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__MAX_WORKERS", str(cfg.max_workers))
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__TARGET_PER_WORKER", str(cfg.target_per_worker))
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__IDLE_COOLDOWN_SECONDS", str(cfg.idle_cooldown_seconds))
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__SCALE_UP_COOLDOWN_SECONDS", "5")
    monkeypatch.setenv(
        "MINERU_GATEWAY_SCHEDULER__RECONCILE_POLL_INTERVAL_SECONDS", str(cfg.scheduler_poll_interval_seconds)
    )
    monkeypatch.setenv(
        "MINERU_GATEWAY_RECONCILIATION__LAUNCH_READINESS_TIMEOUT_SECONDS", str(cfg.launch_readiness_timeout_seconds)
    )
    monkeypatch.setenv("MINERU_GATEWAY_AUTH__ENABLED", "false")
    monkeypatch.setenv("MINERU_GATEWAY_CACHE__ENABLED", "false")


def prime_e2e_cloud_env(cfg: E2eCloudConfig) -> None:
    """Push cloud/scaling env without touching ``MINERU_GATEWAY_DATABASE_URL``."""
    os.environ["MINERU_GATEWAY_DEPLOYMENT_ID"] = cfg.deployment_id
    os.environ["MINERU_GATEWAY_CLOUD_WORKERS_ENABLED"] = "true"
    os.environ["MINERU_GATEWAY_CLOUD__PROVIDER"] = "aws"
    os.environ["MINERU_GATEWAY_CLOUD__AWS__REGION"] = cfg.region
    os.environ["MINERU_GATEWAY_CLOUD__AWS__BUCKET"] = cfg.bucket
    os.environ["MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID"] = cfg.launch_template_id
    os.environ["MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_VERSION"] = cfg.launch_template_version
    os.environ["MINERU_GATEWAY_CLOUD__AWS__WORKER_ADDRESS"] = cfg.worker_address
    os.environ["MINERU_GATEWAY_SCALING__MIN_WORKERS"] = str(cfg.min_workers)
    os.environ["MINERU_GATEWAY_SCALING__MAX_WORKERS"] = str(cfg.max_workers)
    os.environ["MINERU_GATEWAY_SCALING__TARGET_PER_WORKER"] = str(cfg.target_per_worker)
    os.environ["MINERU_GATEWAY_SCALING__IDLE_COOLDOWN_SECONDS"] = str(cfg.idle_cooldown_seconds)
    os.environ["MINERU_GATEWAY_SCALING__SCALE_UP_COOLDOWN_SECONDS"] = "5"
    os.environ["MINERU_GATEWAY_SCHEDULER__RECONCILE_POLL_INTERVAL_SECONDS"] = str(cfg.scheduler_poll_interval_seconds)
    os.environ["MINERU_GATEWAY_AUTH__ENABLED"] = "false"
    os.environ["MINERU_GATEWAY_CACHE__ENABLED"] = "false"
    os.environ.pop("MINERU_GATEWAY_CLOUD__AWS__ENDPOINT_URL", None)
    os.environ.pop("MINERU_GATEWAY_CLOUD__AWS__EC2_ENDPOINT_URL", None)


def prime_e2e_os_environ(cfg: E2eCloudConfig) -> None:
    """Session-scoped env (no monkeypatch)."""
    prime_e2e_cloud_env(cfg)
    os.environ["MINERU_GATEWAY_DATABASE_URL"] = cfg.database_url


def build_e2e_settings(cfg: E2eCloudConfig) -> GatewaySettings:
    reset_settings_cache()
    no_yaml = Path(__file__).resolve().parent.parent / ".e2e-settings-absent.yaml"
    return load_settings(
        config_path=no_yaml,
        deployment_id=cfg.deployment_id,
        database_url=cfg.database_url,
        cloud_workers_enabled=True,
        auth={"enabled": False},
        scaling={
            "min_workers": cfg.min_workers,
            "max_workers": cfg.max_workers,
            "target_per_worker": cfg.target_per_worker,
            "idle_cooldown_seconds": cfg.idle_cooldown_seconds,
            "scale_up_cooldown_seconds": 5,
        },
        scheduler={"reconcile_poll_interval_seconds": cfg.scheduler_poll_interval_seconds},
        reconciliation={"launch_readiness_timeout_seconds": cfg.launch_readiness_timeout_seconds},
        cloud={
            "provider": "aws",
            "aws": {
                "region": cfg.region,
                "bucket": cfg.bucket,
                "launch_template_id": cfg.launch_template_id,
                "launch_template_version": cfg.launch_template_version,
                "worker_address": cfg.worker_address,
                "endpoint_url": None,
                "ec2_endpoint_url": None,
            },
        },
        cache={"enabled": False},
    )


async def terminate_discovered_vms(provider: ComputeProvider, deployment_id: str) -> list[str]:
    """Terminate all EC2 instances tagged for ``deployment_id``. Returns instance ids."""
    terminated: list[str] = []
    for inst in await provider.discover(deployment_id):
        if inst.state in _GONE_INSTANCE_STATES:
            continue
        try:
            await provider.terminate(inst.instance_id)
            terminated.append(inst.instance_id)
        except Exception:
            pass
    if terminated:
        e2e_log(f"terminate requested for {len(terminated)} VM(s): {terminated}", always=True)
    elif e2e_verbose():
        e2e_log(f"no live VMs for deployment={deployment_id}")
    return terminated


async def mark_all_workers_terminated(settings: GatewaySettings) -> None:
    repo = WorkerRepository(settings)
    for worker in await repo.list_deployment_workers():
        await repo.commit_fields(worker.id, desired_state=CLOUD_STATE_TERMINATED)


async def wait_for_serviceable_workers(
    settings: GatewaySettings,
    *,
    count: int,
    timeout_seconds: float,
    poll_interval: float | None = None,
    provider: ComputeProvider | None = None,
) -> int:
    """Poll until at least ``count`` serviceable workers exist."""
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    repo = WorkerRepository(settings)
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress(f"wait serviceable>={count}")
    e2e_log(f"waiting for >={count} serviceable workers (timeout={timeout_seconds:.0f}s)")
    last = 0
    while asyncio.get_running_loop().time() < deadline:
        last = await repo.count_serviceable_workers()
        if last >= count:
            e2e_log(f"serviceable workers ready: {last}/{count}", always=True)
            return last
        progress.report(await format_gateway_snapshot(settings, provider), signature=f"svc={last}/{count}")
        await asyncio.sleep(poll)
    pytest.fail(f"expected >={count} serviceable workers within {timeout_seconds}s (last={last})")


async def wait_for_workers_gone(
    provider: ComputeProvider, settings: GatewaySettings, *, timeout_seconds: float, poll_interval: float | None = None
) -> None:
    """Wait until no live VMs remain (terminated or already shutting-down is OK)."""
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress("wait VMs gone")
    e2e_log(f"waiting for all VMs gone (timeout={timeout_seconds:.0f}s)", always=True)
    while asyncio.get_running_loop().time() < deadline:
        discovered = await provider.discover(settings.deployment_id)
        active = [i for i in discovered if i.state not in _GONE_INSTANCE_STATES]
        if not active:
            e2e_log("all VMs gone", always=True)
            return
        progress.report(
            ", ".join(f"{i.instance_id}:{i.state}" for i in active), signature=",".join(i.instance_id for i in active)
        )
        await asyncio.sleep(poll)
    discovered = await provider.discover(settings.deployment_id)
    active = [i for i in discovered if i.state not in _GONE_INSTANCE_STATES]
    pytest.fail(f"workers still active after {timeout_seconds}s: {[i.instance_id for i in active]}")


def require_e2e_cloud_config(*, database_url: str) -> E2eCloudConfig:
    if os.environ.get("MINERU_GATEWAY_E2E", "").strip() != "1":
        pytest.skip("EC2 E2E launches real GPU instances (costs money) — set MINERU_GATEWAY_E2E=1 to opt in")
    cfg = load_e2e_cloud_config(database_url=database_url)
    if cfg is None:
        pytest.skip(
            "EC2 E2E requires MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID and MINERU_GATEWAY_CLOUD__AWS__BUCKET"
        )
    return cfg


def init_e2e_provider(settings: GatewaySettings) -> ComputeProvider:
    provider = init_provider(settings)
    if provider is None:
        raise RuntimeError("EC2 E2E requires cloud_workers_enabled=true and cloud.aws launch template + bucket")
    return provider

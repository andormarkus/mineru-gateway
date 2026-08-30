"""Progress logging for long-running AWS E2E tests."""

from __future__ import annotations

import asyncio
import logging
import time

from mineru_gateway.cloud.base import ComputeProvider
from mineru_gateway.cloud.types import InstanceState
from mineru_gateway.config import GatewaySettings
from mineru_gateway.scheduler.worker_repository import WorkerRepository

_LOGGER = logging.getLogger("mineru_gateway.e2e")

_GONE_INSTANCE_STATES = frozenset({InstanceState.TERMINATED, InstanceState.TERMINATING})

E2E_PROGRESS_LOG_INTERVAL_SECONDS = 30.0


def e2e_verbose() -> bool:
    return True


def e2e_progress_interval_seconds() -> float:
    return E2E_PROGRESS_LOG_INTERVAL_SECONDS


def e2e_log(message: str, *, always: bool = False) -> None:
    if always or e2e_verbose():
        _LOGGER.info(message)


class E2eProgress:
    """Emit progress when a watched value changes or on a fixed interval."""

    def __init__(self, label: str, *, report_every: float | None = None) -> None:
        self.label = label
        self.report_every = e2e_progress_interval_seconds() if report_every is None else report_every
        self._started = time.monotonic()
        self._last_report = 0.0
        self._last_signature: str | None = None

    def report(self, detail: str, *, signature: str | None = None) -> None:
        if not e2e_verbose():
            return
        now = time.monotonic()
        sig = detail if signature is None else signature
        changed = sig != self._last_signature
        due = (now - self._last_report) >= self.report_every
        if not changed and not due:
            return
        elapsed = int(now - self._started)
        e2e_log(f"{self.label} (+{elapsed}s): {detail}")
        self._last_signature = sig
        self._last_report = now


def _worker_snapshot_part(worker) -> str:
    part = f"{worker.id[:8]}:{worker.cloud_state}/{'ok' if worker.healthy else 'x'}"
    if worker.draining:
        part += " drain"
    if not worker.healthy:
        if worker.base_url:
            part += f" @{worker.base_url.removeprefix('http://')}"
        if worker.provisioning_detail:
            text = worker.provisioning_detail.replace("\n", " ")[:60]
            part += f" status={text!r}"
        elif worker.last_error:
            text = worker.last_error.replace("\n", " ")[:60]
            part += f" err={text!r}"
    return part


async def format_gateway_snapshot(settings: GatewaySettings, provider: ComputeProvider | None = None) -> str:
    repo = WorkerRepository(settings)
    if provider is not None:
        serviceable, starting, queue, workers, discovered = await asyncio.gather(
            repo.count_serviceable_workers(),
            repo.count_starting_workers(),
            repo.count_queue_depth(),
            repo.list_deployment_workers(),
            provider.discover(settings.deployment_id),
        )
    else:
        serviceable, starting, queue, workers = await asyncio.gather(
            repo.count_serviceable_workers(),
            repo.count_starting_workers(),
            repo.count_queue_depth(),
            repo.list_deployment_workers(),
        )
        discovered = None

    workers = [w for w in workers if w.terminated_at is None]
    worker_bits = ",".join(_worker_snapshot_part(w) for w in workers[:4])
    if len(workers) > 4:
        worker_bits += f",+{len(workers) - 4}more"

    ec2_bits = ""
    if discovered is not None:
        live = [i for i in discovered if i.state not in _GONE_INSTANCE_STATES]
        ec2_bits = " ec2=[" + ",".join(f"{i.instance_id[-8:]}:{i.state}" for i in live[:6]) + "]"

    return f"svc={serviceable} starting={starting} queue={queue} workers=[{worker_bits or '-'}]{ec2_bits}"


async def log_gateway_snapshot(
    settings: GatewaySettings, provider: ComputeProvider | None = None, *, label: str = "state"
) -> None:
    e2e_log(f"{label}: {await format_gateway_snapshot(settings, provider)}")


def format_admin_workers(workers: list[dict]) -> str:
    if not workers:
        return "(none)"
    bits: list[str] = []
    for w in workers[:8]:
        wid = str(w.get("id", "?"))[:8]
        bits.append(
            f"{wid}:{w.get('cloud_state', '?')}/"
            f"{'ok' if w.get('healthy') else 'x'}" + (" drain" if w.get("draining") else "")
        )
    if len(workers) > 8:
        bits.append(f"+{len(workers) - 8}more")
    return "[" + ", ".join(bits) + "]"

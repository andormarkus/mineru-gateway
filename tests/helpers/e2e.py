"""Polling helpers for AWS EC2 E2E tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress

import pytest
from httpx import AsyncClient

from mineru_gateway.config import GatewaySettings
from mineru_gateway.scheduler.scaling import compute_scaling_signal
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from tests.helpers.cloud import scheduler_poll_interval_seconds
from tests.helpers.e2e_log import E2eProgress, e2e_log, format_admin_workers


def min_queue_depth_for_desired_workers(desired: int, *, target_per_worker: int) -> int:
    """Queue depth floor so autoscale ``desired_workers`` stays at or above ``desired``."""
    if desired <= 0:
        return 0
    return (desired - 1) * target_per_worker + 1


async def fetch_admin_workers(client: AsyncClient) -> list[dict]:
    resp = await client.get("/admin/workers")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    return list(body.get("workers") or [])


def is_serviceable_admin_worker(worker: dict) -> bool:
    """Admin /workers row matches scheduler serviceable criteria."""
    return bool(
        worker.get("healthy")
        and worker.get("ready_at")
        and not worker.get("draining")
        and worker.get("desired_state") == "running"
        and worker.get("cloud_state") == "running"
        and worker.get("base_url")
    )


def find_replacement_for(workers: list[dict], old_worker_id: str) -> dict | None:
    for worker in workers:
        if worker.get("replacement_for") == old_worker_id:
            return worker
    return None


async def wait_for_rotation_complete(
    client: AsyncClient,
    old_worker_id: str,
    *,
    timeout_seconds: float,
    poll_interval: float | None = None,
    settings: GatewaySettings | None = None,
    maintain_queue: bool = False,
    queue_feed_bytes: bytes | None = None,
) -> tuple[dict, dict]:
    """Wait for replacement to become serviceable, then old worker draining.

    Fails if the old worker is drained before its replacement is serviceable
    (e.g. autoscaler idle stop racing ahead of rotation).

    When ``maintain_queue`` is true, submits a task whenever queue depth hits 0
    so autoscaler idle drain does not stop the rotated worker during cold boot.
    """
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress(f"wait rotation: {old_worker_id[:8]}")
    e2e_log(f"waiting for rotation of {old_worker_id} (timeout={timeout_seconds:.0f}s)")
    repo = WorkerRepository(settings) if settings is not None else None
    feed_index = 0

    replacement: dict | None = None
    while asyncio.get_running_loop().time() < deadline:
        if maintain_queue and repo is not None and queue_feed_bytes is not None:
            if await repo.count_queue_depth() == 0:
                await submit_pdf_task(
                    client,
                    filename=f"e2e-feed-{feed_index % 3 + 1}.pdf",
                    pdf_bytes=queue_feed_bytes,
                )
                feed_index += 1

        workers = await fetch_admin_workers(client)
        replacement = find_replacement_for(workers, old_worker_id)
        old = next((w for w in workers if w["id"] == old_worker_id), None)
        if old is None:
            pytest.fail(f"rotated worker {old_worker_id} disappeared from admin API")

        repl_serviceable = replacement is not None and is_serviceable_admin_worker(replacement)
        if old.get("draining") and not repl_serviceable:
            if old.get("drain_target") == "stopped":
                pytest.fail(
                    f"worker {old_worker_id} was idle-stopped by autoscaler before replacement was serviceable "
                    f"(replacement={format_admin_workers([replacement]) if replacement else 'missing'})"
                )
            pytest.fail(
                f"worker {old_worker_id} began draining before replacement was serviceable "
                f"(replacement={format_admin_workers([replacement]) if replacement else 'missing'})"
            )

        if repl_serviceable and old.get("draining"):
            e2e_log(
                f"rotation complete: old={old_worker_id[:8]} drain replacement={replacement['id'][:8]} ok",
                always=True,
            )
            return old, replacement

        bits = format_admin_workers(workers)
        if replacement is not None:
            bits += f" repl_svc={is_serviceable_admin_worker(replacement)} old_drain={old.get('draining')}"
        progress.report(bits)
        await asyncio.sleep(poll)

    pytest.fail(
        f"rotation of {old_worker_id} did not complete within {timeout_seconds}s "
        f"(replacement serviceable + old draining; last repl={replacement})"
    )


@asynccontextmanager
async def maintain_min_queue_depth(
    client: AsyncClient,
    settings: GatewaySettings,
    *,
    pdf_bytes: bytes,
    min_depth: int = 1,
    poll_interval: float | None = None,
    check_interval_seconds: float = 2.0,
) -> AsyncIterator[None]:
    """Submit tasks while queue depth stays below ``min_depth`` (holds autoscale desired count)."""
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    check_interval = min(check_interval_seconds, poll)
    stop = asyncio.Event()
    feed_index = 0

    async def _feed() -> None:
        nonlocal feed_index
        repo = WorkerRepository(settings)
        while not stop.is_set():
            try:
                while not stop.is_set() and await repo.count_queue_depth() < min_depth:
                    await submit_pdf_task(
                        client,
                        filename=f"e2e-keepalive-{feed_index % 3 + 1}.pdf",
                        pdf_bytes=pdf_bytes,
                    )
                    feed_index += 1
            except Exception:
                pass
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=check_interval)

    feeder = asyncio.create_task(_feed())
    try:
        yield
    finally:
        stop.set()
        feeder.cancel()
        with suppress(asyncio.CancelledError):
            await feeder


def _baseline_idle_stop_started(workers: list[dict], baseline_running: set[str]) -> bool:
    """True when autoscaler idle stop is visible on a baseline worker."""
    for worker in workers:
        if worker["id"] not in baseline_running:
            continue
        if worker.get("desired_state") == "stopped":
            return True
        if worker.get("draining") and worker.get("drain_target") == "stopped":
            return True
    return False


async def wait_for_autoscaler_idle_drain(
    client: AsyncClient,
    settings: GatewaySettings,
    *,
    idle_timeout_seconds: float,
    poll_interval: float | None = None,
    precondition_timeout_seconds: float | None = None,
) -> list[dict]:
    """Wait for autoscaler idle stop on a worker that was running when the queue emptied.

    Snapshot baseline workers immediately after the queue drains. Rotation terminate may
    still be in flight; once it clears, idle stop usually follows within one scheduler tick
    (workers are already past cooldown after test_03 backlog drains).
    """
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    precondition_timeout = (
        precondition_timeout_seconds if precondition_timeout_seconds is not None else idle_timeout_seconds
    )
    await wait_for_queue_empty(settings, timeout_seconds=precondition_timeout, poll_interval=poll)

    initial = await fetch_admin_workers(client)
    baseline_running = {
        w["id"]
        for w in initial
        if w.get("desired_state") == "running" and w.get("cloud_state") == "running"
    }
    e2e_log(
        f"idle-drain baseline: running_workers={sorted(baseline_running) or '(none)'}",
        always=True,
    )
    if not baseline_running:
        pytest.fail("idle-drain precondition: expected at least one running worker")

    repo = WorkerRepository(settings)
    scaling = settings.scaling
    deadline = asyncio.get_running_loop().time() + precondition_timeout + idle_timeout_seconds
    progress = E2eProgress("wait idle drain")
    e2e_log(
        "waiting for autoscaler idle stop on baseline worker "
        f"(timeout={precondition_timeout + idle_timeout_seconds:.0f}s)",
        always=True,
    )
    last_workers: list[dict] = []
    last_signal = ""
    while asyncio.get_running_loop().time() < deadline:
        last_workers = await fetch_admin_workers(client)
        if _baseline_idle_stop_started(last_workers, baseline_running):
            e2e_log(f"idle drain observed: {format_admin_workers(last_workers)}", always=True)
            return last_workers

        inputs = await repo.collect_scaling_inputs()
        signal = compute_scaling_signal(
            inputs=inputs,
            target_per_worker=scaling.target_per_worker,
            min_workers=scaling.min_workers,
            max_workers=scaling.max_workers,
        )
        last_signal = (
            f"svc={signal.serviceable_workers} desired={signal.desired_workers} "
            f"starting={signal.starting_workers} draining={signal.draining_workers} "
            f"stopping={signal.stopping_workers} queue={signal.queue_depth}"
        )
        progress.report(f"{format_admin_workers(last_workers)} | {last_signal}")
        await asyncio.sleep(poll)

    pytest.fail(
        "autoscaler idle stop not observed within "
        f"{precondition_timeout + idle_timeout_seconds:.0f}s "
        f"(baseline={sorted(baseline_running)} last={last_workers} signal={last_signal})"
    )


async def wait_for_admin_workers(
    client: AsyncClient,
    *,
    predicate: Callable[[list[dict]], bool],
    timeout_seconds: float,
    poll_interval: float | None = None,
    description: str = "condition",
) -> list[dict]:
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress(f"wait admin: {description}")
    e2e_log(f"waiting for admin workers: {description} (timeout={timeout_seconds:.0f}s)")
    last: list[dict] = []
    while asyncio.get_running_loop().time() < deadline:
        last = await fetch_admin_workers(client)
        if predicate(last):
            e2e_log(f"admin workers ready: {format_admin_workers(last)}", always=True)
            return last
        progress.report(format_admin_workers(last))
        await asyncio.sleep(poll)
    pytest.fail(f"admin workers did not satisfy {description} within {timeout_seconds}s (last={last})")


async def wait_for_provisioned_workers(
    settings: GatewaySettings,
    *,
    count: int,
    timeout_seconds: float,
    poll_interval: float | None = None,
) -> int:
    """Wait until serviceable + starting workers reach ``count``."""
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    repo = WorkerRepository(settings)
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress(f"wait provisioned>={count}")
    e2e_log(f"waiting for >={count} provisioned workers (timeout={timeout_seconds:.0f}s)")
    last = 0
    while asyncio.get_running_loop().time() < deadline:
        serviceable, starting = await asyncio.gather(
            repo.count_serviceable_workers(),
            repo.count_starting_workers(),
        )
        last = serviceable + starting
        if last >= count:
            e2e_log(f"provisioned workers ready: {last}/{count}", always=True)
            return last
        progress.report(f"provisioned={last}/{count}", signature=f"prov={last}/{count}")
        await asyncio.sleep(poll)
    pytest.fail(f"expected >={count} provisioned workers within {timeout_seconds}s (last={last})")


async def wait_for_queue_empty(
    settings: GatewaySettings,
    *,
    timeout_seconds: float,
    poll_interval: float | None = None,
) -> None:
    poll = scheduler_poll_interval_seconds() if poll_interval is None else poll_interval
    repo = WorkerRepository(settings)
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    progress = E2eProgress("wait queue empty")
    e2e_log(f"waiting for queue empty (timeout={timeout_seconds:.0f}s)")
    last = -1
    while asyncio.get_running_loop().time() < deadline:
        last = await repo.count_queue_depth()
        if last == 0:
            e2e_log("queue empty", always=True)
            return
        progress.report(f"queue_depth={last}", signature=f"q={last}")
        await asyncio.sleep(poll)
    pytest.fail(f"queue not empty within {timeout_seconds}s (last depth={last})")


async def submit_pdf_task(client: AsyncClient, *, filename: str, pdf_bytes: bytes) -> str:
    e2e_log(f"submitting task file={filename} bytes={len(pdf_bytes)}", always=True)
    resp = await client.post(
        "/tasks",
        files=[("files", (filename, pdf_bytes, "application/pdf"))],
        data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
    )
    assert resp.status_code == 202, resp.text
    return resp.json()["task_id"]

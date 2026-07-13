"""Single sequential scheduler loop."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING

import httpx

from mineru_gateway.cloud.errors import CloudError
from mineru_gateway.cloud.types import (
    CLOUD_STATE_PENDING,
    CLOUD_STATE_RUNNING,
    CLOUD_STATE_STOPPED,
    CLOUD_STATE_STOPPING,
    CLOUD_STATE_TERMINATED,
    CLOUD_STATE_TERMINATING,
    CLOUD_STATE_UNKNOWN,
    TAG_MANAGED,
    TAG_ROLE,
    TAG_ROLE_WORKER,
    DiscoveredInstance,
    cloud_state_from_instance,
)
from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.models import Worker
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scheduler._http import HEALTH_TIMEOUT
from mineru_gateway.scheduler.cache_service import CacheService
from mineru_gateway.scheduler.scaling import ScalingSignal, compute_scaling_signal
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.tasks.storage import safe_delete
from mineru_gateway.util.datetime import ensure_aware_utc, now_utc

if TYPE_CHECKING:
    from mineru_gateway.cloud.base import CloudStorageProvider, ComputeProvider
    from mineru_gateway.scheduler.lock import PostgresAdvisoryLock, _NoOpLock

logger = logging.getLogger(__name__)

_LOCK = type("Lock", (), {"held": True})()
_HEALTH_BATCH = 32


@dataclass(frozen=True)
class _ReconcileResult:
    reset_cloud_retries: bool
    failure_recorded: bool


class Scheduler:
    """One scheduler — sequential tick with bounded work per step."""

    def __init__(
        self,
        *,
        settings: GatewaySettings,
        store: CloudStorageProvider,
        client: httpx.AsyncClient,
        provider: ComputeProvider | None,
        lock: PostgresAdvisoryLock | _NoOpLock | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._client = client
        self._provider = provider
        self._lock = lock or _LOCK
        self._workers = WorkerRepository(settings)
        self._cache = CacheService(settings, store)
        self._tasks = TaskRepository(settings, store, client, self._workers)
        self._scale_up_cooldown = timedelta(seconds=settings.scaling.scale_up_cooldown_seconds)
        self._idle_cooldown = timedelta(seconds=settings.scaling.idle_cooldown_seconds)
        self._last_scale_up: datetime | None = None
        self._next_cleanup_at: datetime | None = None
        self._next_cache_sweep_at: datetime | None = None
        self._next_rotation_check_at: datetime | None = None
        self._poll_interval = settings.scheduler.poll_interval_seconds

    async def run(self) -> None:
        logger.info("Scheduler loop starting")
        while self._lock_held():
            started = monotonic()
            try:
                await self.tick()
            except Exception:
                logger.exception("Scheduler tick failed")
                await asyncio.sleep(self._poll_interval)
                continue
            elapsed = monotonic() - started
            await asyncio.sleep(max(0.0, self._poll_interval - elapsed))
        logger.warning("Scheduler loop stopped — lock lost")

    def _lock_held(self) -> bool:
        if hasattr(self._lock, "verify"):
            return self._lock.held  # type: ignore[union-attr]
        return getattr(self._lock, "held", True)

    async def tick(self) -> None:
        if not await self._verify_lock():
            return
        await self._reconcile_workers()
        await self._converge_stalled_workers()
        await self._refresh_worker_health()
        await self._tasks.recover_stale_dispatch_claims()
        await self._synchronize_tasks_and_results()
        expired = await self._tasks.expire_client_sla_tasks(sla_seconds=self._settings.task_sla_seconds)
        if expired:
            metrics.record_sla_expired(expired)
            logger.info("Client SLA expired for %d tasks", expired)
        await self._dispatch_queued_tasks()
        await self._apply_autoscaling()
        await self._advance_drains_and_rotations()
        await self._cleanup_if_due()

    async def _verify_lock(self) -> bool:
        if not hasattr(self._lock, "verify"):
            return True
        if not await self._lock.verify():  # type: ignore[union-attr]
            self._lock.held = False  # type: ignore[union-attr]
            return False
        return True

    async def _reconcile_workers(self) -> None:
        if self._provider is None or not await self._verify_lock():
            return
        now = now_utc()
        workers = await self._workers.list_deployment_workers()
        discovered = await self._provider.discover(self._settings.deployment_id)
        worker_ids = {w.id for w in workers}
        worker_by_id = {w.id: w for w in workers}

        discovered_managed, discovered_primary = await self._discover_and_dedupe(discovered, worker_by_id)

        for worker in workers:
            if self._should_skip_reconcile(worker, now):
                continue
            try:
                result = await self._reconcile_one_worker(worker, discovered_primary.get(worker.id))
                if result.reset_cloud_retries and not result.failure_recorded:
                    await self._workers.reset_cloud_failures(worker.id)
            except CloudError as exc:
                metrics.record_cloud_error(category=exc.category.value, retryable=exc.retryable)
                await self._workers.record_failure(worker.id, str(exc), retryable=exc.retryable)
            except Exception as exc:
                metrics.record_cloud_error(category="unknown", retryable=True)
                await self._workers.record_failure(worker.id, str(exc), retryable=True)

        await self._terminate_orphan_vms(discovered_managed, worker_ids)

    async def _discover_and_dedupe(
        self, discovered: list[DiscoveredInstance], worker_by_id: dict[str, Worker]
    ) -> tuple[list[DiscoveredInstance], dict[str, DiscoveredInstance]]:
        """Filter discovered VMs to managed workers and resolve duplicate VMs per worker.

        Returns ``(managed_instances, primary_instance_by_worker_id)``. Duplicate VMs (more than
        one instance tagged with the same worker id) are de-duplicated by keeping the canonical
        instance id and terminating the rest.
        """
        discovered_managed: list[DiscoveredInstance] = []
        discovered_by_worker: dict[str, list[DiscoveredInstance]] = defaultdict(list)
        for inst in discovered:
            if inst.tags.get(TAG_MANAGED) != "true" or inst.tags.get(TAG_ROLE) != TAG_ROLE_WORKER:
                continue
            discovered_managed.append(inst)
            if inst.worker_id:
                discovered_by_worker[inst.worker_id].append(inst)

        discovered_primary: dict[str, DiscoveredInstance] = {}
        for worker_id, instances in discovered_by_worker.items():
            if len(instances) == 1:
                discovered_primary[worker_id] = instances[0]
                continue
            discovered_primary[worker_id] = await self._resolve_duplicate_vms(
                worker_id, instances, worker_by_id.get(worker_id)
            )
        return discovered_managed, discovered_primary

    async def _resolve_duplicate_vms(
        self, worker_id: str, instances: list[DiscoveredInstance], worker: Worker | None
    ) -> DiscoveredInstance:
        """Pick the canonical instance for a worker with duplicate VMs; terminate the others."""
        discovered_ids = {inst.instance_id for inst in instances}
        if worker is not None and worker.instance_id and worker.instance_id in discovered_ids:
            canonical_id = worker.instance_id
        else:
            canonical_id = sorted(discovered_ids)[0]
        primary = next(inst for inst in instances if inst.instance_id == canonical_id)
        if worker is not None and worker.instance_id != canonical_id:
            await self._workers.commit_fields(
                worker.id, instance_id=canonical_id, cloud_state=cloud_state_from_instance(primary.state)
            )
            worker.instance_id = canonical_id
        for inst in instances:
            if inst.instance_id == canonical_id:
                continue
            await self._terminate_unowned_vm(inst, reason=f"duplicate vm for worker {worker_id} (kept {canonical_id})")
        return primary

    def _should_skip_reconcile(self, worker: Worker, now: datetime) -> bool:
        """True when a worker is stalled or still within its retry-backoff window."""
        max_failures = self._settings.reconciliation.max_failure_count
        if worker.failure_count >= max_failures and worker.desired_state != CLOUD_STATE_TERMINATED:
            return True
        return (
            worker.failure_count < max_failures
            and worker.retry_after is not None
            and ensure_aware_utc(worker.retry_after) > now
        )

    async def _terminate_orphan_vms(self, discovered_managed: list[DiscoveredInstance], worker_ids: set[str]) -> None:
        """Terminate managed VMs that don't map to a known worker."""
        for inst in discovered_managed:
            if inst.worker_id and inst.worker_id in worker_ids:
                continue
            reason = (
                f"orphan vm for unknown worker {inst.worker_id}"
                if inst.worker_id
                else "managed vm missing worker id tag"
            )
            await self._terminate_unowned_vm(inst, reason=reason)

    async def _terminate_unowned_vm(self, inst: DiscoveredInstance, *, reason: str) -> None:
        if self._provider is None:
            return
        logger.warning("Terminating unowned VM %s (%s)", inst.instance_id, reason)
        try:
            await self._provider.terminate(inst.instance_id)
            await self._workers.record_scaling_event_now(
                action="terminate", reason=reason, triggered_by="reconciliation"
            )
        except Exception:
            logger.exception("Failed to terminate unowned VM %s", inst.instance_id)

    async def _reconcile_one_worker(self, worker: Worker, discovered: DiscoveredInstance | None) -> _ReconcileResult:
        provider = self._provider
        if provider is None:
            return _ReconcileResult(reset_cloud_retries=False, failure_recorded=False)

        if discovered is not None:
            await self._sync_discovered_state(worker, discovered)

        if worker.instance_id is None and worker.desired_state == CLOUD_STATE_RUNNING:
            return await self._launch_missing_instance(worker, provider, discovered)

        if worker.instance_id is None:
            if worker.desired_state == CLOUD_STATE_TERMINATED:
                await self._workers.commit_fields(
                    worker.id, cloud_state=CLOUD_STATE_TERMINATED, terminated_at=now_utc()
                )
            return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

        state = await provider.get_state(worker.instance_id)
        cloud_state = cloud_state_from_instance(state)
        if cloud_state == CLOUD_STATE_TERMINATED and worker.desired_state in (CLOUD_STATE_RUNNING, CLOUD_STATE_STOPPED):
            await self._workers.finalize_disappeared_instance(worker.id, reason="cloud instance terminated or missing")
            return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

        if cloud_state != worker.cloud_state:
            await self._workers.commit_fields(worker.id, cloud_state=cloud_state)
            worker.cloud_state = cloud_state
            if cloud_state in (CLOUD_STATE_STOPPED, CLOUD_STATE_TERMINATED):
                await self._workers.commit_fields(
                    worker.id, base_url=None, ready_at=None, healthy=False, last_health_checked_at=now_utc()
                )

        transitioned = await self._transition_power_state(worker, provider, cloud_state)
        if transitioned is not None:
            return transitioned

        failure_recorded = False
        if worker.desired_state == CLOUD_STATE_RUNNING and cloud_state == CLOUD_STATE_RUNNING:
            failure_recorded = await self._check_running_readiness(worker, provider)
        return _ReconcileResult(reset_cloud_retries=not failure_recorded, failure_recorded=failure_recorded)

    async def _sync_discovered_state(self, worker: Worker, discovered: DiscoveredInstance) -> None:
        """Apply instance-id and cloud-state updates from a freshly discovered VM."""
        cloud_state = cloud_state_from_instance(discovered.state)
        if worker.instance_id != discovered.instance_id:
            await self._workers.commit_fields(worker.id, instance_id=discovered.instance_id)
            worker.instance_id = discovered.instance_id
        if cloud_state != CLOUD_STATE_UNKNOWN and worker.cloud_state != cloud_state:
            await self._workers.commit_fields(worker.id, cloud_state=cloud_state)
            worker.cloud_state = cloud_state

    async def _launch_missing_instance(
        self, worker: Worker, provider: ComputeProvider, discovered: DiscoveredInstance | None
    ) -> _ReconcileResult:
        """Bind a discovered VM or launch a new one for a running worker with no instance yet."""
        if discovered is not None:
            await self._workers.commit_fields(
                worker.id, instance_id=discovered.instance_id, cloud_state=cloud_state_from_instance(discovered.state)
            )
            worker.instance_id = discovered.instance_id
        else:
            instance_id = await provider.launch(
                worker.id, deployment_id=worker.deployment_id, generation=worker.generation
            )
            await self._workers.commit_fields(worker.id, instance_id=instance_id, cloud_state=CLOUD_STATE_PENDING)
        return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

    async def _transition_power_state(
        self, worker: Worker, provider: ComputeProvider, cloud_state: str
    ) -> _ReconcileResult | None:
        """Issue start/stop/terminate to align cloud_state with desired_state; returns None when no-op."""
        if worker.desired_state == CLOUD_STATE_RUNNING and cloud_state == CLOUD_STATE_STOPPED:
            await provider.start(worker.instance_id)  # type: ignore[arg-type]
            await self._workers.commit_fields(
                worker.id,
                cloud_state=CLOUD_STATE_PENDING,
                failure_count=0,
                retry_after=None,
                start_requested_at=now_utc(),
                ready_at=None,
            )
            return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

        if worker.desired_state == CLOUD_STATE_STOPPED and cloud_state == CLOUD_STATE_RUNNING:
            await provider.stop(worker.instance_id)  # type: ignore[arg-type]
            await self._workers.commit_fields(worker.id, cloud_state=CLOUD_STATE_STOPPING)
            return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

        if worker.desired_state == CLOUD_STATE_TERMINATED and cloud_state != CLOUD_STATE_TERMINATED:
            await provider.terminate(worker.instance_id)  # type: ignore[arg-type]
            await self._workers.commit_fields(worker.id, cloud_state=CLOUD_STATE_TERMINATING)
            return _ReconcileResult(reset_cloud_retries=True, failure_recorded=False)

        return None

    async def _check_running_readiness(self, worker: Worker, provider: ComputeProvider) -> bool:
        """Sync the base_url and record a failure if the launch-readiness deadline passes. Returns True on failure."""
        ip = await provider.get_private_ip(worker.instance_id)  # type: ignore[arg-type]
        if ip and worker.base_url != f"http://{ip}:8000":
            await self._workers.commit_fields(worker.id, base_url=f"http://{ip}:8000")
        timeout = self._settings.reconciliation.launch_readiness_timeout_seconds
        deadline = worker.start_requested_at or worker.created_at
        if worker.ready_at is None and deadline is not None:
            age = (now_utc() - ensure_aware_utc(deadline)).total_seconds()
            if age > timeout and not worker.healthy:
                await self._workers.record_failure(worker.id, "launch readiness timeout", retryable=True)
                return True
        return False

    async def _converge_stalled_workers(self) -> None:
        grace_seconds = self._settings.reconciliation.stalled_worker_grace_seconds
        now = now_utc()
        for worker in await self._workers.find_stalled_workers():
            inflight = await self._workers.count_inflight_tasks(worker.id)
            unstored = await self._workers.count_unstored_results(worker.id)
            active_work = inflight > 0 or unstored > 0

            if not active_work:
                if worker.desired_state != CLOUD_STATE_TERMINATED:
                    await self._workers.commit_fields(worker.id, desired_state=CLOUD_STATE_TERMINATED)
                    await self._workers.record_scaling_event_now(
                        action="terminate",
                        reason="stalled worker with no active upstream work",
                        worker_id=worker.id,
                        triggered_by="reconciliation",
                    )
                continue

            stalled_at = worker.stalled_at
            if stalled_at is None:
                await self._workers.commit_fields(worker.id, stalled_at=now)
                stalled_at = now

            age_seconds = (now - ensure_aware_utc(stalled_at)).total_seconds()
            if age_seconds >= grace_seconds and worker.desired_state != CLOUD_STATE_TERMINATED:
                await self._workers.commit_fields(worker.id, desired_state=CLOUD_STATE_TERMINATED)
                await self._workers.record_scaling_event_now(
                    action="terminate",
                    reason="stalled worker grace period expired",
                    worker_id=worker.id,
                    triggered_by="reconciliation",
                )

    async def _refresh_worker_health(self) -> None:
        workers = await self._workers.list_health_check_candidates(limit=_HEALTH_BATCH)
        if not workers:
            return

        async def _check(worker: Worker) -> tuple[Worker, bool, str | None]:
            ok, error = await self._poll_health(worker.base_url or "")
            return worker, ok, error

        results = await asyncio.gather(*[_check(w) for w in workers])
        healthy_count = await self._workers.apply_health_checks(results)
        if healthy_count < len(workers):
            logger.info("Health: %d/%d workers healthy", healthy_count, len(workers))

    async def _poll_health(self, base_url: str) -> tuple[bool, str | None]:
        try:
            resp = await self._client.get(f"{base_url}/health", timeout=HEALTH_TIMEOUT)
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}"
            body = resp.json()
            if str(body.get("status", "")).lower() not in ("healthy", "ok"):
                return False, f"status={body.get('status')!r}"
            return True, None
        except (httpx.HTTPError, ValueError) as exc:
            return False, str(exc)

    async def _synchronize_tasks_and_results(self) -> None:
        await self._tasks.sync_dispatched_task_statuses()
        tasks = await self._tasks.list_unstored_completed()
        stored, failed = await self._tasks.persist_pending_results(tasks, cache_service=self._cache)
        if stored:
            metrics.record_result_stored(count=stored)
        if failed:
            metrics.record_result_store_failed(count=failed)

    async def _dispatch_queued_tasks(self) -> None:
        await self._tasks.dispatch_queued_tasks()

    async def _apply_autoscaling(self) -> None:
        if self._provider is None:
            return
        signal = await self._compute_scaling_signal()
        provisioned = signal.serviceable_workers + signal.starting_workers

        if signal.should_scale_up:
            now = now_utc()
            zero_recovery = provisioned == 0 and signal.desired_workers > 0
            if not zero_recovery and self._last_scale_up and now - self._last_scale_up < self._scale_up_cooldown:
                logger.debug("Autoscale skipped reason=cooldown")
                return
            if await self._scale_up():
                self._last_scale_up = now
                await self._workers.record_scaling_event_now(
                    action="start",
                    reason=f"desired {signal.desired_workers} > provisioned {provisioned}",
                    triggered_by="autoscaler",
                )
                metrics.record_worker_scaled(action="start")
                logger.info(
                    "Autoscale action=%s desired=%d provisioned=%d queue_depth=%d worker=%s",
                    "start",
                    signal.desired_workers,
                    provisioned,
                    signal.queue_depth,
                    "-",
                )
            return

        if signal.should_scale_down:
            idle = await self._workers.find_idle_worker(idle_before=now_utc() - self._idle_cooldown)
            if idle is not None:
                await self._workers.commit_fields(idle.id, draining=True, drain_target=CLOUD_STATE_STOPPED)
                await self._workers.record_scaling_event_now(
                    action="stop", reason="serviceable exceeds desired", worker_id=idle.id, triggered_by="autoscaler"
                )
                metrics.record_worker_scaled(action="stop")
                logger.info(
                    "Autoscale action=%s desired=%d provisioned=%d queue_depth=%d worker=%s",
                    "stop",
                    signal.desired_workers,
                    provisioned,
                    signal.queue_depth,
                    idle.id,
                )

    async def _compute_scaling_signal(self) -> ScalingSignal:
        cfg = self._settings.scaling
        return compute_scaling_signal(
            queue_depth=await self._workers.count_queue_depth(),
            serviceable_workers=await self._workers.count_serviceable_workers(),
            starting_workers=await self._workers.count_starting_workers(),
            target_per_worker=cfg.target_per_worker,
            min_workers=cfg.min_workers,
            max_workers=cfg.max_workers,
        )

    async def _scale_up(self) -> bool:
        stopped = await self._workers.find_stopped_worker()
        if stopped is not None:
            await self._workers.commit_fields(
                stopped.id,
                desired_state=CLOUD_STATE_RUNNING,
                failure_count=0,
                retry_after=None,
                start_requested_at=now_utc(),
                ready_at=None,
            )
            logger.info("Scale up path=restarted_stopped worker=%s", stopped.id)
            return True
        if await self._workers.count_workers() < self._settings.scaling.max_workers:
            new_worker_id = await self._workers.create_cloud_worker()
            logger.info("Scale up path=new_worker worker=%s", new_worker_id)
            return True
        stalled = await self._workers.count_stalled_workers()
        if (
            stalled > 0
            and await self._workers.count_serviceable_workers() == 0
            and await self._workers.count_workers() < self._settings.scaling.max_workers + 1
        ):
            new_worker_id = await self._workers.create_cloud_worker()
            logger.info("Scale up path=emergency worker=%s", new_worker_id)
            return True
        return False

    async def _advance_drains_and_rotations(self) -> None:
        await self._progress_rotations()
        await self._advance_drains()
        await self._finalize_terminations()
        now = now_utc()
        if self._next_rotation_check_at is None or now >= self._next_rotation_check_at:
            await self._start_new_rotations()
            self._next_rotation_check_at = now + timedelta(seconds=self._settings.rotation.interval_seconds)

    async def _advance_drains(self) -> None:
        for worker in await self._workers.find_draining_workers():
            inflight = await self._workers.count_inflight_tasks(worker.id)
            unstored = await self._workers.count_unstored_results(worker.id)
            if inflight > 0 or unstored > 0:
                continue
            if await self._workers.has_active_replacement_for(worker.id):
                if await self._workers.get_ready_replacement_for(worker.id) is None:
                    logger.warning("Deferring termination of %s — replacement not serviceable", worker.id)
                    continue
                await self._workers.commit_fields(worker.id, desired_state=CLOUD_STATE_TERMINATED)
            elif worker.drain_target == CLOUD_STATE_TERMINATED:
                await self._workers.commit_fields(worker.id, desired_state=CLOUD_STATE_TERMINATED)
            else:
                await self._workers.commit_fields(
                    worker.id, desired_state=CLOUD_STATE_STOPPED, draining=False, drain_target=None
                )

    def _worker_is_ready(self, worker: Worker) -> bool:
        return (
            worker.desired_state == CLOUD_STATE_RUNNING
            and worker.cloud_state == CLOUD_STATE_RUNNING
            and worker.healthy
            and worker.ready_at is not None
            and worker.base_url is not None
        )

    async def _progress_rotations(self) -> None:
        readiness_timeout = self._settings.rotation.readiness_timeout_seconds
        now = now_utc()

        for replacement in await self._workers.find_replacement_workers():
            old_id = replacement.replacement_for
            if old_id is None:
                continue

            old = await self._workers.get_worker(old_id) if old_id else None

            if (
                replacement.desired_state != CLOUD_STATE_TERMINATED
                and not self._worker_is_ready(replacement)
                and replacement.start_requested_at is not None
            ):
                age = (now - ensure_aware_utc(replacement.start_requested_at)).total_seconds()
                if age > readiness_timeout:
                    await self._workers.commit_fields(replacement.id, desired_state=CLOUD_STATE_TERMINATED)
                    logger.warning(
                        "Rotation replacement %s missed readiness deadline; terminating replacement", replacement.id
                    )
                    continue

            if (
                self._worker_is_ready(replacement)
                and old is not None
                and not old.draining
                and old.terminated_at is None
            ):
                await self._workers.commit_fields(old.id, draining=True)

    async def _finalize_terminations(self) -> None:
        for worker in await self._workers.find_workers_pending_termination_finalize():
            await self._workers.finalize_terminated_worker(worker.id)

    async def _start_new_rotations(self) -> None:
        if self._provider is None:
            return
        if await self._workers.count_active_replacements() > 0:
            return
        if await self._workers.count_workers() >= self._settings.scaling.max_workers + 1:
            return

        target = await self._workers.find_emergency_rotation_target()
        if target is None:
            cutoff = now_utc() - timedelta(seconds=self._settings.rotation.interval_seconds)
            target = await self._workers.find_scheduled_rotation_target(created_before=cutoff)
        if target is None:
            return

        replacement_id = await self._workers.start_rotation_replacement(target)
        logger.info("Started rotation: replacement %s for worker %s", replacement_id, target.id)

    async def _cleanup_if_due(self) -> None:
        now = now_utc()
        if self._next_cleanup_at is not None and now < self._next_cleanup_at:
            return
        self._next_cleanup_at = now + timedelta(seconds=self._settings.retention.cleanup_interval_seconds)
        await self._cleanup_expired_tasks()
        if self._next_cache_sweep_at is None or now >= self._next_cache_sweep_at:
            self._next_cache_sweep_at = now + timedelta(seconds=self._settings.cache.sweeper_interval_seconds)
            removed = await self._cache.sweep_expired()
            if removed:
                metrics.record_cache_sweep_removed(count=removed)

    async def _cleanup_expired_tasks(self) -> None:
        cutoff = now_utc() - timedelta(days=self._settings.retention.retention_days)
        tasks = await self._tasks.list_retention_expired_tasks(cutoff=cutoff)

        for task in tasks:
            if not await self._delete_object_keys(task.payload_key, task.result_key):
                continue
            if await self._tasks.delete_task(task.task_id):
                metrics.record_retention_deleted(kind="task")

    async def _delete_object_keys(self, *keys: str | None) -> bool:
        for key in keys:
            if not await safe_delete(self._store, key, label="object"):
                return False
        return True

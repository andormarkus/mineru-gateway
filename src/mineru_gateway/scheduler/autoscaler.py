"""Target-tracking autoscaler loop.

Background loop (~10s): compute the queue-depth signal, and:
  - signal > target_per_worker AND running < max_workers → start one worker
    (audit → scaling_events).
  - worker idle longer than idle_cooldown AND running > min_workers → stop.
  - scale_up_cooldown prevents thrash.

Why target-tracking: cloud-agnostic (signal is DB-side), survives restart, maps onto AWS target-tracking later. Knobs
configurable (min == max for fixed).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Worker
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scaling.audit import record_scaling_event
from mineru_gateway.scaling.policy import ScalingSignal, compute_signal
from mineru_gateway.util.datetime import ensure_aware_utc

logger = logging.getLogger(__name__)


class Autoscaler:
    """Background loop that scales workers based on queue depth (CLOUD_WORKERS.md two-tier model).

    Scale-up: Tier A ``resume_instance`` on a suspended worker (fast, ~30-60s). Falls back to Tier B
    ``launch_instance`` from the launch template only if no stopped workers exist and under ``max_workers``.
    Scale-down: Tier A ``suspend_instance`` (never terminate — preserves disk + private IP for fast resume).
    """

    def __init__(
        self,
        *,
        target_per_worker: int = 4,
        min_workers: int = 0,
        max_workers: int = 12,
        idle_cooldown_seconds: int = 300,
        scale_up_cooldown_seconds: int = 60,
        provider: Any = None,
    ) -> None:
        self.target = target_per_worker
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.idle_cooldown = timedelta(seconds=idle_cooldown_seconds)
        self.scale_up_cooldown = timedelta(seconds=scale_up_cooldown_seconds)
        self._provider = provider
        self._last_scale_up: datetime | None = None

    @classmethod
    def from_settings(cls, provider: Any = None) -> Autoscaler:
        """Build an Autoscaler from the gateway's ScalingConfig, optionally with a cloud provider."""
        cfg = get_settings().scaling
        return cls(
            target_per_worker=cfg.target_per_worker,
            min_workers=cfg.min_workers,
            max_workers=cfg.max_workers,
            idle_cooldown_seconds=cfg.idle_cooldown_seconds,
            scale_up_cooldown_seconds=cfg.scale_up_cooldown_seconds,
            provider=provider,
        )

    async def evaluate_once(self) -> ScalingSignal:
        """One evaluation tick — uses Tier A (resume/suspend) with Tier B (launch) fallback."""
        signal = await compute_signal(self.target)

        if signal.should_scale_up and signal.running_workers < self.max_workers:
            now = datetime.now(UTC)
            if self._within_cooldown(now):
                logger.debug("Autoscaler: scale-up skipped (cooldown active)")
                return signal
            logger.info(
                "Scaling up: signal=%.1f > target=%d, workers=%d/%d",
                signal.signal,
                self.target,
                signal.running_workers,
                self.max_workers,
            )
            if await self._scale_up():
                await self._record_scale_event(action="start", reason=f"signal {signal.signal:.1f} > {self.target}")
                self._last_scale_up = now
            return signal

        if signal.running_workers > self.min_workers:
            idle_worker = await self._find_idle_worker()
            if idle_worker is not None:
                logger.info("Scaling down: worker %s idle", idle_worker.id)
                if await self._scale_down(idle_worker):
                    await self._record_scale_event(
                        action="stop", reason="idle cooldown elapsed", worker_id=idle_worker.id
                    )
                return signal

        logger.debug(
            "Autoscaler: no action (signal=%.1f target=%d running=%d/%d)",
            signal.signal,
            self.target,
            signal.running_workers,
            self.max_workers,
        )
        return signal

    async def _scale_up(self) -> bool:
        """Tier A: resume a suspended worker. Fallback: Tier B launch from template."""
        if self._provider is None:
            logger.warning("Autoscaler scale-up requested but no cloud provider configured")
            return False
        stopped = await self._find_stopped_worker()
        if stopped is not None and self._provider is not None:
            logger.info("Tier A scale-up: resuming suspended worker %s", stopped.id)
            if stopped.cloud_instance_id:
                await self._provider.resume_instance(stopped.cloud_instance_id)
                stopped.state = "starting"
                await self._commit_worker(stopped)
                metrics.record_scaling_event(action="start", tier="A")
                return True
            logger.warning("Stopped worker %s has no cloud_instance_id — skipping Tier A start", stopped.id)

        if await self._count_workers() < self.max_workers:
            logger.info("Tier B scale-up: launching from launch template")
            return await self._launch_from_template()
        return False

    async def _scale_down(self, worker: Worker) -> bool:
        """Tier A: suspend (never terminate — preserves disk + private IP for fast resume)."""
        if self._provider is not None and worker.cloud_instance_id:
            logger.info("Tier A scale-down: suspending worker %s", worker.id)
            await self._provider.suspend_instance(worker.cloud_instance_id)
        elif worker.cloud_instance_id is None:
            logger.debug("Scale-down for worker %s — no cloud instance to stop", worker.id)
        worker.state = "stopped"
        await self._commit_worker(worker)
        metrics.record_scaling_event(action="stop", tier="A")
        return True

    async def _launch_from_template(self) -> bool:
        """Tier B fallback: launch a new instance from the configured launch template."""
        if self._provider is None:
            return False
        cloud = get_settings().cloud
        template_id, template_version, region = cloud.launch_template()
        if not template_id:
            logger.warning("Cannot launch: no launch_template_id configured for provider %s", cloud.provider)
            return False
        try:
            instance_id = await self._provider.launch_instance(template_id, version=template_version)
            ip = await self._provider.get_private_ip(instance_id)
            worker_id = f"cloud-{instance_id}"
            async with get_db_session() as session:
                session.add(
                    Worker(
                        id=worker_id,
                        source=cloud.provider,
                        cloud_instance_id=instance_id,
                        cloud_region=region,
                        base_url=f"http://{ip}:8000",
                        state="starting",
                    )
                )
                await session.commit()
            logger.info("Tier B launch succeeded: worker %s instance %s", worker_id, instance_id)
            metrics.record_scaling_event(action="launch", tier="B")
            return True
        except Exception:
            logger.exception("Tier B launch failed")
            return False

    async def _find_stopped_worker(self) -> Worker | None:
        """Find a stopped cloud worker that can be resumed (Tier A)."""
        async with get_db_session() as session:
            stmt = (
                select(Worker)
                .where(Worker.enabled.is_(True), Worker.state == "stopped", Worker.cloud_instance_id.isnot(None))
                .limit(1)
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def _count_workers(self) -> int:
        """Count all enabled workers."""
        async with get_db_session() as session:
            stmt = select(func.count(Worker.id)).where(Worker.enabled.is_(True))
            return (await session.execute(stmt)).scalar() or 0

    async def _commit_worker(self, worker: Worker) -> None:
        """Persist a worker's state change."""
        async with get_db_session() as session:
            row = await session.get(Worker, worker.id)
            if row is not None:
                row.state = worker.state
                await session.commit()

    def _within_cooldown(self, now: datetime) -> bool:
        """True if we're still in the scale-up cooldown window."""
        if self._last_scale_up is None:
            return False
        return now - self._last_scale_up < self.scale_up_cooldown

    async def _find_idle_worker(self) -> Worker | None:
        """Find a running worker whose last_active_at exceeds the idle cooldown."""
        cutoff = datetime.now(UTC) - self.idle_cooldown
        async with get_db_session() as session:
            stmt = (
                select(Worker)
                .where(Worker.enabled.is_(True), Worker.state == "running")
                .order_by(Worker.last_active_at.asc())
            )
            candidates = (await session.execute(stmt)).scalars().all()
        for w in candidates:
            last_active = w.last_active_at
            if last_active is not None and ensure_aware_utc(last_active) < cutoff:
                return w
        return None

    async def _record_scale_event(self, *, action: str, reason: str, worker_id: str | None = None) -> None:
        """Record an autoscaler-triggered ScalingEvent row (its own session + commit)."""
        async with get_db_session() as session:
            await record_scaling_event(
                session, action=action, reason=reason, worker_id=worker_id, triggered_by="autoscaler", commit=True
            )

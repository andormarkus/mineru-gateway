"""Deployment-scoped worker persistence and dispatch selection."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import ScalingEvent, Task, Worker
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.tasks.status import (
    TASK_STATUSES_AUTOSCALE_DEMAND,
    TASK_STATUSES_COMPUTE_CAPACITY,
    TASK_STATUSES_DRAIN_BLOCKERS,
)
from mineru_gateway.util.datetime import now_utc

_RETRY_SECONDS: tuple[int, ...] = (5, 15, 30, 60, 120)
_RETRY_CAP_SECONDS = 300


def retry_delay(failure_count: int) -> int:
    if failure_count <= len(_RETRY_SECONDS):
        return _RETRY_SECONDS[failure_count - 1]
    return _RETRY_CAP_SECONDS


class WorkerRepository:
    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings

    def _deployment_clause(self):
        return and_(
            Worker.deployment_id == self._settings.deployment_id,
            Worker.provider == self._settings.cloud.provider,
            Worker.terminated_at.is_(None),
        )

    async def list_deployment_workers(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(self._deployment_clause())
            return list((await session.execute(query)).scalars().all())

    async def commit_fields(self, worker_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields = {**fields, "updated_at": now_utc()}
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None:
                return
            for key, value in fields.items():
                setattr(row, key, value)
            await session.commit()

    def _stalled_clause(self):
        return Worker.failure_count >= self._settings.reconciliation.max_failure_count

    def _serviceable_clause(self):
        return and_(
            self._deployment_clause(),
            Worker.desired_state == "running",
            Worker.cloud_state == "running",
            Worker.healthy.is_(True),
            Worker.base_url.isnot(None),
            Worker.draining.is_(False),
            Worker.failure_count < self._settings.reconciliation.max_failure_count,
        )

    def _ready_serviceable_clause(self):
        return and_(self._serviceable_clause(), Worker.ready_at.isnot(None))

    def _dispatchable_clause(self, *, active_count):
        return and_(self._serviceable_clause(), active_count < self._settings.scaling.target_per_worker)

    def _compute_capacity_subquery(self):
        return (
            select(func.count(Task.task_id))
            .where(Task.worker_id == Worker.id, Task.status.in_(TASK_STATUSES_COMPUTE_CAPACITY))
            .correlate(Worker)
            .scalar_subquery()
        )

    def _drain_blocker_subquery(self):
        return (
            select(func.count(Task.task_id))
            .where(Task.worker_id == Worker.id, Task.status.in_(TASK_STATUSES_DRAIN_BLOCKERS))
            .correlate(Worker)
            .scalar_subquery()
        )

    async def acquire_dispatchable(
        self, session: AsyncSession, *, excluded_ids: set[str] | None = None
    ) -> Worker | None:
        excluded = excluded_ids or set()
        active_count = self._compute_capacity_subquery()
        query = (
            select(Worker)
            .where(self._dispatchable_clause(active_count=active_count))
            .order_by(active_count.asc(), Worker.last_active_at.asc().nullsfirst(), Worker.id.asc())
            .with_for_update(skip_locked=True)
        )
        if excluded:
            query = query.where(~Worker.id.in_(excluded))
        for worker in (await session.execute(query)).scalars().all():
            worker.last_active_at = now_utc()
            return worker
        return None

    async def count_workers(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(self._deployment_clause())
            return (await session.execute(query)).scalar() or 0

    async def count_serviceable_workers(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(self._serviceable_clause())
            return (await session.execute(query)).scalar() or 0

    async def count_starting_workers(self) -> int:
        max_failures = self._settings.reconciliation.max_failure_count
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(
                self._deployment_clause(),
                Worker.desired_state == "running",
                Worker.cloud_state.in_(("pending", "unknown", "starting")),
                Worker.draining.is_(False),
                Worker.failure_count < max_failures,
            )
            return (await session.execute(query)).scalar() or 0

    async def find_stalled_workers(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(
                self._deployment_clause(),
                self._stalled_clause(),
                Worker.desired_state != "terminated",
                Worker.terminated_at.is_(None),
            )
            return list((await session.execute(query)).scalars().all())

    async def count_stalled_workers(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(
                self._deployment_clause(),
                self._stalled_clause(),
                Worker.desired_state != "terminated",
                Worker.terminated_at.is_(None),
            )
            return (await session.execute(query)).scalar() or 0

    async def count_queue_depth(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Task.task_id)).where(Task.status.in_(TASK_STATUSES_AUTOSCALE_DEMAND))
            return (await session.execute(query)).scalar() or 0

    async def count_recoverable_workers(self) -> int:
        return (await self.count_serviceable_workers()) + (await self.count_starting_workers())

    async def find_stopped_worker(self) -> Worker | None:
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(
                    self._deployment_clause(),
                    Worker.desired_state == "stopped",
                    Worker.cloud_state == "stopped",
                    Worker.instance_id.isnot(None),
                    Worker.draining.is_(False),
                )
                .limit(1)
            )
            return (await session.execute(query)).scalar_one_or_none()

    async def find_idle_worker(self, *, idle_before: datetime) -> Worker | None:
        inflight = self._drain_blocker_subquery()
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(
                    self._deployment_clause(),
                    Worker.desired_state == "running",
                    Worker.cloud_state == "running",
                    Worker.healthy.is_(True),
                    Worker.draining.is_(False),
                    Worker.replacement_for.is_(None),
                    inflight == 0,
                    or_(Worker.last_active_at.is_(None), Worker.last_active_at < idle_before),
                )
                .order_by(Worker.last_active_at.asc().nullsfirst())
                .limit(1)
            )
            return (await session.execute(query)).scalar_one_or_none()

    async def find_draining_workers(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(
                self._deployment_clause(), Worker.draining.is_(True), Worker.desired_state != "terminated"
            )
            return list((await session.execute(query)).scalars().all())

    async def count_inflight_tasks(self, worker_id: str) -> int:
        async with get_db_session() as session:
            query = select(func.count(Task.task_id)).where(
                Task.worker_id == worker_id, Task.status.in_(TASK_STATUSES_DRAIN_BLOCKERS)
            )
            return (await session.execute(query)).scalar() or 0

    async def count_unstored_results(self, worker_id: str) -> int:
        async with get_db_session() as session:
            query = select(func.count(Task.task_id)).where(
                Task.worker_id == worker_id,
                Task.status == "storing_result",
                or_(Task.result_key.is_(None), Task.result_key == ""),
            )
            return (await session.execute(query)).scalar() or 0

    async def get_ready_replacement_for(self, worker_id: str) -> Worker | None:
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(self._ready_serviceable_clause(), Worker.replacement_for == worker_id)
                .order_by(Worker.created_at.asc())
                .limit(1)
            )
            return (await session.execute(query)).scalar_one_or_none()

    async def has_active_replacement_for(self, worker_id: str) -> bool:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(
                self._deployment_clause(),
                Worker.replacement_for == worker_id,
                Worker.terminated_at.is_(None),
                Worker.desired_state != "terminated",
            )
            return ((await session.execute(query)).scalar() or 0) > 0

    async def count_active_replacements(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(
                self._deployment_clause(),
                Worker.replacement_for.isnot(None),
                Worker.desired_state != "terminated",
                Worker.terminated_at.is_(None),
            )
            return (await session.execute(query)).scalar() or 0

    async def find_replacement_workers(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(
                self._deployment_clause(), Worker.replacement_for.isnot(None), Worker.terminated_at.is_(None)
            )
            return list((await session.execute(query)).scalars().all())

    def _rotation_eligible_clause(self):
        return and_(
            self._deployment_clause(),
            Worker.desired_state == "running",
            Worker.cloud_state == "running",
            Worker.healthy.is_(True),
            Worker.draining.is_(False),
            Worker.replacement_for.is_(None),
            Worker.instance_id.isnot(None),
        )

    async def find_emergency_rotation_target(self) -> Worker | None:
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(self._rotation_eligible_clause(), Worker.rotation_requested.is_(True))
                .order_by(Worker.created_at.asc())
                .limit(1)
            )
            return (await session.execute(query)).scalar_one_or_none()

    async def find_scheduled_rotation_target(self, *, created_before: datetime) -> Worker | None:
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(
                    self._rotation_eligible_clause(), Worker.created_at.isnot(None), Worker.created_at < created_before
                )
                .order_by(Worker.created_at.asc())
                .limit(1)
            )
            return (await session.execute(query)).scalar_one_or_none()

    async def find_workers_pending_termination_finalize(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(
                self._deployment_clause(),
                Worker.desired_state == "terminated",
                Worker.cloud_state == "terminated",
                Worker.terminated_at.is_(None),
            )
            return list((await session.execute(query)).scalars().all())

    async def list_health_check_candidates(self, *, limit: int) -> list[Worker]:
        async with get_db_session() as session:
            query = (
                select(Worker)
                .where(
                    Worker.deployment_id == self._settings.deployment_id,
                    Worker.terminated_at.is_(None),
                    Worker.base_url.isnot(None),
                )
                .order_by(Worker.last_health_checked_at.asc().nullsfirst(), Worker.id.asc())
                .limit(limit)
            )
            return list((await session.execute(query)).scalars().all())

    async def _record_scaling_event(
        self,
        session: AsyncSession,
        *,
        action: str,
        reason: str,
        worker_id: str | None = None,
        triggered_by: str = "autoscaler",
        requester: str | None = None,
        commit: bool = False,
    ) -> None:
        session.add(
            ScalingEvent(
                worker_id=worker_id, action=action, reason=reason, triggered_by=triggered_by, requester=requester
            )
        )
        if commit:
            await session.commit()

    async def record_scaling_event_now(
        self,
        *,
        action: str,
        reason: str,
        worker_id: str | None = None,
        triggered_by: str = "autoscaler",
        requester: str | None = None,
    ) -> None:
        async with get_db_session() as session:
            await self._record_scaling_event(
                session,
                action=action,
                reason=reason,
                worker_id=worker_id,
                triggered_by=triggered_by,
                requester=requester,
                commit=True,
            )

    async def record_failure(self, worker_id: str, error: str, *, retryable: bool) -> None:
        max_failures = self._settings.reconciliation.max_failure_count
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None:
                return
            row.failure_count += 1
            row.last_error = error
            if row.failure_count >= max_failures:
                row.healthy = False
                row.retry_after = None
                if row.stalled_at is None:
                    row.stalled_at = now_utc()
                row.draining = True
                row.drain_target = "terminated"
            elif retryable:
                row.retry_after = now_utc() + timedelta(seconds=retry_delay(row.failure_count))
            await session.commit()

    async def apply_health_checks(self, results: list[tuple[Worker, bool, str | None]]) -> int:
        healthy_count = 0
        async with get_db_session() as session:
            for worker, ok, error in results:
                row = await session.get(Worker, worker.id)
                if row is None:
                    continue
                row.last_health_checked_at = now_utc()
                if ok:
                    row.healthy = True
                    row.last_error = None
                    if row.desired_state == "running" and row.cloud_state == "running" and row.ready_at is None:
                        row.ready_at = now_utc()
                        row.start_requested_at = None
                    healthy_count += 1
                    metrics.record_health_check(outcome="healthy")
                else:
                    row.healthy = False
                    row.last_error = error
                    metrics.record_health_check(outcome="unhealthy")
            await session.commit()
        return healthy_count

    async def create_cloud_worker(self) -> str:
        import uuid

        worker_id = f"cloud-{uuid.uuid4().hex[:12]}"
        worker = Worker(
            id=worker_id,
            provider=self._settings.cloud.provider,
            deployment_id=self._settings.deployment_id,
            desired_state="running",
            cloud_state="pending",
            start_requested_at=now_utc(),
        )
        async with get_db_session() as session:
            session.add(worker)
            await session.commit()
        return worker_id

    async def reset_cloud_failures(self, worker_id: str) -> None:
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None:
                return
            row.failure_count = 0
            row.retry_after = None
            if row.healthy:
                row.last_error = None
            await session.commit()

    async def reset_worker_failures(self, worker_id: str) -> None:
        """Manual recovery — reset cloud retry state only; health is updated by the probe."""
        await self.reset_cloud_failures(worker_id)

    async def finalize_disappeared_instance(self, worker_id: str, *, reason: str) -> None:
        await self.commit_fields(
            worker_id,
            desired_state="terminated",
            cloud_state="terminated",
            instance_id=None,
            base_url=None,
            ready_at=None,
            healthy=False,
            retry_after=None,
            last_error=reason,
            terminated_at=now_utc(),
        )

    async def finalize_terminated_worker(self, worker_id: str) -> None:
        async with get_db_session() as session:
            replacements = (
                (await session.execute(select(Worker).where(Worker.replacement_for == worker_id))).scalars().all()
            )
            for repl in replacements:
                repl.replacement_for = None
            row = await session.get(Worker, worker_id)
            if row is not None:
                row.terminated_at = now_utc()
            await session.commit()

    async def start_rotation_replacement(self, old: Worker) -> str:
        import uuid

        replacement_id = f"cloud-{uuid.uuid4().hex[:12]}"
        replacement = Worker(
            id=replacement_id,
            provider=self._settings.cloud.provider,
            deployment_id=self._settings.deployment_id,
            desired_state="running",
            cloud_state="pending",
            replacement_for=old.id,
            generation=old.generation + 1,
            start_requested_at=now_utc(),
        )
        async with get_db_session() as session:
            session.add(replacement)
            row = await session.get(Worker, old.id)
            if row is not None:
                row.rotation_requested = False
            await session.commit()
        return replacement_id

    async def get_worker(self, worker_id: str) -> Worker | None:
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None or row.terminated_at is not None:
                return None
            if row.deployment_id != self._settings.deployment_id or row.provider != self._settings.cloud.provider:
                return None
            return row

    async def list_workers(self) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(self._deployment_clause()).order_by(Worker.created_at)
            return list((await session.execute(query)).scalars().all())

    async def set_drain_intent(
        self, worker_id: str, *, drain_target: str, reason: str, requester: str | None = None
    ) -> Worker | None:
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None or not self._row_in_deployment(row):
                return None
            row.draining = True
            row.drain_target = drain_target
            await self._record_scaling_event(
                session, action="drain", reason=reason, worker_id=worker_id, triggered_by="admin", requester=requester
            )
            await session.commit()
            await session.refresh(row)
            return row

    async def set_rotation_requested(
        self, worker_id: str, *, reason: str, requester: str | None = None
    ) -> Worker | None:
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None or not self._row_in_deployment(row):
                return None
            row.rotation_requested = True
            await self._record_scaling_event(
                session, action="rotate", reason=reason, worker_id=worker_id, triggered_by="admin", requester=requester
            )
            await session.commit()
            await session.refresh(row)
            return row

    async def recover_worker(self, worker_id: str, *, reason: str, requester: str | None = None) -> Worker | None:
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None or not self._row_in_deployment(row):
                return None
            row.failure_count = 0
            row.retry_after = None
            if row.stalled_at is not None:
                row.stalled_at = None
                row.draining = False
                row.drain_target = None
            await self._record_scaling_event(
                session, action="recover", reason=reason, worker_id=worker_id, triggered_by="admin", requester=requester
            )
            await session.commit()
            await session.refresh(row)
            return row

    def _row_in_deployment(self, row: Worker) -> bool:
        return row.deployment_id == self._settings.deployment_id and row.provider == self._settings.cloud.provider

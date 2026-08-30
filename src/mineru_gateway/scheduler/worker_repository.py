"""Deployment-scoped worker persistence and dispatch selection."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.expression import ScalarSelect

from mineru_gateway.cloud.types import (
    CLOUD_STATE_PENDING,
    CLOUD_STATE_RUNNING,
    CLOUD_STATE_STOPPED,
    CLOUD_STATE_STOPPING,
    CLOUD_STATE_TERMINATED,
    CLOUD_STATE_TERMINATING,
    CLOUD_STATE_UNKNOWN,
)
from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import ScalingEvent, Task, Worker
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scheduler.scaling import ScalingInputs
from mineru_gateway.tasks.status import (
    TASK_STATUSES_AUTOSCALE_DEMAND,
    TASK_STATUSES_COMPUTE_CAPACITY,
    TASK_STATUSES_DRAIN_BLOCKERS,
    TASK_STORING_RESULT,
)
from mineru_gateway.util.datetime import now_utc
from mineru_gateway.util.ids import worker_id

logger = logging.getLogger(__name__)

_RETRY_SECONDS: tuple[int, ...] = (5, 15, 30, 60, 120)
_RETRY_CAP_SECONDS = 300


def retry_delay(failure_count: int) -> int:
    if failure_count <= len(_RETRY_SECONDS):
        return _RETRY_SECONDS[failure_count - 1]
    return _RETRY_CAP_SECONDS


class WorkerRepository:
    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings

    def _deployment_clause(self) -> ColumnElement[bool]:
        return and_(
            Worker.deployment_id == self._settings.deployment_id,
            Worker.provider == self._settings.cloud.provider,
            Worker.terminated_at.is_(None),
        )

    async def list_deployment_workers(self) -> list[Worker]:
        return await self._list_where(self._deployment_clause())

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

    async def _count_where(self, *clauses: ColumnElement[bool]) -> int:
        async with get_db_session() as session:
            query = select(func.count(Worker.id)).where(*clauses)
            return (await session.execute(query)).scalar() or 0

    async def _list_where(
        self, *clauses: ColumnElement[bool], order_by: Any = None, limit: int | None = None
    ) -> list[Worker]:
        async with get_db_session() as session:
            query = select(Worker).where(*clauses)
            if order_by is not None:
                order_clauses = order_by if isinstance(order_by, (list, tuple)) else (order_by,)
                query = query.order_by(*order_clauses)
            if limit is not None:
                query = query.limit(limit)
            return list((await session.execute(query)).scalars().all())

    async def _get_one_where(self, *clauses: ColumnElement[bool], order_by: Any = None) -> Worker | None:
        async with get_db_session() as session:
            query = select(Worker).where(*clauses)
            if order_by is not None:
                query = query.order_by(order_by)
            query = query.limit(1)
            return (await session.execute(query)).scalar_one_or_none()

    def _stalled_clause(self) -> ColumnElement[bool]:
        return Worker.failure_count >= self._settings.reconciliation.max_failure_count

    def _serviceable_clause(self) -> ColumnElement[bool]:
        return and_(
            self._deployment_clause(),
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state == CLOUD_STATE_RUNNING,
            Worker.healthy.is_(True),
            Worker.base_url.isnot(None),
            Worker.draining.is_(False),
            Worker.failure_count < self._settings.reconciliation.max_failure_count,
        )

    def _health_check_clause(self) -> ColumnElement[bool]:
        """Workers we still expect to answer health probes (excludes drain/stop wind-down)."""
        return and_(
            self._deployment_clause(),
            Worker.terminated_at.is_(None),
            Worker.base_url.isnot(None),
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state.in_((CLOUD_STATE_RUNNING, CLOUD_STATE_PENDING)),
            Worker.draining.is_(False),
        )

    def _ready_serviceable_clause(self) -> ColumnElement[bool]:
        return and_(self._serviceable_clause(), Worker.ready_at.isnot(None))

    def _dispatchable_clause(self, *, active_count: ScalarSelect[int]) -> ColumnElement[bool]:
        return and_(self._serviceable_clause(), active_count < self._settings.scaling.target_per_worker)

    def _task_count_subquery(self, statuses: tuple[str, ...]) -> ScalarSelect[int]:
        """Correlated count of a worker's tasks in ``statuses`` — used as a per-worker load signal."""
        query = select(func.count(Task.task_id)).where(Task.worker_id == Worker.id, Task.status.in_(statuses))
        return query.correlate(Worker).scalar_subquery()

    def _compute_capacity_subquery(self) -> ScalarSelect[int]:
        return self._task_count_subquery(TASK_STATUSES_COMPUTE_CAPACITY)

    def _drain_blocker_subquery(self) -> ScalarSelect[int]:
        return self._task_count_subquery(TASK_STATUSES_DRAIN_BLOCKERS)

    async def acquire_dispatchable(
        self, session: AsyncSession, *, excluded_ids: set[str] | None = None
    ) -> Worker | None:
        active_count = self._compute_capacity_subquery()
        clauses: list[ColumnElement[bool]] = [self._dispatchable_clause(active_count=active_count)]
        if excluded_ids:
            clauses.append(~Worker.id.in_(excluded_ids))

        order_by = (active_count.asc(), Worker.last_active_at.asc().nullsfirst(), Worker.id.asc())
        query = select(Worker).where(*clauses).order_by(*order_by).with_for_update(skip_locked=True)

        worker = (await session.execute(query)).scalars().first()
        if worker is None:
            return None
        worker.last_active_at = now_utc()
        return worker

    async def count_workers(self) -> int:
        return await self._count_where(self._deployment_clause())

    async def count_serviceable_workers(self) -> int:
        return await self._count_where(self._serviceable_clause())

    def _provisioning_clause(self) -> ColumnElement[bool]:
        """Workers launched but not yet serviceable (pending EC2 or bootstrapping MinerU)."""
        max_failures = self._settings.reconciliation.max_failure_count
        launching = and_(
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state.in_((CLOUD_STATE_PENDING, CLOUD_STATE_UNKNOWN, "starting")),
        )
        bootstrapping = and_(
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state == CLOUD_STATE_RUNNING,
            Worker.healthy.is_(False),
            Worker.ready_at.is_(None),
        )
        return and_(
            self._deployment_clause(),
            or_(launching, bootstrapping),
            Worker.draining.is_(False),
            Worker.failure_count < max_failures,
        )

    async def count_starting_workers(self) -> int:
        return await self._count_where(self._provisioning_clause())

    def _draining_clause(self) -> ColumnElement[bool]:
        return and_(
            self._deployment_clause(),
            Worker.draining.is_(True),
            Worker.desired_state != CLOUD_STATE_TERMINATED,
            Worker.terminated_at.is_(None),
        )

    async def count_draining_workers(self) -> int:
        return await self._count_where(self._draining_clause())

    def _stopping_clause(self) -> ColumnElement[bool]:
        """Workers converging to stopped/terminated (EC2 stop/terminate in flight)."""
        return and_(
            self._deployment_clause(),
            Worker.terminated_at.is_(None),
            or_(
                Worker.cloud_state.in_((CLOUD_STATE_STOPPING, CLOUD_STATE_TERMINATING)),
                and_(
                    Worker.desired_state.in_((CLOUD_STATE_STOPPED, CLOUD_STATE_TERMINATED)),
                    Worker.cloud_state.notin_((CLOUD_STATE_STOPPED, CLOUD_STATE_TERMINATED)),
                ),
            ),
        )

    async def count_stopping_workers(self) -> int:
        return await self._count_where(self._stopping_clause())

    def _stalled_and_active_clauses(self) -> tuple[ColumnElement[bool], ...]:
        return (self._deployment_clause(), self._stalled_clause(), Worker.desired_state != CLOUD_STATE_TERMINATED)

    async def find_stalled_workers(self) -> list[Worker]:
        return await self._list_where(*self._stalled_and_active_clauses())

    async def count_stalled_workers(self) -> int:
        return await self._count_where(*self._stalled_and_active_clauses())

    async def count_queue_depth(self) -> int:
        async with get_db_session() as session:
            query = select(func.count(Task.task_id)).where(Task.status.in_(TASK_STATUSES_AUTOSCALE_DEMAND))
            return (await session.execute(query)).scalar() or 0

    async def count_recoverable_workers(self) -> int:
        serviceable, starting = await asyncio.gather(self.count_serviceable_workers(), self.count_starting_workers())
        return serviceable + starting

    async def collect_scaling_inputs(self) -> ScalingInputs:
        """Load autoscale counters concurrently (independent read-only queries)."""
        (queue_depth, serviceable_workers, starting_workers, draining_workers, stopping_workers) = await asyncio.gather(
            self.count_queue_depth(),
            self.count_serviceable_workers(),
            self.count_starting_workers(),
            self.count_draining_workers(),
            self.count_stopping_workers(),
        )
        return ScalingInputs(
            queue_depth=queue_depth,
            serviceable_workers=serviceable_workers,
            starting_workers=starting_workers,
            draining_workers=draining_workers,
            stopping_workers=stopping_workers,
        )

    async def find_stopped_worker(self) -> Worker | None:
        return await self._get_one_where(
            self._deployment_clause(),
            Worker.desired_state == CLOUD_STATE_STOPPED,
            Worker.cloud_state == CLOUD_STATE_STOPPED,
            Worker.instance_id.isnot(None),
            Worker.draining.is_(False),
        )

    async def find_idle_worker(self, *, idle_before: datetime) -> Worker | None:
        """Return the worker idle longest that has been serviceable for the full cooldown."""
        inflight = self._drain_blocker_subquery()
        idle_since = func.coalesce(Worker.last_active_at, Worker.ready_at)
        return await self._get_one_where(
            self._deployment_clause(),
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state == CLOUD_STATE_RUNNING,
            Worker.healthy.is_(True),
            Worker.draining.is_(False),
            Worker.replacement_for.is_(None),
            Worker.ready_at.isnot(None),
            Worker.ready_at < idle_before,
            inflight == 0,
            or_(Worker.last_active_at.is_(None), Worker.last_active_at < idle_before),
            order_by=idle_since.asc(),
        )

    async def find_draining_workers(self) -> list[Worker]:
        return await self._list_where(self._draining_clause())

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
                Task.status == TASK_STORING_RESULT,
                or_(Task.result_key.is_(None), Task.result_key == ""),
            )
            return (await session.execute(query)).scalar() or 0

    async def get_ready_replacement_for(self, worker_id: str) -> Worker | None:
        return await self._get_one_where(
            self._ready_serviceable_clause(), Worker.replacement_for == worker_id, order_by=Worker.created_at.asc()
        )

    async def has_active_replacement_for(self, worker_id: str) -> bool:
        count = await self._count_where(
            self._deployment_clause(),
            Worker.replacement_for == worker_id,
            Worker.desired_state != CLOUD_STATE_TERMINATED,
        )
        return count > 0

    async def count_active_replacements(self) -> int:
        return await self._count_where(
            self._deployment_clause(),
            Worker.replacement_for.isnot(None),
            Worker.desired_state != CLOUD_STATE_TERMINATED,
        )

    async def find_replacement_workers(self) -> list[Worker]:
        return await self._list_where(self._deployment_clause(), Worker.replacement_for.isnot(None))

    def _rotation_eligible_clause(self) -> ColumnElement[bool]:
        return and_(
            self._deployment_clause(),
            Worker.desired_state == CLOUD_STATE_RUNNING,
            Worker.cloud_state == CLOUD_STATE_RUNNING,
            Worker.healthy.is_(True),
            Worker.draining.is_(False),
            Worker.replacement_for.is_(None),
            Worker.instance_id.isnot(None),
        )

    async def find_emergency_rotation_target(self) -> Worker | None:
        return await self._get_one_where(
            self._rotation_eligible_clause(), Worker.rotation_requested.is_(True), order_by=Worker.created_at.asc()
        )

    async def find_scheduled_rotation_target(self, *, created_before: datetime) -> Worker | None:
        return await self._get_one_where(
            self._rotation_eligible_clause(),
            Worker.created_at.isnot(None),
            Worker.created_at < created_before,
            order_by=Worker.created_at.asc(),
        )

    async def find_workers_pending_termination_finalize(self) -> list[Worker]:
        return await self._list_where(
            self._deployment_clause(),
            Worker.desired_state == CLOUD_STATE_TERMINATED,
            Worker.cloud_state == CLOUD_STATE_TERMINATED,
        )

    async def list_health_check_candidates(self, *, limit: int) -> list[Worker]:
        return await self._list_where(
            self._health_check_clause(),
            order_by=(Worker.last_health_checked_at.asc().nullsfirst(), Worker.id.asc()),
            limit=limit,
        )

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
        stalled = False
        failure_count = 0
        retry_after: datetime | None = None
        async with get_db_session() as session:
            row = await session.get(Worker, worker_id)
            if row is None:
                return
            row.failure_count += 1
            row.last_error = error
            failure_count = row.failure_count
            if row.failure_count >= max_failures:
                stalled = True
                row.healthy = False
                row.retry_after = None
                if row.stalled_at is None:
                    row.stalled_at = now_utc()
                row.draining = True
                row.drain_target = CLOUD_STATE_TERMINATED
            elif retryable:
                row.retry_after = now_utc() + timedelta(seconds=retry_delay(row.failure_count))
            retry_after = row.retry_after
            await session.commit()
        if stalled:
            logger.error("Worker %s stalled — draining", worker_id)
        else:
            logger.warning(
                "Worker %s failure #%d retryable=%s retry_after=%s error=%s",
                worker_id,
                failure_count,
                retryable,
                retry_after,
                error,
            )

    async def apply_health_checks(self, results: list[tuple[Worker, bool, str | None, bool]]) -> int:
        healthy_count = 0
        async with get_db_session() as session:
            for worker, ok, error, provisioning in results:
                row = await session.get(Worker, worker.id)
                if row is None:
                    continue
                row.last_health_checked_at = now_utc()
                if ok:
                    row.healthy = True
                    row.last_error = None
                    row.provisioning_detail = None
                    if (
                        row.desired_state == CLOUD_STATE_RUNNING
                        and row.cloud_state == CLOUD_STATE_RUNNING
                        and row.ready_at is None
                    ):
                        row.ready_at = now_utc()
                        row.start_requested_at = None
                    healthy_count += 1
                    metrics.record_health_check(outcome="healthy")
                else:
                    row.healthy = False
                    if provisioning:
                        row.provisioning_detail = error
                    else:
                        row.last_error = error
                    metrics.record_health_check(outcome="unhealthy")
            await session.commit()
        return healthy_count

    async def create_cloud_worker(self) -> str:
        new_worker_id = worker_id()
        worker = Worker(
            id=new_worker_id,
            provider=self._settings.cloud.provider,
            deployment_id=self._settings.deployment_id,
            desired_state=CLOUD_STATE_RUNNING,
            cloud_state=CLOUD_STATE_PENDING,
            start_requested_at=now_utc(),
        )
        async with get_db_session() as session:
            session.add(worker)
            await session.commit()
        logger.info("Created worker id=%s", new_worker_id)
        return new_worker_id

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
            desired_state=CLOUD_STATE_TERMINATED,
            cloud_state=CLOUD_STATE_TERMINATED,
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
            query = select(Worker).where(Worker.replacement_for == worker_id)
            replacements = (await session.execute(query)).scalars().all()
            for repl in replacements:
                repl.replacement_for = None
            row = await session.get(Worker, worker_id)
            if row is not None:
                row.terminated_at = now_utc()
            await session.commit()

    async def start_rotation_replacement(self, old: Worker) -> str:
        replacement_id = worker_id()
        replacement = Worker(
            id=replacement_id,
            provider=self._settings.cloud.provider,
            deployment_id=self._settings.deployment_id,
            desired_state=CLOUD_STATE_RUNNING,
            cloud_state=CLOUD_STATE_PENDING,
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
        return await self._list_where(self._deployment_clause(), order_by=Worker.created_at)

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
            logger.info(
                "Drain intent worker=%s target=%s reason=%s requester=%s", worker_id, drain_target, reason, requester
            )
            metrics.record_worker_scaled(action="drain")
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
            logger.info("Rotation requested worker=%s reason=%s requester=%s", worker_id, reason, requester)
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
            logger.info("Worker recovered worker=%s reason=%s requester=%s", worker_id, reason, requester)
            return row

    def _row_in_deployment(self, row: Worker) -> bool:
        """In-memory mirror of :meth:`_deployment_clause` for a fetched row.

        ``_deployment_clause`` expresses the same (deployment_id, provider, not-terminated) filter
        in SQL; this Python variant is used when the row is already loaded and we only need to
        confirm membership without re-querying. Keep the two in sync.
        """
        return row.deployment_id == self._settings.deployment_id and row.provider == self._settings.cloud.provider

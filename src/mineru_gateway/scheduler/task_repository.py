"""Task queue claim, dispatch, and upstream status synchronization."""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any, Literal

import httpx
from sqlalchemy import or_, select
from sqlalchemy.sql.elements import ColumnElement

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.mineru_compat import MultipartPayload, StagedUpload, submit_payload_to_upstream
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.scheduler._http import RESULT_FETCH_TIMEOUT_SECONDS, UPSTREAM_REFRESH_TIMEOUT, worker_json_get
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.tasks.errors import classify_error, should_retry
from mineru_gateway.tasks.status import (
    TASK_COMPLETED,
    TASK_DISPATCHING,
    TASK_EXPIRED,
    TASK_FAILED,
    TASK_PROCESSING,
    TASK_QUEUED,
    TASK_STATUSES_TERMINAL,
    TASK_STORING_RESULT,
    apply_upstream_payload,
    normalize_upstream_status,
)
from mineru_gateway.util.datetime import ensure_aware_utc, now_utc
from mineru_gateway.util.upload_paths import internal_upload_path

logger = logging.getLogger(__name__)

_HEALTH_BATCH = 32
_DISPATCH_CLAIM_TIMEOUT_SECONDS = 120
_SYNC_CONCURRENCY = 4
_RESULT_CONCURRENCY = 4
_POLL_RETRY_DELAY_SECONDS = 30.0

_DispatchFailureOutcome = Worker | Literal["requeued", "failed"]


class TaskRepository:
    def __init__(
        self,
        settings: GatewaySettings,
        store: CloudStorageProvider | None = None,
        client: httpx.AsyncClient | None = None,
        workers: WorkerRepository | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._client = client
        self._workers = workers
        self._dispatch_deferred_since: datetime | None = None

    @classmethod
    def for_gateway(cls, settings: GatewaySettings) -> TaskRepository:
        """Gateway-side task persistence without dispatch dependencies."""
        return cls(settings)

    def _require_store(self) -> CloudStorageProvider:
        if self._store is None:
            raise RuntimeError("TaskRepository.store is required for this operation")
        return self._store

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("TaskRepository.client is required for this operation")
        return self._client

    def _require_workers(self) -> WorkerRepository:
        if self._workers is None:
            raise RuntimeError("TaskRepository.workers is required for this operation")
        return self._workers

    async def create_queued_task(
        self,
        *,
        task_id: str,
        file_names: list[str],
        backend: str,
        parse_method: str,
        options: dict[str, Any],
        payload_key: str | None,
        source: str,
        cache_key: str | None = None,
    ) -> Task:
        options_blob = {**options, "file_names": file_names}
        async with get_db_session() as session:
            row = Task(
                task_id=task_id,
                backend=backend,
                parse_method=parse_method,
                file_names=file_names,
                status=TASK_QUEUED,
                source=source,
                payload_key=payload_key,
                options_blob=options_blob,
                cache_key=cache_key,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def clear_cache_key(self, task_id: str) -> None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return
            row.cache_key = None
            await session.commit()

    async def expire_client_sla_tasks(self, *, sla_seconds: int) -> int:
        """Mark client SLA expiry without changing execution status."""
        cutoff = now_utc() - timedelta(seconds=sla_seconds)
        async with get_db_session() as session:
            query = select(Task).where(
                Task.status.in_((TASK_PROCESSING, TASK_DISPATCHING, TASK_STORING_RESULT)),
                Task.client_expired_at.is_(None),
                Task.dispatch_started_at.isnot(None),
                Task.dispatch_started_at < cutoff,
            )
            rows = (await session.execute(query)).scalars().all()
            for row in rows:
                row.client_expired_at = now_utc()
            await session.commit()
            return len(rows)

    async def complete_with_result(self, task_id: str, *, result_key_value: str, result_format: str) -> str | None:
        """Atomically set result pointer and completed status; returns cache_key if set."""
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                raise KeyError(task_id)
            row.result_key = result_key_value
            row.result_format = result_format
            row.status = TASK_COMPLETED
            if row.completed_at is None:
                row.completed_at = now_utc()
            cache_key = row.cache_key
            await session.commit()
            return cache_key

    async def recover_stale_dispatch_claims(self) -> int:
        """Re-queue dispatch claims that never recorded an upstream task ID.

        Submission is at-least-once across the crash window between upstream accept
        and persisting ``upstream_task_id``: a recovered row may be submitted again
        even though upstream already accepted the first attempt. When MinerU supports
        idempotency tokens, pass the gateway task ID; otherwise duplicate compute is
        accepted and only the recorded upstream result is returned to clients.
        """
        cutoff = now_utc() - timedelta(seconds=_DISPATCH_CLAIM_TIMEOUT_SECONDS)
        async with get_db_session() as session:
            query = select(Task).where(
                Task.status == TASK_DISPATCHING,
                Task.upstream_task_id.is_(None),
                Task.dispatch_started_at.isnot(None),
                Task.dispatch_started_at < cutoff,
            )
            rows = (await session.execute(query)).scalars().all()
            for row in rows:
                row.status = TASK_QUEUED
                row.dispatch_started_at = None
                row.worker_id = None
            await session.commit()
            recovered = len(rows)
            if recovered:
                logger.info("Recovered %d stale dispatch claims", recovered)
                metrics.record_stale_claims_recovered(count=recovered)
            return recovered

    async def dispatch_queued_tasks(self, *, max_per_tick: int = 32) -> int:
        dispatched = 0
        for _ in range(max_per_tick):
            claim = await self._claim_next_dispatch()
            if claim is None:
                break
            task, worker = claim
            if await self._submit_claimed_task(task, worker):
                dispatched += 1
        return dispatched

    async def _claim_next_dispatch(self) -> tuple[Task, Worker] | None:
        """Atomically claim one queued task and assign a dispatchable worker."""
        async with get_db_session() as session:
            queued_query = (
                select(Task)
                .where(Task.status == TASK_QUEUED, Task.upstream_task_id.is_(None), Task.upstream_base_url.is_(None))
                .order_by(Task.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            candidate = (await session.execute(queued_query)).scalar_one_or_none()
            if candidate is None:
                return None
            worker = await self._require_workers().acquire_dispatchable(session)
            if worker is None:
                if self._dispatch_deferred_since is None:
                    self._dispatch_deferred_since = now_utc()
                    logger.info("Dispatch deferred task=%s reason=no_dispatchable_worker", candidate.task_id)
                return None
            if self._dispatch_deferred_since is not None:
                elapsed = (now_utc() - ensure_aware_utc(self._dispatch_deferred_since)).total_seconds()
                logger.info("Dispatch resumed after %.0fs", elapsed)
                self._dispatch_deferred_since = None
            candidate.status = TASK_DISPATCHING
            candidate.dispatch_started_at = now_utc()
            candidate.worker_id = worker.id
            await session.commit()
            return candidate, worker

    async def _submit_claimed_task(self, task: Task, worker: Worker) -> bool:
        start = time.perf_counter()
        excluded: set[str] = set()

        while True:
            payload = await self._rebuild_payload(task)
            if payload is None:
                logger.warning("Task failed at dispatch task=%s reason=payload_missing", task.task_id)
                await self._mark_failed(task.task_id, error="Payload unavailable in object storage")
                metrics.record_dispatch_failed(error_class="payload_missing")
                metrics.record_dispatch_duration(outcome="failed", duration_ms=(time.perf_counter() - start) * 1000)
                return False

            base_url = worker.base_url or ""
            if not base_url:
                payload.cleanup()
                reassigned = await self._reassign_after_exclusion(task, worker, excluded, start=start)
                if reassigned is None:
                    return False
                worker = reassigned
                continue

            try:
                upstream = await submit_payload_to_upstream(base_url, payload)
            except Exception as exc:
                payload.cleanup()
                outcome = await self._handle_dispatch_failure(task, worker, exc, excluded, start=start)
                if isinstance(outcome, str):
                    if outcome == "failed":
                        metrics.record_dispatch_duration(
                            outcome="failed", duration_ms=(time.perf_counter() - start) * 1000
                        )
                    return False
                worker = outcome
                continue

            payload.cleanup()
            await self._mark_processing(
                task.task_id,
                worker_id=worker.id,
                upstream_task_id=upstream["task_id"],
                upstream_base_url=base_url,
                upstream_status=upstream["status"],
            )
            duration_ms = (time.perf_counter() - start) * 1000
            metrics.record_task_dispatched(worker_id=worker.id, backend=task.backend)
            metrics.record_dispatch_duration(outcome="success", duration_ms=duration_ms)
            logger.info(
                "Dispatched task=%s worker=%s upstream=%s backend=%s duration_ms=%.0f",
                task.task_id,
                worker.id,
                upstream["task_id"],
                task.backend,
                duration_ms,
            )
            return True

    async def _reassign_after_exclusion(
        self, task: Task, worker: Worker, excluded: set[str], *, start: float
    ) -> Worker | None:
        """Exclude ``worker`` and acquire a replacement; returns None when no worker is available."""
        excluded.add(worker.id)
        return await self._reassign_dispatch_worker(task.task_id, excluded_ids=excluded, start=start)

    async def _handle_dispatch_failure(
        self, task: Task, worker: Worker, exc: Exception, excluded: set[str], *, start: float
    ) -> _DispatchFailureOutcome:
        """Process a dispatch exception: reassign on retryable infra errors, mark failed otherwise.

        Returns a replacement ``Worker`` to retry with, ``"requeued"`` when the task was returned to
        the queue, or ``"failed"`` when the task is terminal (content error).
        """
        if should_retry(exc):
            logger.warning(
                "Dispatch to worker %s failed (infra): task=%s error_class=%s error=%s",
                worker.id,
                task.task_id,
                classify_error(exc).value,
                exc,
            )
            metrics.record_dispatch_failed(error_class="infra")
            reassigned = await self._reassign_after_exclusion(task, worker, excluded, start=start)
            if reassigned is None:
                return "requeued"
            return reassigned
        logger.warning("Task failed at dispatch task=%s reason=content_error error=%s", task.task_id, exc)
        await self._mark_failed(task.task_id, error=f"Content error: {exc}")
        metrics.record_dispatch_failed(error_class="content")
        return "failed"

    async def _reassign_dispatch_worker(self, task_id: str, *, excluded_ids: set[str], start: float) -> Worker | None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None or row.status != TASK_DISPATCHING:
                return None
            worker = await self._require_workers().acquire_dispatchable(session, excluded_ids=excluded_ids)
            if worker is None:
                row.status = TASK_QUEUED
                row.dispatch_started_at = None
                row.worker_id = None
                await session.commit()
                logger.warning("Dispatch re-queued task=%s excluded_workers=%d", task_id, len(excluded_ids))
                metrics.record_dispatch_requeued()
                metrics.record_dispatch_duration(outcome="requeued", duration_ms=(time.perf_counter() - start) * 1000)
                return None
            row.worker_id = worker.id
            await session.commit()
            return worker

    async def _rebuild_payload(self, task: Task) -> MultipartPayload | None:
        if task.payload_key is None:
            return None
        try:
            blob = await self._require_store().get(key=task.payload_key)
        except KeyError:
            return None
        if len(blob) < 4:
            return None
        header_len = struct.unpack("!I", blob[:4])[0]
        header = json.loads(blob[4 : 4 + header_len].decode("utf-8"))
        file_data = blob[4 + header_len :]
        temp_dir = tempfile.mkdtemp(prefix="mineru-scheduler-dispatch-")
        uploads: list[StagedUpload] = []
        for u in header.get("uploads", []):
            offset = u["offset"]
            size = u["size"]
            data = file_data[offset : offset + size]
            client_name = u.get("upload_name", "upload")
            path, safe_name = internal_upload_path(temp_dir, client_name=client_name)
            with open(path, "wb") as f:
                f.write(data)
            uploads.append(
                StagedUpload(
                    field_name=u["field_name"], upload_name=safe_name, content_type=u["content_type"], path=path
                )
            )
        fields: list[tuple[str, str]] = [tuple(f) for f in header.get("fields", [])]
        return MultipartPayload(temp_dir=temp_dir, fields=fields, uploads=uploads)

    async def _mark_processing(
        self, task_id: str, *, worker_id: str, upstream_task_id: str, upstream_base_url: str, upstream_status: str
    ) -> None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return
            row.worker_id = worker_id
            row.upstream_task_id = upstream_task_id
            row.upstream_base_url = upstream_base_url
            normalized = normalize_upstream_status(upstream_status, current=TASK_PROCESSING)
            if normalized == TASK_COMPLETED:
                row.status = TASK_STORING_RESULT
            elif normalized in TASK_STATUSES_TERMINAL:
                row.status = normalized
                row.completed_at = now_utc()
            else:
                row.status = normalized
            if row.started_at is None:
                row.started_at = now_utc()
            await session.commit()

    async def _mark_failed(self, task_id: str, error: str) -> None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return
            row.status = TASK_FAILED
            row.error = error
            row.completed_at = now_utc()
            await session.commit()

    async def apply_upstream_payload(self, task_id: str, payload: dict) -> Task | None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return None
            updates = apply_upstream_payload(row.status, payload)
            new_status = updates.get("status")
            if new_status == TASK_COMPLETED:
                updates["status"] = TASK_STORING_RESULT
            for key, value in updates.items():
                setattr(row, key, value)
            if new_status in (TASK_FAILED, TASK_EXPIRED) and row.completed_at is None:
                row.completed_at = now_utc()
            await session.commit()
            await session.refresh(row)
            return row

    async def defer_task_poll(self, task_id: str) -> None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return
            row.next_poll_at = now_utc() + timedelta(seconds=_POLL_RETRY_DELAY_SECONDS)
            await session.commit()

    def _pollable_clause(self) -> ColumnElement[bool]:
        now = now_utc()
        return or_(Task.next_poll_at.is_(None), Task.next_poll_at <= now)

    async def refresh_task_from_upstream(self, task_id: str) -> Task | None:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
        if row is None:
            return None
        if row.status in TASK_STATUSES_TERMINAL:
            return row
        if not row.upstream_task_id or not row.upstream_base_url:
            return row
        payload = await worker_json_get(
            self._require_client(),
            f"{row.upstream_base_url}/tasks/{row.upstream_task_id}",
            timeout=UPSTREAM_REFRESH_TIMEOUT,
        )
        if payload is None:
            await self.defer_task_poll(task_id)
            return row
        updated = await self.apply_upstream_payload(task_id, payload)
        if updated is not None:
            async with get_db_session() as session:
                db_row = await session.get(Task, task_id)
                if db_row is not None and db_row.next_poll_at is not None:
                    db_row.next_poll_at = None
                    await session.commit()
        return updated

    async def sync_dispatched_task_statuses(self, *, max_per_tick: int = _HEALTH_BATCH) -> int:
        async with get_db_session() as session:
            query = (
                select(Task)
                .where(
                    Task.status.in_((TASK_PROCESSING, TASK_DISPATCHING)),
                    Task.upstream_task_id.isnot(None),
                    Task.upstream_base_url.isnot(None),
                    self._pollable_clause(),
                )
                .order_by(Task.next_poll_at.asc().nullsfirst(), Task.updated_at.asc())
                .limit(max_per_tick)
            )
            tasks = (await session.execute(query)).scalars().all()

        semaphore = asyncio.Semaphore(_SYNC_CONCURRENCY)

        async def _sync_one(task: Task) -> bool:
            async with semaphore:
                before = task.status
                updated = await self.refresh_task_from_upstream(task.task_id)
                if updated is None or updated.status == before:
                    return False
                if updated.status in TASK_STATUSES_TERMINAL and updated.worker_id:
                    await self._require_workers().commit_fields(updated.worker_id, last_active_at=now_utc())
                return True

        results = await asyncio.gather(*(_sync_one(task) for task in tasks))
        return sum(1 for changed in results if changed)

    async def list_unstored_completed(self, *, limit: int = _HEALTH_BATCH) -> list[Task]:
        async with get_db_session() as session:
            query = (
                select(Task)
                .where(
                    Task.status == TASK_STORING_RESULT,
                    Task.result_key.is_(None),
                    Task.upstream_task_id.isnot(None),
                    Task.upstream_base_url.isnot(None),
                    self._pollable_clause(),
                )
                .order_by(Task.next_poll_at.asc().nullsfirst(), Task.updated_at.asc())
                .limit(limit)
            )
            return list((await session.execute(query)).scalars().all())

    async def persist_pending_results(self, tasks: list[Task], *, cache_service: Any | None = None) -> tuple[int, int]:
        """Fetch and store results with bounded concurrency. Returns (stored, failed)."""
        if not tasks:
            return 0, 0
        semaphore = asyncio.Semaphore(_RESULT_CONCURRENCY)
        stored = 0
        failed = 0

        async def _persist(task: Task) -> None:
            nonlocal stored, failed
            async with semaphore:
                if not task.upstream_task_id or not task.upstream_base_url:
                    return
                try:
                    from mineru_gateway.tasks.results import fetch_and_store_result

                    await fetch_and_store_result(
                        task_id=task.task_id,
                        upstream_task_id=task.upstream_task_id,
                        upstream_base_url=task.upstream_base_url,
                        store=self._require_store(),
                        client=self._require_client(),
                        cache_service=cache_service,
                        task_repository=self,
                        timeout=RESULT_FETCH_TIMEOUT_SECONDS,
                    )
                    stored += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("Could not store result for task %s: %s", task.task_id, exc)
                    await self.defer_task_poll(task.task_id)

        await asyncio.gather(*(_persist(task) for task in tasks))
        return stored, failed

    async def list_retention_expired_tasks(self, *, cutoff: datetime, limit: int = 100) -> list[Task]:
        async with get_db_session() as session:
            query = (
                select(Task)
                .where(
                    Task.status.in_(TASK_STATUSES_TERMINAL), Task.completed_at.isnot(None), Task.completed_at < cutoff
                )
                .order_by(Task.completed_at.asc())
                .limit(limit)
            )
            return list((await session.execute(query)).scalars().all())

    async def delete_task(self, task_id: str) -> bool:
        async with get_db_session() as session:
            row = await session.get(Task, task_id)
            if row is None:
                return False
            await session.delete(row)
            await session.commit()
            return True

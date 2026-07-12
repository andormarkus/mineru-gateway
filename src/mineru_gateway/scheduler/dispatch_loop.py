"""Scheduler dispatch loop — pulls queued tasks from DB, pushes 1-2 at a time to each worker.

This is the core of the central-queue model. The gateway ingests tasks (``dispatch_state="queued"``);
this loop polls for them, selects a healthy worker with capacity (bounded dispatch: skip workers where
``processing_tasks + queued_tasks >= max_concurrent_requests``), downloads the payload bytes from S3,
rebuilds the multipart form, pushes to the worker via HTTP, and updates the task row to ``dispatch_state="dispatched"``.

On failure: classify the error (infra vs content). Infra errors retry on the next worker; content errors
fail-fast. Workers in ``draining`` state are skipped entirely.
"""

from __future__ import annotations

import json
import logging
import struct
import tempfile
import time
from datetime import UTC, datetime

import httpx
from sqlalchemy import select

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker
from mineru_gateway.mineru_compat import MultipartPayload, StagedUpload, submit_payload_to_upstream
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.tasks.errors import should_retry
from mineru_gateway.workers.selection import acquire_worker_db

logger = logging.getLogger(__name__)


async def dispatch_pending_tasks(store: CloudStorageProvider | None, client: httpx.AsyncClient) -> int:
    """Pull queued tasks from DB and push them to available workers. Returns count dispatched."""
    async with get_db_session() as session:
        stmt = select(Task).where(Task.dispatch_state == "queued").order_by(Task.created_at.asc())
        queued = (await session.execute(stmt)).scalars().all()

    if not queued:
        return 0

    logger.debug("Dispatch loop: %d queued task(s) to process", len(queued))
    dispatched = 0
    for task in queued:
        success = await _dispatch_one(task=task, store=store, client=client)
        if success:
            dispatched += 1

    if dispatched < len(queued):
        logger.debug("Dispatch loop: dispatched %d/%d task(s)", dispatched, len(queued))
    return dispatched


async def _dispatch_one(task: Task, store: CloudStorageProvider | None, client: httpx.AsyncClient) -> bool:
    """Dispatch a single queued task to a worker. Returns True on success."""
    excluded: set[str] = set()
    start = time.perf_counter()

    while True:
        async with get_db_session() as session:
            worker = await _acquire_worker_with_capacity(session=session, excluded=excluded)
            if worker is None:
                logger.debug("No available worker for task %s (excluded: %s)", task.task_id, excluded)
                return False

            # Download payload from S3 and rebuild the multipart.
            payload = await _rebuild_payload(task=task, store=store, worker=worker)
            if payload is None:
                logger.warning("Could not rebuild payload for task %s — marking failed", task.task_id)
                metrics.record_dispatch_failed(error_class="payload_missing")
                metrics.record_dispatch_duration(
                    outcome="payload_missing", duration_ms=(time.perf_counter() - start) * 1000
                )
                await _mark_task_failed(task_id=task.task_id, error="Payload unavailable in S3")
                return False

            try:
                base_url = worker.base_url or ""
                if not base_url:
                    logger.warning("Worker %s has no base_url — skipping", worker.id)
                    excluded.add(worker.id)
                    continue

                upstream = await submit_payload_to_upstream(base_url, payload)
                payload.cleanup()

                # Update the task row with upstream info.
                await _mark_task_dispatched(
                    task_id=task.task_id,
                    worker_id=worker.id,
                    upstream_task_id=upstream["task_id"],
                    upstream_base_url=base_url,
                    upstream_status=upstream["status"],
                )
                logger.info(
                    "Dispatched task %s → worker %s (upstream: %s)", task.task_id, worker.id, upstream["task_id"]
                )
                metrics.record_task_dispatched(worker_id=worker.id, backend=task.backend)
                metrics.record_dispatch_duration(
                    outcome="success", duration_ms=(time.perf_counter() - start) * 1000
                )
                return True

            except Exception as exc:
                payload.cleanup()
                if should_retry(exc):
                    excluded.add(worker.id)
                    logger.warning("Dispatch to worker %s failed (infra, will retry): %s", worker.id, exc)
                    continue
                else:
                    await _mark_task_failed(task_id=task.task_id, error=f"Content error: {exc}")
                    logger.warning("Dispatch to worker %s failed (content, no retry): %s", worker.id, exc)
                    metrics.record_dispatch_failed(error_class="content")
                    metrics.record_dispatch_duration(
                        outcome="content_error", duration_ms=(time.perf_counter() - start) * 1000
                    )
                    return False


async def _acquire_worker_with_capacity(session, excluded: set[str]) -> Worker | None:
    """Acquire a healthy worker with spare capacity (bounded dispatch: 1-2 per worker).

    Uses the existing SKIP LOCKED selection, then filters by capacity:
    ``processing_tasks + queued_tasks < max_concurrent_requests``.
    Skips workers in ``draining`` state.
    """
    excluded = excluded or set()

    # Try to find a worker with capacity via the existing SKIP LOCKED query.
    candidate = await acquire_worker_db(session=session, excluded_server_ids=excluded)
    if candidate is None:
        return None

    # Check if this worker is draining.
    if candidate.state == "draining":
        logger.debug("Skipping draining worker %s for dispatch", candidate.id)
        excluded.add(candidate.id)
        return await _acquire_worker_with_capacity(session=session, excluded=excluded)

    # Check capacity (bounded dispatch).
    inflight = candidate.queued_tasks + candidate.processing_tasks
    if inflight >= candidate.max_concurrent_requests:
        logger.debug(
            "Worker %s at capacity (%d/%d) — trying next",
            candidate.id,
            inflight,
            candidate.max_concurrent_requests,
        )
        excluded.add(candidate.id)
        return await _acquire_worker_with_capacity(session=session, excluded=excluded)

    return candidate


async def _rebuild_payload(task: Task, store: CloudStorageProvider | None, worker: Worker) -> MultipartPayload | None:
    """Download payload bytes from S3 and rebuild the MultipartPayload for the worker push."""
    if task.payload_key is None or store is None:
        return None

    try:
        blob = await store.get(key=task.payload_key)
    except KeyError:
        logger.warning("Payload missing in object store for task %s (key=%s)", task.task_id, task.payload_key)
        return None

    # Unpack: [4-byte header_len][header JSON][file bytes...]
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
        path = f"{temp_dir}/{u['upload_name']}"
        with open(path, "wb") as f:
            f.write(data)
        uploads.append(
            StagedUpload(
                field_name=u["field_name"], upload_name=u["upload_name"], content_type=u["content_type"], path=path
            )
        )

    fields: list[tuple[str, str]] = [tuple(f) for f in header.get("fields", [])]

    return MultipartPayload(temp_dir=temp_dir, fields=fields, uploads=uploads)


async def _mark_task_dispatched(
    task_id: str, *, worker_id: str, upstream_task_id: str, upstream_base_url: str, upstream_status: str
) -> None:
    """Update a task row after successful dispatch to a worker."""
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
        if row is None:
            return
        row.upstream_server_id = worker_id
        row.upstream_task_id = upstream_task_id
        row.upstream_base_url = upstream_base_url
        row.status = upstream_status
        row.dispatch_state = "dispatched"
        row.started_at = None  # will be set when worker reports processing
        await session.commit()


async def _mark_task_failed(task_id: str, error: str) -> None:
    """Mark a task as failed (terminal)."""
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
        if row is None:
            return
        row.status = "failed"
        row.dispatch_state = "terminal"
        row.error = error
        row.completed_at = datetime.now(UTC)
        await session.commit()

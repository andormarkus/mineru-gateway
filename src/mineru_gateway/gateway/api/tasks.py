"""MinerU-style task endpoints: /tasks, /tasks/{id}, /tasks/{id}/result, /file_parse.

The gateway is thin — POST /tasks ingests to the DB (the scheduler dispatches later). Status polling reads
the DB row (refreshed by the scheduler's health monitor + completion watcher). Result retrieval reads from
S3 (durable pointer set by the scheduler).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.gateway.admission import check_admission
from mineru_gateway.gateway.ingest import ingest_task
from mineru_gateway.gateway.task_flow import (
    fetch_result_or_none,
    get_store,
    poll_task_until_terminal,
    require_store,
    resolve_sync_result,
)
from mineru_gateway.tasks.status import TASK_COMPLETED, TASK_EXPIRED, TASK_FAILED
from mineru_gateway.tasks.storage import task_result_url, task_status_url
from mineru_gateway.util.datetime import to_iso

logger = logging.getLogger(__name__)
router = APIRouter(tags=["tasks"])


@router.post("/tasks", status_code=202)
async def submit_task(request: Request) -> Response:
    """Enqueue an async parse task. The scheduler process dispatches it to a worker."""
    await check_admission(request)
    store = require_store(request, detail="Object store not configured — POST /tasks requires durable payload storage.")
    if isinstance(store, JSONResponse):
        return store
    result = await ingest_task(request, store=store, settings=request.app.state.settings)
    logger.info("Submitted task %s (cache_hit=%s status=%s)", result.task_id, result.cache_hit, result.status)
    return result.response


@router.get("/tasks/{task_id}", name="get_router_task_status")
async def get_task_status(task_id: str, request: Request) -> Response:
    """Poll task status from the DB (the scheduler updates it as the task progresses)."""
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
    if row is None:
        logger.debug("Task status lookup: %s not found", task_id)
        return JSONResponse(status_code=404, content={"detail": "Task not found"})
    logger.debug("Task status lookup: %s status=%s", task_id, row.status)
    return JSONResponse(
        content={
            "task_id": row.task_id,
            "status": row.status,
            "backend": row.backend,
            "file_names": row.file_names,
            "error": row.error,
            "created_at": to_iso(row.created_at),
            "started_at": to_iso(row.started_at),
            "completed_at": to_iso(row.completed_at),
            "client_expired_at": to_iso(row.client_expired_at),
            "status_url": task_status_url(task_id),
            "result_url": task_result_url(task_id),
        }
    )


@router.get("/tasks/{task_id}/result", name="get_router_task_result")
async def get_task_result(task_id: str, request: Request) -> Response:
    """Fetch task result from S3 (durable pointer set by the scheduler)."""
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
    if row is None:
        logger.debug("Task result lookup: %s not found", task_id)
        return JSONResponse(status_code=404, content={"detail": "Task not found"})

    if row.status in (TASK_FAILED, TASK_EXPIRED):
        logger.warning("Task result lookup: %s %s (%s)", task_id, row.status, row.error)
        return JSONResponse(status_code=409, content={"detail": "Task execution failed", "error": row.error})
    if row.status != TASK_COMPLETED or not row.result_key:
        logger.debug("Task result lookup: %s not ready (status=%s)", task_id, row.status)
        return JSONResponse(
            status_code=202,
            content={"task_id": task_id, "status": row.status, "message": "Task result is not ready yet"},
        )

    store = get_store(request)
    if store is not None:
        data = await fetch_result_or_none(task_id=task_id, store=store)
        if data is not None:
            logger.debug("Returning result ZIP for task %s (%d bytes)", task_id, len(data))
            return Response(
                content=data,
                media_type="application/zip",
                headers={"content-disposition": f'attachment; filename="{task_id}.zip"'},
            )

    logger.warning("Task result lookup: %s completed but result not in object store", task_id)
    return JSONResponse(status_code=404, content={"detail": "Result not yet stored"})


@router.post("/file_parse")
async def file_parse(request: Request) -> Response:
    """Synchronous parse: ingest, poll DB until terminal, return result."""
    await check_admission(request)
    store = require_store(request, detail="Object store not configured — /file_parse requires durable payload storage.")
    if isinstance(store, JSONResponse):
        return store
    result = await ingest_task(request, store=store, settings=request.app.state.settings)
    logger.info("file_parse started for task %s (cache_hit=%s)", result.task_id, result.cache_hit)

    # Cache hit → result is already in S3; return immediately.
    if result.cache_hit and store is not None:
        data = await fetch_result_or_none(task_id=result.task_id, store=store)
        if data is not None:
            logger.info("file_parse cache hit for task %s (%d bytes)", result.task_id, len(data))
            return Response(content=data, media_type="application/zip")

    # Poll the DB until the scheduler dispatches and the task reaches terminal state.
    row = await poll_task_until_terminal(result.task_id, route="file_parse")
    resolved = await resolve_sync_result(task_id=result.task_id, row=row, store=store, route="file_parse")
    if isinstance(resolved, JSONResponse):
        return resolved
    return Response(content=resolved, media_type="application/zip")

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
    client_sla_expired_response,
    fetch_result_or_none,
    get_store,
    is_poll_complete,
    poll_task_until_terminal,
    try_fetch_result_bytes,
)
from mineru_gateway.util.datetime import to_iso

logger = logging.getLogger(__name__)
router = APIRouter(tags=["tasks"])


@router.post("/tasks", status_code=202)
async def submit_task(request: Request) -> Response:
    """Enqueue an async parse task. The scheduler process dispatches it to a worker."""
    await check_admission(request)
    store = get_store(request)
    if store is None:
        logger.error("Task submit rejected: object store not configured")
        return JSONResponse(
            status_code=503,
            content={"detail": "Object store not configured — POST /tasks requires durable payload storage."},
        )
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
            "status_url": f"/tasks/{task_id}",
            "result_url": f"/tasks/{task_id}/result",
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

    if row.status in ("failed", "expired"):
        logger.warning("Task result lookup: %s %s (%s)", task_id, row.status, row.error)
        return JSONResponse(status_code=409, content={"detail": "Task execution failed", "error": row.error})
    if row.status != "completed" or not row.result_key:
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
    store = get_store(request)
    if store is None:
        logger.error("file_parse rejected: object store not configured")
        return JSONResponse(
            status_code=503,
            content={"detail": "Object store not configured — /file_parse requires durable payload storage."},
        )
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
    if row is None:
        logger.warning("file_parse failed for task %s: task disappeared", result.task_id)
        return JSONResponse(status_code=409, content={"detail": "Task execution failed", "error": "task disappeared"})
    if row.status in ("failed", "expired"):
        logger.warning("file_parse failed for task %s: %s", result.task_id, row.error)
        return JSONResponse(status_code=409, content={"detail": "Task execution failed", "error": row.error})

    data = await try_fetch_result_bytes(result.task_id, row, store)
    if data is not None:
        logger.info("file_parse completed for task %s (%d bytes)", result.task_id, len(data))
        return Response(content=data, media_type="application/zip")

    if row.client_expired_at is not None:
        logger.info("file_parse client SLA expired for task %s (status=%s)", result.task_id, row.status)
        return client_sla_expired_response(result.task_id, status=row.status)

    if not is_poll_complete(row):
        logger.warning("file_parse timed out for task %s (status=%s)", result.task_id, row.status)
        return JSONResponse(
            status_code=202,
            content={"task_id": result.task_id, "status": row.status, "message": "Task result is not ready yet"},
        )

    logger.warning("file_parse completed for task %s but result not in object store", result.task_id)
    return JSONResponse(status_code=404, content={"detail": "Result not yet stored"})

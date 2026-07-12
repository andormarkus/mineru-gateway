"""POST /v1/ocr — Mistral-compatible OCR facade.

Accepts a Mistral OCR request ({document: {type, document_url/image_url/file}}), fetches the document bytes, builds a
MultipartPayload internally, ingests it into the central DB queue, polls for completion, reads the result from S3,
normalizes it to Mistral {pages: [...]} shape, and returns inline.

The gateway is a pure HTTP facade: it does NOT dispatch to workers. The scheduler process pulls the queued task,
dispatches it, and stores the result to S3. This keeps a single dispatch path for all routes.

LiteLLM points its OCR route at ``http://<gateway>/v1/ocr``.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from mineru_gateway.gateway.admission import check_admission
from mineru_gateway.gateway.ingest import _should_skip_cache, build_payload, extract_document, ingest_payload
from mineru_gateway.gateway.task_flow import (
    client_sla_expired_response,
    fetch_result_or_none,
    get_store,
    is_poll_complete,
    poll_task_until_terminal,
    try_fetch_result_bytes,
)
from mineru_gateway.protocol.normalize import normalize_result
from mineru_gateway.protocol.ocr_models import OCRRequest
from mineru_gateway.util.upload_paths import UnsafeUploadNameError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1", tags=["ocr"])


@router.post("/ocr")
async def create_ocr(request: Request, body: OCRRequest) -> JSONResponse:
    """Synchronous OCR: extract document, ingest to the queue, poll, normalize, return inline."""
    # Admission gate first — reject fast (503 draining / 413 too large) before any work.
    await check_admission(request)

    settings = request.app.state.settings
    file_bytes, file_name = await extract_document(
        body,
        settings=settings,
        public_bind_exposed=getattr(request.app.state, "public_bind_exposed", False),
        allow_public_http_client=getattr(request.app.state, "allow_public_http_client", False),
    )
    if file_bytes is None:
        logger.warning("OCR request rejected: could not extract document (model=%s)", body.model)
        return JSONResponse(
            status_code=400, content={"detail": "Could not extract document. Provide document_url, image_url, or file."}
        )

    store = get_store(request)
    if store is None:
        logger.error("OCR request rejected: object store not configured")
        return JSONResponse(
            status_code=503,
            content={"detail": "Object store not configured — /v1/ocr requires durable result storage."},
        )

    try:
        payload = build_payload(file_bytes=file_bytes, file_name=file_name, body=body)
    except UnsafeUploadNameError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    logger.info("OCR request for %s (%d bytes, model=%s)", file_name, len(file_bytes), body.model)
    try:
        result = await ingest_payload(
            payload,
            store=store,
            source="ocr",
            skip_cache=_should_skip_cache(request),
            public_bind_exposed=getattr(request.app.state, "public_bind_exposed", False),
            allow_public_http_client=getattr(request.app.state, "allow_public_http_client", False),
            settings=settings,
        )
    finally:
        payload.cleanup()

    # Cache hit → the result is already in S3; normalize and return immediately.
    if result.cache_hit:
        data = await fetch_result_or_none(task_id=result.task_id, store=store)
        if data is None:
            logger.warning("OCR cache hit for task %s but result missing from object store", result.task_id)
            return JSONResponse(status_code=404, content={"detail": "Cached result not found in object store"})
        logger.info("OCR cache hit for task %s (%d bytes)", result.task_id, len(data))
        return _ocr_response(body, data)

    # Poll the DB until the scheduler dispatches the task and it reaches a terminal state.
    row = await poll_task_until_terminal(result.task_id, route="ocr")
    if row is None:
        logger.warning("OCR failed for task %s: task disappeared", result.task_id)
        return JSONResponse(status_code=409, content={"detail": "OCR processing failed", "error": "task disappeared"})
    if row.status in ("failed", "expired"):
        logger.warning("OCR failed for task %s: %s", result.task_id, row.error)
        return JSONResponse(status_code=409, content={"detail": "OCR processing failed", "error": row.error})

    data = await try_fetch_result_bytes(result.task_id, row, store)
    if data is not None:
        logger.info("OCR completed for task %s (%d bytes)", result.task_id, len(data))
        return _ocr_response(body, data)

    if row.client_expired_at is not None:
        logger.info("OCR client SLA expired for task %s (status=%s)", result.task_id, row.status)
        return client_sla_expired_response(result.task_id, status=row.status)

    if not is_poll_complete(row):
        logger.warning("OCR timed out for task %s (status=%s)", result.task_id, row.status)
        return JSONResponse(
            status_code=202,
            content={"task_id": result.task_id, "status": row.status, "message": "OCR result is not ready yet"},
        )

    logger.warning("OCR completed for task %s but result not in object store", result.task_id)
    return JSONResponse(status_code=404, content={"detail": "Result not yet stored"})


def _ocr_response(body: OCRRequest, result_bytes: bytes) -> JSONResponse:
    """Normalize raw result ZIP bytes to Mistral {pages: [...]} shape and wrap in a JSONResponse."""
    pages = normalize_result(result_bytes)
    return JSONResponse(content={"model": body.model, "pages": [p.model_dump() for p in pages]})

"""Task intake pipeline: accept a request, stage the upload, store payload bytes, insert a queued row.

The gateway does NOT push to workers. It stages the upload, computes the dedup hash, checks the cache
(instant hit if found), uploads the payload bytes to S3 for the scheduler to fetch later, and inserts a
``dispatch_state="queued"`` Task row. The scheduler process pulls from the queue and dispatches.

Dedup cache hits are handled synchronously (no ingest needed — the result is already in S3).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import struct
import tempfile
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.mineru_compat import (
    MultipartPayload,
    StagedUpload,
    stage_multipart_request,
    validate_public_http_client_request,
)
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.protocol.ocr_models import OCRRequest
from mineru_gateway.tasks.cache import (
    compute_cache_key,
    compute_content_sha256,
    create_cache_hit_task,
    lookup_cache,
    populate_cache,
)

logger = logging.getLogger(__name__)

PAYLOAD_PREFIX = "payloads"

_RESULT_AFFECTING_FIELDS = ("backend", "effort", "parse_method", "start_page_id", "end_page_id")

_DEFAULT_BACKEND = "hybrid-engine"
_DEFAULT_PARSE_METHOD = "auto"
_DEFAULT_EFFORT = "medium"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    """Outcome of a task ingest attempt."""

    task_id: str
    cache_hit: bool
    status: str
    response: JSONResponse


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


async def ingest_task(request: Request, *, store: CloudStorageProvider | None = None, source: str = "tasks") -> IngestResult:
    """Stage a multipart upload from an HTTP request, then ingest it.

    Thin Request-coupled wrapper around :func:`ingest_payload` for the multipart routes
    (``/tasks``, ``/file_parse``). Returns immediately with 202 + task_id.
    """
    payload = await stage_multipart_request(request)
    try:
        return await ingest_payload(
            payload,
            store=store,
            source=source,
            skip_cache=_should_skip_cache(request),
            public_bind_exposed=getattr(request.app.state, "public_bind_exposed", False),
            allow_public_http_client=getattr(request.app.state, "allow_public_http_client", False),
        )
    finally:
        payload.cleanup()


async def ingest_payload(
    payload: MultipartPayload,
    *,
    store: CloudStorageProvider | None = None,
    source: str = "tasks",
    skip_cache: bool = False,
    public_bind_exposed: bool = False,
    allow_public_http_client: bool = False,
) -> IngestResult:
    """Validate, hash, dedup-check, upload payload, insert queued Task row.

    Request-free entry point: callers that already hold a ``MultipartPayload`` (e.g. ``/v1/ocr``,
    which builds one from extracted document bytes instead of receiving a multipart upload) call this
    directly. The caller owns ``payload.cleanup()``.
    """
    _validate_http_client_policy(
        payload, public_bind_exposed=public_bind_exposed, allow_public_http_client=allow_public_http_client
    )

    file_names = [u.upload_name for u in payload.uploads]
    content_sha256, options = _hash_staged_payload(payload)
    backend = options.get("backend", _DEFAULT_BACKEND)
    parse_method = options.get("parse_method", _DEFAULT_PARSE_METHOD)

    # Dedup cache hit → instant complete, no ingest needed.
    cache_key, options_hash = compute_cache_key(content_sha256=content_sha256, options=options)
    cache_cfg = get_settings().cache
    if not skip_cache and cache_cfg.enabled:
        cache_entry = await lookup_cache(cache_key, ttl_seconds=cache_cfg.ttl_seconds)
        if cache_entry is not None and cache_entry.result_key:
            logger.info("Dedup cache hit for content_sha256=%s… (source=%s)", content_sha256[:12], source)
            result = await _build_cache_hit_result(cache=cache_entry, file_names=file_names)
            metrics.record_task_ingested(source=source, cache_hit=True, backend=backend)
            return result

    # Upload payload bytes to S3 so the scheduler can fetch them at dispatch time.
    task_id = str(uuid.uuid4())
    payload_key = await _store_payload(task_id=task_id, payload=payload, store=store)
    logger.info(
        "Queued task %s (source=%s backend=%s files=%d payload_key=%s)",
        task_id,
        source,
        backend,
        len(file_names),
        payload_key or "none",
    )
    logger.debug("Task %s options=%s file_names=%s", task_id, options, file_names)

    # Insert the queued Task row + pre-populate the dedup cache entry.
    await _insert_queued_task(
        task_id=task_id,
        file_names=file_names,
        backend=backend,
        parse_method=parse_method,
        options=options,
        payload_key=payload_key,
        source=source,
    )
    await populate_cache(
        cache_key=cache_key,
        content_sha256=content_sha256,
        options_hash=options_hash,
        backend=backend,
        parse_method=parse_method,
        effort=options.get("effort", _DEFAULT_EFFORT),
        result_key=None,
        result_format="zip",
        source_task_id=task_id,
    )

    metrics.record_task_ingested(source=source, cache_hit=False, backend=backend)
    return IngestResult(
        task_id=task_id, cache_hit=False, status="pending", response=_pending_response(task_id=task_id, source=source)
    )


# ---------------------------------------------------------------------------
# Intake helpers
# ---------------------------------------------------------------------------


async def _build_cache_hit_result(*, cache: Any, file_names: list[str]) -> IngestResult:
    """Create a completed task row from a dedup cache hit and wrap it in an IngestResult."""
    task_id = str(uuid.uuid4())
    await create_cache_hit_task(task_id=task_id, cache=cache, file_names=file_names)
    logger.debug("Created cache-hit task row %s (result_key=%s)", task_id, cache.result_key)
    return IngestResult(
        task_id=task_id,
        cache_hit=True,
        status="completed",
        response=JSONResponse(status_code=202, content={"task_id": task_id, "status": "completed", "source": "cache"}),
    )


async def _store_payload(*, task_id: str, payload: MultipartPayload, store: CloudStorageProvider | None) -> str | None:
    """Pack + upload the payload to S3, returning the object key (or None when no store is configured)."""
    if store is None:
        logger.debug("Skipping payload upload for task %s — no object store configured", task_id)
        return None
    payload_bytes = _pack_payload(payload)
    payload_key = f"{PAYLOAD_PREFIX}/{task_id}.bin"
    await store.put(key=payload_key, data=payload_bytes)
    logger.debug("Stored payload for task %s at %s (%d bytes)", task_id, payload_key, len(payload_bytes))
    return payload_key


async def _insert_queued_task(
    *,
    task_id: str,
    file_names: list[str],
    backend: str,
    parse_method: str,
    options: dict[str, Any],
    payload_key: str | None,
    source: str,
) -> None:
    """Insert a ``dispatch_state="queued"`` Task row for the scheduler to pull."""
    options_blob = {**options, "file_names": file_names}
    async with get_db_session() as session:
        session.add(
            Task(
                task_id=task_id,
                backend=backend,
                parse_method=parse_method,
                file_names=file_names,
                status="pending",
                source=source,
                dispatch_state="queued",
                payload_key=payload_key,
                options_blob=options_blob,
            )
        )
        await session.commit()


def _pending_response(*, task_id: str, source: str) -> JSONResponse:
    """Build the 202 'pending' response for a freshly-queued task."""
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": "pending",
            "source": source,
            "status_url": f"/tasks/{task_id}",
            "result_url": f"/tasks/{task_id}/result",
        },
    )


def _hash_staged_payload(payload: MultipartPayload) -> tuple[str, dict[str, Any]]:
    """Read staged files, compute content hash, and extract options dict."""
    contents: list[bytes] = []
    for upload in payload.uploads:
        with open(upload.path, "rb") as f:
            contents.append(f.read())

    content_sha256 = compute_content_sha256(contents)

    options: dict[str, Any] = {}
    for field_name, field_value in payload.fields:
        if field_name in _RESULT_AFFECTING_FIELDS:
            options[field_name] = field_value
        elif field_name == "lang_list":
            options.setdefault("lang_list", []).append(field_value)

    if "lang_list" in options:
        options["lang_list"] = sorted(options["lang_list"])

    return content_sha256, options


def _should_skip_cache(request: Request) -> bool:
    """True when the client asked to bypass the dedup cache (``force=true`` or ``Cache-Control: no-cache``)."""
    force = request.query_params.get("force", "").lower()
    if force in {"true", "1", "yes"}:
        return True
    cache_control = request.headers.get("cache-control", "")
    return "no-cache" in cache_control.lower()


def _pack_payload(payload: MultipartPayload) -> bytes:
    """Pack the staged uploads + fields into a single blob for S3 storage.

    Format: JSON header (fields + upload metadata) followed by concatenated file bytes.
    The scheduler unpacks this to rebuild the multipart form for the worker.
    """
    header: dict[str, Any] = {"fields": payload.fields, "uploads": []}
    file_chunks: list[bytes] = []
    offset = 0
    for upload in payload.uploads:
        with open(upload.path, "rb") as f:
            data = f.read()
        header["uploads"].append(
            {
                "field_name": upload.field_name,
                "upload_name": upload.upload_name,
                "content_type": upload.content_type,
                "offset": offset,
                "size": len(data),
            }
        )
        file_chunks.append(data)
        offset += len(data)

    header_bytes = json.dumps(header).encode("utf-8")
    header_len = struct.pack("!I", len(header_bytes))
    return header_len + header_bytes + b"".join(file_chunks)


def _validate_http_client_policy(
    payload: MultipartPayload, *, public_bind_exposed: bool = False, allow_public_http_client: bool = False
) -> None:
    """SSRF guard: validate server_url / *-http-client backend on public binds."""
    validate_public_http_client_request(
        public_bind_exposed=public_bind_exposed,
        allow_public_http_client=allow_public_http_client,
        backend=payload.get_field_value("backend") or "",
        server_url=payload.get_field_value("server_url"),
    )


# ---------------------------------------------------------------------------
# OCR request preprocessing (/v1/ocr-specific)
#
# /tasks and /file_parse receive a multipart upload directly; /v1/ocr receives JSON with a base64
# file / document_url / image_url. These functions turn the JSON shape into the same MultipartPayload
# the multipart routes produce, so the rest of the intake pipeline is shared.
# ---------------------------------------------------------------------------


async def extract_document(
    body: OCRRequest,
    *,
    public_bind_exposed: bool = False,
    allow_public_http_client: bool = False,
) -> tuple[bytes | None, str]:
    """Extract raw document bytes from the Mistral OCR request shape.

    Returns ``(bytes, file_name)`` or ``(None, "")`` when the request carries no usable document.
    """
    doc = body.document

    if doc.type == "file" and doc.file:
        try:
            return base64.b64decode(doc.file), doc.file_name or "document"
        except Exception:
            logger.warning("Failed to base64-decode inline document file", exc_info=True)
            return None, ""

    if doc.type in ("document_url", "image_url"):
        url = doc.document_url or doc.image_url
        if url is None:
            return None, ""
        validate_public_http_client_request(
            public_bind_exposed=public_bind_exposed,
            allow_public_http_client=allow_public_http_client,
            backend="",
            server_url=url,
        )
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                name = url.rsplit("/", 1)[-1] or "document"
                return resp.content, name
        except Exception:
            logger.warning("Failed to fetch document from %s", url, exc_info=True)
            return None, ""

    return None, ""


def build_payload(file_bytes: bytes, file_name: str, body: OCRRequest) -> MultipartPayload:
    """Build a MultipartPayload from OCR request fields.

    The stock ``stage_multipart_request`` reads from an HTTP multipart form, but /v1/ocr receives JSON. We construct the
    same payload type directly by writing the document to a temp file and populating the form fields.
    """
    temp_dir = tempfile.mkdtemp(prefix="mineru-gateway-ocr-")

    # Ensure the upload has a file extension (MinerU infers the parser from it); default to .pdf.
    safe_name = file_name if "." in file_name else f"{file_name}.pdf"
    file_path = os.path.join(temp_dir, safe_name)
    with open(file_path, "wb") as f:
        f.write(file_bytes)

    upload = StagedUpload(
        field_name="files", upload_name=safe_name, content_type="application/octet-stream", path=file_path
    )

    fields: list[tuple[str, str]] = [
        ("backend", body.backend),
        ("effort", body.effort),
        ("parse_method", body.parse_method),
    ]
    languages = body.language if body.language else ["ch"]
    for lang in languages:
        fields.append(("lang_list", lang))

    return MultipartPayload(temp_dir=temp_dir, fields=fields, uploads=[upload])

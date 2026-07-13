"""Task intake pipeline: accept a request, stage the upload, store payload bytes, insert a queued row.

The gateway does NOT push to workers. It stages the upload, computes the dedup hash, checks the cache
(instant hit if found), uploads the payload bytes to S3 for the scheduler to fetch later, and inserts a
``status="queued"`` Task row. The scheduler process pulls from the queue and dispatches.

Dedup cache hits are handled synchronously (no ingest needed — the result is already in S3).
"""

from __future__ import annotations

import base64
import json
import logging
import struct
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import UploadFile as StarletteUploadFile

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.gateway.admission import enforce_byte_limit
from mineru_gateway.mineru_compat import (
    MultipartPayload,
    StagedUpload,
    normalize_upload_filename,
    validate_public_http_client_request,
)
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.protocol.ocr_models import OCRRequest
from mineru_gateway.scheduler._http import DOCUMENT_DOWNLOAD_TIMEOUT
from mineru_gateway.scheduler.cache_service import (
    CacheService,
    build_cache_options_from_payload,
    compute_file_records_from_paths,
    content_sha256_from_records,
)
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.tasks.status import TASK_COMPLETED, TASK_QUEUED
from mineru_gateway.tasks.storage import payload_key as object_payload_key
from mineru_gateway.tasks.storage import safe_delete, task_result_url, task_status_url
from mineru_gateway.util.io import READ_CHUNK_SIZE, iter_bounded_file
from mineru_gateway.util.upload_paths import UnsafeUploadNameError, internal_upload_path, rewrite_staged_uploads

logger = logging.getLogger(__name__)

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


async def ingest_task(
    request: Request,
    *,
    store: CloudStorageProvider | None = None,
    source: str = "tasks",
    settings: GatewaySettings | None = None,
    cache_service: CacheService | None = None,
) -> IngestResult:
    """Stage a multipart upload from an HTTP request, then ingest it.

    Thin Request-coupled wrapper around :func:`ingest_payload` for the multipart routes
    (``/tasks``, ``/file_parse``). Returns immediately with 202 + task_id.
    """
    resolved_settings = settings or request.app.state.settings
    payload = await stage_bounded_multipart_request(request, settings=resolved_settings)
    try:
        rewrite_staged_uploads(payload)
    except UnsafeUploadNameError as exc:
        payload.cleanup()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        return await ingest_payload(
            payload,
            store=store,
            source=source,
            skip_cache=_should_skip_cache(request),
            public_bind_exposed=getattr(request.app.state, "public_bind_exposed", False),
            allow_public_http_client=getattr(request.app.state, "allow_public_http_client", False),
            settings=resolved_settings,
            cache_service=cache_service,
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
    settings: GatewaySettings,
    cache_service: CacheService | None = None,
) -> IngestResult:
    """Validate, hash, dedup-check, upload payload, insert queued Task row.

    Request-free entry point: callers that already hold a ``MultipartPayload`` (e.g. ``/v1/ocr``,
    which builds one from extracted document bytes instead of receiving a multipart upload) call this
    directly. The caller owns ``payload.cleanup()``.
    """
    resolved_settings = settings
    _validate_http_client_policy(
        payload, public_bind_exposed=public_bind_exposed, allow_public_http_client=allow_public_http_client
    )

    file_names = [u.upload_name for u in payload.uploads]
    file_records, options = _hash_staged_payload(payload, settings=resolved_settings)
    backend = options.get("backend", _DEFAULT_BACKEND)
    parse_method = options.get("parse_method", _DEFAULT_PARSE_METHOD)

    cache = cache_service
    if cache is None and store is not None and resolved_settings.cache.enabled:
        cache = CacheService(resolved_settings, store)

    cache_key, cache_hit = await _resolve_cache(
        cache=cache,
        file_records=file_records,
        file_names=file_names,
        options=options,
        backend=backend,
        parse_method=parse_method,
        skip_cache=skip_cache,
        store=store,
        source=source,
        settings=resolved_settings,
    )
    if cache_hit is not None:
        return cache_hit

    task_id = str(uuid.uuid4())
    await _create_task_with_payload(
        task_id=task_id,
        payload=payload,
        file_names=file_names,
        backend=backend,
        parse_method=parse_method,
        options=options,
        cache_key=cache_key,
        store=store,
        source=source,
        settings=resolved_settings,
    )

    metrics.record_task_ingested(source=source, cache_hit=False, backend=backend)
    return IngestResult(
        task_id=task_id, cache_hit=False, status=TASK_QUEUED, response=_pending_response(task_id=task_id, source=source)
    )


async def _resolve_cache(
    *,
    cache: CacheService | None,
    file_records: list[tuple[str, str]],
    file_names: list[str],
    options: dict[str, Any],
    backend: str,
    parse_method: str,
    skip_cache: bool,
    store: CloudStorageProvider | None,
    source: str,
    settings: GatewaySettings,
) -> tuple[str | None, IngestResult | None]:
    """Look up the dedup cache; return ``(cache_key_for_task, cache_hit_result)``.

    On a hit, returns ``(cache_key, IngestResult)`` and the caller returns immediately.
    On a miss (or disabled/skipped cache), returns ``(cache_key_or_None, None)`` after creating
    a placeholder so the eventual result can populate the cache.
    """
    cache_cfg = settings.cache
    if cache is None or skip_cache or not cache_cfg.enabled:
        return None, None

    cache_key, options_hash = cache.compute_cache_key(file_records, options)
    content_sha256 = content_sha256_from_records(file_records)
    cache_entry = await cache.lookup(cache_key)
    if cache_entry is not None and cache_entry.object_key:
        logger.info("Dedup cache hit for cache_key=%s… (source=%s)", cache_key[:12], source)
        result = await _build_cache_hit_result(
            cache=cache_entry, file_names=file_names, store=store, cache_service=cache
        )
        metrics.record_task_ingested(source=source, cache_hit=True, backend=backend)
        return cache_key, result

    try:
        await cache.create_placeholder(
            cache_key=cache_key,
            content_sha256=content_sha256,
            options_hash=options_hash,
            backend=backend,
            parse_method=parse_method,
            effort=options.get("effort", _DEFAULT_EFFORT),
            ttl_seconds=cache_cfg.ttl_seconds,
        )
    except Exception:
        logger.exception("Cache placeholder failed for %s — continuing without cache", cache_key[:12])
        return None, None
    return cache_key, None


async def _create_task_with_payload(
    *,
    task_id: str,
    payload: MultipartPayload,
    file_names: list[str],
    backend: str,
    parse_method: str,
    options: dict[str, Any],
    cache_key: str | None,
    store: CloudStorageProvider | None,
    source: str,
    settings: GatewaySettings,
) -> None:
    """Store the payload bytes to S3 and insert the queued Task row.

    Cleans up the orphan payload object if the DB insert fails.
    """
    task_repo = TaskRepository.for_gateway(settings)
    payload_key: str | None = None
    try:
        payload_key = await _store_payload(task_id=task_id, payload=payload, store=store, settings=settings)
        logger.info(
            "Queued task %s (source=%s backend=%s files=%d payload_key=%s)",
            task_id,
            source,
            backend,
            len(file_names),
            payload_key or "none",
        )
        await task_repo.create_queued_task(
            task_id=task_id,
            file_names=file_names,
            backend=backend,
            parse_method=parse_method,
            options=options,
            payload_key=payload_key,
            source=source,
            cache_key=cache_key,
        )
    except Exception:
        if payload_key is not None and store is not None:
            await safe_delete(store, payload_key, label="orphan payload")
        raise


# ---------------------------------------------------------------------------
# Intake helpers
# ---------------------------------------------------------------------------


async def _build_cache_hit_result(
    *, cache: Any, file_names: list[str], store: CloudStorageProvider | None, cache_service: CacheService
) -> IngestResult:
    task_id = str(uuid.uuid4())
    if store is None:
        raise RuntimeError("Object store required for cache hits")
    await cache_service.create_hit_task(task_id=task_id, cache=cache, file_names=file_names)
    return IngestResult(
        task_id=task_id,
        cache_hit=True,
        status=TASK_COMPLETED,
        response=JSONResponse(
            status_code=202, content={"task_id": task_id, "status": TASK_COMPLETED, "source": "cache"}
        ),
    )


async def _store_payload(
    *, task_id: str, payload: MultipartPayload, store: CloudStorageProvider | None, settings: GatewaySettings
) -> str | None:
    """Pack + upload the payload to S3, returning the object key (or None when no store is configured)."""
    if store is None:
        logger.debug("Skipping payload upload for task %s — no object store configured", task_id)
        return None
    payload_bytes = _pack_payload(payload, settings=settings)
    key = object_payload_key(task_id)
    await store.put(key=key, data=payload_bytes)
    logger.debug("Stored payload for task %s at %s (%d bytes)", task_id, key, len(payload_bytes))
    return key


def _pending_response(*, task_id: str, source: str) -> JSONResponse:
    """Build the 202 'pending' response for a freshly-queued task."""
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": TASK_QUEUED,
            "source": source,
            "status_url": task_status_url(task_id),
            "result_url": task_result_url(task_id),
        },
    )


def _hash_staged_payload(
    payload: MultipartPayload, *, settings: GatewaySettings
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Hash staged files incrementally and extract cache options."""
    file_names = [upload.upload_name for upload in payload.uploads]
    paths = [upload.path for upload in payload.uploads]
    file_records = compute_file_records_from_paths(file_names, paths, settings=settings)
    options = build_cache_options_from_payload(payload.fields)
    return file_records, options


def _read_bounded_file(path: str, *, settings: GatewaySettings, label: str = "multipart upload") -> bytes:
    """Read a staged file in chunks while enforcing the configured byte limit."""
    return b"".join(iter_bounded_file(path, settings=settings, label=label))


async def stage_bounded_multipart_request(request: Request, *, settings: GatewaySettings) -> MultipartPayload:
    """Stage multipart uploads with per-chunk size enforcement.

    MinerU's stock stager streams to disk but does not enforce gateway limits.
    """
    from mineru.cli.router import cleanup_path

    temp_dir = tempfile.mkdtemp(prefix="mineru-gateway-request-")
    uploads: list[StagedUpload] = []
    fields: list[tuple[str, str]] = []
    total_bytes = 0

    try:
        form = await request.form()
        for key, value in form.multi_items():
            if isinstance(value, StarletteUploadFile):
                original_name = value.filename or f"upload-{uuid.uuid4()}"
                filename = normalize_upload_filename(original_name)
                destination = _build_upload_destination(temp_dir, filename)
                with open(destination, "wb") as handle:
                    while True:
                        chunk = await value.read(READ_CHUNK_SIZE)
                        if not chunk:
                            break
                        total_bytes += len(chunk)
                        enforce_byte_limit(total_bytes, settings=settings, label="multipart upload")
                        handle.write(chunk)
                uploads.append(
                    StagedUpload(
                        field_name=key,
                        upload_name=original_name,
                        content_type=value.content_type or "application/octet-stream",
                        path=str(destination),
                    )
                )
                await value.close()
            else:
                fields.append((key, str(value)))
    except Exception:
        cleanup_path(temp_dir)
        raise

    return MultipartPayload(temp_dir=temp_dir, fields=fields, uploads=uploads)


def _build_upload_destination(upload_dir: str, filename: str) -> Path:
    destination = Path(upload_dir) / filename
    if not destination.exists():
        return destination

    base_name = Path(filename).stem
    suffix = Path(filename).suffix
    index = 2
    while True:
        candidate = Path(upload_dir) / f"{base_name}__upload_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _should_skip_cache(request: Request) -> bool:
    """True when the client asked to bypass the dedup cache (``force=true`` or ``Cache-Control: no-cache``)."""
    force = request.query_params.get("force", "").lower()
    if force in {"true", "1", "yes"}:
        return True
    cache_control = request.headers.get("cache-control", "")
    return "no-cache" in cache_control.lower()


def _pack_payload(payload: MultipartPayload, *, settings: GatewaySettings) -> bytes:
    """Pack the staged uploads + fields into a single blob for S3 storage.

    Format: JSON header (fields + upload metadata) followed by concatenated file bytes.
    The scheduler unpacks this to rebuild the multipart form for the worker.
    """
    header: dict[str, Any] = {"fields": payload.fields, "uploads": []}
    file_chunks: list[bytes] = []
    offset = 0
    for upload in payload.uploads:
        data = _read_bounded_file(upload.path, settings=settings, label="multipart upload")
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
    settings: GatewaySettings,
    public_bind_exposed: bool = False,
    allow_public_http_client: bool = False,
) -> tuple[bytes | None, str]:
    """Extract raw document bytes from the Mistral OCR request shape.

    Returns ``(bytes, file_name)`` or ``(None, "")`` when the request carries no usable document.
    """
    resolved_settings = settings
    doc = body.document

    if doc.type == "file" and doc.file:
        return _decode_inline_file(doc, settings=resolved_settings)

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
        return await _fetch_document_url(url, settings=resolved_settings)

    return None, ""


def _decode_inline_file(doc: Any, *, settings: GatewaySettings) -> tuple[bytes | None, str]:
    """Decode a base64-encoded inline document, enforcing the byte limit on the encoded + decoded sizes."""
    try:
        encoded = doc.file.strip()
        padding = (-len(encoded)) % 4
        estimated = (len(encoded) + padding) * 3 // 4
        enforce_byte_limit(estimated, settings=settings, label="base64 document")
        data = base64.b64decode(encoded, validate=True)
        enforce_byte_limit(len(data), settings=settings, label="base64 document")
        return data, doc.file_name or "document"
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to base64-decode inline document file")
        return None, ""


async def _fetch_document_url(url: str, *, settings: GatewaySettings) -> tuple[bytes | None, str]:
    """Stream-download a document from ``url``, enforcing the byte limit on the running total."""
    try:
        async with httpx.AsyncClient(timeout=DOCUMENT_DOWNLOAD_TIMEOUT) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    enforce_byte_limit(total, settings=settings, label="downloaded document")
                    chunks.append(chunk)
                data = b"".join(chunks)
            name = url.rsplit("/", 1)[-1] or "document"
            return data, name
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to fetch document from %s", url)
        return None, ""


def build_payload(file_bytes: bytes, file_name: str, body: OCRRequest) -> MultipartPayload:
    """Build a MultipartPayload from OCR request fields.

    The stock ``stage_multipart_request`` reads from an HTTP multipart form, but /v1/ocr receives JSON. We construct the
    same payload type directly by writing the document to a temp file and populating the form fields.
    """
    temp_dir = tempfile.mkdtemp(prefix="mineru-gateway-ocr-")

    safe_name = file_name if "." in file_name else f"{file_name}.pdf"
    file_path, safe_name = internal_upload_path(temp_dir, client_name=safe_name)
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

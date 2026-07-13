"""Gateway admission control — upload size limits during intake."""

from __future__ import annotations

import contextlib
import logging

from fastapi import HTTPException, Request

from mineru_gateway.config import MAX_FILE_SIZE_HARD_BYTES, GatewaySettings
from mineru_gateway.observability.metrics import metrics

logger = logging.getLogger(__name__)


def effective_file_size_limit(settings: GatewaySettings) -> int:
    """Return the enforced byte limit: min(soft, hard), or hard when soft is unset (0)."""
    soft = settings.max_file_size_bytes
    if soft <= 0:
        return MAX_FILE_SIZE_HARD_BYTES
    return min(soft, MAX_FILE_SIZE_HARD_BYTES)


def check_file_size(content_length: int | None, *, settings: GatewaySettings) -> None:
    """Reject with 413 if the upload exceeds the effective size limit (Content-Length preflight)."""
    limit = effective_file_size_limit(settings)
    if content_length is not None and content_length > limit:
        logger.warning("Rejecting upload: content-length %d exceeds limit %d", content_length, limit)
        metrics.record_admission_rejected(reason="file_too_large")
        raise HTTPException(status_code=413, detail=f"File too large ({content_length} > {limit} bytes).")


def enforce_byte_limit(size: int, *, settings: GatewaySettings, label: str = "upload") -> None:
    """Reject when accumulated bytes exceed the effective size limit."""
    limit = effective_file_size_limit(settings)
    if size > limit:
        logger.warning("Rejecting %s: %d bytes exceeds limit %d", label, size, limit)
        metrics.record_admission_rejected(reason="file_too_large")
        raise HTTPException(status_code=413, detail=f"File too large ({size} > {limit} bytes).")


async def check_admission(request: Request) -> None:
    """Reject requests when the upload is too large (413)."""
    settings = request.app.state.settings
    cl = request.headers.get("content-length")
    if cl is not None:
        with contextlib.suppress(ValueError):
            check_file_size(int(cl), settings=settings)
    logger.debug("Admission passed: %s %s", request.method, request.url.path)

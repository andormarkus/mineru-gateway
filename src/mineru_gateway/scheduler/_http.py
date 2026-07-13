"""Shared httpx client helpers and timeout constants for the scheduler.

Centralizes the worker/upstream HTTP GET pattern and the timeout values that were previously
scattered as inline magic numbers across the scheduler and task repository.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Timeout durations in seconds (source of truth).
HEALTH_TIMEOUT_SECONDS = 5.0
UPSTREAM_REFRESH_TIMEOUT_SECONDS = 10.0
RESULT_FETCH_TIMEOUT_SECONDS = 30.0
DOCUMENT_DOWNLOAD_TIMEOUT_SECONDS = 60.0
SCHEDULER_CLIENT_TIMEOUT_SECONDS = 120.0

# Pre-built httpx.Timeout objects for call sites that pass them directly.
HEALTH_TIMEOUT = httpx.Timeout(HEALTH_TIMEOUT_SECONDS)
UPSTREAM_REFRESH_TIMEOUT = httpx.Timeout(UPSTREAM_REFRESH_TIMEOUT_SECONDS)
DOCUMENT_DOWNLOAD_TIMEOUT = httpx.Timeout(DOCUMENT_DOWNLOAD_TIMEOUT_SECONDS)
SCHEDULER_CLIENT_TIMEOUT = httpx.Timeout(SCHEDULER_CLIENT_TIMEOUT_SECONDS)


async def worker_json_get(client: httpx.AsyncClient, url: str, *, timeout: httpx.Timeout) -> dict[str, Any] | None:
    """GET ``url`` and return its JSON body, or ``None`` on transport/parse failure.

    Wraps the repeated ``client.get → raise_for_status → json()`` pattern with the
    ``(httpx.HTTPError, ValueError)`` handler used by health probing and upstream status
    refresh. Failures are logged at DEBUG (callers decide whether to defer or mark unhealthy).
    """
    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.debug("Worker HTTP GET failed for %s: %s", url, exc)
        return None

"""Canonical object-store key builders for payloads, results, and cache."""

from __future__ import annotations

import logging

from mineru_gateway.cloud.base import CloudStorageProvider

logger = logging.getLogger(__name__)

PAYLOAD_PREFIX = "payloads"
RESULT_PREFIX = "results"
CACHE_PREFIX = "cache"

RESULT_FORMAT_ZIP = "zip"
RESULT_FORMAT_MISTRAL_JSON = "mistral_json"


def payload_key(task_id: str) -> str:
    return f"{PAYLOAD_PREFIX}/{task_id}.bin"


def result_key(task_id: str, *, result_format: str = RESULT_FORMAT_ZIP) -> str:
    if result_format == RESULT_FORMAT_MISTRAL_JSON:
        return f"{RESULT_PREFIX}/{task_id}.json"
    return f"{RESULT_PREFIX}/{task_id}.zip"


def cache_object_key(cache_key: str, *, generation: str | None = None) -> str:
    base = f"{CACHE_PREFIX}/{cache_key}"
    if generation:
        return f"{base}/{generation}"
    return base


def task_status_url(task_id: str) -> str:
    """Canonical relative status URL for a task (``/tasks/{id}``)."""
    return f"/tasks/{task_id}"


def task_result_url(task_id: str) -> str:
    """Canonical relative result URL for a task (``/tasks/{id}/result``)."""
    return f"/tasks/{task_id}/result"


async def safe_delete(store: CloudStorageProvider, key: str | None, *, label: str) -> bool:
    """Best-effort object delete; logs and returns False on failure. No-op on None/empty key.

    Used for orphan cleanup (payloads, results, cache objects) where a delete failure must not
    abort the caller. ``label`` identifies the object class in the log line.
    """
    if not key:
        return True
    try:
        await store.delete(key)
        return True
    except Exception:
        logger.exception("Failed to delete orphan %s %s", label, key)
        return False

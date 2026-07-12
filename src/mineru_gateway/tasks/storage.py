"""Canonical object-store key builders for payloads, results, and cache."""

from __future__ import annotations

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

"""Result handling: fetch from worker → store to S3 → set DB pointer.

On task completion, the gateway fetches the result ZIP from the worker, uploads it to S3, and sets ``tasks.result_key``.
Retrieval reads S3 by that pointer — so it works after the producing worker is stopped AND after the router restarts.
The result outlives the worker.

Raw MinerU ZIP is stored (native bundle for /tasks clients). The Mistral JSON normalized view (for /v1/ocr) is produced
by normalize.py in Phase 5.
"""

from __future__ import annotations

import logging

import httpx

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.tasks.cache import update_cache_result_pointer

logger = logging.getLogger(__name__)

RESULT_PREFIX = "results"
RESULT_FORMAT_ZIP = "zip"
RESULT_FORMAT_MISTRAL_JSON = "mistral_json"


def _result_key(task_id: str, fmt: str = RESULT_FORMAT_ZIP) -> str:
    """Canonical S3 key for a task's result."""
    if fmt == RESULT_FORMAT_MISTRAL_JSON:
        return f"{RESULT_PREFIX}/{task_id}.json"
    return f"{RESULT_PREFIX}/{task_id}.zip"


async def store_result(task_id: str, data: bytes, store: CloudStorageProvider, *, result_format: str = RESULT_FORMAT_ZIP) -> str:
    """Upload result bytes to S3 and return the object key.

    Does NOT set the DB pointer — callers do that via :func:`set_task_result_pointer` so the two steps are
    independently testable.
    """
    key = _result_key(task_id, result_format)
    content_type = "application/json" if result_format == RESULT_FORMAT_MISTRAL_JSON else "application/zip"
    nbytes = await store.put(key=key, data=data, content_type=content_type)
    logger.debug("Stored result: task_id=%s key=%s bytes=%d format=%s", task_id, key, nbytes, result_format)
    return key


async def set_task_result_pointer(task_id: str, result_key: str, result_format: str) -> None:
    """Set the S3 result pointer on the task row."""
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
        if row is None:
            return
        row.result_key = result_key
        row.result_format = result_format
        await session.commit()


async def fetch_and_store_result(
    task_id: str,
    upstream_task_id: str,
    upstream_base_url: str,
    store: CloudStorageProvider,
    client: httpx.AsyncClient,
    *,
    update_cache: bool = True,
    return_body: bool = False,
) -> str | tuple[str, bytes]:
    """Fetch the result ZIP from the worker, store it to S3, and set the DB pointer.

    The core durability step: once the result is in S3 + the pointer is set, it survives worker scale-down and router
    restart. When ``update_cache`` is True (default), the dedup cache entry for this task is pointed at the stored key
    so future requests for the same content hit the cache.

    Args:
        return_body: if True, also return the raw result bytes (the caller needs them, e.g. for
            Mistral-shape normalization); the return type becomes ``tuple[str, bytes]`` ``(key, body)``.
    """
    result_url = f"{upstream_base_url}/tasks/{upstream_task_id}/result"
    resp = await client.get(result_url)
    resp.raise_for_status()
    key = await store_result(task_id, resp.content, store=store, result_format=RESULT_FORMAT_ZIP)
    await set_task_result_pointer(task_id, key, result_format=RESULT_FORMAT_ZIP)
    if update_cache:
        await update_cache_result_pointer(task_id, key)
    logger.info(
        "Fetched and stored result: task_id=%s upstream=%s key=%s bytes=%d",
        task_id,
        upstream_task_id,
        key,
        len(resp.content),
    )
    if return_body:
        return key, resp.content
    return key


async def read_result(task_id: str, store: CloudStorageProvider) -> tuple[bytes, str | None]:
    """Read a task's result from S3 by its DB pointer.

    Returns ``(data, result_format)``. Raises KeyError if no result is stored.
    """
    async with get_db_session() as session:
        row = await session.get(Task, task_id)
        if row is None or row.result_key is None:
            raise KeyError(task_id)
        key = row.result_key
        fmt = row.result_format

    data = await store.get(key=key)
    logger.debug("Read result: task_id=%s key=%s bytes=%d", task_id, key, len(data))
    return data, fmt

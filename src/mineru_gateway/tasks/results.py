"""Result handling: fetch from worker → store to object storage → set DB pointer."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.tasks.storage import RESULT_FORMAT_ZIP, result_key

if TYPE_CHECKING:
    from mineru_gateway.scheduler.cache_service import CacheService
    from mineru_gateway.scheduler.task_repository import TaskRepository

logger = logging.getLogger(__name__)


async def store_result(
    task_id: str, data: bytes, store: CloudStorageProvider, *, result_format: str = RESULT_FORMAT_ZIP
) -> str:
    key = result_key(task_id, result_format=result_format)
    content_type = "application/zip" if result_format == RESULT_FORMAT_ZIP else "application/json"
    nbytes = await store.put(key=key, data=data, content_type=content_type)
    logger.debug("Stored result: task_id=%s key=%s bytes=%d format=%s", task_id, key, nbytes, result_format)
    return key


async def fetch_and_store_result(
    task_id: str,
    upstream_task_id: str,
    upstream_base_url: str,
    store: CloudStorageProvider,
    client: httpx.AsyncClient,
    *,
    task_repository: TaskRepository,
    cache_service: CacheService | None = None,
    return_body: bool = False,
    timeout: float = 30.0,
) -> str | tuple[str, bytes]:
    result_url = f"{upstream_base_url}/tasks/{upstream_task_id}/result"
    resp = await client.get(result_url, timeout=httpx.Timeout(timeout))
    resp.raise_for_status()
    key = await store_result(task_id, resp.content, store=store, result_format=RESULT_FORMAT_ZIP)

    try:
        cache_key = await task_repository.complete_with_result(
            task_id, result_key_value=key, result_format=RESULT_FORMAT_ZIP
        )
    except KeyError:
        try:
            await store.delete(key)
        except Exception:
            logger.exception("Failed to delete orphan result object %s", key)
        raise
    except Exception:
        try:
            await store.delete(key)
        except Exception:
            logger.exception("Failed to delete orphan result object %s", key)
        raise

    if cache_service is not None and cache_key:
        try:
            await cache_service.populate_from_task(task_id, cache_key=cache_key, result_format=RESULT_FORMAT_ZIP)
        except Exception:
            logger.exception("Cache population failed for task %s (result is durable)", task_id)

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
    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import Task

    async with get_db_session() as session:
        row = await session.get(Task, task_id)
        if row is None or row.result_key is None:
            raise KeyError(task_id)
        key = row.result_key
        fmt = row.result_format

    data = await store.get(key=key)
    logger.debug("Read result: task_id=%s key=%s bytes=%d", task_id, key, len(data))
    return data, fmt

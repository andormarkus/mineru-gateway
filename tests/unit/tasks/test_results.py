"""Result durability: store → pointer → read round-trip via in-memory CloudStorageProvider."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_session_factory
from mineru_gateway.db.models import Task
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.tasks.results import read_result, store_result


@pytest.mark.asyncio
async def test_store_and_read_result(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    async with get_session_factory()() as session:
        session.add(Task(task_id="t1", backend="pipeline", file_names=["a.pdf"]))
        await session.commit()

    data = b"PK\x03\x04 fake zip"
    key = await store_result("t1", data, memory_store, result_format="zip")
    assert key == "results/t1.zip"
    assert await memory_store.exists(key)

    repo = TaskRepository(get_settings(), memory_store)
    await repo.complete_with_result("t1", result_key_value=key, result_format="zip")
    result_data, fmt = await read_result("t1", memory_store)
    assert result_data == data
    assert fmt == "zip"


@pytest.mark.asyncio
async def test_result_survives_worker_gone(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    async with get_session_factory()() as session:
        session.add(Task(task_id="t2", backend="pipeline", file_names=["x.pdf"]))
        await session.commit()

    data = b"result bytes that outlive the worker"
    key = await store_result("t2", data, memory_store)
    repo = TaskRepository(get_settings(), memory_store)
    await repo.complete_with_result("t2", result_key_value=key, result_format="zip")

    result_data, _ = await read_result("t2", memory_store)
    assert result_data == data


@pytest.mark.asyncio
async def test_store_mistral_json_format(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    async with get_session_factory()() as session:
        session.add(Task(task_id="t3", backend="pipeline", file_names=["y.pdf"]))
        await session.commit()

    data = b'{"pages": []}'
    key = await store_result("t3", data, memory_store, result_format="mistral_json")
    assert key.endswith(".json")


@pytest.mark.asyncio
async def test_read_missing_result_raises(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    async with get_session_factory()() as session:
        session.add(Task(task_id="t4", backend="pipeline", file_names=["z.pdf"]))
        await session.commit()

    with pytest.raises(KeyError):
        await read_result("t4", memory_store)


@pytest.mark.asyncio
async def test_store_then_read_result(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    """Core store→pointer→read cycle shared by sync and async gateway paths."""
    async with get_session_factory()() as session:
        session.add(Task(task_id="sync-async-1", status="completed", backend="pipeline", file_names=["d.pdf"]))
        await session.commit()

    data = b"PK\x03\x04 result bytes"
    key = await store_result("sync-async-1", data, memory_store)
    repo = TaskRepository(get_settings(), memory_store)
    await repo.complete_with_result("sync-async-1", result_key_value=key, result_format="zip")

    result_data, fmt = await read_result("sync-async-1", memory_store)
    assert result_data == data
    assert fmt == "zip"


@pytest.mark.asyncio
async def test_ocr_result_storage_round_trip(db_session: AsyncSession, memory_store: CloudStorageProvider) -> None:
    async with get_session_factory()() as session:
        session.add(Task(task_id="ocr-1", status="completed", backend="pipeline", file_names=["img.pdf"]))
        await session.commit()

    raw_zip = b"PK\x03\x04 ocr result"
    key = await store_result("ocr-1", raw_zip, memory_store)
    repo = TaskRepository(get_settings(), memory_store)
    await repo.complete_with_result("ocr-1", result_key_value=key, result_format="zip")

    result_data, _ = await read_result("ocr-1", memory_store)
    assert result_data == raw_zip


@pytest.mark.asyncio
async def test_complete_with_result_raises_for_missing_task(
    db_session: AsyncSession, memory_store: CloudStorageProvider
) -> None:
    repo = TaskRepository(get_settings(), memory_store)
    with pytest.raises(KeyError, match="missing-task"):
        await repo.complete_with_result("missing-task", result_key_value="results/x.zip", result_format="zip")

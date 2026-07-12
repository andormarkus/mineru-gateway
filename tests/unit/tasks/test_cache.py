"""Phase 4 tests: content-addressed dedup.

Tests cache key correctness (same content+options → hit, different options →
miss), hit_count increment, TTL expiry, and cache-hit task creation.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore

from mineru_gateway.config import get_settings
from mineru_gateway.scheduler.cache_service import (
    CacheService,
    compute_cache_key,
    compute_file_records,
    content_sha256_from_records,
)


def _records(*pairs: tuple[str, bytes]) -> list[tuple[str, str]]:
    names = [name for name, _ in pairs]
    contents = [content for _, content in pairs]
    return compute_file_records(names, contents)


# --- hashing correctness ----------------------------------------------------


def test_ordered_file_records_change_with_filename_swap() -> None:
    """Swapping contents between filenames must change the cache key."""
    r1 = _records(("a.pdf", b"file-a"), ("b.pdf", b"file-b"))
    r2 = _records(("a.pdf", b"file-b"), ("b.pdf", b"file-a"))
    opts = {"backend": "pipeline"}
    assert compute_cache_key(r1, opts)[0] != compute_cache_key(r2, opts)[0]


def test_different_content_different_hash() -> None:
    r1 = _records(("a.pdf", b"a"))
    r2 = _records(("a.pdf", b"b"))
    assert content_sha256_from_records(r1) != content_sha256_from_records(r2)


def test_cache_key_changes_with_options() -> None:
    """Same content + different parse_method → different cache key."""
    records = _records(("doc.pdf", b"data"))
    key1, _ = compute_cache_key(records, {"backend": "pipeline", "parse_method": "auto"})
    key2, _ = compute_cache_key(records, {"backend": "pipeline", "parse_method": "ocr"})
    assert key1 != key2


def test_cache_key_includes_output_packaging_fields() -> None:
    """Output-packaging flags affect the cache key."""
    records = _records(("doc.pdf", b"data"))
    opts_base = {"backend": "pipeline", "parse_method": "auto"}
    opts_pkg = {**opts_base, "return_md": True, "response_format_zip": False}
    key1, _ = compute_cache_key(records, opts_base)
    key2, _ = compute_cache_key(records, opts_pkg)
    assert key1 != key2


def test_cache_key_includes_file_names() -> None:
    records_a = _records(("a.pdf", b"data"))
    records_b = _records(("b.pdf", b"data"))
    opts = {"backend": "pipeline"}
    k1, _ = compute_cache_key(records_a, opts)
    k2, _ = compute_cache_key(records_b, opts)
    assert k1 != k2


def test_cache_key_different_backend() -> None:
    records = _records(("doc.pdf", b"data"))
    k1, _ = compute_cache_key(records, {"backend": "pipeline"})
    k2, _ = compute_cache_key(records, {"backend": "hybrid-engine"})
    assert k1 != k2


# --- cache lookup / populate ------------------------------------------------


@pytest.mark.asyncio
async def test_cache_miss_then_hit(db_session: AsyncSession) -> None:
    records = _records(("doc.pdf", b"doc"))
    cache_key, opts_hash = compute_cache_key(records, {"backend": "pipeline", "parse_method": "auto"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), InMemoryStore(name="test"))

    result = await svc.lookup(cache_key)
    assert result is None

    await svc.create_placeholder(
        cache_key=cache_key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry

    async with get_db_session() as session:
        row = await session.get(CacheEntry, cache_key)
        assert row is not None
        row.object_key = "cache/abc.zip"
        await session.commit()

    hit = await svc.lookup(cache_key)
    assert hit is not None
    assert hit.object_key == "cache/abc.zip"
    assert hit.hit_count == 1


@pytest.mark.asyncio
async def test_hit_count_increments(db_session: AsyncSession) -> None:
    records = _records(("x.pdf", b"x"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), InMemoryStore(name="test"))

    await svc.create_placeholder(
        cache_key=key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry

    async with get_db_session() as session:
        row = await session.get(CacheEntry, key)
        assert row is not None
        row.object_key = "cache/x"
        await session.commit()

    await svc.lookup(key)
    await svc.lookup(key)
    await svc.lookup(key)
    hit = await svc.lookup(key)
    assert hit is not None
    assert hit.hit_count == 4


@pytest.mark.asyncio
async def test_placeholder_does_not_clear_existing_object_key(db_session: AsyncSession) -> None:
    records = _records(("doc.pdf", b"doc"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), InMemoryStore(name="test"))

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry

    async with get_db_session() as session:
        session.add(
            CacheEntry(
                cache_key=key,
                content_sha256=content_sha256,
                options_hash=opts_hash,
                backend="pipeline",
                parse_method="auto",
                effort="medium",
                object_key="cache/existing.zip",
                result_format="zip",
            )
        )
        await session.commit()

    await svc.create_placeholder(
        cache_key=key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )

    async with get_db_session() as session:
        row = await session.get(CacheEntry, key)
        assert row is not None
        assert row.object_key == "cache/existing.zip"


@pytest.mark.asyncio
async def test_populate_from_task_uses_canonical_result_key(db_session: AsyncSession) -> None:
    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry, Task
    from mineru_gateway.tasks.storage import result_key

    store = InMemoryStore(name="test")
    task_id = "task-cache-pop"
    cache_key = "abc123"
    await store.put(result_key(task_id), b"zip-bytes")

    async with get_db_session() as session:
        session.add(
            CacheEntry(
                cache_key=cache_key,
                content_sha256="x",
                options_hash="y",
                backend="pipeline",
                parse_method="auto",
                effort="medium",
            )
        )
        session.add(
            Task(
                task_id=task_id,
                status="storing_result",
                backend="pipeline",
                file_names=["a.pdf"],
                cache_key=cache_key,
                result_key=result_key(task_id),
            )
        )
        await session.commit()

    await CacheService(get_settings(), store).populate_from_task(task_id, cache_key=cache_key, result_format="zip")
    async with get_db_session() as session:
        row = await session.get(CacheEntry, cache_key)
    assert row is not None
    assert row.object_key is not None
    assert row.object_key.startswith(f"cache/{cache_key}/")
    assert await store.get(row.object_key) == b"zip-bytes"


@pytest.mark.asyncio
async def test_create_cache_hit_task(db_session: AsyncSession) -> None:
    """A cache hit creates a completed task row with source=cache."""
    store = InMemoryStore(name="test")
    records = _records(("d.pdf", b"d"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    object_key = f"cache/{key}"
    await store.put(object_key, b"cached-result")
    svc = CacheService(get_settings(), store)

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry

    async with get_db_session() as session:
        session.add(
            CacheEntry(
                cache_key=key,
                content_sha256=content_sha256,
                options_hash=opts_hash,
                backend="pipeline",
                parse_method="auto",
                effort="medium",
                object_key=object_key,
                result_format="zip",
            )
        )
        await session.commit()
        cache = await session.get(CacheEntry, key)

    assert cache is not None
    task = await svc.create_hit_task(task_id="cached-task-1", cache=cache, file_names=["d.pdf"])
    assert task.status == "completed"
    assert task.source == "cache"
    assert task.result_key == "results/cached-task-1.zip"
    assert task.completed_at is not None
    assert await store.get("results/cached-task-1.zip") == b"cached-result"


# --- TTL expiry + sweeper ---------------------------------------------------


@pytest.mark.asyncio
async def test_ttl_expiry(db_session: AsyncSession) -> None:
    """An expired entry returns None from lookup."""
    from datetime import timedelta

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry
    from mineru_gateway.util.datetime import now_utc

    records = _records(("doc.pdf", b"expiring"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), InMemoryStore(name="test"))

    await svc.create_placeholder(
        cache_key=key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )

    async with get_db_session() as session:
        row = await session.get(CacheEntry, key)
        assert row is not None
        row.object_key = "cache/exp"
        row.expires_at = now_utc() - timedelta(seconds=1)
        await session.commit()

    hit = await svc.lookup(key)
    assert hit is None


@pytest.mark.asyncio
async def test_expired_placeholder_refreshes_in_place(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry
    from mineru_gateway.util.datetime import ensure_aware_utc, now_utc

    records = _records(("doc.pdf", b"expiring"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), InMemoryStore(name="test"))

    async with get_db_session() as session:
        session.add(
            CacheEntry(
                cache_key=key,
                content_sha256=content_sha256,
                options_hash=opts_hash,
                backend="pipeline",
                parse_method="auto",
                effort="medium",
                object_key="cache/old.zip",
                object_generation=3,
                result_format="zip",
                expires_at=now_utc() - timedelta(seconds=1),
            )
        )
        await session.commit()

    row = await svc.create_placeholder(
        cache_key=key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )
    assert row is not None
    assert row.object_key == ""
    assert row.object_generation == 4
    assert row.expires_at is not None
    assert ensure_aware_utc(row.expires_at) > now_utc()


@pytest.mark.asyncio
async def test_sweep_removes_expired(db_session: AsyncSession) -> None:
    from datetime import timedelta

    from mineru_gateway.db.base import get_db_session
    from mineru_gateway.db.models import CacheEntry
    from mineru_gateway.util.datetime import now_utc

    store = InMemoryStore()
    records = _records(("doc.pdf", b"sweep"))
    key, opts_hash = compute_cache_key(records, {"backend": "pipeline"})
    content_sha256 = content_sha256_from_records(records)
    svc = CacheService(get_settings(), store)

    await svc.create_placeholder(
        cache_key=key,
        content_sha256=content_sha256,
        options_hash=opts_hash,
        backend="pipeline",
        parse_method="auto",
        effort="medium",
    )

    async with get_db_session() as session:
        row = await session.get(CacheEntry, key)
        assert row is not None
        row.object_key = "cache/sweep"
        row.expires_at = now_utc() - timedelta(seconds=1)
        await session.commit()

    removed = await svc.sweep_expired()
    assert removed == 1

"""Cache lookup, population, invalidation, and sweeper."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import CacheEntry, Task
from mineru_gateway.tasks.status import TASK_COMPLETED
from mineru_gateway.tasks.storage import cache_object_key, result_key, safe_delete
from mineru_gateway.util.datetime import ensure_aware_utc, now_utc
from mineru_gateway.util.ids import short_id
from mineru_gateway.util.io import iter_bounded_file

logger = logging.getLogger(__name__)

CACHE_IGNORED_FIELDS = frozenset()
REPEATED_OPTION_FIELDS = frozenset({"lang_list"})


@dataclass(slots=True)
class _RefreshKwargs:
    """Bundle of the fields used to refresh/create a cache placeholder row.

    Passed to :meth:`CacheService._reset_expired_row` from both the found-expired path and the
    IntegrityError recovery path of :meth:`create_placeholder`, avoiding repetition of the
    seven keyword arguments.
    """

    content_sha256: str
    options_hash: str
    backend: str
    parse_method: str
    effort: str
    result_format: str = "zip"
    ttl_seconds: int | None = None

    def as_kwargs(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "options_hash": self.options_hash,
            "backend": self.backend,
            "parse_method": self.parse_method,
            "effort": self.effort,
            "result_format": self.result_format,
            "ttl_seconds": self.ttl_seconds,
        }


def compute_file_records(file_names: list[str], contents: list[bytes]) -> list[tuple[str, str]]:
    """Ordered (filename, per-file sha256) pairs preserving upload order."""
    return [(name, hashlib.sha256(content).hexdigest()) for name, content in zip(file_names, contents, strict=True)]


def hash_file_at_path(path: str, *, settings: GatewaySettings, label: str = "upload") -> str:
    """Hash a staged file incrementally while enforcing the configured byte limit."""
    digest = hashlib.sha256()
    for chunk in iter_bounded_file(path, settings=settings, label=label):
        digest.update(chunk)
    return digest.hexdigest()


def compute_file_records_from_paths(
    file_names: list[str], paths: list[str], *, settings: GatewaySettings
) -> list[tuple[str, str]]:
    """Ordered file records from on-disk staged uploads."""
    return [(name, hash_file_at_path(path, settings=settings)) for name, path in zip(file_names, paths, strict=True)]


def build_cache_options_from_payload(fields: list[tuple[str, str]]) -> dict[str, Any]:
    """Build cache identity options from every forwarded multipart field."""
    options: dict[str, Any] = {}
    for field_name, field_value in fields:
        if field_name in CACHE_IGNORED_FIELDS:
            continue
        if field_name in REPEATED_OPTION_FIELDS:
            options.setdefault(field_name, []).append(field_value)
        else:
            options[field_name] = field_value
    for key in REPEATED_OPTION_FIELDS:
        if key in options:
            options[key] = sorted(options[key])
    return options


def content_sha256_from_records(file_records: list[tuple[str, str]]) -> str:
    """Aggregate digest for cache metadata from ordered file records."""
    joined = "\n".join(f"{name}:{digest}" for name, digest in file_records).encode()
    return hashlib.sha256(joined).hexdigest()


def compute_cache_key(file_records: list[tuple[str, str]], options: dict[str, Any]) -> tuple[str, str]:
    """Hash ordered filename/content pairs together with result-affecting options."""
    records_blob = json.dumps(file_records, separators=(",", ":"))
    options_blob = CacheService._canonical_options(options)
    options_hash = hashlib.sha256(options_blob.encode()).hexdigest()
    key_material = f"{records_blob}\n{options_blob}".encode()
    return hashlib.sha256(key_material).hexdigest(), options_hash


class CacheService:
    def __init__(self, settings: GatewaySettings, store: CloudStorageProvider) -> None:
        self._settings = settings
        self._store = store

    def compute_cache_key(self, file_records: list[tuple[str, str]], options: dict[str, Any]) -> tuple[str, str]:
        return compute_cache_key(file_records, options)

    @staticmethod
    def _canonical_options(options: dict[str, Any]) -> str:
        filtered = {k: v for k, v in options.items() if k not in CACHE_IGNORED_FIELDS and v is not None}
        return json.dumps(filtered, sort_keys=True, default=str)

    def _row_is_expired(self, row: CacheEntry) -> bool:
        return row.expires_at is not None and now_utc() > ensure_aware_utc(row.expires_at)

    async def _reset_expired_row(
        self,
        session: AsyncSession,
        row: CacheEntry,
        *,
        content_sha256: str | None = None,
        options_hash: str | None = None,
        backend: str | None = None,
        parse_method: str | None = None,
        effort: str | None = None,
        result_format: str | None = None,
        ttl_seconds: int | None = None,
    ) -> str | None:
        """Clear an expired generation in place; return the previous object key."""
        stale_key = row.object_key or None
        row.object_key = ""
        row.object_generation += 1
        row.hit_count = 0
        if content_sha256 is not None:
            row.content_sha256 = content_sha256
            row.options_hash = options_hash or row.options_hash
            row.backend = backend or row.backend
            row.parse_method = parse_method or row.parse_method
            row.effort = effort or row.effort
            row.result_format = result_format or row.result_format
            ttl = ttl_seconds if ttl_seconds is not None else self._settings.cache.ttl_seconds
            row.expires_at = now_utc() + timedelta(seconds=ttl) if ttl else None
        await session.commit()
        await session.refresh(row)
        return stale_key if stale_key else None

    async def _delete_stale_object(self, object_key: str | None) -> None:
        await safe_delete(self._store, object_key, label="stale cache object")

    async def lookup(self, cache_key: str) -> CacheEntry | None:
        async with get_db_session() as session:
            row = await session.get(CacheEntry, cache_key)
            if row is None:
                return None
            if self._row_is_expired(row):
                stale_key = await self._reset_expired_row(session, row)
                await self._delete_stale_object(stale_key)
                return None
            if not row.object_key:
                return None
            row.hit_count += 1
            await session.commit()
            return row

    async def create_placeholder(
        self,
        *,
        cache_key: str,
        content_sha256: str,
        options_hash: str,
        backend: str,
        parse_method: str,
        effort: str,
        result_format: str = "zip",
        ttl_seconds: int | None = None,
    ) -> CacheEntry | None:
        """Insert-if-absent placeholder; refresh expired rows in place."""
        refresh_kwargs = _RefreshKwargs(
            content_sha256=content_sha256,
            options_hash=options_hash,
            backend=backend,
            parse_method=parse_method,
            effort=effort,
            result_format=result_format,
            ttl_seconds=ttl_seconds,
        )
        async with get_db_session() as session:
            row = await session.get(CacheEntry, cache_key)
            if row is not None:
                if self._row_is_expired(row):
                    stale_key = await self._reset_expired_row(session, row, **refresh_kwargs.as_kwargs())
                    await self._delete_stale_object(stale_key)
                return row

            row = self._build_placeholder_row(cache_key=cache_key, refresh=refresh_kwargs, ttl_seconds=ttl_seconds)
            session.add(row)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                row = await session.get(CacheEntry, cache_key)
                if row is None:
                    raise
                if self._row_is_expired(row):
                    stale_key = await self._reset_expired_row(session, row, **refresh_kwargs.as_kwargs())
                    await self._delete_stale_object(stale_key)
            await session.refresh(row)
            return row

    def _build_placeholder_row(self, *, cache_key: str, refresh: _RefreshKwargs, ttl_seconds: int | None) -> CacheEntry:
        """Construct a fresh placeholder ``CacheEntry`` row with an expiry derived from the TTL."""
        effective_ttl = ttl_seconds or self._settings.cache.ttl_seconds
        return CacheEntry(
            cache_key=cache_key,
            content_sha256=refresh.content_sha256,
            options_hash=refresh.options_hash,
            backend=refresh.backend,
            parse_method=refresh.parse_method,
            effort=refresh.effort,
            object_key="",
            object_generation=0,
            result_format=refresh.result_format,
            expires_at=(now_utc() + timedelta(seconds=effective_ttl) if effective_ttl else None),
        )

    async def populate_from_task(self, task_id: str, *, cache_key: str, result_format: str) -> None:
        """Copy task result to cache-owned object after a miss completes."""
        src = result_key(task_id)
        generation = short_id()
        dest = cache_object_key(cache_key, generation=generation)
        try:
            await self._store.copy(src, dest)
        except KeyError:
            logger.warning("Cannot populate cache — task result missing: %s", src)
            return
        async with get_db_session() as session:
            row = await session.get(CacheEntry, cache_key)
            if row is None:
                await safe_delete(self._store, dest, label="orphan cache object")
                return
            expected_generation = row.object_generation
            stmt = (
                update(CacheEntry)
                .where(
                    CacheEntry.cache_key == cache_key,
                    CacheEntry.object_generation == expected_generation,
                    or_(CacheEntry.object_key.is_(None), CacheEntry.object_key == ""),
                )
                .values(object_key=dest, result_format=result_format, object_generation=expected_generation + 1)
            )
            result = await session.execute(stmt)
            if not result.rowcount:  # type: ignore[attr-defined]
                await session.rollback()
                await safe_delete(self._store, dest, label="orphan cache object")
                return
            try:
                await session.commit()
            except Exception:
                await session.rollback()
                await safe_delete(self._store, dest, label="orphan cache object")
                raise

    async def create_hit_task(self, *, task_id: str, cache: CacheEntry, file_names: list[str]) -> Task:
        """Server-side copy cache object to an independent task result."""
        dest_key = result_key(task_id)
        copied = False
        if cache.object_key:
            await self._store.copy(cache.object_key, dest_key)
            copied = True
        try:
            async with get_db_session() as session:
                row = Task(
                    task_id=task_id,
                    backend=cache.backend,
                    parse_method=cache.parse_method,
                    file_names=file_names,
                    status=TASK_COMPLETED,
                    source="cache",
                    result_key=dest_key,
                    result_format=cache.result_format,
                    cache_key=cache.cache_key,
                    completed_at=now_utc(),
                )
                session.add(row)
                await session.commit()
                await session.refresh(row)
                return row
        except Exception:
            if copied:
                await safe_delete(self._store, dest_key, label="orphan cache-hit result")
            raise

    async def invalidate(self, cache_key: str) -> bool:
        async with get_db_session() as session:
            row = await session.get(CacheEntry, cache_key)
            if row is None:
                return False
            object_key = row.object_key or None
            claimed_generation = row.object_generation
            claim = await session.execute(
                update(CacheEntry)
                .where(CacheEntry.cache_key == cache_key, CacheEntry.object_generation == claimed_generation)
                .values(object_key="", object_generation=claimed_generation + 1)
            )
            if not claim.rowcount:  # type: ignore[attr-defined]
                await session.rollback()
                return False
            await session.commit()
            new_generation = claimed_generation + 1

        if object_key and not await safe_delete(self._store, object_key, label="cache object during invalidation"):
            return False

        async with get_db_session() as session:
            removed = await session.execute(
                delete(CacheEntry).where(
                    CacheEntry.cache_key == cache_key,
                    CacheEntry.object_generation == new_generation,
                    or_(CacheEntry.object_key.is_(None), CacheEntry.object_key == ""),
                )
            )
            if not removed.rowcount:  # type: ignore[attr-defined]
                await session.rollback()
                return False
            await session.commit()
        return True

    async def sweep_expired(self) -> int:
        now = now_utc()
        async with get_db_session() as session:
            query = select(CacheEntry).where(CacheEntry.expires_at.is_not(None), CacheEntry.expires_at < now).limit(100)
            rows = list((await session.execute(query)).scalars().all())

        removed = 0
        for row in rows:
            if row.object_key and not await safe_delete(self._store, row.object_key, label="expired cache object"):
                continue
            async with get_db_session() as session:
                db_row = await session.get(CacheEntry, row.cache_key)
                if db_row is None or db_row.object_generation != row.object_generation:
                    continue
                await session.delete(db_row)
                await session.commit()
                removed += 1
        return removed

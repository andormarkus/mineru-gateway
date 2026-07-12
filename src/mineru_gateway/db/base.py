"""Async SQLAlchemy engine + session factory + declarative base.

Backend via ``DATABASE_URL`` scheme:
  sqlite+aiosqlite → local dev and unit tests only;
  postgresql+asyncpg → production (install ``[postgres]`` extra).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from mineru_gateway.config import get_settings


class Base(DeclarativeBase):
    """Declarative base for all gateway ORM models."""


# ---------------------------------------------------------------------------
# Engine / session factory
#
# We create these lazily from settings so tests can point at an in-memory or file sqlite without importing the
# production config. The module-level singletons are populated by ``init_engine`` at app startup; tests call it
# directly with a custom URL.
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _build_engine(database_url: str, **kwargs: Any) -> AsyncEngine:
    """Create an async engine, applying sensible defaults per backend."""
    connect_args: dict[str, Any] = {}
    if database_url.startswith("sqlite"):
        # Async sqlite needs check_same_thread=False when shared across tasks.
        connect_args = {"check_same_thread": False}
    return create_async_engine(database_url, connect_args=connect_args, **kwargs)


def init_engine(database_url: str | None = None, *, echo: bool = False, **kwargs: Any) -> AsyncEngine:
    """Create and store the global engine + session factory.

    Called from the app lifespan. If an engine already exists for the same URL it is reused.
    Otherwise the previous engine is replaced (call ``await shutdown_engine()`` first to dispose
    cleanly when switching URLs — tests do this in fixture teardown).
    """
    global _engine, _session_factory
    url = database_url or get_settings().database_url
    if _engine is not None and str(_engine.url) == url:
        return _engine
    _engine = _build_engine(url, echo=echo, **kwargs)
    _session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    return _engine


def get_engine() -> AsyncEngine:
    """Return the current global engine (must have been initialized)."""
    if _engine is None:
        raise RuntimeError("DB engine not initialized. Call init_engine() first.")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the global session factory."""
    if _session_factory is None:
        raise RuntimeError("DB session factory not initialized. Call init_engine() first.")
    return _session_factory


def get_db_session() -> AsyncSession:
    """Return a fresh async session from the global factory."""
    if _session_factory is None:
        raise RuntimeError("DB session factory not initialized. Call init_engine() first.")
    return _session_factory()


async def shutdown_engine() -> None:
    """Dispose the global engine (app shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None

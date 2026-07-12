"""Test-only DB helpers.

Production schema management is Alembic-only (``task migrate``). Tests build the schema directly
from the SQLAlchemy models via ``create_all`` for speed and isolation — running full migrations
per-test would couple every test to migration history and slow the suite.

This module lives under ``tests/`` so it can never be imported by production code.
"""

from __future__ import annotations

from mineru_gateway.db.base import Base, get_engine


async def create_all_tables() -> None:
    """Create all tables from model metadata (tests/dev only — never call from production).

    Production must use Alembic (``alembic upgrade head``). This ``create_all`` shortcut exists so
    unit tests can spin up an isolated in-memory sqlite schema in one call without running the
    migration runner.
    """
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

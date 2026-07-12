"""PostgreSQL Alembic migration tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

from mineru_gateway.db.base import Base

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
EXPECTED_TABLES = frozenset(Base.metadata.tables.keys())


def _run_alembic(*args: str, database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["MINERU_GATEWAY_DATABASE_URL"] = database_url
    subprocess.run([sys.executable, "-m", "alembic", *args], check=True, cwd=ROOT, env=env)


@pytest.mark.asyncio
async def test_initial_migration_upgrade_and_downgrade(postgres_url: str) -> None:
    """0001_initial applies cleanly on PostgreSQL and downgrades to base."""
    _run_alembic("upgrade", "head", database_url=postgres_url)

    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as conn:
            tables = await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
    finally:
        await engine.dispose()

    assert EXPECTED_TABLES.issubset(tables)

    _run_alembic("downgrade", "base", database_url=postgres_url)

    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as conn:
            tables = await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
    finally:
        await engine.dispose()

    assert tables.isdisjoint(EXPECTED_TABLES)

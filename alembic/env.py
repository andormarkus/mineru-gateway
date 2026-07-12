"""Alembic env — async-aware, reads DATABASE_URL from config/env.

Migrations target ``Base.metadata`` from ``mineru_gateway.db.base``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from logging.config import fileConfig

# Ensure the package (src layout) is importable before importing from it.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from mineru_gateway.config import get_settings
from mineru_gateway.db import models  # noqa: F401  (register models on metadata)
from mineru_gateway.db.base import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_database_url() -> str:
    """DATABASE_URL precedence: alembic.ini [-x dburl=...] > env > app config."""
    # 1. Alembic -x dburl=... (CLI override)
    dburl = context.get_x_argument(as_dictionary=True).get("dburl")
    if dburl:
        return dburl
    # 2. Environment
    env_dburl = os.environ.get("DATABASE_URL") or os.environ.get("MINERU_GATEWAY_DATABASE_URL")
    if env_dburl:
        return env_dburl
    # 3. App config (config.yaml / defaults)
    return get_settings().database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout)."""
    context.configure(
        url=_resolve_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode with an async engine."""
    dburl = _resolve_database_url()
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = dburl

    connectable = async_engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

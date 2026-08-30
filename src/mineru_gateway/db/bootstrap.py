"""Optional sqlite schema bootstrap (dev/test).

Production PostgreSQL must use Alembic. For sqlite (local dev and tests), create ORM
tables when missing so startup does not fail on an empty file DB.
"""

from __future__ import annotations

from sqlalchemy import inspect

from mineru_gateway.db.base import Base, get_engine


async def ensure_sqlite_schema() -> None:
    """Create ORM tables on sqlite when the schema is missing."""
    engine = get_engine()
    if not str(engine.url).startswith("sqlite"):
        return

    async with engine.connect() as conn:

        def _has_workers(sync_conn) -> bool:
            return inspect(sync_conn).has_table("workers")

        has_workers = await conn.run_sync(_has_workers)

    if has_workers:
        return

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

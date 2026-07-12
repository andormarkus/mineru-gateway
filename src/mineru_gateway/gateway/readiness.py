"""Readiness probe — database and object storage connectivity."""

from __future__ import annotations

import logging

from sqlalchemy import text

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_engine

logger = logging.getLogger(__name__)


async def check_readiness(*, settings: GatewaySettings, store: CloudStorageProvider | None) -> tuple[bool, str]:
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("Readiness database check failed: %s", exc)
        return False, "database unavailable"

    if store is None:
        return False, "object storage not configured"
    try:
        await store.validate()
    except Exception as exc:
        logger.warning("Readiness storage check failed: %s", exc)
        return False, "object storage unavailable"

    return True, "ok"

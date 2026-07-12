"""Startup guard — refuse public bind without auth."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Worker
from mineru_gateway.mineru_compat import is_public_bind_host, validate_mineru_compat

if TYPE_CHECKING:
    from mineru_gateway.cloud.base import CloudStorageProvider

logger = logging.getLogger(__name__)


class PublicBindWithoutAuthError(RuntimeError):
    """Raised when the gateway tries to bind a public host without auth enabled."""


class StartupDependencyError(RuntimeError):
    """Raised when required startup dependencies are missing or inconsistent."""


def enforce_bind_guard(settings: GatewaySettings, *, cli_host: str | None = None) -> None:
    if settings.auth.enabled:
        return
    host = cli_host or settings.host
    if is_public_bind_host(host):
        raise PublicBindWithoutAuthError(
            f"Refusing to bind public host '{host}' without auth enabled. "
            "Enable auth in config or bind to a loopback address."
        )


async def validate_startup_dependencies(
    settings: GatewaySettings, *, store: CloudStorageProvider | None = None
) -> None:
    validate_mineru_compat()
    if not settings.deployment_id.strip():
        raise StartupDependencyError("deployment_id is required")

    if not settings.cloud.is_object_store_configured():
        raise StartupDependencyError(f"cloud.{settings.cloud.provider} bucket/container name is required")

    if store is None:
        raise StartupDependencyError("Object storage is required — configure cloud bucket and ensure access")

    if settings.auth.enabled and not settings.auth.resolved_keys():
        raise StartupDependencyError("auth.api_key is required when auth.enabled is true")

    if settings.cloud_workers_enabled:
        template_id, _, _ = settings.cloud.launch_template()
        if settings.cloud.provider == "aws" and not template_id:
            raise StartupDependencyError("cloud.aws.launch_template_id is required when cloud workers are enabled")

    await _validate_worker_provider_ownership(settings)


async def _validate_worker_provider_ownership(settings: GatewaySettings) -> None:
    async with get_db_session() as session:
        query = select(Worker).where(
            Worker.terminated_at.is_(None), Worker.provider.isnot(None), Worker.provider != settings.cloud.provider
        )
        conflicts = list((await session.execute(query)).scalars().all())

    if conflicts:
        sample = ", ".join(w.id for w in conflicts[:5])
        suffix = "..." if len(conflicts) > 5 else ""
        raise StartupDependencyError(
            f"DB contains workers for provider {conflicts[0].provider!r} "
            f"but configured provider is {settings.cloud.provider!r} "
            f"(conflicting workers: {sample}{suffix})"
        )

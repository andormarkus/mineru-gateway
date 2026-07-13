"""Scheduler process CLI entry point."""

from __future__ import annotations

import asyncio
import logging

import click
import httpx

from mineru_gateway.cli import load_settings_from_cli
from mineru_gateway.cloud.registry import init_provider, init_store
from mineru_gateway.db.base import init_engine, shutdown_engine
from mineru_gateway.observability.otel import init_otel
from mineru_gateway.scheduler._http import SCHEDULER_CLIENT_TIMEOUT
from mineru_gateway.scheduler.lock import scheduler_lock
from mineru_gateway.scheduler.scheduler import Scheduler
from mineru_gateway.startup_guard import validate_startup_dependencies

logger = logging.getLogger(__name__)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="mineru-gateway")
@click.option("--config", "config_path", default="config.yaml", show_default=True, type=click.Path(dir_okay=False))
@click.option("--database-url", default=None)
@click.option(
    "--log-level", default=None, help="Log level (DEBUG, INFO, WARNING, ERROR). Defaults to config log_level or INFO."
)
def main(config_path: str, database_url: str | None, log_level: str | None) -> None:
    """Run the mineru-gateway scheduler process (single sequential tick loop)."""
    settings, resolved_level = load_settings_from_cli(
        config_path=config_path, database_url=database_url, log_level=log_level
    )
    logger.info("Starting scheduler process (log_level=%s)", resolved_level.upper())

    asyncio.run(_run_scheduler(settings))


async def _run_scheduler(settings) -> None:
    init_otel(settings=settings, service_name=settings.otel.scheduler_service_name)
    init_engine(settings.database_url)

    store = await init_store(settings)
    if store is None:
        raise RuntimeError("Object store is required for scheduler")
    await validate_startup_dependencies(settings, store=store)

    provider = init_provider(settings)
    if settings.cloud_workers_enabled and provider is None:
        raise RuntimeError("Cloud workers enabled but compute provider failed to initialize")
    client = httpx.AsyncClient(timeout=SCHEDULER_CLIENT_TIMEOUT)

    try:
        async with scheduler_lock(settings.database_url) as lock:
            scheduler = Scheduler(settings=settings, store=store, client=client, provider=provider, lock=lock)
            await scheduler.run()
    finally:
        await client.aclose()
        await shutdown_engine()

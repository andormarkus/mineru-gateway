"""Scheduler process CLI entry point — the brain of the system.

Runs as a separate process (``mineru-scheduler``). Active-passive via DB heartbeat:
one active, one standby. The active runs 8 background loops:

  1. dispatch — pull queued tasks, push 1-2 per worker
  2. autoscaler — scale up (Tier A start / Tier B launch) + scale down (Tier A stop)
  3. rotation — launch fresh → drain old → terminate (weekly / template change)
  4. drain — process draining workers: wait in-flight → stop/terminate
  5. completion watcher — fetch results from workers → store to S3
  6. cache sweeper — drop expired dedup entries
  7. SLA timer — fail dispatched-but-stuck tasks
  8. health monitor — poll worker /health, mirror stats

The gateway process is stateless (API only). This process owns all background work.
"""

from __future__ import annotations

import asyncio
import logging
import os

import click
import httpx

from mineru_gateway.cloud.registry import init_provider, init_store
from mineru_gateway.config import get_settings, load_settings, reset_settings_cache
from mineru_gateway.db.base import init_engine, shutdown_engine
from mineru_gateway.logging_config import configure_logging
from mineru_gateway.observability.metrics import metrics
from mineru_gateway.observability.otel import init_otel
from mineru_gateway.scheduler.autoscaler import Autoscaler
from mineru_gateway.scheduler.completion_watcher import store_unstored_results
from mineru_gateway.scheduler.dispatch_loop import dispatch_pending_tasks
from mineru_gateway.scheduler.health_monitor import refresh_worker_health
from mineru_gateway.scheduler.loops import run_as_leader
from mineru_gateway.scheduler.rotation import run_rotation
from mineru_gateway.scheduler.scheduler_lock import run_scheduler_as_leader
from mineru_gateway.scheduler.worker_drain import process_draining_workers
from mineru_gateway.tasks.cache import sweep_expired
from mineru_gateway.tasks.errors import expire_stale_tasks

logger = logging.getLogger(__name__)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="mineru-gateway")
@click.option("--config", "config_path", default="config.yaml", show_default=True, type=click.Path(dir_okay=False))
@click.option("--database-url", default=None)
@click.option(
    "--log-level",
    default=None,
    help="Log level (DEBUG, INFO, WARNING, ERROR). Defaults to config log_level or INFO.",
)
def main(config_path: str, database_url: str | None, log_level: str | None) -> None:
    """Run the mineru-gateway scheduler process (background loops, active-passive)."""
    if database_url:
        os.environ["MINERU_GATEWAY_DATABASE_URL"] = database_url

    reset_settings_cache()
    settings = load_settings(config_path)

    resolved_level = log_level or settings.log_level
    configure_logging(resolved_level)
    logger.info("Starting scheduler process (host=%s log_level=%s)", settings.host, resolved_level.upper())

    asyncio.run(_run_scheduler(settings))


async def _run_scheduler(settings) -> None:
    """Initialize DB, store, provider, client; run all loops under active-passive leadership."""
    logger.info("Scheduler initializing")
    init_otel(service_name=settings.otel.scheduler_service_name)
    # --- Init DB ---
    # Schema is managed by Alembic (``alembic upgrade head`` / ``task migrate``) — the scheduler
    # assumes migrations have already been applied. Run migrations before starting the scheduler.
    init_engine(settings.database_url)
    logger.info("Database engine initialized")

    # --- Init store ---
    store = await init_store()
    if store is None:
        logger.warning("Object store unavailable — completion watcher will be disabled")
    else:
        logger.info("Object store initialized (bucket=%s)", settings.cloud.object_store_bucket())

    # --- Init provider ---
    provider = init_provider()
    if provider is None:
        logger.warning("Cloud provider unavailable — autoscaling/rotation/drain cloud ops disabled")
    else:
        logger.info("Cloud provider initialized (provider=%s)", settings.cloud.provider)

    # --- Init httpx client (standalone — not from pool) ---
    client = httpx.AsyncClient(timeout=httpx.Timeout(120.0))
    logger.debug("HTTP client initialized (timeout=120s)")

    # --- Build the autoscaler with the provider ---
    autoscaler = Autoscaler.from_settings(provider=provider)

    # --- Background loop tick functions ---
    async def _dispatch_tick() -> None:
        count = await dispatch_pending_tasks(store, client)
        if count:
            logger.info("Dispatch tick: dispatched %d task(s)", count)
        else:
            logger.debug("Dispatch tick: nothing dispatched")

    async def _autoscaler_tick() -> None:
        signal = await autoscaler.evaluate_once()
        metrics.set_queue_depth(signal.queue_depth)
        metrics.set_workers_running(signal.running_workers)
        metrics.set_scaling_signal(signal.signal)

    async def _rotation_tick() -> None:
        rotated = await run_rotation(provider)
        if rotated:
            logger.info("Rotation tick: rotated %d worker(s)", rotated)
        else:
            logger.debug("Rotation tick: no rotation due")

    async def _drain_tick() -> None:
        drained = await process_draining_workers(provider)
        if drained:
            logger.info("Drain tick: finalized %d worker(s)", drained)
        else:
            logger.debug("Drain tick: no workers ready to finalize")

    async def _watcher_tick() -> None:
        if store is not None:
            count = await store_unstored_results(store, client)
            if count:
                logger.info("Completion watcher tick: stored %d result(s)", count)
            else:
                logger.debug("Completion watcher tick: nothing to store")

    async def _sweeper_tick() -> None:
        removed = await sweep_expired()
        if removed:
            metrics.record_cache_sweep_removed(removed)
            logger.info("Cache sweeper tick: removed %d expired entries", removed)
        else:
            logger.debug("Cache sweeper tick: no expired entries")

    async def _sla_tick() -> None:
        expired = await expire_stale_tasks(settings.task_sla_seconds)
        if expired:
            metrics.record_sla_expired(expired)
            logger.warning("SLA timer tick: expired %d stale task(s)", expired)
        else:
            logger.debug("SLA timer tick: no stale tasks")

    async def _health_tick() -> None:
        healthy = await refresh_worker_health(client)
        metrics.set_workers_healthy(healthy)
        logger.debug("Health monitor tick: %d worker(s) healthy", healthy)

    # --- The main scheduler tick: run all loops once ---
    async def _all_loops_tick() -> None:
        """One tick of all background work. Called by the active-passive coordinator."""
        sched_cfg = get_settings().scheduler
        loops = [
            ("dispatch", _dispatch_tick, sched_cfg.dispatch_interval_seconds),
            ("autoscaler", _autoscaler_tick, get_settings().scaling.poll_interval_seconds),
            ("rotation", _rotation_tick, get_settings().rotation.interval_seconds),
            ("drain", _drain_tick, sched_cfg.drain_interval_seconds),
            ("completion_watcher", _watcher_tick, 15.0),
            ("cache_sweeper", _sweeper_tick, get_settings().cache.sweeper_interval_seconds),
            ("sla_timer", _sla_tick, 60.0),
            ("health_monitor", _health_tick, sched_cfg.health_monitor_interval_seconds),
        ]

        # Run each loop independently with its own leader lock + interval.
        # We track them as background tasks.
        bg_tasks: list[asyncio.Task[None]] = []
        for name, fn, interval in loops:
            bg_tasks.append(asyncio.create_task(run_as_leader(name, fn, interval=interval), name=f"scheduler-{name}"))
            logger.info("Started background loop %s (interval=%.1fs)", name, interval)

        logger.info("Scheduler loops running (%d background tasks)", len(bg_tasks))

        try:
            await asyncio.gather(*bg_tasks)
        except asyncio.CancelledError:
            logger.info("Scheduler loops shutting down")
            for t in bg_tasks:
                t.cancel()
            await asyncio.gather(*bg_tasks, return_exceptions=True)
            raise

    # --- Run under active-passive leadership ---
    sched_cfg = settings.scheduler
    try:
        await run_scheduler_as_leader(
            _all_loops_tick,
            heartbeat_interval=sched_cfg.heartbeat_interval_seconds,
            lease_timeout=sched_cfg.lease_timeout_seconds,
        )
    finally:
        logger.info("Scheduler shutdown starting")
        await client.aclose()
        await shutdown_engine()
        logger.info("Scheduler shutdown complete")

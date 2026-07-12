"""FastAPI application factory."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from mineru_gateway import __version__
from mineru_gateway.attribution import install_attribution, upstream_info
from mineru_gateway.cloud.registry import init_store
from mineru_gateway.config import GatewaySettings, get_settings
from mineru_gateway.db.base import init_engine, shutdown_engine
from mineru_gateway.db.registry import DBTaskRegistry
from mineru_gateway.gateway.admission import get_drain_controller
from mineru_gateway.gateway.api.admin import router as admin_router
from mineru_gateway.gateway.api.ocr import router as ocr_router
from mineru_gateway.gateway.api.tasks import router as tasks_router
from mineru_gateway.gateway.logging_middleware import RequestLoggingMiddleware
from mineru_gateway.mineru_compat import is_public_bind_host
from mineru_gateway.observability.otel import init_otel
from mineru_gateway.startup_guard import enforce_bind_guard

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup/shutdown."""
    settings = get_settings()
    app.state.settings = settings
    logger.info("Gateway lifespan startup (version=%s host=%s)", __version__, settings.host)

    # --- Startup guard: refuse public bind without auth ---
    enforce_bind_guard(settings)
    logger.debug("Startup guard passed (auth.enabled=%s)", settings.auth.enabled)

    # --- SSRF policy flags (read by /tasks and /v1/ocr to validate server_url) ---
    app.state.public_bind_exposed = is_public_bind_host(settings.host)
    app.state.allow_public_http_client = False
    logger.debug("SSRF policy: public_bind_exposed=%s", app.state.public_bind_exposed)

    # --- Observability (OTel) ---
    init_otel(app=app)
    logger.debug("OTel initialized (enabled=%s)", settings.otel.enabled)

    # --- Graceful drain (SIGTERM handler) ---
    drain = get_drain_controller()
    drain.install_signal_handler()

    # --- DB ---
    init_engine(settings.database_url)
    logger.info("Database engine initialized")

    # --- Task registry (DB-backed) ---
    registry = DBTaskRegistry(task_retention_seconds=86400, cleanup_interval_seconds=300)
    await registry.start()
    app.state.router_task_registry = registry
    logger.debug("Task registry started")

    # --- Object store (for result reads + payload upload at ingest time) ---
    app.state.object_store = await init_store()
    if app.state.object_store is None:
        logger.warning("Object store unavailable — running without durable payload/result storage")
    else:
        logger.info("Object store initialized (bucket=%s)", settings.cloud.object_store_bucket())

    logger.info("Gateway ready")
    yield

    # --- Shutdown ---
    logger.info("Gateway shutdown starting")
    await registry.shutdown()
    await shutdown_engine()
    logger.info("Gateway shutdown complete")


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Construct the FastAPI app."""
    resolved = settings if settings is not None else get_settings()

    app = FastAPI(
        title="mineru-gateway",
        description=(
            "A DB-backed, S3-durable, autoscaling gateway that wraps "
            "[MinerU](https://github.com/opendatalab/MinerU) with a "
            "Mistral-compatible `/v1/ocr` facade.\n\n"
            "**Built on top of MinerU** by Opendatalab — see attribution headers."
        ),
        version=__version__,
        lifespan=lifespan,
    )

    if resolved.attribution.enabled:
        install_attribution(app)

    app.add_middleware(RequestLoggingMiddleware)
    _register_routes(app)
    return app


def _register_routes(app: FastAPI) -> None:
    """Register routes: /health, GET /, /tasks*, /file_parse."""
    app.include_router(tasks_router)
    app.include_router(ocr_router)
    app.include_router(admin_router)

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, object]:
        """Aggregated health. Includes the ``upstream`` attribution block (§2)."""
        return {"status": "ok", "version": __version__, "upstream": upstream_info()}

    @app.get("/", tags=["meta"])
    async def root() -> dict[str, object]:
        """Attribution page (MinerU §2 obligation)."""
        return {
            "service": "mineru-gateway",
            "version": __version__,
            "description": "A gateway built on top of MinerU.",
            "upstream": upstream_info(),
        }

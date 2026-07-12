"""Click CLI entry point → uvicorn.

Run via ``task run`` or directly as ``mineru-gateway`` (the console script).
"""

from __future__ import annotations

import logging

import click
import uvicorn

from mineru_gateway import __version__
from mineru_gateway.config import load_settings, reset_settings_cache
from mineru_gateway.gateway.app import create_app
from mineru_gateway.logging_config import configure_logging
from mineru_gateway.startup_guard import enforce_bind_guard

logger = logging.getLogger(__name__)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="mineru-gateway")
@click.option("--host", default=None, help="Bind host (overrides config when set).")
@click.option("-p", "--port", default=None, type=int, help="Bind port (overrides config when set).")
@click.option(
    "--config",
    "config_path",
    default="config.yaml",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Path to config.yaml (gitignored; holds secrets).",
)
@click.option(
    "--database-url", default=None, help="Override DATABASE_URL (async SQLAlchemy URL). Defaults to config/env."
)
@click.option("--reload", is_flag=True, default=False, help="Enable uvicorn auto-reload (development).")
@click.option(
    "--log-level", default=None, help="Log level (DEBUG, INFO, WARNING, ERROR). Defaults to config log_level or INFO."
)
def main(
    host: str | None, port: int | None, config_path: str, database_url: str | None, reload: bool, log_level: str | None
) -> None:
    """Run the mineru-gateway server."""
    reset_settings_cache()
    cli_overrides: dict[str, object] = {}
    if host is not None:
        cli_overrides["host"] = host
    if port is not None:
        cli_overrides["port"] = port
    if database_url:
        cli_overrides["database_url"] = database_url
    if log_level:
        cli_overrides["log_level"] = log_level
    settings = load_settings(config_path, **cli_overrides)

    bind_host = host if host is not None else settings.host
    bind_port = port if port is not None else settings.port

    resolved_level = log_level or settings.log_level
    configure_logging(resolved_level)
    logger.info("Starting gateway (host=%s port=%d log_level=%s)", bind_host, bind_port, resolved_level.upper())

    enforce_bind_guard(settings, cli_host=bind_host)

    uvicorn.run(create_app(settings), host=bind_host, port=bind_port, reload=reload, log_level=resolved_level.lower())


if __name__ == "__main__":
    main()

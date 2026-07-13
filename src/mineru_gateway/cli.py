"""Shared Click CLI bootstrap for the gateway and scheduler entry points.

Both processes share the same settings-loading preamble: reset the cache, assemble CLI
overrides, load settings, and configure logging. Each ``main()`` keeps its own Click
options and process-specific wiring but delegates the shared part here.
"""

from __future__ import annotations

from typing import Any

from mineru_gateway.config import GatewaySettings, load_settings, reset_settings_cache
from mineru_gateway.logging_config import configure_logging


def load_settings_from_cli(
    *, config_path: str, database_url: str | None, log_level: str | None, **overrides: Any
) -> tuple[GatewaySettings, str]:
    """Load settings applying CLI overrides, then configure logging.

    ``overrides`` are extra field overrides (e.g. ``host``, ``port`` for the gateway).
    Returns ``(settings, resolved_log_level)``.
    """
    reset_settings_cache()
    cli_overrides: dict[str, Any] = {}
    if database_url:
        cli_overrides["database_url"] = database_url
    if log_level:
        cli_overrides["log_level"] = log_level
    cli_overrides.update(overrides)
    settings = load_settings(config_path, **cli_overrides)

    resolved_level = log_level or settings.log_level
    configure_logging(resolved_level)
    return settings, resolved_level

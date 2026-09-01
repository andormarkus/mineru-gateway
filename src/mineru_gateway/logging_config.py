"""Shared logging setup for gateway and scheduler processes."""

from __future__ import annotations

import logging
import sys

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)-5s [%(name)s] %(message)s"

# Keep httpx/httpcore chatter at WARNING unless the root level is DEBUG.
_QUIET_LOGGERS = ("httpx", "httpcore", "botocore", "aiobotocore")


def resolve_log_level(level: str | int) -> int:
    """Normalize a log level name or numeric value to a ``logging`` level."""
    if isinstance(level, int):
        return level
    numeric = logging.getLevelName(level.upper())
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level!r}")
    return numeric


def configure_logging(level: str | int = "INFO") -> int:
    """Configure process-wide logging. Returns the resolved numeric level."""
    numeric = resolve_log_level(level)
    logging.basicConfig(level=numeric, format=DEFAULT_LOG_FORMAT, stream=sys.stderr, force=True)
    apply_quiet_loggers(numeric)
    return numeric


def apply_quiet_loggers(level: int) -> None:
    """Raise httpx/botocore loggers to WARNING unless root level is DEBUG."""
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(max(level, logging.WARNING))

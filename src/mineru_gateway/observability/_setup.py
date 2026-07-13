"""Shared OpenTelemetry SDK bootstrap helpers.

Both the metrics facade (:mod:`mineru_gateway.observability.metrics`) and the trace
instrumentation (:mod:`mineru_gateway.observability.otel`) build a Resource, construct a
provider, and emit the same "opentelemetry not installed" warning when the ``otel`` extra is
absent. This module centralizes those shared pieces.
"""

from __future__ import annotations

import logging

_OTEL_MISSING_WARNING = "OTel enabled but opentelemetry packages not installed. Install with: uv sync --extra otel"

logger = logging.getLogger(__name__)


def otel_resource(service_name: str):  # type: ignore[no-untyped-def]
    """Build an OTel ``Resource`` tagged with ``service.name``.

    Must be called inside the caller's ``try: from opentelemetry.sdk.resources import Resource``
    block — importing the SDK at module top would defeat the optional-extra pattern.
    """
    from opentelemetry.sdk.resources import Resource

    return Resource.create({"service.name": service_name})


def log_otel_unavailable() -> None:
    """Emit the standard warning when OTel is enabled but the SDK extra is missing."""
    logger.warning(_OTEL_MISSING_WARNING)

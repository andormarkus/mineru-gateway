"""OpenTelemetry instrumentation.

Auto-instruments FastAPI and httpx for traces; exports custom business metrics via OTLP.
Off by default — enable via ``otel.enabled = true`` and ``otel.endpoint`` in config.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mineru_gateway.config import GatewaySettings
from mineru_gateway.observability._setup import log_otel_unavailable, otel_resource
from mineru_gateway.observability.metrics import metrics

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


def init_otel(*, settings: GatewaySettings, app: FastAPI | None = None, service_name: str | None = None) -> None:
    """Initialize OTel traces and metrics if enabled in config.

    Gateway passes ``app`` for FastAPI auto-instrumentation. Scheduler passes only
    ``service_name`` for metrics + httpx tracing.
    """
    if not settings.otel.enabled:
        return

    endpoint = settings.otel.endpoint
    if not endpoint:
        logger.warning("OTel enabled but no endpoint configured — skipping instrumentation")
        return

    resolved_name = service_name or settings.otel.service_name

    metrics.init_metrics(
        service_name=resolved_name,
        endpoint=endpoint,
        export_interval_seconds=settings.otel.metrics_export_interval_seconds,
    )

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = otel_resource(resolved_name)
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        trace.set_tracer_provider(provider)

        if app is not None:
            FastAPIInstrumentor.instrument_app(app)  # type: ignore[call-arg,misc]
        HTTPXClientInstrumentor.instrument()  # type: ignore[misc]
        logger.info("OTel traces enabled: service=%s endpoint=%s", resolved_name, endpoint)

    except ImportError:
        log_otel_unavailable()

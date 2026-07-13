"""OpenTelemetry business metrics facade.

All record methods are no-ops until :func:`init_metrics` enables the real implementation.
Call sites can import ``metrics`` unconditionally without requiring the ``otel`` extra.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

from mineru_gateway.observability._setup import log_otel_unavailable, otel_resource

logger = logging.getLogger(__name__)


def _when_enabled(method: Callable[..., None]) -> Callable[..., None]:
    """Skip the record call when metrics are not enabled (no-op until ``init_metrics``)."""

    @functools.wraps(method)
    def wrapper(self: Metrics, *args: Any, **kwargs: Any) -> None:
        if not self._enabled:
            return
        return method(self, *args, **kwargs)

    return wrapper


def _when_enabled_with_count(method: Callable[..., None]) -> Callable[..., None]:
    """Like :func:`_when_enabled` but also skips when ``count <= 0``.

    For cumulative counters that accept a count (SLA-expired, results-stored, retention-deleted).
    The ``count`` argument must be passed as a keyword.
    """

    @functools.wraps(method)
    def wrapper(self: Metrics, *args: Any, **kwargs: Any) -> None:
        if not self._enabled or kwargs.get("count", 1) <= 0:
            return
        return method(self, *args, **kwargs)

    return wrapper


class Metrics:
    """Typed facade over OTel instruments for gateway and scheduler."""

    def __init__(self) -> None:
        self._enabled = False
        self._meter: Any = None
        self._tasks_ingested: Any = None
        self._admission_rejected: Any = None
        self._tasks_dispatched: Any = None
        self._tasks_dispatch_failed: Any = None
        self._tasks_sla_expired: Any = None
        self._results_stored: Any = None
        self._results_store_failed: Any = None
        self._workers_health_checks: Any = None
        self._tasks_poll_duration: Any = None
        self._tasks_dispatch_duration: Any = None
        self._retention_deleted: Any = None
        self._workers_scaled: Any = None
        self._workers_cloud_errors: Any = None
        self._cache_sweep_removed: Any = None
        self._tasks_dispatch_requeued: Any = None
        self._tasks_stale_claims_recovered: Any = None

    def init_metrics(self, *, service_name: str, endpoint: str, export_interval_seconds: int = 60) -> None:
        """Wire OTLP metric export. Called once at process startup."""
        try:
            from opentelemetry import metrics as otel_metrics
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

            resource = otel_resource(service_name)
            reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint), export_interval_millis=export_interval_seconds * 1000
            )
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            otel_metrics.set_meter_provider(provider)
            self._meter = otel_metrics.get_meter("mineru_gateway")
            self._create_instruments()
            self._enabled = True
            logger.info("OTel metrics enabled: service=%s endpoint=%s", service_name, endpoint)
        except ImportError:
            log_otel_unavailable()

    def _create_instruments(self) -> None:
        meter = self._meter
        self._tasks_ingested = meter.create_counter("mineru.tasks.ingested", description="Tasks accepted at ingest")
        self._admission_rejected = meter.create_counter(
            "mineru.admission.rejected", description="Requests rejected at admission gate"
        )
        self._tasks_dispatched = meter.create_counter(
            "mineru.tasks.dispatched", description="Tasks dispatched to workers"
        )
        self._tasks_dispatch_failed = meter.create_counter(
            "mineru.tasks.dispatch.failed", description="Task dispatch failures"
        )
        self._tasks_sla_expired = meter.create_counter(
            "mineru.tasks.sla_expired", description="Tasks expired by SLA timer"
        )
        self._results_stored = meter.create_counter(
            "mineru.results.stored", description="Task results stored to object store"
        )
        self._results_store_failed = meter.create_counter(
            "mineru.results.store_failed", description="Failed result store attempts"
        )
        self._workers_health_checks = meter.create_counter(
            "mineru.workers.health_checks", description="Worker health check outcomes"
        )
        self._tasks_poll_duration = meter.create_histogram(
            "mineru.tasks.poll.duration", unit="ms", description="Sync route poll-until-terminal duration"
        )
        self._tasks_dispatch_duration = meter.create_histogram(
            "mineru.tasks.dispatch.duration", unit="ms", description="Single task dispatch duration"
        )
        self._retention_deleted = meter.create_counter(
            "mineru.retention.deleted", description="Objects/rows deleted by retention cleanup"
        )
        self._workers_scaled = meter.create_counter(
            "mineru.workers.scaled", description="Worker autoscale and drain actions"
        )
        self._workers_cloud_errors = meter.create_counter(
            "mineru.workers.cloud_errors", description="Cloud provider errors during worker reconcile"
        )
        self._cache_sweep_removed = meter.create_counter(
            "mineru.cache.sweep_removed", description="Expired cache entries removed by sweeper"
        )
        self._tasks_dispatch_requeued = meter.create_counter(
            "mineru.tasks.dispatch.requeued", description="Tasks returned to queue after dispatch exhaustion"
        )
        self._tasks_stale_claims_recovered = meter.create_counter(
            "mineru.tasks.stale_claims_recovered", description="Stale dispatch claims recovered to queued"
        )

    @_when_enabled
    def record_task_ingested(self, *, source: str, cache_hit: bool, backend: str) -> None:
        self._tasks_ingested.add(1, {"source": source, "cache_hit": str(cache_hit).lower(), "backend": backend})

    @_when_enabled
    def record_admission_rejected(self, *, reason: str) -> None:
        self._admission_rejected.add(1, {"reason": reason})

    @_when_enabled
    def record_task_dispatched(self, *, worker_id: str, backend: str) -> None:
        self._tasks_dispatched.add(1, {"worker_id": worker_id, "backend": backend})

    @_when_enabled
    def record_dispatch_failed(self, *, error_class: str) -> None:
        self._tasks_dispatch_failed.add(1, {"error_class": error_class})

    @_when_enabled
    def record_dispatch_duration(self, *, duration_ms: float, outcome: str) -> None:
        self._tasks_dispatch_duration.record(duration_ms, {"outcome": outcome})

    @_when_enabled_with_count
    def record_sla_expired(self, count: int = 1) -> None:
        self._tasks_sla_expired.add(count)

    @_when_enabled_with_count
    def record_result_stored(self, count: int = 1) -> None:
        self._results_stored.add(count)

    @_when_enabled_with_count
    def record_result_store_failed(self, count: int = 1) -> None:
        self._results_store_failed.add(count)

    @_when_enabled
    def record_health_check(self, *, outcome: str) -> None:
        self._workers_health_checks.add(1, {"outcome": outcome})

    @_when_enabled
    def record_poll_duration(self, *, route: str, outcome: str, duration_ms: float) -> None:
        self._tasks_poll_duration.record(duration_ms, {"route": route, "outcome": outcome})

    @_when_enabled_with_count
    def record_retention_deleted(self, *, kind: str, count: int = 1) -> None:
        self._retention_deleted.add(count, {"kind": kind})

    @_when_enabled
    def record_worker_scaled(self, *, action: str) -> None:
        self._workers_scaled.add(1, {"action": action})

    @_when_enabled
    def record_cloud_error(self, *, category: str, retryable: bool) -> None:
        self._workers_cloud_errors.add(1, {"category": category, "retryable": str(retryable).lower()})

    @_when_enabled_with_count
    def record_cache_sweep_removed(self, count: int = 1) -> None:
        self._cache_sweep_removed.add(count)

    @_when_enabled_with_count
    def record_dispatch_requeued(self, count: int = 1) -> None:
        self._tasks_dispatch_requeued.add(count)

    @_when_enabled_with_count
    def record_stale_claims_recovered(self, count: int = 1) -> None:
        self._tasks_stale_claims_recovered.add(count)


metrics = Metrics()

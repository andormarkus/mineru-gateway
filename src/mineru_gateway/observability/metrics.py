"""OpenTelemetry business metrics facade.

All record methods are no-ops until :func:`init_metrics` enables the real implementation.
Call sites can import ``metrics`` unconditionally without requiring the ``otel`` extra.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
        self._scaling_events: Any = None
        self._cache_sweep_removed: Any = None
        self._workers_health_checks: Any = None
        self._tasks_poll_duration: Any = None
        self._tasks_dispatch_duration: Any = None
        self._queue_depth: Any = None
        self._workers_running: Any = None
        self._workers_healthy: Any = None
        self._scaling_signal: Any = None
        self._scheduler_is_leader: Any = None

    def init_metrics(
        self,
        *,
        service_name: str,
        endpoint: str,
        export_interval_seconds: int = 60,
    ) -> None:
        """Wire OTLP metric export. Called once at process startup."""
        try:
            from opentelemetry import metrics as otel_metrics
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource

            resource = Resource.create({"service.name": service_name})
            reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint),
                export_interval_millis=export_interval_seconds * 1000,
            )
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            otel_metrics.set_meter_provider(provider)
            self._meter = otel_metrics.get_meter("mineru_gateway")
            self._create_instruments()
            self._enabled = True
            logger.info("OTel metrics enabled: service=%s endpoint=%s", service_name, endpoint)
        except ImportError:
            logger.warning(
                "OTel metrics enabled but opentelemetry packages not installed. Install with: "
                "uv sync --extra otel"
            )

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
        self._scaling_events = meter.create_counter(
            "mineru.scaling.events", description="Autoscaler scaling actions"
        )
        self._cache_sweep_removed = meter.create_counter(
            "mineru.cache.sweep_removed", description="Expired cache entries removed"
        )
        self._workers_health_checks = meter.create_counter(
            "mineru.workers.health_checks", description="Worker health check outcomes"
        )
        self._tasks_poll_duration = meter.create_histogram(
            "mineru.tasks.poll.duration",
            unit="ms",
            description="Sync route poll-until-terminal duration",
        )
        self._tasks_dispatch_duration = meter.create_histogram(
            "mineru.tasks.dispatch.duration",
            unit="ms",
            description="Single task dispatch duration",
        )
        self._queue_depth = meter.create_gauge("mineru.queue.depth", description="Pending + processing tasks")
        self._workers_running = meter.create_gauge(
            "mineru.workers.running", description="Healthy running workers"
        )
        self._workers_healthy = meter.create_gauge(
            "mineru.workers.healthy", description="Workers passing last health check"
        )
        self._scaling_signal = meter.create_gauge(
            "mineru.scaling.signal", description="Queue depth per running worker"
        )
        self._scheduler_is_leader = meter.create_gauge(
            "mineru.scheduler.is_leader", description="1 if this scheduler holds leadership"
        )

    def record_task_ingested(self, *, source: str, cache_hit: bool, backend: str) -> None:
        if not self._enabled:
            return
        self._tasks_ingested.add(
            1,
            {"source": source, "cache_hit": str(cache_hit).lower(), "backend": backend},
        )

    def record_admission_rejected(self, *, reason: str) -> None:
        if not self._enabled:
            return
        self._admission_rejected.add(1, {"reason": reason})

    def record_task_dispatched(self, *, worker_id: str, backend: str) -> None:
        if not self._enabled:
            return
        self._tasks_dispatched.add(1, {"worker_id": worker_id, "backend": backend})

    def record_dispatch_failed(self, *, error_class: str) -> None:
        if not self._enabled:
            return
        self._tasks_dispatch_failed.add(1, {"error_class": error_class})

    def record_dispatch_duration(self, *, duration_ms: float, outcome: str) -> None:
        if not self._enabled:
            return
        self._tasks_dispatch_duration.record(duration_ms, {"outcome": outcome})

    def record_sla_expired(self, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        self._tasks_sla_expired.add(count)

    def record_result_stored(self, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        self._results_stored.add(count)

    def record_result_store_failed(self, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        self._results_store_failed.add(count)

    def record_scaling_event(self, *, action: str, tier: str) -> None:
        if not self._enabled:
            return
        self._scaling_events.add(1, {"action": action, "tier": tier})

    def record_cache_sweep_removed(self, count: int) -> None:
        if not self._enabled or count <= 0:
            return
        self._cache_sweep_removed.add(count)

    def record_health_check(self, *, outcome: str) -> None:
        if not self._enabled:
            return
        self._workers_health_checks.add(1, {"outcome": outcome})

    def record_poll_duration(self, *, route: str, outcome: str, duration_ms: float) -> None:
        if not self._enabled:
            return
        self._tasks_poll_duration.record(duration_ms, {"route": route, "outcome": outcome})

    def set_queue_depth(self, value: int) -> None:
        if not self._enabled:
            return
        self._queue_depth.set(value)

    def set_workers_running(self, value: int) -> None:
        if not self._enabled:
            return
        self._workers_running.set(value)

    def set_workers_healthy(self, value: int) -> None:
        if not self._enabled:
            return
        self._workers_healthy.set(value)

    def set_scaling_signal(self, value: float) -> None:
        if not self._enabled:
            return
        self._scaling_signal.set(value)

    def set_scheduler_leadership(self, *, is_leader: bool, hostname: str) -> None:
        if not self._enabled:
            return
        self._scheduler_is_leader.set(1 if is_leader else 0, {"hostname": hostname})


metrics = Metrics()

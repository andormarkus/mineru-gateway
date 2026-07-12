"""Tests for the OTel metrics facade."""

from __future__ import annotations

import importlib.util

import pytest

from mineru_gateway.observability.metrics import Metrics, metrics


def test_metrics_noop_when_disabled() -> None:
    """Record methods must not raise when OTel is not initialized."""
    m = Metrics()
    m.record_task_ingested(source="tasks", cache_hit=False, backend="hybrid-engine")
    m.record_admission_rejected(reason="draining")
    m.record_task_dispatched(worker_id="w1", backend="hybrid-engine")
    m.record_dispatch_failed(error_class="content")
    m.record_dispatch_duration(duration_ms=12.5, outcome="success")
    m.record_sla_expired(2)
    m.record_result_stored(1)
    m.record_result_store_failed(1)
    m.record_health_check(outcome="healthy")
    m.record_poll_duration(route="ocr", outcome="completed", duration_ms=100.0)
    m.record_retention_deleted(kind="task")


def test_module_singleton_is_noop_by_default() -> None:
    assert metrics._enabled is False
    metrics.record_task_ingested(source="ocr", cache_hit=True, backend="hybrid-engine")


@pytest.mark.skipif(importlib.util.find_spec("opentelemetry.sdk") is None, reason="otel extra not installed")
def test_metrics_inmemory_reader() -> None:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    m = Metrics()
    m._meter = provider.get_meter("test")
    m._create_instruments()
    m._enabled = True

    m.record_task_ingested(source="tasks", cache_hit=False, backend="hybrid-engine")
    m.record_retention_deleted(kind="task")

    collected = reader.get_metrics_data()
    assert collected is not None
    metric_names = {
        metric.name
        for resource_metrics in collected.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
    }
    assert "mineru.tasks.ingested" in metric_names
    assert "mineru.retention.deleted" in metric_names

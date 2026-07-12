"""Tests for cross-cloud normalized types."""

from __future__ import annotations

from mineru_gateway.cloud.types import DiscoveredInstance, InstanceState, cloud_state_from_instance


def test_instance_state_values() -> None:
    assert InstanceState.RUNNING == "running"
    assert InstanceState.SUSPENDED == "suspended"
    assert InstanceState.UNKNOWN == "unknown"


def test_cloud_state_from_instance() -> None:
    assert cloud_state_from_instance(InstanceState.SUSPENDED) == "stopped"
    assert cloud_state_from_instance(InstanceState.TERMINATED) == "terminated"


def test_discovered_instance_dataclass() -> None:
    discovered = DiscoveredInstance(
        instance_id="i-123",
        worker_id="worker-1",
        state=InstanceState.RUNNING,
        tags={"mineru-gateway-deployment": "dep-1"},
    )
    assert discovered.worker_id == "worker-1"
    assert discovered.tags["mineru-gateway-deployment"] == "dep-1"
